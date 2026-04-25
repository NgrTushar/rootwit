package stdout

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rootwit/rootwit/config"
)

// ==========================================================================
// SwapTables — schema renamed first, then data
// ==========================================================================

func TestSwapTables_HappyPath(t *testing.T) {
	dir := t.TempDir()
	ld := &LocalDestination{outputDir: dir}

	// Create staging files.
	stagingSchema := filepath.Join(dir, "staging.schema.json")
	stagingData := filepath.Join(dir, "staging.jsonl")
	if err := os.WriteFile(stagingSchema, []byte(`{"schema":"new"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stagingData, []byte(`{"row":"new"}`+"\n"), 0600); err != nil {
		t.Fatal(err)
	}

	// Create existing dest files (will be overwritten).
	destSchema := filepath.Join(dir, "dest.schema.json")
	destData := filepath.Join(dir, "dest.jsonl")
	if err := os.WriteFile(destSchema, []byte(`{"schema":"old"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destData, []byte(`{"row":"old"}`+"\n"), 0600); err != nil {
		t.Fatal(err)
	}

	if err := ld.SwapTables("staging", "dest"); err != nil {
		t.Fatalf("SwapTables failed: %v", err)
	}

	// Verify dest files now have staging content.
	schemaContent, _ := os.ReadFile(destSchema)
	if string(schemaContent) != `{"schema":"new"}` {
		t.Errorf("schema content = %q, want new", string(schemaContent))
	}
	dataContent, _ := os.ReadFile(destData)
	if string(dataContent) != `{"row":"new"}`+"\n" {
		t.Errorf("data content = %q, want new", string(dataContent))
	}

	// Staging files should no longer exist (they were renamed).
	if _, err := os.Stat(stagingSchema); !os.IsNotExist(err) {
		t.Error("staging schema file should not exist after swap")
	}
	if _, err := os.Stat(stagingData); !os.IsNotExist(err) {
		t.Error("staging data file should not exist after swap")
	}
}

func TestSwapTables_SchemaFirst_PartialFailure(t *testing.T) {
	dir := t.TempDir()
	ld := &LocalDestination{outputDir: dir}

	// Create ONLY the schema staging file (no data file).
	// This means schema rename will succeed but data rename will fail.
	stagingSchema := filepath.Join(dir, "staging.schema.json")
	if err := os.WriteFile(stagingSchema, []byte(`{"schema":"new"}`), 0600); err != nil {
		t.Fatal(err)
	}

	err := ld.SwapTables("staging", "dest")
	if err == nil {
		t.Fatal("SwapTables should fail when data file is missing")
	}

	// The schema should have been renamed (schema-first ordering).
	destSchema := filepath.Join(dir, "dest.schema.json")
	if _, statErr := os.Stat(destSchema); os.IsNotExist(statErr) {
		t.Error("dest schema should exist after partial swap (schema-first ordering)")
	}
}

// ==========================================================================
// Path traversal — rejected by validateOutputDir
// ==========================================================================

func TestPathTraversal_Rejected(t *testing.T) {
	tests := []struct {
		name      string
		datasetID string
	}{
		{"parent escape", "../../../etc/foo"},
		{"parent only", ".."},
		{"absolute path", "/tmp/evil"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.DestConfig{DatasetID: tt.datasetID}
			ld := NewLocalDestination(cfg)
			err := ld.Connect()
			if err == nil {
				t.Errorf("Connect() should reject dataset_id=%q", tt.datasetID)
			}
		})
	}
}

func TestPathTraversal_Allowed(t *testing.T) {
	tests := []struct {
		name      string
		datasetID string
	}{
		{"simple relative", "output"},
		{"nested relative", "data/output"},
		{"dot-relative", "./rootwit-output"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := config.DestConfig{DatasetID: filepath.Join(dir, tt.datasetID)}
			ld := NewLocalDestination(cfg)
			// This will fail because we joined with t.TempDir() which is absolute.
			// Instead, test validateOutputDir directly.
			err := validateOutputDir(tt.datasetID)
			if err != nil {
				t.Errorf("validateOutputDir(%q) should pass, got: %v", tt.datasetID, err)
			}
			_ = ld // suppress unused
		})
	}
}
