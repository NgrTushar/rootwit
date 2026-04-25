// Package stdout provides a local file-based destination for testing and
// development. Writes rows as JSONL files — one file per table per run.
//
// This is NOT a production destination. It exists so you can see data flowing
// through the pipeline without needing BigQuery credentials.
package stdout

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rootwit/rootwit/config"
	"github.com/rootwit/rootwit/types"
)

// LocalDestination implements destinations.DestinationConnector by writing
// rows as JSONL (one JSON object per line) to a local output directory.
type LocalDestination struct {
	outputDir string
}

// NewLocalDestination creates a new LocalDestination. Files are written to
// the specified output directory (defaults to ./rootwit-output/).
func NewLocalDestination(cfg config.DestConfig) *LocalDestination {
	dir := cfg.DatasetID // reuse dataset_id as the output dir
	if dir == "" {
		dir = "./rootwit-output"
	}
	return &LocalDestination{outputDir: dir}
}

// validateOutputDir checks that the output directory is a safe path.
// Rejects absolute paths and paths that escape via ".." components.
func validateOutputDir(dir string) error {
	cleaned := filepath.Clean(dir)
	if filepath.IsAbs(cleaned) {
		return fmt.Errorf("local: dataset_id must be a relative path, got absolute: %s", dir)
	}
	// After Clean, any path starting with ".." escapes the working directory.
	if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return fmt.Errorf("local: dataset_id must not escape the working directory: %s", dir)
	}
	return nil
}

// Connect creates the output directory if it doesn't exist.
func (l *LocalDestination) Connect() error {
	if err := validateOutputDir(l.outputDir); err != nil {
		return err
	}
	if err := os.MkdirAll(l.outputDir, 0750); err != nil {
		return fmt.Errorf("local: failed to create output dir %s: %w", l.outputDir, err)
	}
	fmt.Printf("[local-dest] output directory: %s\n", l.outputDir)
	return nil
}

// GetSchema returns nil (table doesn't exist locally) — this triggers CreateTable.
func (l *LocalDestination) GetSchema(table string) (*types.Schema, error) {
	schemaFile := filepath.Join(l.outputDir, table+".schema.json")
	data, err := os.ReadFile(schemaFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // table doesn't exist
		}
		return nil, fmt.Errorf("local: failed to read schema: %w", err)
	}

	var schema types.Schema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, fmt.Errorf("local: failed to parse schema: %w", err)
	}
	return &schema, nil
}

// CreateTable writes the schema to a .schema.json file.
func (l *LocalDestination) CreateTable(table string, schema types.Schema) error {
	schemaFile := filepath.Join(l.outputDir, table+".schema.json")
	data, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return fmt.Errorf("local: failed to marshal schema: %w", err)
	}
	fmt.Printf("[local-dest] creating table schema: %s\n", schemaFile)
	return os.WriteFile(schemaFile, data, 0600)
}

// AlterTable is a no-op for local testing.
func (l *LocalDestination) AlterTable(table string, changes types.SchemaChanges) error {
	fmt.Printf("[local-dest] schema change on %s: %d changes\n", table, len(changes.Changes))
	return nil
}

// WriteBatch appends rows as JSONL to a .jsonl file (one per table).
func (l *LocalDestination) WriteBatch(table string, rows []types.Row) (types.WriteResult, error) {
	if len(rows) == 0 {
		return types.WriteResult{}, nil
	}

	dataFile := filepath.Join(l.outputDir, table+".jsonl")
	f, err := os.OpenFile(dataFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return types.WriteResult{}, fmt.Errorf("local: failed to open %s: %w", dataFile, err)
	}
	defer f.Close()

	var bytesWritten int64
	for _, row := range rows {
		data, err := json.Marshal(row)
		if err != nil {
			return types.WriteResult{}, fmt.Errorf("local: failed to marshal row: %w", err)
		}
		n, _ := f.Write(data)
		f.Write([]byte("\n"))
		bytesWritten += int64(n + 1)
	}

	return types.WriteResult{
		RowsWritten:  int64(len(rows)),
		BytesWritten: bytesWritten,
	}, nil
}

// TruncateTable removes the data file (recreates it empty on next write).
func (l *LocalDestination) TruncateTable(table string) error {
	dataFile := filepath.Join(l.outputDir, table+".jsonl")
	if err := os.Remove(dataFile); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("local: failed to truncate %s: %w", table, err)
	}
	fmt.Printf("[local-dest] truncated %s\n", table)
	return nil
}

// SwapTables replaces dest's data and schema files with staging's files.
// Uses os.Rename which is atomic on the same filesystem.
//
// ORDERING: Schema is renamed FIRST, then data. If only schema succeeds
// (e.g. disk full), the reader sees the new schema (a superset) with old data —
// which is read-compatible. The reverse (new data + old schema) causes type
// mismatches.
func (l *LocalDestination) SwapTables(staging, dest string) error {
	stagingSchema := filepath.Join(l.outputDir, staging+".schema.json")
	destSchema := filepath.Join(l.outputDir, dest+".schema.json")
	if err := os.Rename(stagingSchema, destSchema); err != nil {
		return fmt.Errorf("local: swap failed (schema): %w", err)
	}

	stagingData := filepath.Join(l.outputDir, staging+".jsonl")
	destData := filepath.Join(l.outputDir, dest+".jsonl")
	if err := os.Rename(stagingData, destData); err != nil {
		return fmt.Errorf("local: swap failed (data): %w", err)
	}

	fmt.Printf("[local-dest] swapped %s → %s\n", staging, dest)
	return nil
}

// Close is a no-op.
func (l *LocalDestination) Close() error {
	return nil
}

// PrintSummary prints the row counts for each table file in the output directory.
func (l *LocalDestination) PrintSummary() {
	fmt.Println("\n📁 Output files:")
	entries, err := os.ReadDir(l.outputDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, _ := e.Info()
		if info != nil {
			if filepath.Ext(e.Name()) == ".jsonl" {
				lines := countLines(filepath.Join(l.outputDir, e.Name()))
				fmt.Printf("   %s — %d rows, %s\n", e.Name(), lines, formatBytes(info.Size()))
			} else {
				fmt.Printf("   %s — %s\n", e.Name(), formatBytes(info.Size()))
			}
		}
	}
}

func countLines(path string) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	count := 0
	for _, b := range data {
		if b == '\n' {
			count++
		}
	}
	return count
}

func formatBytes(b int64) string {
	switch {
	case b >= 1024*1024:
		return fmt.Sprintf("%.1f MB", float64(b)/(1024*1024))
	case b >= 1024:
		return fmt.Sprintf("%.1f KB", float64(b)/1024)
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// Timestamp returns a formatted timestamp for file naming.
func Timestamp() string {
	return time.Now().Format("20060102-150405")
}
