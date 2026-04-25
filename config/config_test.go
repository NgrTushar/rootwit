package config

import (
	"os"
	"path/filepath"
	"testing"
)

// helper to write a temp config file and return its path.
func writeTestConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

const validConfig = `
version: "1"
name: "test-pipeline"

source:
  type: postgres
  host: localhost
  port: 5432
  database: testdb
  username: testuser
  password: testpass
  ssl_mode: disable

destination:
  type: bigquery
  project_id: my-project
  dataset_id: my_dataset

sync:
  schedule: "*/30 * * * *"
  batch_size: 5000
  tables:
    - name: users
      sync_mode: incremental
      cursor_field: updated_at
    - name: products
      sync_mode: full_refresh
`

func TestLoad_ValidConfig(t *testing.T) {
	path := writeTestConfig(t, validConfig)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if cfg.Name != "test-pipeline" {
		t.Errorf("expected name 'test-pipeline', got %q", cfg.Name)
	}
	if cfg.Source.Type != "postgres" {
		t.Errorf("expected source type 'postgres', got %q", cfg.Source.Type)
	}
	if cfg.Destination.Type != "bigquery" {
		t.Errorf("expected destination type 'bigquery', got %q", cfg.Destination.Type)
	}
	if len(cfg.Sync.Tables) != 2 {
		t.Errorf("expected 2 tables, got %d", len(cfg.Sync.Tables))
	}
	if cfg.Sync.BatchSize != 5000 {
		t.Errorf("expected batch_size 5000, got %d", cfg.Sync.BatchSize)
	}
}

func TestLoad_Defaults(t *testing.T) {
	config := `
version: "1"
name: "test"
source:
  type: postgres
  host: localhost
  database: db
  username: user
  password: pass
  ssl_mode: disable
destination:
  type: bigquery
  project_id: proj
  dataset_id: ds
sync:
  schedule: "* * * * *"
  tables:
    - name: t1
      sync_mode: full_refresh
`
	path := writeTestConfig(t, config)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Check defaults.
	if cfg.Source.Port != 5432 {
		t.Errorf("default port should be 5432, got %d", cfg.Source.Port)
	}
	if cfg.Source.MaxConnections != 5 {
		t.Errorf("default max_connections should be 5, got %d", cfg.Source.MaxConnections)
	}
	if cfg.Sync.BatchSize != 10000 {
		t.Errorf("default batch_size should be 10000, got %d", cfg.Sync.BatchSize)
	}
	if cfg.Sync.StateFile != "./rootwit-state.json" {
		t.Errorf("default state_file should be './rootwit-state.json', got %q", cfg.Sync.StateFile)
	}
	if cfg.Destination.Location != "US" {
		t.Errorf("default location should be 'US', got %q", cfg.Destination.Location)
	}
}

func TestLoad_EnvVarSubstitution(t *testing.T) {
	t.Setenv("TEST_PG_HOST", "prod-db.internal")
	t.Setenv("TEST_PG_PASS", "s3cret!")

	config := `
version: "1"
name: "env-test"
source:
  type: postgres
  host: ${TEST_PG_HOST}
  database: mydb
  username: admin
  password: ${TEST_PG_PASS}
  ssl_mode: require
destination:
  type: bigquery
  project_id: proj
  dataset_id: ds
sync:
  schedule: "* * * * *"
  tables:
    - name: t
      sync_mode: full_refresh
`
	path := writeTestConfig(t, config)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Source.Host != "prod-db.internal" {
		t.Errorf("expected host 'prod-db.internal', got %q", cfg.Source.Host)
	}
	if cfg.Source.Password != "s3cret!" {
		t.Errorf("expected password 's3cret!', got %q", cfg.Source.Password)
	}
}

func TestLoad_MissingRequiredFields(t *testing.T) {
	tests := []struct {
		name   string
		config string
		errMsg string
	}{
		{
			name:   "missing version",
			config: `name: "test"`,
			errMsg: "version",
		},
		{
			name: "missing source type",
			config: `
version: "1"
name: "test"
source:
  host: localhost
`,
			errMsg: "source.type",
		},
		{
			name: "missing tables",
			config: `
version: "1"
name: "test"
source:
  type: postgres
  host: h
  database: d
  username: u
  password: p
  ssl_mode: disable
destination:
  type: bigquery
  project_id: p
  dataset_id: d
sync:
  schedule: "* * * * *"
  tables: []
`,
			errMsg: "tables",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeTestConfig(t, tt.config)
			_, err := Load(path)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !contains(err.Error(), tt.errMsg) {
				t.Errorf("expected error containing %q, got: %v", tt.errMsg, err)
			}
		})
	}
}

func TestLoad_InvalidSyncMode(t *testing.T) {
	config := `
version: "1"
name: "test"
source:
  type: postgres
  host: h
  database: d
  username: u
  password: p
  ssl_mode: disable
destination:
  type: bigquery
  project_id: p
  dataset_id: d
sync:
  schedule: "* * * * *"
  tables:
    - name: t
      sync_mode: invalid_mode
`
	path := writeTestConfig(t, config)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid sync mode")
	}
	if !contains(err.Error(), "invalid") {
		t.Errorf("error should mention 'invalid', got: %v", err)
	}
}

func TestLoad_MissingCursorField(t *testing.T) {
	config := `
version: "1"
name: "test"
source:
  type: postgres
  host: h
  database: d
  username: u
  password: p
  ssl_mode: disable
destination:
  type: bigquery
  project_id: p
  dataset_id: d
sync:
  schedule: "* * * * *"
  tables:
    - name: events
      sync_mode: incremental
`
	path := writeTestConfig(t, config)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing cursor_field on incremental mode")
	}
	if !contains(err.Error(), "cursor_field") {
		t.Errorf("error should mention 'cursor_field', got: %v", err)
	}
}

func TestLoad_AppendOnlyRequiresCursorField(t *testing.T) {
	config := `
version: "1"
name: "test"
source:
  type: postgres
  host: h
  database: d
  username: u
  password: p
  ssl_mode: disable
destination:
  type: bigquery
  project_id: p
  dataset_id: d
sync:
  schedule: "* * * * *"
  tables:
    - name: events
      sync_mode: append_only
`
	path := writeTestConfig(t, config)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing cursor_field on append_only mode")
	}
}

func TestHasLiteralCredential(t *testing.T) {
	rawWithLiteral := `
source:
  password: my_actual_password
  username: ${PG_USER}
alerts:
  on_failure:
    slack_webhook: https://hooks.slack.com/XXXXX
`
	warnings := HasLiteralCredential(rawWithLiteral)
	if len(warnings) != 2 {
		t.Errorf("expected 2 warnings (password + slack_webhook), got %d: %v", len(warnings), warnings)
	}

	rawWithEnvVars := `
source:
  password: ${PG_PASS}
alerts:
  on_failure:
    slack_webhook: ${SLACK_WEBHOOK_URL}
`
	warnings = HasLiteralCredential(rawWithEnvVars)
	if len(warnings) != 0 {
		t.Errorf("expected 0 warnings for env var config, got %d: %v", len(warnings), warnings)
	}
}

func TestLoad_UnsupportedSourceType(t *testing.T) {
	config := `
version: "1"
name: "test"
source:
  type: mysql
  host: h
  database: d
  username: u
  password: p
  ssl_mode: disable
destination:
  type: bigquery
  project_id: p
  dataset_id: d
sync:
  schedule: "* * * * *"
  tables:
    - name: t
      sync_mode: full_refresh
`
	path := writeTestConfig(t, config)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for unsupported source type")
	}
	if !contains(err.Error(), "unsupported source type") {
		t.Errorf("error should mention unsupported source type, got: %v", err)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && containsSubstring(s, substr)
}

func containsSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
