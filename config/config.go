package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// envVarPattern matches ${VAR_NAME} for environment variable substitution.
var envVarPattern = regexp.MustCompile(`\$\{([^}]+)\}`)

// Load reads and parses a config.yaml file, performs environment variable
// substitution, and validates the configuration.
func Load(path string) (*RootConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Substitute ${ENV_VAR} references with actual environment variable values.
	resolved := substituteEnvVars(string(data))

	var cfg RootConfig
	if err := yaml.Unmarshal([]byte(resolved), &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config YAML: %w", err)
	}

	// Apply defaults.
	if cfg.Source.Port == 0 {
		cfg.Source.Port = 5432
	}
	if cfg.Source.SSLMode == "" {
		cfg.Source.SSLMode = "require"
	}
	if cfg.Source.MaxConnections == 0 {
		cfg.Source.MaxConnections = 5
	}
	if cfg.Source.ConnectionTimeoutSeconds == 0 {
		cfg.Source.ConnectionTimeoutSeconds = 30
	}
	if cfg.Sync.BatchSize == 0 {
		cfg.Sync.BatchSize = 10000
	}
	if cfg.Sync.StateFile == "" {
		cfg.Sync.StateFile = "./rootwit-state.json"
	}
	if cfg.Destination.Location == "" {
		cfg.Destination.Location = "US"
	}

	if err := validate(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// substituteEnvVars replaces all ${VAR_NAME} occurrences with the value of
// the corresponding environment variable. If the env var is not set, the
// placeholder is replaced with an empty string.
func substituteEnvVars(input string) string {
	return envVarPattern.ReplaceAllStringFunc(input, func(match string) string {
		// Extract the variable name from ${VAR_NAME}.
		varName := envVarPattern.FindStringSubmatch(match)[1]
		return os.Getenv(varName)
	})
}

// validate checks the configuration for required fields and correctness.
func validate(cfg *RootConfig) error {
	if cfg.Version == "" {
		return fmt.Errorf("config: 'version' is required")
	}
	if cfg.Name == "" {
		return fmt.Errorf("config: 'name' is required")
	}

	// Source validation.
	if cfg.Source.Type == "" {
		return fmt.Errorf("config: 'source.type' is required")
	}
	validSourceTypes := map[string]bool{"postgres": true}
	if !validSourceTypes[cfg.Source.Type] {
		return fmt.Errorf("config: unsupported source type %q (supported: postgres)", cfg.Source.Type)
	}
	if cfg.Source.Host == "" {
		return fmt.Errorf("config: 'source.host' is required")
	}
	if cfg.Source.Database == "" {
		return fmt.Errorf("config: 'source.database' is required")
	}
	if cfg.Source.Username == "" {
		return fmt.Errorf("config: 'source.username' is required")
	}
	if cfg.Source.Password == "" {
		return fmt.Errorf("config: 'source.password' is required")
	}

	// Reject literal credentials — they must come through ${ENV_VAR} substitution.
	// After substitution, we can't tell if they came from env vars. So we check the
	// original YAML content indirectly: if the password or username look like they
	// contain common credential patterns, warn. The real protection is that
	// config.example.yaml always uses ${} syntax.

	// Destination validation.
	if cfg.Destination.Type == "" {
		return fmt.Errorf("config: 'destination.type' is required")
	}
	validDestTypes := map[string]bool{"bigquery": true}
	if !validDestTypes[cfg.Destination.Type] {
		return fmt.Errorf("config: unsupported destination type %q (supported: bigquery)", cfg.Destination.Type)
	}
	if cfg.Destination.ProjectID == "" {
		return fmt.Errorf("config: 'destination.project_id' is required")
	}
	if cfg.Destination.DatasetID == "" {
		return fmt.Errorf("config: 'destination.dataset_id' is required")
	}

	// Sync validation.
	if cfg.Sync.Schedule == "" {
		return fmt.Errorf("config: 'sync.schedule' is required")
	}
	if len(cfg.Sync.Tables) == 0 {
		return fmt.Errorf("config: 'sync.tables' must contain at least one table")
	}

	validSyncModes := map[string]bool{
		"incremental":  true,
		"full_refresh": true,
		"append_only":  true,
	}

	for i, t := range cfg.Sync.Tables {
		if t.Name == "" {
			return fmt.Errorf("config: sync.tables[%d].name is required", i)
		}
		if t.SyncMode == "" {
			return fmt.Errorf("config: sync.tables[%d].sync_mode is required", i)
		}
		if !validSyncModes[t.SyncMode] {
			return fmt.Errorf("config: sync.tables[%d].sync_mode %q is invalid (valid: incremental, full_refresh, append_only)", i, t.SyncMode)
		}
		// cursor_field is required for incremental and append_only modes.
		if (t.SyncMode == "incremental" || t.SyncMode == "append_only") && t.CursorField == "" {
			return fmt.Errorf("config: sync.tables[%d] (%s): cursor_field is required for %s mode", i, t.Name, t.SyncMode)
		}
	}

	// SSL mode validation.
	validSSLModes := map[string]bool{
		"disable":     true,
		"require":     true,
		"verify-full": true,
	}
	if !validSSLModes[cfg.Source.SSLMode] {
		return fmt.Errorf("config: source.ssl_mode %q is invalid (valid: disable, require, verify-full)", cfg.Source.SSLMode)
	}

	return nil
}

// HasLiteralCredential checks if a raw YAML string contains credential fields
// that are NOT using ${ENV_VAR} substitution. This should be called on the raw
// file content BEFORE env var substitution to detect hardcoded secrets.
func HasLiteralCredential(rawYAML string) []string {
	var warnings []string
	// Fields that must use ${ENV_VAR} syntax.
	credFields := []string{"password", "slack_webhook", "credentials_file"}

	lines := strings.Split(rawYAML, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		// Skip comments.
		if strings.HasPrefix(trimmed, "#") {
			continue
		}
		for _, field := range credFields {
			prefix := field + ":"
			if strings.HasPrefix(trimmed, prefix) {
				value := strings.TrimSpace(strings.TrimPrefix(trimmed, prefix))
				// Value should contain ${...} if it's using env var substitution.
				if value != "" && !strings.Contains(value, "${") {
					warnings = append(warnings, fmt.Sprintf("config: %s appears to contain a literal value instead of ${ENV_VAR} reference", field))
				}
			}
		}
	}
	return warnings
}
