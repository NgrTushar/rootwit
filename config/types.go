// Package config defines the configuration types for RootWit.
package config

// RootConfig is the top-level configuration loaded from config.yaml.
type RootConfig struct {
	Version     string       `yaml:"version"`
	Name        string       `yaml:"name"`
	Source      SourceConfig `yaml:"source"`
	Destination DestConfig   `yaml:"destination"`
	Sync        SyncConfig   `yaml:"sync"`
	Alerts      AlertConfig  `yaml:"alerts"`
}

// SourceConfig holds configuration for the data source.
type SourceConfig struct {
	Type                     string `yaml:"type"`
	Host                     string `yaml:"host"`
	Port                     int    `yaml:"port"`
	Database                 string `yaml:"database"`
	Username                 string `yaml:"username"`
	Password                 string `yaml:"password"`
	SSLMode                  string `yaml:"ssl_mode"`
	MaxConnections           int    `yaml:"max_connections"`
	ConnectionTimeoutSeconds int    `yaml:"connection_timeout_seconds"`
}

// DestConfig holds configuration for the data destination.
type DestConfig struct {
	Type              string `yaml:"type"`
	ProjectID         string `yaml:"project_id"`
	DatasetID         string `yaml:"dataset_id"`
	CredentialsFile   string `yaml:"credentials_file"`
	AutoCreateDataset bool   `yaml:"auto_create_dataset"`
	Location          string `yaml:"location"`
}

// SyncConfig holds configuration for sync behavior.
type SyncConfig struct {
	Schedule  string            `yaml:"schedule"`
	BatchSize int               `yaml:"batch_size"`
	StateFile string            `yaml:"state_file"`
	Tables    []SyncTableConfig `yaml:"tables"`
}

// SyncTableConfig holds per-table sync configuration.
type SyncTableConfig struct {
	Name             string `yaml:"name"`
	SyncMode         string `yaml:"sync_mode"`
	CursorField      string `yaml:"cursor_field"`
	DestinationTable string `yaml:"destination_table"`
	TimeoutMinutes   int    `yaml:"timeout_minutes"` // 0 means use default (30 min)
}

// AlertConfig holds alerting configuration.
type AlertConfig struct {
	OnFailure      AlertFailureConfig `yaml:"on_failure"`
	OnSchemaChange bool               `yaml:"on_schema_change"`
	OnSyncGap      bool               `yaml:"on_sync_gap"`
}

// AlertFailureConfig holds configuration for failure alerts.
type AlertFailureConfig struct {
	SlackWebhook string           `yaml:"slack_webhook"`
	Email        AlertEmailConfig `yaml:"email"`
}

// AlertEmailConfig holds SMTP email configuration for alerts.
type AlertEmailConfig struct {
	SMTPHost string   `yaml:"smtp_host"`
	SMTPPort int      `yaml:"smtp_port"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
	From     string   `yaml:"from"`
	To       []string `yaml:"to"`
}
