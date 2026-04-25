// Package alerts provides alerting for sync failures and schema changes.
// Supports Slack webhooks and SMTP email.
package alerts

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/smtp"
	"strings"

	"github.com/rootwit/rootwit/config"
	"github.com/rootwit/rootwit/types"
)

// PostSlack sends a message to a Slack webhook URL.
// The webhook URL must be a valid Slack incoming webhook.
func PostSlack(webhookURL string, message string) error {
	payload := map[string]string{
		"text": message,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("alerts: failed to marshal Slack payload: %w", err)
	}

	resp, err := http.Post(webhookURL, "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("alerts: failed to POST to Slack: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("alerts: Slack returned status %d", resp.StatusCode)
	}

	return nil
}

// SendEmail sends an email via SMTP using the provided configuration.
// IMPORTANT: Never include credentials or connection strings in the email body.
func SendEmail(cfg config.AlertEmailConfig, subject string, body string) error {
	if cfg.SMTPHost == "" {
		return fmt.Errorf("alerts: SMTP host not configured")
	}

	addr := fmt.Sprintf("%s:%d", cfg.SMTPHost, cfg.SMTPPort)
	auth := smtp.PlainAuth("", cfg.Username, cfg.Password, cfg.SMTPHost)

	// Build the email message.
	var msg strings.Builder
	msg.WriteString(fmt.Sprintf("From: %s\r\n", cfg.From))
	msg.WriteString(fmt.Sprintf("To: %s\r\n", strings.Join(cfg.To, ", ")))
	msg.WriteString(fmt.Sprintf("Subject: [RootWit] %s\r\n", subject))
	msg.WriteString("Content-Type: text/plain; charset=UTF-8\r\n")
	msg.WriteString("\r\n")
	msg.WriteString(body)

	err := smtp.SendMail(addr, auth, cfg.From, cfg.To, []byte(msg.String()))
	if err != nil {
		return fmt.Errorf("alerts: failed to send email: %w", err)
	}

	return nil
}

// FormatFailureAlert creates a human-readable failure summary from sync results.
// IMPORTANT: This must NEVER include credentials or connection URLs.
func FormatFailureAlert(failures []types.SyncResult) string {
	var sb strings.Builder

	sb.WriteString("⚠️ RootWit Sync Failure Report\n")
	sb.WriteString("================================\n\n")

	for _, r := range failures {
		sb.WriteString(fmt.Sprintf("Table: %s\n", r.TableName))
		sb.WriteString(fmt.Sprintf("  Duration: %s\n", r.Duration))
		if r.Error != nil {
			// Sanitize error message — remove anything that looks like a connection string.
			errMsg := sanitizeErrorMessage(r.Error.Error())
			sb.WriteString(fmt.Sprintf("  Error: %s\n", errMsg))
		}
		sb.WriteString("\n")
	}

	sb.WriteString(fmt.Sprintf("Total failed tables: %d\n", len(failures)))
	return sb.String()
}

// FormatSchemaChangeAlert creates a human-readable schema change summary.
func FormatSchemaChangeAlert(changes types.SchemaChanges) string {
	var sb strings.Builder

	sb.WriteString("📋 RootWit Schema Change Detected\n")
	sb.WriteString("====================================\n\n")
	sb.WriteString(fmt.Sprintf("Table: %s\n\n", changes.TableName))

	if changes.HasIncompatible {
		sb.WriteString("⛔ INCOMPATIBLE CHANGES DETECTED — sync halted for this table\n\n")
	}

	for _, c := range changes.Changes {
		switch c.ChangeType {
		case types.SchemaChangeAddColumn:
			sb.WriteString(fmt.Sprintf("  + Added column: %s (%s)\n", c.FieldName, c.NewField.Type))
		case types.SchemaChangeRemoveColumn:
			sb.WriteString(fmt.Sprintf("  - Removed column: %s (was %s)\n", c.FieldName, c.OldField.Type))
		case types.SchemaChangeTypeWiden:
			sb.WriteString(fmt.Sprintf("  ↑ Type widened: %s (%s → %s)\n", c.FieldName, c.OldField.Type, c.NewField.Type))
		case types.SchemaChangeTypeIncompatible:
			sb.WriteString(fmt.Sprintf("  ⛔ Incompatible type change: %s (%s → %s)\n", c.FieldName, c.OldField.Type, c.NewField.Type))
		}
	}

	return sb.String()
}

// sanitizeErrorMessage removes potential credential or connection string
// information from error messages before including them in alerts.
func sanitizeErrorMessage(msg string) string {
	// Remove anything that looks like a connection string.
	// Pattern: user:password@host:port or postgresql://...
	if idx := strings.Index(msg, "://"); idx >= 0 {
		// Find the start of the URL scheme.
		start := idx
		for start > 0 && msg[start-1] != ' ' && msg[start-1] != '"' && msg[start-1] != '\'' {
			start--
		}
		// Find the end of the URL.
		end := idx + 3
		for end < len(msg) && msg[end] != ' ' && msg[end] != '"' && msg[end] != '\'' {
			end++
		}
		msg = msg[:start] + "[REDACTED]" + msg[end:]
	}

	return msg
}
