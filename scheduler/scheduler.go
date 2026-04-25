// Package scheduler provides cron-based scheduling for the sync engine.
package scheduler

import (
	"fmt"
	"os"
	"os/signal"
	gosync "sync"
	"syscall"

	"github.com/robfig/cron/v3"
	"github.com/rootwit/rootwit/alerts"
	"github.com/rootwit/rootwit/config"
	"github.com/rootwit/rootwit/destinations"
	"github.com/rootwit/rootwit/logger"
	"github.com/rootwit/rootwit/sources"
	rwsync "github.com/rootwit/rootwit/sync"
	"github.com/rootwit/rootwit/types"
)

// syncRunMu serializes scheduled sync runs. If a cron tick fires while the
// previous RunSync is still in flight, the new tick skips instead of running
// concurrently. Without this, two overlapping goroutines would call
// dst.WriteBatch() for the same table simultaneously and race on state.json.
var syncRunMu gosync.Mutex

// Start begins the cron scheduler and blocks until SIGTERM or SIGINT is received.
// On shutdown, it waits for the current sync run to finish before exiting.
func Start(cfg *config.RootConfig, src sources.SourceConnector, dst destinations.DestinationConnector) error {
	c := cron.New(cron.WithParser(cron.NewParser(
		cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow,
	)))

	engine := rwsync.NewEngine(cfg, src, dst)

	_, err := c.AddFunc(cfg.Sync.Schedule, func() {
		if !syncRunMu.TryLock() {
			logger.L.Infow("previous sync still running — skipping tick", "schedule", cfg.Sync.Schedule)
			return
		}
		defer syncRunMu.Unlock()

		logger.L.Infow("sync started", "schedule", cfg.Sync.Schedule)
		results := engine.RunSync()
		handleResults(cfg, results)
	})
	if err != nil {
		return fmt.Errorf("scheduler: invalid cron expression %q: %w", cfg.Sync.Schedule, err)
	}

	c.Start()
	logger.L.Infow("scheduler started", "schedule", cfg.Sync.Schedule)

	// Block until SIGTERM or SIGINT.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh

	logger.L.Info("shutdown signal received, waiting for current sync to finish")

	// Stop the scheduler and wait for running jobs to complete.
	ctx := c.Stop()
	<-ctx.Done()

	logger.L.Info("scheduler stopped gracefully")
	return nil
}

// handleResults processes sync results: logs outcomes and sends alerts for failures.
func handleResults(cfg *config.RootConfig, results []types.SyncResult) {
	var failures []types.SyncResult

	for _, r := range results {
		if r.Success {
			logger.L.Infow("table synced", "table", r.TableName, "rows", r.RowsSynced, "duration", r.Duration)
		} else {
			logger.L.Errorw("table sync failed", "table", r.TableName, "duration", r.Duration, "error", r.Error)
			failures = append(failures, r)
		}

		// Alert on schema changes.
		if r.SchemaChange != nil && len(r.SchemaChange.Changes) > 0 && cfg.Alerts.OnSchemaChange {
			msg := alerts.FormatSchemaChangeAlert(*r.SchemaChange)
			sendAlerts(cfg, "Schema Change Detected", msg)
		}
	}

	// Alert on failures.
	if len(failures) > 0 {
		msg := alerts.FormatFailureAlert(failures)
		sendAlerts(cfg, "Sync Failure", msg)
	}
}

// sendAlerts sends an alert via configured channels (Slack and/or email).
func sendAlerts(cfg *config.RootConfig, subject, message string) {
	if cfg.Alerts.OnFailure.SlackWebhook != "" {
		if err := alerts.PostSlack(cfg.Alerts.OnFailure.SlackWebhook, message); err != nil {
			logger.L.Warnw("failed to send Slack alert", "error", err)
		}
	}

	if cfg.Alerts.OnFailure.Email.SMTPHost != "" {
		if err := alerts.SendEmail(cfg.Alerts.OnFailure.Email, subject, message); err != nil {
			logger.L.Warnw("failed to send email alert", "error", err)
		}
	}
}
