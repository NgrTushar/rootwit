// Package scheduler provides cron-based scheduling for the sync engine.
package scheduler

import (
	"fmt"
	"os"
	"os/signal"
	gosync "sync"
	"syscall"
	"time"

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
	// SecondOptional accepts BOTH standard 5-field cron ("*/30 * * * *") and
	// 6-field cron with a leading seconds field ("*/5 * * * * *"). Using plain
	// cron.Second here would REQUIRE 6 fields and reject every 5-field schedule
	// — including the ones we ship in config.example.yaml.
	c := cron.New(cron.WithParser(cron.NewParser(
		cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow,
	)))

	engine := rwsync.NewEngine(cfg, src, dst)

	// gapThreshold is 2× the nominal schedule interval. If a table has not
	// completed a successful sync within this window, on_sync_gap alerts fire.
	// Computed once; 0 means the schedule couldn't be measured, which disables
	// gap checks (AddFunc below surfaces the real parse error if any).
	gapThreshold := 2 * scheduleInterval(cfg.Sync.Schedule)

	_, err := c.AddFunc(cfg.Sync.Schedule, func() {
		if !syncRunMu.TryLock() {
			logger.L.Infow("previous sync still running — skipping tick", "schedule", cfg.Sync.Schedule)
			return
		}
		defer syncRunMu.Unlock()

		logger.L.Infow("sync started", "schedule", cfg.Sync.Schedule)
		results := engine.RunSync()
		handleResults(cfg, results)

		// After each run, alert on any table that hasn't succeeded within the
		// expected window — the backstop for a table failing run after run.
		if cfg.Alerts.OnSyncGap {
			checkSyncGaps(cfg, gapThreshold)
		}
	})
	if err != nil {
		return fmt.Errorf("scheduler: invalid cron expression %q: %w", cfg.Sync.Schedule, err)
	}

	c.Start()
	logger.L.Infow("scheduler started", "schedule", cfg.Sync.Schedule)

	// Check for a gap at startup too: if the process was stopped for longer
	// than the window, the last completion times will already be stale and the
	// operator finds out immediately rather than on the next tick.
	if cfg.Alerts.OnSyncGap {
		checkSyncGaps(cfg, gapThreshold)
	}

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

// scheduleInterval returns the nominal gap between two consecutive fires of the
// given cron schedule (e.g. "*/30 * * * *" → 30m). It asks the parsed schedule
// for its next two activation times and measures the difference. Returns 0 if
// the schedule can't be parsed — the caller treats 0 as "gap checks disabled".
func scheduleInterval(schedule string) time.Duration {
	parser := cron.NewParser(
		cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow,
	)
	sched, err := parser.Parse(schedule)
	if err != nil {
		return 0
	}
	now := time.Now()
	first := sched.Next(now)
	if first.IsZero() {
		return 0
	}
	second := sched.Next(first)
	if second.IsZero() {
		return 0
	}
	return second.Sub(first)
}

// detectSyncGaps returns a description for every configured table whose last
// successful sync is older than threshold. It is pure (no I/O, clock passed in)
// so the gap logic can be unit-tested without disk, network, or a live cron.
//
// Tables with no recorded completion are skipped: a never-completed table is
// either a first run or is already covered by the per-run failure alert, so
// flagging it here would only add a duplicate/false alarm.
func detectSyncGaps(conn *rwsync.ConnectionState, tables []config.SyncTableConfig, threshold time.Duration, now time.Time) []string {
	if conn == nil || threshold <= 0 {
		return nil
	}
	var stale []string
	for _, tc := range tables {
		ts, ok := conn.Tables[tc.Name]
		if !ok || ts.LastSyncCompleted == nil {
			continue
		}
		completed, err := time.Parse(time.RFC3339, *ts.LastSyncCompleted)
		if err != nil {
			continue
		}
		if gap := now.Sub(completed); gap > threshold {
			stale = append(stale, fmt.Sprintf("%s (last success %s ago)", tc.Name, gap.Round(time.Second)))
		}
	}
	return stale
}

// checkSyncGaps loads the current state from disk, runs gap detection, and
// alerts if any table is stale. Called after each run and once at startup.
func checkSyncGaps(cfg *config.RootConfig, threshold time.Duration) {
	if threshold <= 0 {
		return
	}
	state, err := rwsync.LoadState(cfg.Sync.StateFile)
	if err != nil {
		logger.L.Warnw("sync-gap check skipped: cannot load state", "error", err)
		return
	}
	stale := detectSyncGaps(state.Connections[cfg.Name], cfg.Sync.Tables, threshold, time.Now().UTC())
	if len(stale) == 0 {
		return
	}
	logger.L.Warnw("sync gap detected", "tables", stale, "threshold", threshold.String())
	msg := alerts.FormatSyncGapAlert(cfg.Name, threshold, stale)
	sendAlerts(cfg, "Sync Gap Detected", msg)
}
