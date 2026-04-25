package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/rootwit/rootwit/config"
	"github.com/rootwit/rootwit/destinations"
	bqdest "github.com/rootwit/rootwit/destinations/bigquery"
	localdest "github.com/rootwit/rootwit/destinations/local"
	"github.com/rootwit/rootwit/logger"
	"github.com/rootwit/rootwit/scheduler"
	"github.com/rootwit/rootwit/sources"
	pgsrc "github.com/rootwit/rootwit/sources/postgres"
	rwsync "github.com/rootwit/rootwit/sync"
)

func main() {
	logger.Init()

	configPath := flag.String("config", "", "path to config.yaml (required)")
	dryRun := flag.Bool("dry-run", false, "detect schema changes, print diff, do not sync")
	once := flag.Bool("once", false, "run sync once and exit, no scheduler")
	validate := flag.Bool("validate", false, "test source and destination connections, then exit")
	repairState := flag.Bool("repair-state", false, "attempt to recover a corrupted state.json, then exit")
	confirmFresh := flag.Bool("confirm-fresh", false, "confirm writing a fresh empty state when --repair-state finds no .tmp to recover")
	destOverride := flag.String("dest", "", "override destination: 'local' writes JSONL files instead of BigQuery")
	flag.Parse()

	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "error: --config flag is required")
		fmt.Fprintln(os.Stderr, "usage: rootwit --config <path-to-config.yaml> [--dry-run] [--once] [--validate] [--dest local]")
		os.Exit(1)
	}

	// Check for literal credentials in the raw config file before loading.
	rawData, err := os.ReadFile(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: failed to read config file: %v\n", err)
		os.Exit(1)
	}
	if warnings := config.HasLiteralCredential(string(rawData)); len(warnings) > 0 {
		fmt.Fprintln(os.Stderr, "error: literal credentials detected in config file:")
		for _, w := range warnings {
			fmt.Fprintf(os.Stderr, "  - %s\n", w)
		}
		fmt.Fprintln(os.Stderr, "  Use ${ENV_VAR} syntax for all credentials. Refusing to start.")
		os.Exit(1)
	}

	// Load and validate config.
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	// Construct concrete connectors based on config.
	// This is the ONLY place where concrete implementations are referenced.
	// The sync engine receives interfaces, not concrete types.
	src := pgsrc.NewPostgresSource(cfg.Source)

	var dst destinations.DestinationConnector
	if *destOverride == "local" {
		dst = localdest.NewLocalDestination(cfg.Destination)
	} else {
		dst = bqdest.NewBigQueryDestination(cfg.Destination)
	}

	// Route to the appropriate mode.
	switch {
	case *repairState:
		os.Exit(runRepairState(cfg, *confirmFresh))
	case *dryRun:
		os.Exit(runDryRun(cfg, src))
	case *validate:
		os.Exit(runValidate(cfg, src, dst, *destOverride))
	case *once:
		// Acquire exclusive file lock on state.json to prevent concurrent instances.
		lockFile, lockErr := rwsync.AcquireStateLock(cfg.Sync.StateFile)
		if lockErr != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", lockErr)
			os.Exit(1)
		}
		defer rwsync.ReleaseStateLock(lockFile)
		os.Exit(runOnce(cfg, src, dst))
	default:
		// Acquire exclusive file lock on state.json to prevent concurrent instances.
		lockFile, lockErr := rwsync.AcquireStateLock(cfg.Sync.StateFile)
		if lockErr != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", lockErr)
			os.Exit(1)
		}
		defer rwsync.ReleaseStateLock(lockFile)
		os.Exit(runScheduler(cfg, src, dst))
	}
}

// runRepairState attempts to recover state.json without touching the source or
// destination. Safe to run while the pipeline is stopped.
func runRepairState(cfg *config.RootConfig, confirmFresh bool) int {
	statePath := cfg.Sync.StateFile
	fmt.Printf("[rootwit] repair-state: attempting recovery of %s\n", statePath)

	msg, err := rwsync.RepairState(statePath)
	if err != nil {
		if errors.Is(err, rwsync.ErrFreshStateNeedsConfirmation) {
			if !confirmFresh {
				fmt.Fprintln(os.Stderr, "error: no .tmp file found to recover from.")
				fmt.Fprintln(os.Stderr, "  The only option is to write a fresh empty state, which will cause")
				fmt.Fprintln(os.Stderr, "  ALL tables to re-sync from scratch on the next run.")
				fmt.Fprintln(os.Stderr, "  To confirm, re-run with: --repair-state --confirm-fresh")
				return 1
			}
			// Operator confirmed — backup and write fresh state.
			msg, err = rwsync.RepairStateFresh(statePath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: repair failed: %v\n", err)
				return 1
			}
			fmt.Printf("[rootwit] repair-state: %s\n", msg)
			return 0
		}
		fmt.Fprintf(os.Stderr, "error: repair failed: %v\n", err)
		return 1
	}

	fmt.Printf("[rootwit] repair-state: %s\n", msg)
	return 0
}

// runDryRun connects to source ONLY, reads schemas, and prints them.
// Does NOT require a destination connection — so you can preview schemas
// during initial setup before configuring BigQuery.
func runDryRun(cfg *config.RootConfig, src sources.SourceConnector) int {
	fmt.Println("[rootwit] dry-run mode: detecting schema changes...")

	if err := src.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "error: failed to connect to source: %v\n", err)
		return 1
	}
	defer src.Close()

	for _, tc := range cfg.Sync.Tables {
		destTable := tc.DestinationTable
		if destTable == "" {
			destTable = tc.Name
		}

		srcSchema, err := src.GetSchema(tc.Name)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: failed to get source schema for %s: %v\n", tc.Name, err)
			return 1
		}

		fmt.Printf("\n📋 %s → %s (mode: %s)\n", tc.Name, destTable, tc.SyncMode)
		fmt.Printf("   %d columns:\n", len(srcSchema.Fields))
		for _, f := range srcSchema.Fields {
			nullable := ""
			if f.Nullable {
				nullable = " (nullable)"
			}
			itemInfo := ""
			if f.ItemType != "" {
				itemInfo = fmt.Sprintf(" [items: %s]", f.ItemType)
			}
			fmt.Printf("     - %-20s %s%s%s\n", f.Name, f.Type, itemInfo, nullable)
		}
		if tc.CursorField != "" {
			fmt.Printf("   cursor: %s\n", tc.CursorField)
		}
	}

	return 0
}

// runValidate tests source and destination connections independently.
// Source is validated fully before attempting destination.
func runValidate(cfg *config.RootConfig, src sources.SourceConnector, dst destinations.DestinationConnector, destType string) int {
	fmt.Println("[rootwit] validate mode: testing connections...")

	exitCode := 0

	// --- Source validation ---
	// Host and db are redacted to prevent leaking connection details into CI logs.
	fmt.Printf("  Source (postgres@%s/%s): testing connection... ", redact(cfg.Source.Host), redact(cfg.Source.Database))
	if err := src.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "FAILED\n    connection error (details redacted)\n")
		return 1
	}
	defer src.Close()
	fmt.Println("OK")

	// List source tables.
	tables, err := src.GetTables()
	if err != nil {
		fmt.Fprintf(os.Stderr, "  Failed to list tables: %v\n", err)
		return 1
	}
	fmt.Printf("  Available tables (%d): %v\n", len(tables), tables)

	// Verify configured tables exist in source.
	tableSet := make(map[string]bool)
	for _, t := range tables {
		tableSet[t] = true
	}
	fmt.Println("\n  Configured table validation:")
	for _, tc := range cfg.Sync.Tables {
		if tableSet[tc.Name] {
			fmt.Printf("    ✓ %s (mode: %s", tc.Name, tc.SyncMode)
			if tc.CursorField != "" {
				fmt.Printf(", cursor: %s", tc.CursorField)
			}
			fmt.Println(")")
		} else {
			fmt.Printf("    ✗ %s — NOT FOUND in source\n", tc.Name)
			exitCode = 1
		}
	}

	// --- Destination validation ---
	destLabel := "bigquery"
	if destType == "local" {
		destLabel = "local (JSONL files)"
	}
	fmt.Printf("\n  Destination (%s): testing connection... ", destLabel)
	if err := dst.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "FAILED\n    %v\n", err)
		fmt.Println("\n  ℹ️  Source is valid. Destination needs configuration.")
		fmt.Println("  Tip: use --dest local to test with local JSONL files.")
		return 1
	}
	defer dst.Close()
	fmt.Println("OK")

	if exitCode == 0 {
		fmt.Println("\n✓ All connections valid. Ready to sync.")
	} else {
		fmt.Println("\n⚠️  Some configured tables were not found in the source.")
	}

	return exitCode
}

// runOnce runs the sync engine once for all tables and exits.
func runOnce(cfg *config.RootConfig, src sources.SourceConnector, dst destinations.DestinationConnector) int {
	fmt.Println("[rootwit] once mode: running single sync...")

	if err := src.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "error: failed to connect to source: %v\n", err)
		return 1
	}
	defer src.Close()

	if err := dst.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "error: failed to connect to destination: %v\n", err)
		return 1
	}
	defer dst.Close()

	engine := rwsync.NewEngine(cfg, src, dst)
	results := engine.RunOnce()

	hasFailures := false
	for _, r := range results {
		if r.Success {
			fmt.Printf("  ✓ %s: synced %d rows in %s\n", r.TableName, r.RowsSynced, r.Duration)
		} else {
			fmt.Printf("  ✗ %s: FAILED after %s: %v\n", r.TableName, r.Duration, r.Error)
			hasFailures = true
		}
	}

	// Print output summary for local destination.
	if ld, ok := dst.(*localdest.LocalDestination); ok {
		ld.PrintSummary()
	}

	if hasFailures {
		return 1
	}
	return 0
}

// runScheduler starts the cron scheduler and blocks until shutdown signal.
func runScheduler(cfg *config.RootConfig, src sources.SourceConnector, dst destinations.DestinationConnector) int {
	fmt.Println("[rootwit] starting scheduler mode...")

	if err := src.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "error: failed to connect to source: %v\n", err)
		return 1
	}
	defer src.Close()

	if err := dst.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "error: failed to connect to destination: %v\n", err)
		return 1
	}
	defer dst.Close()

	if err := scheduler.Start(cfg, src, dst); err != nil {
		fmt.Fprintf(os.Stderr, "error: scheduler failed: %v\n", err)
		return 1
	}

	return 0
}

// redact masks a string for safe display in logs and CLI output.
// Returns a fixed mask with no partial content or length hint to prevent
// fingerprinting of unique hostnames or database names.
func redact(s string) string {
	return "***"
}
