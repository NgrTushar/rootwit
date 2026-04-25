package sync

import (
	"context"
	"fmt"
	gosync "sync"
	"time"

	"github.com/rootwit/rootwit/config"
	"github.com/rootwit/rootwit/destinations"
	"github.com/rootwit/rootwit/sources"
	"github.com/rootwit/rootwit/types"
)

// Engine orchestrates the sync process. It connects to the source and
// destination, runs schema checks, picks the appropriate strategy per table,
// and manages state.
//
// CRITICAL DESIGN: The engine imports ONLY types, sources (interface), and
// destinations (interface). It NEVER imports sources/postgres or
// destinations/bigquery. Connector selection happens in main.go.
type Engine struct {
	cfg       *config.RootConfig
	src       sources.SourceConnector
	dst       destinations.DestinationConnector
	statePath string
}

// NewEngine creates a new sync engine.
func NewEngine(cfg *config.RootConfig, src sources.SourceConnector, dst destinations.DestinationConnector) *Engine {
	return &Engine{
		cfg:       cfg,
		src:       src,
		dst:       dst,
		statePath: cfg.Sync.StateFile,
	}
}

// RunSync runs the sync for all configured tables. Each table runs in its own
// goroutine with isolated state. One table failing does NOT prevent other
// tables from syncing.
//
// Returns a SyncResult for every table.
func (e *Engine) RunSync() []types.SyncResult {
	state, err := LoadState(e.statePath)
	if err != nil {
		// State load failure is fatal for this run.
		var results []types.SyncResult
		for _, t := range e.cfg.Sync.Tables {
			results = append(results, types.SyncResult{
				TableName: t.Name,
				Success:   false,
				Error:     fmt.Errorf("failed to load state: %w", err),
			})
		}
		return results
	}

	results := make([]types.SyncResult, len(e.cfg.Sync.Tables))
	var wg gosync.WaitGroup

	for i, tableCfg := range e.cfg.Sync.Tables {
		wg.Add(1)
		go func(idx int, tc config.SyncTableConfig) {
			defer wg.Done()
			results[idx] = e.syncTable(state, tc)
		}(i, tableCfg)
	}

	wg.Wait()
	return results
}

// RunOnce is a convenience wrapper that runs RunSync once and returns.
// Used by the --once CLI flag.
func (e *Engine) RunOnce() []types.SyncResult {
	return e.RunSync()
}

const defaultTimeoutMinutes = 30

// syncTable syncs a single table. This runs in its own goroutine — one table's
// failure must not affect any other table.
func (e *Engine) syncTable(state *State, tc config.SyncTableConfig) types.SyncResult {
	start := time.Now()

	timeoutMinutes := tc.TimeoutMinutes
	if timeoutMinutes <= 0 {
		timeoutMinutes = defaultTimeoutMinutes
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMinutes)*time.Minute)
	defer cancel()
	destTable := tc.DestinationTable
	if destTable == "" {
		destTable = tc.Name
	}

	result := types.SyncResult{
		TableName: tc.Name,
	}

	// --- Step 1: Schema handling ---

	// Get source schema.
	srcSchema, err := e.src.GetSchema(tc.Name)
	if err != nil {
		result.Error = fmt.Errorf("failed to get source schema: %w", err)
		result.Duration = time.Since(start)
		_ = MarkSyncFailed(state, e.statePath, e.cfg.Name, tc.Name, result.Error)
		return result
	}

	// Get destination schema (nil if table doesn't exist).
	dstSchema, err := e.dst.GetSchema(destTable)
	if err != nil {
		result.Error = fmt.Errorf("failed to get destination schema: %w", err)
		result.Duration = time.Since(start)
		_ = MarkSyncFailed(state, e.statePath, e.cfg.Name, tc.Name, result.Error)
		return result
	}

	// Handle table creation or schema changes.
	if dstSchema == nil {
		// Table doesn't exist — create it.
		if err := e.dst.CreateTable(destTable, srcSchema); err != nil {
			result.Error = fmt.Errorf("failed to create destination table: %w", err)
			result.Duration = time.Since(start)
			_ = MarkSyncFailed(state, e.statePath, e.cfg.Name, tc.Name, result.Error)
			return result
		}
	} else {
		// Table exists — check for schema changes.
		changes := DiffSchema(srcSchema, dstSchema)
		if len(changes.Changes) > 0 {
			result.SchemaChange = &changes

			if changes.HasIncompatible {
				// Incompatible schema change — halt this table.
				result.Error = fmt.Errorf("incompatible schema change on %s: %w", tc.Name, ErrIncompatibleSchema)
				result.Duration = time.Since(start)
				_ = MarkSyncFailed(state, e.statePath, e.cfg.Name, tc.Name, result.Error)
				return result
			}

			// Apply compatible changes.
			if err := e.dst.AlterTable(destTable, changes); err != nil {
				result.Error = fmt.Errorf("failed to alter destination table: %w", err)
				result.Duration = time.Since(start)
				_ = MarkSyncFailed(state, e.statePath, e.cfg.Name, tc.Name, result.Error)
				return result
			}
		}
	}

	// --- Step 2: Determine cursor and run strategy ---

	// Get cursor value from state (handles crash recovery).
	tableState := getTableState(state, e.cfg.Name, tc.Name)
	cursorValue := GetCursorValue(tableState)

	// Check if we're resuming from a crash.
	if tableState != nil && ShouldResume(tableState) {
		// Crash detected — resume from previous cursor_value (NOT in-progress).
		cursorValue = GetCursorValue(tableState)
	}

	// Mark sync started BEFORE reading any rows.
	// We record the OLD cursor value here — if we crash, we'll resume from this point.
	if err := MarkSyncStarted(state, e.statePath, e.cfg.Name, tc.Name, cursorValue); err != nil {
		result.Error = fmt.Errorf("failed to mark sync started: %w", err)
		result.Duration = time.Since(start)
		return result
	}

	// Run the appropriate strategy.
	var syncOutput SyncOutput
	syncErr := WithRetry(3, func() error {
		var err error
		switch types.SyncMode(tc.SyncMode) {
		case types.SyncModeFullRefresh:
			syncOutput, err = RunFullRefresh(ctx, e.src, e.dst, tc.Name, destTable, e.cfg.Sync.BatchSize, srcSchema)
		case types.SyncModeIncremental:
			syncOutput, err = RunIncremental(ctx, e.src, e.dst, tc.Name, destTable, tc.CursorField, cursorValue, e.cfg.Sync.BatchSize)
		case types.SyncModeAppendOnly:
			syncOutput, err = RunAppendOnly(ctx, e.src, e.dst, tc.Name, destTable, tc.CursorField, cursorValue, e.cfg.Sync.BatchSize)
		default:
			return fmt.Errorf("unknown sync mode: %s", tc.SyncMode)
		}
		return err
	})

	if syncErr != nil {
		result.Error = syncErr
		result.Duration = time.Since(start)
		_ = MarkSyncFailed(state, e.statePath, e.cfg.Name, tc.Name, syncErr)
		return result
	}

	// --- Step 3: Mark completed with NEW cursor ---
	// Update the in-progress cursor to the new value BEFORE promoting it.
	// This ensures MarkSyncCompleted promotes the correct (new) cursor.
	if syncOutput.NewCursorValue != nil {
		stateMu.Lock()
		ts := ensureTableState(state, e.cfg.Name, tc.Name)
		ts.CursorValueInProgress = syncOutput.NewCursorValue
		stateMu.Unlock()
	}

	if err := MarkSyncCompleted(state, e.statePath, e.cfg.Name, tc.Name, syncOutput.RowsSynced); err != nil {
		result.Error = fmt.Errorf("failed to mark sync completed: %w", err)
		result.Duration = time.Since(start)
		return result
	}

	result.Success = true
	result.RowsSynced = syncOutput.RowsSynced
	result.Duration = time.Since(start)
	return result
}

// getTableState safely gets the table state, returning nil if it doesn't exist.
func getTableState(state *State, connName, table string) *TableState {
	conn, ok := state.Connections[connName]
	if !ok {
		return nil
	}
	ts, ok := conn.Tables[table]
	if !ok {
		return nil
	}
	return ts
}
