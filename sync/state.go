// Package sync implements the sync engine, state management, schema diffing,
// sync strategies, and retry logic.
package sync

import (
	"encoding/json"
	"fmt"
	"os"
	gosync "sync"
	"time"
)

// State represents the entire state file.
type State struct {
	Version     string                      `json:"version"`
	Connections map[string]*ConnectionState `json:"connections"`
}

// ConnectionState represents state for a single named connection.
type ConnectionState struct {
	Tables map[string]*TableState `json:"tables"`
}

// TableState represents sync state for a single table.
type TableState struct {
	SyncMode              string  `json:"sync_mode"`
	CursorField           string  `json:"cursor_field"`
	CursorValue           any     `json:"cursor_value"`
	CursorValueInProgress any     `json:"cursor_value_inprogress"`
	LastSyncStarted       *string `json:"last_sync_started"`   // RFC3339 or nil
	LastSyncCompleted     *string `json:"last_sync_completed"` // RFC3339 or nil
	RowsSyncedLastRun     int64   `json:"rows_synced_last_run"`
	ConsecutiveFailures   int     `json:"consecutive_failures"`
	Status                string  `json:"status"` // "success" | "running" | "failed"
}

// stateMu protects all map mutations AND JSON marshal/write from concurrent
// access by different table goroutines.
var stateMu gosync.Mutex

// LoadState reads state.json from disk. If the file doesn't exist, returns
// an empty state (first run) — not an error.
func LoadState(path string) (*State, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// First run — return empty state.
			return &State{
				Version:     "1",
				Connections: make(map[string]*ConnectionState),
			}, nil
		}
		return nil, fmt.Errorf("failed to read state file: %w", err)
	}

	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to parse state file: %w", err)
	}

	// Ensure maps are initialized.
	if state.Connections == nil {
		state.Connections = make(map[string]*ConnectionState)
	}
	for _, conn := range state.Connections {
		if conn.Tables == nil {
			conn.Tables = make(map[string]*TableState)
		}
	}

	return &state, nil
}

// SaveState writes state to disk atomically: write to path.tmp, then rename
// to path. This ensures that a crash during write never corrupts the existing
// state file.
//
// IMPORTANT: Callers must hold stateMu before calling this function.
// All public Mark* functions acquire the lock and call this.
func saveStateLocked(path string, state *State) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return fmt.Errorf("failed to write temp state file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("failed to rename state file: %w", err)
	}

	return nil
}

// SaveState is the public API for saving state. It acquires the lock,
// marshals, and does an atomic write.
func SaveState(path string, state *State) error {
	stateMu.Lock()
	defer stateMu.Unlock()
	return saveStateLocked(path, state)
}

// ensureTableState guarantees that the connection and table entries exist in
// the state, creating them if necessary.
//
// IMPORTANT: Caller must hold stateMu.
func ensureTableState(state *State, connName, table string) *TableState {
	conn, ok := state.Connections[connName]
	if !ok {
		conn = &ConnectionState{
			Tables: make(map[string]*TableState),
		}
		state.Connections[connName] = conn
	}
	ts, ok := conn.Tables[table]
	if !ok {
		ts = &TableState{}
		conn.Tables[table] = ts
	}
	return ts
}

// MarkSyncStarted sets status=running, records start time and the in-progress
// cursor value, then saves state immediately.
//
// CRASH RECOVERY: This MUST be called BEFORE reading any rows from the source.
// If the process crashes after this call but before MarkSyncCompleted, the
// ShouldResume function will detect the incomplete sync and the engine will
// resume from the previous cursor_value (NOT cursor_value_inprogress).
func MarkSyncStarted(state *State, path, connName, table string, cursorInProgress any) error {
	stateMu.Lock()
	defer stateMu.Unlock()

	ts := ensureTableState(state, connName, table)
	now := time.Now().UTC().Format(time.RFC3339)
	ts.Status = "running"
	ts.LastSyncStarted = &now
	ts.LastSyncCompleted = nil
	ts.CursorValueInProgress = cursorInProgress
	return saveStateLocked(path, state)
}

// MarkSyncCompleted sets status=success, promotes cursor_value_inprogress to
// cursor_value, records completion time and row count, resets consecutive
// failures, then saves state.
//
// CRASH RECOVERY: This is called ONLY after all rows are acknowledged by the
// destination. The cursor_value promotion guarantees that a crash before this
// call causes the engine to re-read from the previous safe cursor.
func MarkSyncCompleted(state *State, path, connName, table string, rowsSynced int64) error {
	stateMu.Lock()
	defer stateMu.Unlock()

	ts := ensureTableState(state, connName, table)
	now := time.Now().UTC().Format(time.RFC3339)
	ts.Status = "success"
	ts.LastSyncCompleted = &now
	ts.CursorValue = ts.CursorValueInProgress
	ts.CursorValueInProgress = nil
	ts.RowsSyncedLastRun = rowsSynced
	ts.ConsecutiveFailures = 0
	return saveStateLocked(path, state)
}

// MarkSyncFailed sets status=failed, increments consecutive_failures, and saves
// state. It does NOT update cursor_value — a failed sync must never advance
// the cursor.
func MarkSyncFailed(state *State, path, connName, table string, syncErr error) error {
	stateMu.Lock()
	defer stateMu.Unlock()

	ts := ensureTableState(state, connName, table)
	ts.Status = "failed"
	ts.ConsecutiveFailures++
	ts.CursorValueInProgress = nil
	return saveStateLocked(path, state)
}

// ShouldResume returns true if the table was mid-sync when the process
// crashed: last_sync_started is set but last_sync_completed is nil.
func ShouldResume(ts *TableState) bool {
	return ts.LastSyncStarted != nil && ts.LastSyncCompleted == nil
}

// GetCursorValue returns the last completed cursor value. This is always the
// safe resume point — never the in-progress cursor.
func GetCursorValue(ts *TableState) any {
	if ts == nil {
		return nil
	}
	return ts.CursorValue
}

// ErrFreshStateNeedsConfirmation is returned by RepairState when no .tmp
// recovery is possible and the only option is to write a fresh empty state.
// This forces the operator to explicitly confirm with --confirm-fresh because
// a fresh state causes ALL tables to re-sync from scratch.
var ErrFreshStateNeedsConfirmation = fmt.Errorf("repair: no .tmp recovery possible; fresh state requires --confirm-fresh")

// RepairState attempts to recover a corrupted or missing state file.
// Strategy:
//  1. If path.tmp exists and is valid JSON, rename it to path (atomic write
//     had failed between WriteFile and Rename — the .tmp is the good copy).
//  2. If path exists but is corrupted (and no .tmp), return
//     ErrFreshStateNeedsConfirmation. The caller must use RepairStateFresh()
//     with explicit confirmation.
//  3. If neither path nor .tmp exists (true first run), write fresh state
//     directly — there is nothing to lose.
//
// Returns a human-readable description of what was done.
func RepairState(path string) (string, error) {
	tmpPath := path + ".tmp"

	// Try to recover from .tmp first.
	if data, err := os.ReadFile(tmpPath); err == nil {
		var candidate State
		if json.Unmarshal(data, &candidate) == nil {
			// .tmp is valid JSON — the rename must have failed. Finish the rename.
			if err := os.Rename(tmpPath, path); err != nil {
				return "", fmt.Errorf("repair: found valid .tmp but rename failed: %w", err)
			}
			return "recovered from state.json.tmp (atomic rename had failed)", nil
		}
	}

	// Check if the main state file exists.
	if _, err := os.Stat(path); err == nil {
		// File exists but is corrupted (or we wouldn't be repairing).
		// Require confirmation before nuking all cursors.
		return "", ErrFreshStateNeedsConfirmation
	}

	// Neither file exists — true first run. Write fresh state directly.
	fresh := &State{
		Version:     "1",
		Connections: make(map[string]*ConnectionState),
	}
	if err := SaveState(path, fresh); err != nil {
		return "", fmt.Errorf("repair: failed to write fresh state: %w", err)
	}
	return "no existing state found — wrote fresh empty state", nil
}

// RepairStateFresh backs up the existing (corrupted) state file and writes
// a fresh empty state. Called only when the operator confirms with --confirm-fresh.
func RepairStateFresh(path string) (string, error) {
	// Backup the existing file before overwriting.
	if _, err := os.Stat(path); err == nil {
		backupPath := path + ".broken-" + time.Now().UTC().Format("20060102T150405Z")
		data, readErr := os.ReadFile(path)
		if readErr == nil {
			if writeErr := os.WriteFile(backupPath, data, 0600); writeErr != nil {
				return "", fmt.Errorf("repair: failed to backup corrupted state to %s: %w", backupPath, writeErr)
			}
			fmt.Printf("[repair-state] backed up corrupted state to %s\n", backupPath)
		}
	}

	fresh := &State{
		Version:     "1",
		Connections: make(map[string]*ConnectionState),
	}
	if err := SaveState(path, fresh); err != nil {
		return "", fmt.Errorf("repair: failed to write fresh state: %w", err)
	}
	return "wrote fresh empty state — all tables will re-sync from scratch on next run", nil
}
