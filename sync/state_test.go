package sync

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	gosync "sync"
	"testing"
)

// ==========================================================================
// Mandatory test scenario 1: First run — no state.json exists
// ==========================================================================

func TestLoadState_FirstRun(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	// File does not exist.
	state, err := LoadState(path)
	if err != nil {
		t.Fatalf("expected no error on first run, got: %v", err)
	}
	if state == nil {
		t.Fatal("expected non-nil state on first run")
	}
	if state.Version != "1" {
		t.Errorf("expected version '1', got %q", state.Version)
	}
	if len(state.Connections) != 0 {
		t.Errorf("expected empty connections on first run, got %d", len(state.Connections))
	}
}

// ==========================================================================
// Mandatory test scenario 2: Crash recovery — started but not completed
// ==========================================================================

func TestCrashRecovery_ResumeFromPreviousCursor(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	state, _ := LoadState(path)

	// Simulate a completed sync with cursor_value = "2025-04-21T10:00:00Z".
	ts := ensureTableState(state, "prod", "users")
	ts.SyncMode = "incremental"
	ts.CursorField = "updated_at"
	ts.CursorValue = "2025-04-21T10:00:00Z"
	ts.Status = "success"
	completed := "2025-04-21T10:00:45Z"
	ts.LastSyncCompleted = &completed

	if err := SaveState(path, state); err != nil {
		t.Fatalf("SaveState failed: %v", err)
	}

	// Now start a new sync — this writes cursor_value_inprogress.
	if err := MarkSyncStarted(state, path, "prod", "users", "2025-04-22T06:00:00Z"); err != nil {
		t.Fatalf("MarkSyncStarted failed: %v", err)
	}

	// === SIMULATE CRASH: MarkSyncCompleted is never called ===

	// Reload state from disk (as the engine would on restart).
	reloaded, err := LoadState(path)
	if err != nil {
		t.Fatalf("LoadState after crash failed: %v", err)
	}

	ts2 := reloaded.Connections["prod"].Tables["users"]

	// ShouldResume must be true.
	if !ShouldResume(ts2) {
		t.Error("ShouldResume should return true after crash mid-sync")
	}

	// GetCursorValue must return the PREVIOUS completed cursor, not in-progress.
	cursor := GetCursorValue(ts2)
	if cursor != "2025-04-21T10:00:00Z" {
		t.Errorf("expected cursor '2025-04-21T10:00:00Z' (previous), got %v", cursor)
	}

	// cursor_value_inprogress should still be set (it wasn't promoted).
	if ts2.CursorValueInProgress != "2025-04-22T06:00:00Z" {
		t.Errorf("expected in-progress cursor '2025-04-22T06:00:00Z', got %v", ts2.CursorValueInProgress)
	}
}

// ==========================================================================
// Mandatory test scenario 3: Atomic write — state.json not corrupted
// ==========================================================================

func TestSaveState_AtomicWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	// Write initial state.
	initial := &State{
		Version: "1",
		Connections: map[string]*ConnectionState{
			"prod": {
				Tables: map[string]*TableState{
					"users": {
						Status:      "success",
						CursorValue: "initial",
					},
				},
			},
		},
	}
	if err := SaveState(path, initial); err != nil {
		t.Fatalf("initial SaveState failed: %v", err)
	}

	// Write a new state. If this succeeds, original file is replaced atomically.
	updated := &State{
		Version: "1",
		Connections: map[string]*ConnectionState{
			"prod": {
				Tables: map[string]*TableState{
					"users": {
						Status:      "success",
						CursorValue: "updated",
					},
				},
			},
		},
	}
	if err := SaveState(path, updated); err != nil {
		t.Fatalf("updated SaveState failed: %v", err)
	}

	// Verify the file contains the updated value.
	reloaded, err := LoadState(path)
	if err != nil {
		t.Fatalf("LoadState after update failed: %v", err)
	}
	cursor := reloaded.Connections["prod"].Tables["users"].CursorValue
	if cursor != "updated" {
		t.Errorf("expected cursor 'updated', got %v", cursor)
	}

	// Verify no .tmp file was left behind.
	tmpPath := path + ".tmp"
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error("temp file should not exist after successful SaveState")
	}
}

// ==========================================================================
// Mandatory test scenario 4: Concurrent table writes don't corrupt
// ==========================================================================

func TestSaveState_ConcurrentSafety(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	state, _ := LoadState(path)

	// Start 10 goroutines, each updating a different table concurrently.
	var wg gosync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			table := fmt.Sprintf("table_%d", idx)
			if err := MarkSyncStarted(state, path, "prod", table, fmt.Sprintf("cursor_%d", idx)); err != nil {
				t.Errorf("concurrent MarkSyncStarted failed for %s: %v", table, err)
			}
		}(i)
	}
	wg.Wait()

	// Reload and verify all 10 tables exist with correct values.
	reloaded, err := LoadState(path)
	if err != nil {
		t.Fatalf("LoadState after concurrent writes failed: %v", err)
	}

	conn, ok := reloaded.Connections["prod"]
	if !ok {
		t.Fatal("expected 'prod' connection in state")
	}
	if len(conn.Tables) != 10 {
		t.Errorf("expected 10 tables, got %d", len(conn.Tables))
	}

	for i := 0; i < 10; i++ {
		table := fmt.Sprintf("table_%d", i)
		ts, ok := conn.Tables[table]
		if !ok {
			t.Errorf("missing table %s", table)
			continue
		}
		if ts.Status != "running" {
			t.Errorf("table %s: expected status 'running', got %q", table, ts.Status)
		}
	}

	// Verify state file is valid JSON.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read state file: %v", err)
	}
	var parsed State
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("state file contains invalid JSON after concurrent writes: %v", err)
	}
}

// ==========================================================================
// Mandatory test scenario 5: Failed sync increments failures; success resets
// ==========================================================================

func TestConsecutiveFailures_IncrementAndReset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	state, _ := LoadState(path)

	// First failure.
	_ = MarkSyncStarted(state, path, "prod", "orders", nil)
	_ = MarkSyncFailed(state, path, "prod", "orders", fmt.Errorf("connection refused"))

	ts := state.Connections["prod"].Tables["orders"]
	if ts.ConsecutiveFailures != 1 {
		t.Errorf("expected 1 failure, got %d", ts.ConsecutiveFailures)
	}
	if ts.Status != "failed" {
		t.Errorf("expected status 'failed', got %q", ts.Status)
	}

	// Second failure.
	_ = MarkSyncStarted(state, path, "prod", "orders", nil)
	_ = MarkSyncFailed(state, path, "prod", "orders", fmt.Errorf("timeout"))

	if ts.ConsecutiveFailures != 2 {
		t.Errorf("expected 2 failures, got %d", ts.ConsecutiveFailures)
	}

	// Now succeed — failures should reset to 0.
	_ = MarkSyncStarted(state, path, "prod", "orders", "2025-04-22T06:00:00Z")
	_ = MarkSyncCompleted(state, path, "prod", "orders", 500)

	if ts.ConsecutiveFailures != 0 {
		t.Errorf("expected 0 failures after success, got %d", ts.ConsecutiveFailures)
	}
	if ts.Status != "success" {
		t.Errorf("expected status 'success', got %q", ts.Status)
	}
	if ts.RowsSyncedLastRun != 500 {
		t.Errorf("expected 500 rows synced, got %d", ts.RowsSyncedLastRun)
	}
}

// ==========================================================================
// Additional test: MarkSyncCompleted promotes cursor correctly
// ==========================================================================

func TestMarkSyncCompleted_PromotesCursor(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	state, _ := LoadState(path)

	// Set initial cursor.
	ts := ensureTableState(state, "prod", "users")
	ts.CursorValue = "old_cursor"

	// Start sync with new cursor.
	_ = MarkSyncStarted(state, path, "prod", "users", "new_cursor")

	// Complete sync.
	_ = MarkSyncCompleted(state, path, "prod", "users", 100)

	// cursor_value should now be "new_cursor".
	if ts.CursorValue != "new_cursor" {
		t.Errorf("expected cursor 'new_cursor', got %v", ts.CursorValue)
	}

	// cursor_value_inprogress should be nil.
	if ts.CursorValueInProgress != nil {
		t.Errorf("expected nil in-progress cursor, got %v", ts.CursorValueInProgress)
	}

	// ShouldResume should be false (completed successfully).
	if ShouldResume(ts) {
		t.Error("ShouldResume should be false after successful completion")
	}
}

// ==========================================================================
// Additional test: MarkSyncFailed does NOT advance cursor
// ==========================================================================

func TestMarkSyncFailed_DoesNotAdvanceCursor(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	state, _ := LoadState(path)

	ts := ensureTableState(state, "prod", "users")
	ts.CursorValue = "safe_cursor"

	_ = MarkSyncStarted(state, path, "prod", "users", "new_cursor")
	_ = MarkSyncFailed(state, path, "prod", "users", fmt.Errorf("write failed"))

	// cursor_value must remain at the safe point.
	if ts.CursorValue != "safe_cursor" {
		t.Errorf("expected cursor 'safe_cursor' (unchanged), got %v", ts.CursorValue)
	}
}

// ==========================================================================
// Additional test: Round-trip serialization
// ==========================================================================

func TestState_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	state := &State{
		Version: "1",
		Connections: map[string]*ConnectionState{
			"prod": {
				Tables: map[string]*TableState{
					"users": {
						SyncMode:            "incremental",
						CursorField:         "updated_at",
						CursorValue:         "2025-04-21T10:30:00Z",
						Status:              "success",
						RowsSyncedLastRun:   1523,
						ConsecutiveFailures: 0,
					},
				},
			},
		},
	}

	if err := SaveState(path, state); err != nil {
		t.Fatalf("SaveState failed: %v", err)
	}

	reloaded, err := LoadState(path)
	if err != nil {
		t.Fatalf("LoadState failed: %v", err)
	}

	ts := reloaded.Connections["prod"].Tables["users"]
	if ts.SyncMode != "incremental" {
		t.Errorf("expected sync_mode 'incremental', got %q", ts.SyncMode)
	}
	if ts.CursorValue != "2025-04-21T10:30:00Z" {
		t.Errorf("expected cursor value '2025-04-21T10:30:00Z', got %v", ts.CursorValue)
	}
	if ts.RowsSyncedLastRun != 1523 {
		t.Errorf("expected 1523 rows synced, got %d", ts.RowsSyncedLastRun)
	}
}
