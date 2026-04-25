package sync

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ==========================================================================
// RepairState — recover from .tmp
// ==========================================================================

func TestRepairState_RecoverFromTmp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	tmpPath := path + ".tmp"

	// Write a valid .tmp file (simulates a crash between WriteFile and Rename).
	good := &State{
		Version: "1",
		Connections: map[string]*ConnectionState{
			"prod": {
				Tables: map[string]*TableState{
					"users": {
						Status:      "success",
						CursorValue: "recovered-cursor",
					},
				},
			},
		},
	}
	data, _ := json.MarshalIndent(good, "", "  ")
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		t.Fatalf("setup: failed to write .tmp: %v", err)
	}

	msg, err := RepairState(path)
	if err != nil {
		t.Fatalf("RepairState should succeed when .tmp is valid, got: %v", err)
	}
	if !strings.Contains(msg, "recovered from state.json.tmp") {
		t.Errorf("unexpected message: %s", msg)
	}

	// Verify the main file now exists with correct content.
	reloaded, err := LoadState(path)
	if err != nil {
		t.Fatalf("LoadState after repair failed: %v", err)
	}
	cursor := reloaded.Connections["prod"].Tables["users"].CursorValue
	if cursor != "recovered-cursor" {
		t.Errorf("expected cursor 'recovered-cursor', got %v", cursor)
	}

	// .tmp should no longer exist (it was renamed).
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error(".tmp file should not exist after successful recovery")
	}
}

// ==========================================================================
// RepairState — corrupted state.json, no .tmp → needs confirmation
// ==========================================================================

func TestRepairState_FreshState_NeedsConfirmation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	// Write a corrupted state file.
	if err := os.WriteFile(path, []byte("not json at all {{{"), 0600); err != nil {
		t.Fatalf("setup: failed to write corrupted file: %v", err)
	}

	_, err := RepairState(path)
	if err == nil {
		t.Fatal("RepairState should return error when corrupted and no .tmp")
	}
	if err != ErrFreshStateNeedsConfirmation {
		t.Errorf("expected ErrFreshStateNeedsConfirmation, got: %v", err)
	}
}

// ==========================================================================
// RepairStateFresh — backup is created, fresh state is written
// ==========================================================================

func TestRepairStateFresh_Backup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	// Write a corrupted state file that will be backed up.
	corruptedContent := []byte("corrupted data 12345")
	if err := os.WriteFile(path, corruptedContent, 0600); err != nil {
		t.Fatalf("setup: failed to write corrupted file: %v", err)
	}

	msg, err := RepairStateFresh(path)
	if err != nil {
		t.Fatalf("RepairStateFresh should succeed, got: %v", err)
	}
	if !strings.Contains(msg, "fresh empty state") {
		t.Errorf("unexpected message: %s", msg)
	}

	// Verify the fresh state was written.
	reloaded, err := LoadState(path)
	if err != nil {
		t.Fatalf("LoadState after fresh write failed: %v", err)
	}
	if len(reloaded.Connections) != 0 {
		t.Error("expected empty connections in fresh state")
	}

	// Verify a backup file was created with the corrupted content.
	entries, _ := os.ReadDir(dir)
	var backupFound bool
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "state.json.broken-") {
			backupFound = true
			backupData, _ := os.ReadFile(filepath.Join(dir, e.Name()))
			if string(backupData) != string(corruptedContent) {
				t.Errorf("backup content mismatch: got %q", string(backupData))
			}
		}
	}
	if !backupFound {
		t.Error("no backup file was created")
	}
}

// ==========================================================================
// RepairState — no state.json, no .tmp → writes fresh directly (first run)
// ==========================================================================

func TestRepairState_NoStateFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	// Neither file exists — true first run.

	msg, err := RepairState(path)
	if err != nil {
		t.Fatalf("RepairState on missing file should succeed, got: %v", err)
	}
	if !strings.Contains(msg, "no existing state found") {
		t.Errorf("unexpected message: %s", msg)
	}

	// Verify fresh state was written.
	reloaded, err := LoadState(path)
	if err != nil {
		t.Fatalf("LoadState after first-run repair failed: %v", err)
	}
	if reloaded.Version != "1" {
		t.Errorf("expected version '1', got %q", reloaded.Version)
	}
	if len(reloaded.Connections) != 0 {
		t.Error("expected empty connections")
	}
}
