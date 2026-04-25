// Package sync provides file-level locking to prevent concurrent RootWit
// instances from corrupting state.json.
//
// The stateMu mutex only serialises writers WITHIN a single process. Two
// separate rootwit processes (e.g. --once while the scheduler runs, or
// two --once invocations in a systemd restart loop) would both load state,
// both compute, and the last writer wins — losing cursor positions and
// consecutive_failures. flock prevents this.

//go:build !windows

package sync

import (
	"fmt"
	"os"
	"syscall"
)

// AcquireStateLock acquires a non-blocking exclusive file lock on the state
// file's lock file (state.json.lock). If another RootWit process holds the
// lock, this returns an error immediately rather than blocking.
//
// The returned *os.File must be kept open for the duration of the process.
// Call ReleaseStateLock when shutting down.
func AcquireStateLock(statePath string) (*os.File, error) {
	lockPath := statePath + ".lock"
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open lock file %s: %w", lockPath, err)
	}

	// LOCK_EX = exclusive, LOCK_NB = non-blocking.
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("another rootwit instance is already running (lock held on %s)", lockPath)
	}

	return f, nil
}

// ReleaseStateLock releases the file lock and closes the lock file.
func ReleaseStateLock(f *os.File) error {
	if f == nil {
		return nil
	}
	// Unlock first, then close. Close also releases the lock, but being
	// explicit is clearer.
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return f.Close()
}
