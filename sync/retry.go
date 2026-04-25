package sync

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// IsFatalError returns true for errors that must not be retried.
// Auth errors, config errors, and schema errors are fatal.
// Network errors, timeouts, and quota errors are retryable.
func IsFatalError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()

	// Auth/permission errors are fatal.
	fatalPatterns := []string{
		"authentication",
		"permission denied",
		"access denied",
		"invalid credentials",
		"unauthorized",
		"forbidden",
		"incompatible schema",
		"incompatible type",
	}

	for _, pattern := range fatalPatterns {
		if containsIgnoreCase(errMsg, pattern) {
			return true
		}
	}

	return false
}

// IsRetryableError returns true for errors that can be retried.
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()

	retryablePatterns := []string{
		"connection refused",
		"connection reset",
		"timeout",
		"deadline exceeded",
		"temporary failure",
		"too many requests",
		"rate limit",
		"quota exceeded",
		"service unavailable",
		"server busy",
		"i/o timeout",
		"broken pipe",
		"EOF",
	}

	for _, pattern := range retryablePatterns {
		if containsIgnoreCase(errMsg, pattern) {
			return true
		}
	}

	return false
}

// WithRetry retries fn up to maxAttempts times with exponential backoff.
// Stops immediately if IsFatalError returns true.
// Backoff: 1s, 2s, 4s, ...
func WithRetry(maxAttempts int, fn func() error) error {
	var err error
	for i := 0; i < maxAttempts; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		if IsFatalError(err) {
			return fmt.Errorf("fatal error (not retrying): %w", err)
		}
		if i < maxAttempts-1 {
			wait := time.Duration(math.Pow(2, float64(i))) * time.Second
			time.Sleep(wait)
		}
	}
	return fmt.Errorf("failed after %d attempts: %w", maxAttempts, err)
}

// ErrIncompatibleSchema is returned when a schema change is incompatible and
// the sync must halt for that table.
var ErrIncompatibleSchema = errors.New("incompatible schema change detected")

// containsIgnoreCase checks if s contains substr (case-insensitive).
func containsIgnoreCase(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
