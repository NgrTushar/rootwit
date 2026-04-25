package sync

import (
	"context"
	"testing"

	"github.com/rootwit/rootwit/types"
)

// stubSource is an in-memory SourceConnector that returns a pre-configured set
// of rows (or none) from its Read methods. Used only in tests.
type stubSource struct {
	rows []types.Row
}

func (s *stubSource) Connect() error               { return nil }
func (s *stubSource) GetTables() ([]string, error) { return nil, nil }
func (s *stubSource) GetSchema(string) (types.Schema, error) {
	return types.Schema{}, nil
}

func (s *stubSource) ReadIncremental(_ context.Context, _ string, _ string, _ any, _ int) (<-chan types.Row, <-chan error) {
	return s.emit()
}

func (s *stubSource) ReadFull(_ context.Context, _ string, _ int) (<-chan types.Row, <-chan error) {
	return s.emit()
}

func (s *stubSource) emit() (<-chan types.Row, <-chan error) {
	rowCh := make(chan types.Row, len(s.rows)+1)
	errCh := make(chan error, 1)
	for _, r := range s.rows {
		rowCh <- r
	}
	close(rowCh)
	close(errCh)
	return rowCh, errCh
}

func (s *stubSource) Close() error { return nil }

// stubDest is an in-memory DestinationConnector that records calls. Used only
// in tests.
type stubDest struct {
	writes    int
	truncated bool
	swapped   bool
}

func (d *stubDest) Connect() error                               { return nil }
func (d *stubDest) GetSchema(string) (*types.Schema, error)      { return nil, nil }
func (d *stubDest) CreateTable(string, types.Schema) error       { return nil }
func (d *stubDest) AlterTable(string, types.SchemaChanges) error { return nil }
func (d *stubDest) TruncateTable(string) error {
	d.truncated = true
	return nil
}
func (d *stubDest) WriteBatch(_ string, rows []types.Row) (types.WriteResult, error) {
	d.writes++
	return types.WriteResult{RowsWritten: int64(len(rows))}, nil
}
func (d *stubDest) SwapTables(_, _ string) error {
	d.swapped = true
	return nil
}
func (d *stubDest) Close() error { return nil }

// TestRunIncremental_ZeroRows_PreservesCursor verifies that when the source
// returns no rows, RunIncremental returns the original cursor value unchanged
// rather than nil. This prevents cursor freeze — if we returned nil, the
// engine would skip the cursor-promotion step on every run and duplicates
// would compound.
func TestRunIncremental_ZeroRows_PreservesCursor(t *testing.T) {
	src := &stubSource{rows: nil}
	dst := &stubDest{}
	originalCursor := "2025-04-20T10:00:00Z"

	out, err := RunIncremental(context.Background(), src, dst, "users", "users", "updated_at", originalCursor, 100)
	if err != nil {
		t.Fatalf("RunIncremental returned unexpected error: %v", err)
	}

	if out.RowsSynced != 0 {
		t.Errorf("RowsSynced = %d, want 0", out.RowsSynced)
	}
	if out.NewCursorValue == nil {
		t.Fatal("NewCursorValue is nil — cursor would freeze on next run")
	}
	if out.NewCursorValue != originalCursor {
		t.Errorf("NewCursorValue = %v, want %q (original cursor)", out.NewCursorValue, originalCursor)
	}
}

// TestRunAppendOnly_ZeroRows_PreservesCursor verifies the same zero-row
// cursor preservation for append_only mode, which shares the incremental
// read path.
func TestRunAppendOnly_ZeroRows_PreservesCursor(t *testing.T) {
	src := &stubSource{rows: nil}
	dst := &stubDest{}
	originalCursor := int64(12345)

	out, err := RunAppendOnly(context.Background(), src, dst, "events", "events", "id", originalCursor, 100)
	if err != nil {
		t.Fatalf("RunAppendOnly returned unexpected error: %v", err)
	}

	if out.RowsSynced != 0 {
		t.Errorf("RowsSynced = %d, want 0", out.RowsSynced)
	}
	if out.NewCursorValue == nil {
		t.Fatal("NewCursorValue is nil — cursor would freeze on next run")
	}
	if out.NewCursorValue != originalCursor {
		t.Errorf("NewCursorValue = %v, want %v (original cursor)", out.NewCursorValue, originalCursor)
	}
}

// TestRunFullRefresh_ZeroRows_ReturnsNilCursor pins the intentional behavior:
// full_refresh has no cursor, so NewCursorValue must be nil. The engine's
// `if syncOutput.NewCursorValue != nil` guard at engine.go:192 depends on
// this — advancing a cursor for a mode that doesn't use one would corrupt
// state for any subsequent switch to incremental mode.
func TestRunFullRefresh_ZeroRows_ReturnsNilCursor(t *testing.T) {
	src := &stubSource{rows: nil}
	dst := &stubDest{}

	out, err := RunFullRefresh(context.Background(), src, dst, "plans", "plans", 100, types.Schema{})
	if err != nil {
		t.Fatalf("RunFullRefresh returned unexpected error: %v", err)
	}

	if out.RowsSynced != 0 {
		t.Errorf("RowsSynced = %d, want 0", out.RowsSynced)
	}
	if out.NewCursorValue != nil {
		t.Errorf("NewCursorValue = %v, want nil (full_refresh has no cursor)", out.NewCursorValue)
	}
	// staging table didn't exist (GetSchema returns nil) so CreateTable is called,
	// not TruncateTable. Swap must always be called.
	if !dst.swapped {
		t.Error("SwapTables was not called — full_refresh must swap staging into dest")
	}
}
