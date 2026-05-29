package sync

import (
	"context"
	"fmt"
	"time"

	"github.com/rootwit/rootwit/destinations"
	"github.com/rootwit/rootwit/sources"
	"github.com/rootwit/rootwit/types"
)

// SyncOutput holds the result of a strategy run: how many rows were synced
// and what the new cursor value is (the maximum cursor_field value seen).
type SyncOutput struct {
	RowsSynced     int64
	NewCursorValue any // max cursor_field value from rows read; nil for full_refresh
}

// RunFullRefresh executes a full refresh sync using a staging-swap pattern to
// avoid an empty-destination window. All rows are written to a staging table
// first; once complete, a single SwapTables call makes them visible to readers.
// The destination table always contains either the old data or the new data —
// never an empty intermediate state.
func RunFullRefresh(
	ctx context.Context,
	src sources.SourceConnector,
	dst destinations.DestinationConnector,
	table string,
	destTable string,
	batchSize int,
	srcSchema types.Schema,
) (SyncOutput, error) {
	stagingTable := destTable + "_rootwit_staging"

	// Prepare staging table: create if missing, truncate if it already exists
	// (leftover from a previous crashed full_refresh).
	stagingSchema, err := dst.GetSchema(stagingTable)
	if err != nil {
		return SyncOutput{}, fmt.Errorf("full_refresh: failed to check staging table: %w", err)
	}
	if stagingSchema == nil {
		if err := dst.CreateTable(stagingTable, srcSchema); err != nil {
			return SyncOutput{}, fmt.Errorf("full_refresh: failed to create staging table: %w", err)
		}
	} else {
		if err := dst.TruncateTable(stagingTable); err != nil {
			return SyncOutput{}, fmt.Errorf("full_refresh: failed to truncate staging table: %w", err)
		}
	}

	// Write all source rows to staging. Destination is untouched during this step.
	rowCh, errCh := src.ReadFull(ctx, table, batchSize)
	totalRows, err := drainAndWrite(dst, stagingTable, rowCh, errCh, batchSize)
	if err != nil {
		return SyncOutput{}, err
	}

	// Atomic swap: staging becomes dest. Readers see old data until this
	// completes, then immediately see new data. Destination is never empty.
	if err := dst.SwapTables(stagingTable, destTable); err != nil {
		return SyncOutput{}, fmt.Errorf("full_refresh: swap failed: %w", err)
	}

	return SyncOutput{RowsSynced: totalRows}, nil
}

// RunIncremental executes an incremental sync: read rows where cursorField >
// cursorValue and write them to the destination in batches.
// Tracks the maximum cursor value seen for state advancement.
func RunIncremental(
	ctx context.Context,
	src sources.SourceConnector,
	dst destinations.DestinationConnector,
	table string,
	destTable string,
	cursorField string,
	cursorValue any,
	batchSize int,
) (SyncOutput, error) {
	rowCh, errCh := src.ReadIncremental(ctx, table, cursorField, cursorValue, batchSize)

	totalRows, maxCursor, err := drainAndWriteWithCursor(dst, destTable, rowCh, errCh, cursorField, batchSize)
	if err != nil {
		return SyncOutput{}, err
	}

	// If no rows were read, the cursor stays at the old value.
	newCursor := cursorValue
	if maxCursor != nil {
		newCursor = maxCursor
	}

	return SyncOutput{RowsSynced: totalRows, NewCursorValue: newCursor}, nil
}

// RunAppendOnly executes an append-only sync: same as incremental, but
// semantically the source data only grows (events, logs). Rows are never
// updated, only new rows are appended.
func RunAppendOnly(
	ctx context.Context,
	src sources.SourceConnector,
	dst destinations.DestinationConnector,
	table string,
	destTable string,
	cursorField string,
	cursorValue any,
	batchSize int,
) (SyncOutput, error) {
	// Append-only uses the same read path as incremental.
	rowCh, errCh := src.ReadIncremental(ctx, table, cursorField, cursorValue, batchSize)

	totalRows, maxCursor, err := drainAndWriteWithCursor(dst, destTable, rowCh, errCh, cursorField, batchSize)
	if err != nil {
		return SyncOutput{}, err
	}

	newCursor := cursorValue
	if maxCursor != nil {
		newCursor = maxCursor
	}

	return SyncOutput{RowsSynced: totalRows, NewCursorValue: newCursor}, nil
}

// drainAndWrite reads rows from rowCh, batches them, and writes to the
// destination. Returns total rows written and any error.
// Used by full_refresh which doesn't need cursor tracking.
func drainAndWrite(
	dst destinations.DestinationConnector,
	destTable string,
	rowCh <-chan types.Row,
	errCh <-chan error,
	batchSize int,
) (int64, error) {
	var totalRows int64
	batch := make([]types.Row, 0, batchSize)

	for row := range rowCh {
		batch = append(batch, row)

		// Flush batch when it reaches batchSize.
		if len(batch) >= batchSize {
			result, err := dst.WriteBatch(destTable, batch)
			if err != nil {
				return totalRows, fmt.Errorf("write batch failed: %w", err)
			}
			totalRows += result.RowsWritten
			batch = batch[:0] // reset batch, reuse backing array
		}
	}

	// Flush remaining rows.
	if len(batch) > 0 {
		result, err := dst.WriteBatch(destTable, batch)
		if err != nil {
			return totalRows, fmt.Errorf("write final batch failed: %w", err)
		}
		totalRows += result.RowsWritten
	}

	// Check for source read errors.
	if err := <-errCh; err != nil {
		return totalRows, fmt.Errorf("source read error: %w", err)
	}

	return totalRows, nil
}

// drainAndWriteWithCursor is like drainAndWrite but also tracks the maximum
// value of cursorField across all rows. This is the new cursor value that
// gets persisted to state after a successful sync.
func drainAndWriteWithCursor(
	dst destinations.DestinationConnector,
	destTable string,
	rowCh <-chan types.Row,
	errCh <-chan error,
	cursorField string,
	batchSize int,
) (int64, any, error) {
	var totalRows int64
	var maxCursor any
	batch := make([]types.Row, 0, batchSize)

	for row := range rowCh {
		// Track the true maximum cursor value across all rows.
		// We compare explicitly rather than assuming source order —
		// SQL sources return ASC but API sources (e.g. Razorpay) return DESC.
		if v, ok := row[cursorField]; ok && v != nil {
			if cursorGreater(v, maxCursor) {
				maxCursor = v
			}
		}

		batch = append(batch, row)

		if len(batch) >= batchSize {
			result, err := dst.WriteBatch(destTable, batch)
			if err != nil {
				return totalRows, maxCursor, fmt.Errorf("write batch failed: %w", err)
			}
			totalRows += result.RowsWritten
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		result, err := dst.WriteBatch(destTable, batch)
		if err != nil {
			return totalRows, maxCursor, fmt.Errorf("write final batch failed: %w", err)
		}
		totalRows += result.RowsWritten
	}

	if err := <-errCh; err != nil {
		return totalRows, maxCursor, fmt.Errorf("source read error: %w", err)
	}

	return totalRows, maxCursor, nil
}

// cursorGreater reports whether a > b for the cursor types RootWit uses.
// Handles time.Time, int64, float64, and string (RFC3339 timestamps compare
// lexicographically). Returns true when b is nil (any real value beats nil).
func cursorGreater(a, b any) bool {
	if b == nil {
		return true
	}
	switch av := a.(type) {
	case time.Time:
		if bv, ok := b.(time.Time); ok {
			return av.After(bv)
		}
	case int64:
		if bv, ok := b.(int64); ok {
			return av > bv
		}
	case float64:
		if bv, ok := b.(float64); ok {
			return av > bv
		}
	case string:
		if bv, ok := b.(string); ok {
			return av > bv
		}
	}
	return false
}
