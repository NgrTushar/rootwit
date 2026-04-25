// Package sources defines the SourceConnector interface.
// Every source (Postgres, Stripe, Razorpay) implements this interface.
// The sync engine only ever talks to this interface — it never knows
// which source it is reading from.
//
// WARNING: This interface is LOCKED after Phase 0. Never add parameters
// or change return types. Breaking the interface = breaking every connector.
package sources

import (
	"context"

	"github.com/rootwit/rootwit/types"
)

// SourceConnector is the interface every source must implement.
type SourceConnector interface {
	// Connect establishes connection to the source.
	// Must be called before any other method.
	Connect() error

	// GetTables returns a list of all available table/entity names.
	// For databases: table names. For APIs: entity names (e.g. "payments", "customers").
	GetTables() ([]string, error)

	// GetSchema returns the current schema for a table.
	// Schema is fetched live from the source — not cached.
	GetSchema(table string) (types.Schema, error)

	// ReadIncremental streams rows where cursorField > cursorValue.
	// Returns a row channel and a one-shot error channel.
	// Read rows until the row channel is closed, then check the error channel.
	// ctx cancellation (e.g. from a per-table timeout) aborts the query and
	// closes the error channel with the context error.
	ReadIncremental(ctx context.Context, table string, cursorField string, cursorValue any, batchSize int) (<-chan types.Row, <-chan error)

	// ReadFull streams every row in the table.
	// Same channel pattern as ReadIncremental.
	ReadFull(ctx context.Context, table string, batchSize int) (<-chan types.Row, <-chan error)

	// Close cleans up connections. Always call in defer after Connect succeeds.
	Close() error
}
