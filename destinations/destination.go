// Package destinations defines the DestinationConnector interface.
// Every destination (BigQuery, Snowflake, ClickHouse) implements this interface.
// The sync engine only ever talks to this interface — it never knows
// which destination it is writing to.
//
// WARNING: This interface is LOCKED after Phase 0. Never add parameters
// or change return types. Breaking the interface = breaking every connector.
package destinations

import "github.com/rootwit/rootwit/types"

// DestinationConnector is the interface every destination must implement.
type DestinationConnector interface {
	// Connect establishes connection to the destination.
	Connect() error

	// GetSchema returns the current schema of a table in the destination.
	// Returns nil (not error) when the table does not yet exist.
	GetSchema(table string) (*types.Schema, error)

	// CreateTable creates a new table with the given schema.
	// Called when GetSchema returns nil (table doesn't exist).
	CreateTable(table string, schema types.Schema) error

	// AlterTable applies schema changes to an existing table.
	// Caller MUST check SchemaChanges.HasIncompatible before invoking;
	// an incompatible change returns an error and the engine halts that table
	// while continuing others.
	AlterTable(table string, changes types.SchemaChanges) error

	// WriteBatch writes a batch of rows to the destination.
	// Implementations must be idempotent where possible (RootWit guarantees
	// at-least-once delivery; duplicate batches can occur after a crash).
	WriteBatch(table string, rows []types.Row) (types.WriteResult, error)

	// TruncateTable removes all rows from the table.
	// Used by FULL_REFRESH staging table preparation.
	TruncateTable(table string) error

	// SwapTables atomically replaces dest with the contents of staging.
	// After a successful call, staging is dropped and dest contains the new data.
	// Used by FULL_REFRESH to avoid an empty-destination window: all writes go
	// to staging first, then a single swap makes them visible to readers.
	SwapTables(staging, dest string) error

	// Close cleans up connections.
	Close() error
}
