// Package types defines the canonical internal type system shared by all packages.
// This package has ZERO imports from other internal packages — it is the leaf of
// the dependency graph.
package types

import "time"

// FieldType represents the canonical internal type system.
// All source types map TO these. All destination types map FROM these.
type FieldType string

const (
	FieldTypeString    FieldType = "STRING"
	FieldTypeInt64     FieldType = "INT64"
	FieldTypeFloat64   FieldType = "FLOAT64"
	FieldTypeBool      FieldType = "BOOL"
	FieldTypeTimestamp FieldType = "TIMESTAMP"
	FieldTypeDate      FieldType = "DATE"
	FieldTypeNumeric   FieldType = "NUMERIC"
	FieldTypeJSON      FieldType = "JSON"
	FieldTypeBytes     FieldType = "BYTES"
	FieldTypeRepeated  FieldType = "REPEATED" // array type; ItemType holds the inner FieldType
)

// Field represents a single column/field in a schema.
type Field struct {
	Name     string
	Type     FieldType
	Nullable bool
	ItemType FieldType // only populated when Type == FieldTypeRepeated
}

// Schema represents the schema of a table.
type Schema struct {
	TableName string
	Fields    []Field
}

// Row is a single record. Values may be nil for nullable fields.
type Row map[string]any

// WriteResult is returned by the destination after writing a batch.
type WriteResult struct {
	RowsWritten  int64
	BytesWritten int64
}

// SyncMode defines how a table is synced.
type SyncMode string

const (
	SyncModeIncremental SyncMode = "incremental"
	SyncModeFullRefresh SyncMode = "full_refresh"
	SyncModeAppendOnly  SyncMode = "append_only"
)

// SchemaChangeType classifies a single schema difference.
type SchemaChangeType string

const (
	SchemaChangeAddColumn        SchemaChangeType = "add_column"
	SchemaChangeRemoveColumn     SchemaChangeType = "remove_column"
	SchemaChangeTypeWiden        SchemaChangeType = "type_widen"
	SchemaChangeTypeIncompatible SchemaChangeType = "type_incompatible"
)

// SchemaChange represents a single detected schema difference.
type SchemaChange struct {
	ChangeType SchemaChangeType
	FieldName  string
	OldField   *Field // nil when ChangeType == add_column
	NewField   *Field // nil when ChangeType == remove_column
}

// SchemaChanges aggregates all schema differences for a table.
type SchemaChanges struct {
	TableName       string
	Changes         []SchemaChange
	HasIncompatible bool // when true, sync MUST halt for this table
}

// SyncResult is returned by the sync engine per table.
type SyncResult struct {
	TableName    string
	Success      bool
	RowsSynced   int64
	Duration     time.Duration
	Error        error
	SchemaChange *SchemaChanges // non-nil if schema changed this run
}
