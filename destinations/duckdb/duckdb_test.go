package duckdb

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rootwit/rootwit/config"
	"github.com/rootwit/rootwit/types"
)

func newTestDest(t *testing.T) (*Destination, func()) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.duckdb")

	cfg := config.DestConfig{
		Type: "duckdb",
		DuckDB: config.DuckDBConfig{
			Path:        path,
			MemoryLimit: "128MB",
		},
	}
	d := NewDuckDBDestination(cfg)
	if err := d.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	return d, func() {
		d.Close()
		os.RemoveAll(dir)
	}
}

var testSchema = types.Schema{
	TableName: "users",
	Fields: []types.Field{
		{Name: "id", Type: types.FieldTypeInt64, Nullable: false},
		{Name: "name", Type: types.FieldTypeString, Nullable: true},
		{Name: "score", Type: types.FieldTypeFloat64, Nullable: true},
		{Name: "active", Type: types.FieldTypeBool, Nullable: true},
		{Name: "created_at", Type: types.FieldTypeTimestamp, Nullable: true},
	},
}

func TestConnect(t *testing.T) {
	d, cleanup := newTestDest(t)
	defer cleanup()
	_ = d
}

func TestCreateTableAndGetSchema(t *testing.T) {
	d, cleanup := newTestDest(t)
	defer cleanup()

	// Table should not exist yet.
	schema, err := d.GetSchema("users")
	if err != nil {
		t.Fatalf("GetSchema (missing): %v", err)
	}
	if schema != nil {
		t.Fatal("expected nil schema for non-existent table")
	}

	// Create the table.
	if err := d.CreateTable("users", testSchema); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Schema should now exist.
	got, err := d.GetSchema("users")
	if err != nil {
		t.Fatalf("GetSchema (after create): %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil schema after CreateTable")
	}
	if len(got.Fields) != len(testSchema.Fields) {
		t.Fatalf("field count: want %d got %d", len(testSchema.Fields), len(got.Fields))
	}
}

func TestWriteBatchAndRead(t *testing.T) {
	d, cleanup := newTestDest(t)
	defer cleanup()

	if err := d.CreateTable("users", testSchema); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	rows := []types.Row{
		{"id": int64(1), "name": "Alice", "score": float64(9.5), "active": true, "created_at": now},
		{"id": int64(2), "name": "Bob", "score": float64(7.0), "active": false, "created_at": now},
		{"id": int64(3), "name": nil, "score": nil, "active": nil, "created_at": nil},
	}

	result, err := d.WriteBatch("users", rows)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if result.RowsWritten != 3 {
		t.Fatalf("RowsWritten: want 3 got %d", result.RowsWritten)
	}

	// Verify row count via SQL.
	var count int
	if err := d.db.QueryRow(`SELECT COUNT(*) FROM "users"`).Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 3 {
		t.Fatalf("row count: want 3 got %d", count)
	}
}

func TestTruncateTable(t *testing.T) {
	d, cleanup := newTestDest(t)
	defer cleanup()

	if err := d.CreateTable("users", testSchema); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	rows := []types.Row{
		{"id": int64(1), "name": "Alice", "score": float64(9.5), "active": true, "created_at": nil},
	}
	if _, err := d.WriteBatch("users", rows); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	if err := d.TruncateTable("users"); err != nil {
		t.Fatalf("TruncateTable: %v", err)
	}

	var count int
	if err := d.db.QueryRow(`SELECT COUNT(*) FROM "users"`).Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 0 {
		t.Fatalf("row count after truncate: want 0 got %d", count)
	}
}

func TestSwapTables(t *testing.T) {
	d, cleanup := newTestDest(t)
	defer cleanup()

	// Create dest table with old data.
	if err := d.CreateTable("users", testSchema); err != nil {
		t.Fatalf("CreateTable dest: %v", err)
	}
	oldRows := []types.Row{{"id": int64(99), "name": "Old", "score": nil, "active": nil, "created_at": nil}}
	if _, err := d.WriteBatch("users", oldRows); err != nil {
		t.Fatalf("WriteBatch dest: %v", err)
	}

	// Create staging table with new data.
	staging := "users_rootwit_staging"
	if err := d.CreateTable(staging, testSchema); err != nil {
		t.Fatalf("CreateTable staging: %v", err)
	}
	newRows := []types.Row{{"id": int64(1), "name": "New", "score": nil, "active": nil, "created_at": nil}}
	if _, err := d.WriteBatch(staging, newRows); err != nil {
		t.Fatalf("WriteBatch staging: %v", err)
	}

	// Swap.
	if err := d.SwapTables(staging, "users"); err != nil {
		t.Fatalf("SwapTables: %v", err)
	}

	// Dest should now have the new row.
	var id int64
	if err := d.db.QueryRow(`SELECT id FROM "users"`).Scan(&id); err != nil {
		t.Fatalf("post-swap query: %v", err)
	}
	if id != 1 {
		t.Fatalf("post-swap id: want 1 got %d", id)
	}

	// Staging table should no longer exist.
	schema, err := d.GetSchema(staging)
	if err != nil {
		t.Fatalf("GetSchema staging after swap: %v", err)
	}
	if schema != nil {
		t.Fatal("staging table should be dropped after swap")
	}
}

func TestAlterTable_AddColumn(t *testing.T) {
	d, cleanup := newTestDest(t)
	defer cleanup()

	if err := d.CreateTable("users", testSchema); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	changes := types.SchemaChanges{
		TableName: "users",
		Changes: []types.SchemaChange{
			{
				ChangeType: types.SchemaChangeAddColumn,
				FieldName:  "email",
				NewField:   &types.Field{Name: "email", Type: types.FieldTypeString, Nullable: true},
			},
		},
	}
	if err := d.AlterTable("users", changes); err != nil {
		t.Fatalf("AlterTable add_column: %v", err)
	}

	schema, err := d.GetSchema("users")
	if err != nil {
		t.Fatalf("GetSchema: %v", err)
	}
	found := false
	for _, f := range schema.Fields {
		if f.Name == "email" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("email column not found after AlterTable add_column")
	}
}

func TestAlterTable_Incompatible(t *testing.T) {
	d, cleanup := newTestDest(t)
	defer cleanup()

	if err := d.CreateTable("users", testSchema); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	changes := types.SchemaChanges{
		TableName:       "users",
		HasIncompatible: true,
		Changes: []types.SchemaChange{
			{
				ChangeType: types.SchemaChangeTypeIncompatible,
				FieldName:  "id",
				OldField:   &types.Field{Name: "id", Type: types.FieldTypeInt64},
				NewField:   &types.Field{Name: "id", Type: types.FieldTypeBool},
			},
		},
	}
	err := d.AlterTable("users", changes)
	if err == nil {
		t.Fatal("expected error for type_incompatible change")
	}
}
