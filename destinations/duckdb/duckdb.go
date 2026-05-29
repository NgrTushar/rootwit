package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	gosync "sync"

	duckdb "github.com/marcboeker/go-duckdb"
	"github.com/rootwit/rootwit/config"
	"github.com/rootwit/rootwit/logger"
	"github.com/rootwit/rootwit/types"
)

// Destination implements destinations.DestinationConnector for DuckDB.
// DuckDB allows only one writer at a time; mu enforces that constraint
// without burdening the engine with DuckDB-specific knowledge.
type Destination struct {
	connector *duckdb.Connector
	db        *sql.DB
	mu        gosync.Mutex
	path      string
	memLimit  string
}

// NewDuckDBDestination creates a new Destination from the given config.
func NewDuckDBDestination(cfg config.DestConfig) *Destination {
	return &Destination{
		path:     cfg.DuckDB.Path,
		memLimit: cfg.DuckDB.MemoryLimit,
	}
}

// Connect opens the DuckDB file and sets the memory limit.
func (d *Destination) Connect() error {
	memLimit := d.memLimit
	connector, err := duckdb.NewConnector(d.path, func(execer driver.ExecerContext) error {
		if memLimit != "" {
			_, err := execer.ExecContext(context.Background(),
				fmt.Sprintf("SET memory_limit='%s'", memLimit), nil)
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("duckdb: failed to open %s: %w", d.path, err)
	}

	db := sql.OpenDB(connector)
	if err := db.PingContext(context.Background()); err != nil {
		db.Close()
		connector.Close()
		return fmt.Errorf("duckdb: ping failed: %w", err)
	}

	d.connector = connector
	d.db = db
	return nil
}

// GetSchema returns the current schema of a table.
// Returns nil (not error) when the table does not yet exist.
func (d *Destination) GetSchema(table string) (*types.Schema, error) {
	// duckdb_columns() gives the full type string (e.g. "INTEGER[]") reliably.
	query := `
		SELECT column_name, data_type, is_nullable
		FROM duckdb_columns()
		WHERE schema_name = 'main' AND table_name = ?
		ORDER BY column_index`

	rows, err := d.db.QueryContext(context.Background(), query, table)
	if err != nil {
		return nil, fmt.Errorf("duckdb: GetSchema query failed for %s: %w", table, err)
	}
	defer rows.Close()

	var fields []types.Field
	for rows.Next() {
		var colName, colType string
		var nullable bool
		if err := rows.Scan(&colName, &colType, &nullable); err != nil {
			return nil, fmt.Errorf("duckdb: GetSchema scan failed: %w", err)
		}
		field := parseColumnType(colName, colType, nullable)
		fields = append(fields, field)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("duckdb: GetSchema rows error: %w", err)
	}

	// Table does not exist — return nil, not error.
	if len(fields) == 0 {
		return nil, nil
	}

	schema := &types.Schema{
		TableName: table,
		Fields:    fields,
	}
	return schema, nil
}

// parseColumnType converts a DuckDB column type string to a types.Field.
func parseColumnType(name, colType string, nullable bool) types.Field {
	// Arrays appear as "INTEGER[]", "VARCHAR[]", etc.
	if strings.HasSuffix(colType, "[]") {
		inner := strings.TrimSuffix(colType, "[]")
		return types.Field{
			Name:     name,
			Type:     types.FieldTypeRepeated,
			Nullable: nullable,
			ItemType: fromDuckDBType(inner),
		}
	}
	return types.Field{
		Name:     name,
		Type:     fromDuckDBType(colType),
		Nullable: nullable,
	}
}

// CreateTable creates a new table with the given schema. All columns are NULLABLE.
func (d *Destination) CreateTable(table string, schema types.Schema) error {
	if len(schema.Fields) == 0 {
		return fmt.Errorf("duckdb: CreateTable called with empty schema for %s", table)
	}

	cols := make([]string, len(schema.Fields))
	for i, f := range schema.Fields {
		cols[i] = fmt.Sprintf(`"%s" %s`, f.Name, toDuckDBType(f))
	}

	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (%s)`, table, strings.Join(cols, ", "))
	_, err := d.db.ExecContext(context.Background(), query)
	if err != nil {
		return fmt.Errorf("duckdb: CreateTable failed for %s: %w", table, err)
	}
	return nil
}

// AlterTable applies schema changes to an existing table.
//   - add_column: ALTER TABLE ADD COLUMN
//   - remove_column: do nothing (column stays, fills NULL for new rows)
//   - type_widen: ALTER TABLE ALTER COLUMN TYPE
//   - type_incompatible: return error immediately (caller halts this table)
func (d *Destination) AlterTable(table string, changes types.SchemaChanges) error {
	ctx := context.Background()
	for _, c := range changes.Changes {
		switch c.ChangeType {
		case types.SchemaChangeAddColumn:
			query := fmt.Sprintf(`ALTER TABLE "%s" ADD COLUMN "%s" %s`,
				table, c.NewField.Name, toDuckDBType(*c.NewField))
			if _, err := d.db.ExecContext(ctx, query); err != nil {
				return fmt.Errorf("duckdb: AlterTable add_column %s.%s failed: %w",
					table, c.NewField.Name, err)
			}

		case types.SchemaChangeRemoveColumn:
			// Keep the column; new rows will have NULL there. No-op.
			logger.L.Infof("duckdb: AlterTable %s: keeping removed column %q (fills NULL)",
				table, c.FieldName)

		case types.SchemaChangeTypeWiden:
			query := fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" TYPE %s`,
				table, c.FieldName, toDuckDBType(*c.NewField))
			if _, err := d.db.ExecContext(ctx, query); err != nil {
				return fmt.Errorf("duckdb: AlterTable type_widen %s.%s failed: %w",
					table, c.FieldName, err)
			}

		case types.SchemaChangeTypeIncompatible:
			return fmt.Errorf("duckdb: incompatible type change for %s.%s: %s → %s (halt)",
				table, c.FieldName, c.OldField.Type, c.NewField.Type)
		}
	}
	return nil
}

// WriteBatch writes rows to the table using the DuckDB Appender API (bulk, not row-by-row INSERT).
// Acquires the mutex for the duration of the write to satisfy DuckDB's single-writer constraint.
func (d *Destination) WriteBatch(table string, rows []types.Row) (types.WriteResult, error) {
	if len(rows) == 0 {
		return types.WriteResult{}, nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	// Fetch destination schema to get column order — Appender requires exact column order.
	schema, err := d.GetSchema(table)
	if err != nil {
		return types.WriteResult{}, fmt.Errorf("duckdb: WriteBatch GetSchema failed: %w", err)
	}
	if schema == nil {
		return types.WriteResult{}, fmt.Errorf("duckdb: WriteBatch called on non-existent table %s", table)
	}

	// Get a driver connection for the Appender (different from sql.DB pool).
	conn, err := d.connector.Connect(context.Background())
	if err != nil {
		return types.WriteResult{}, fmt.Errorf("duckdb: WriteBatch connect failed: %w", err)
	}
	defer conn.Close()

	appender, err := duckdb.NewAppenderFromConn(conn, "", table)
	if err != nil {
		return types.WriteResult{}, fmt.Errorf("duckdb: WriteBatch create appender failed: %w", err)
	}

	var rowsWritten int64
	for _, row := range rows {
		args := make([]driver.Value, len(schema.Fields))
		for i, f := range schema.Fields {
			args[i] = coerceValue(f, row[f.Name])
		}
		if err := appender.AppendRow(args...); err != nil {
			appender.Close()
			return types.WriteResult{RowsWritten: rowsWritten},
				fmt.Errorf("duckdb: WriteBatch AppendRow failed: %w", err)
		}
		rowsWritten++
	}

	if err := appender.Close(); err != nil {
		return types.WriteResult{RowsWritten: rowsWritten},
			fmt.Errorf("duckdb: WriteBatch flush failed: %w", err)
	}

	return types.WriteResult{RowsWritten: rowsWritten}, nil
}

// coerceValue converts a source value to the appropriate Go type for the DuckDB Appender.
func coerceValue(field types.Field, v any) driver.Value {
	if v == nil {
		return nil
	}
	switch field.Type {
	case types.FieldTypeJSON:
		// Postgres returns JSON/JSONB as []byte; DuckDB appender wants string.
		switch val := v.(type) {
		case []byte:
			return string(val)
		case string:
			return val
		default:
			b, err := json.Marshal(v)
			if err != nil {
				logger.L.Warnf("duckdb: coerceValue JSON marshal failed for field %s: %v", field.Name, err)
				return fmt.Sprintf("%v", v)
			}
			return string(b)
		}
	case types.FieldTypeInt64:
		// Normalize all integer variants to int64.
		switch val := v.(type) {
		case int64:
			return val
		case int32:
			return int64(val)
		case int16:
			return int64(val)
		case int:
			return int64(val)
		default:
			return v
		}
	case types.FieldTypeFloat64:
		switch val := v.(type) {
		case float32:
			return float64(val)
		case float64:
			return val
		default:
			return v
		}
	case types.FieldTypeNumeric:
		// pgtype.Numeric or similar — convert to float64 string if unknown.
		switch val := v.(type) {
		case float64:
			return val
		case float32:
			return float64(val)
		case string:
			return val
		default:
			// Fallback: stringify for unknown numeric types (e.g. pgtype.Numeric).
			return fmt.Sprintf("%v", v)
		}
	default:
		return v
	}
}

// TruncateTable removes all rows from the table.
func (d *Destination) TruncateTable(table string) error {
	_, err := d.db.ExecContext(context.Background(), fmt.Sprintf(`DELETE FROM "%s"`, table))
	if err != nil {
		return fmt.Errorf("duckdb: TruncateTable failed for %s: %w", table, err)
	}
	return nil
}

// SwapTables atomically replaces dest with staging using RENAME inside a transaction.
// Acquires the mutex for the full transaction to satisfy DuckDB's single-writer constraint.
func (d *Destination) SwapTables(staging, dest string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	ctx := context.Background()
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("duckdb: SwapTables begin tx failed: %w", err)
	}
	defer tx.Rollback()

	old := dest + "_rootwit_old"
	steps := []string{
		fmt.Sprintf(`ALTER TABLE "%s" RENAME TO "%s"`, dest, old),
		fmt.Sprintf(`ALTER TABLE "%s" RENAME TO "%s"`, staging, dest),
		fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, old),
	}
	for _, q := range steps {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("duckdb: SwapTables step %q failed: %w", q, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("duckdb: SwapTables commit failed: %w", err)
	}
	return nil
}

// Close cleans up the database connection and connector.
func (d *Destination) Close() error {
	var errs []string
	if d.db != nil {
		if err := d.db.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("db.Close: %v", err))
		}
	}
	if d.connector != nil {
		if err := d.connector.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("connector.Close: %v", err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("duckdb: Close errors: %s", strings.Join(errs, "; "))
	}
	return nil
}
