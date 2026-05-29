package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"

	"github.com/rootwit/rootwit/config"
	"github.com/rootwit/rootwit/types"
)

// SQLiteSource implements sources.SourceConnector for SQLite databases.
type SQLiteSource struct {
	cfg  config.SourceConfig
	db   *sql.DB
}

// NewSQLiteSource creates a new SQLiteSource from the given config.
func NewSQLiteSource(cfg config.SourceConfig) *SQLiteSource {
	return &SQLiteSource{cfg: cfg}
}

// Connect opens the SQLite database file.
func (s *SQLiteSource) Connect() error {
	db, err := sql.Open("sqlite", s.cfg.SQLite.Path)
	if err != nil {
		return fmt.Errorf("sqlite: failed to open %s: %w", s.cfg.SQLite.Path, err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		db.Close()
		return fmt.Errorf("sqlite: ping failed: %w", err)
	}
	s.db = db
	return nil
}

// GetTables returns all user-defined tables in the database.
func (s *SQLiteSource) GetTables() ([]string, error) {
	query := `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`
	rows, err := s.db.QueryContext(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("sqlite: GetTables failed: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("sqlite: GetTables scan failed: %w", err)
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

// GetSchema returns the schema of a table using PRAGMA table_info.
func (s *SQLiteSource) GetSchema(table string) (types.Schema, error) {
	// PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
	query := fmt.Sprintf(`PRAGMA table_info("%s")`, strings.ReplaceAll(table, `"`, `""`))
	rows, err := s.db.QueryContext(context.Background(), query)
	if err != nil {
		return types.Schema{}, fmt.Errorf("sqlite: GetSchema PRAGMA failed for %s: %w", table, err)
	}
	defer rows.Close()

	schema := types.Schema{TableName: table}
	for rows.Next() {
		var cid int
		var name, declaredType string
		var notNull int
		var dfltValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &declaredType, &notNull, &dfltValue, &pk); err != nil {
			return types.Schema{}, fmt.Errorf("sqlite: GetSchema scan failed: %w", err)
		}
		schema.Fields = append(schema.Fields, types.Field{
			Name:     name,
			Type:     fromSQLiteType(declaredType),
			Nullable: notNull == 0 && pk == 0,
		})
	}
	return schema, rows.Err()
}

// ReadIncremental streams rows where cursorField > cursorValue, ordered by cursorField.
func (s *SQLiteSource) ReadIncremental(ctx context.Context, table string, cursorField string, cursorValue any, batchSize int) (<-chan types.Row, <-chan error) {
	rowCh := make(chan types.Row, batchSize)
	errCh := make(chan error, 1)

	go func() {
		defer close(rowCh)
		defer close(errCh)

		safeTable := `"` + strings.ReplaceAll(table, `"`, `""`) + `"`
		safeCursor := `"` + strings.ReplaceAll(cursorField, `"`, `""`) + `"`

		var (
			query string
			args  []any
		)
		if cursorValue == nil {
			query = fmt.Sprintf("SELECT * FROM %s ORDER BY %s", safeTable, safeCursor)
		} else {
			query = fmt.Sprintf("SELECT * FROM %s WHERE %s > ? ORDER BY %s", safeTable, safeCursor, safeCursor)
			args = append(args, cursorValue)
		}

		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			errCh <- fmt.Errorf("sqlite: ReadIncremental query failed: %w", err)
			return
		}
		defer rows.Close()

		if err := s.streamRows(ctx, rows, rowCh); err != nil {
			errCh <- err
		}
	}()

	return rowCh, errCh
}

// ReadFull streams all rows from the table, ordered by rowid.
func (s *SQLiteSource) ReadFull(ctx context.Context, table string, batchSize int) (<-chan types.Row, <-chan error) {
	rowCh := make(chan types.Row, batchSize)
	errCh := make(chan error, 1)

	go func() {
		defer close(rowCh)
		defer close(errCh)

		safeTable := `"` + strings.ReplaceAll(table, `"`, `""`) + `"`
		query := fmt.Sprintf("SELECT * FROM %s ORDER BY rowid", safeTable)

		rows, err := s.db.QueryContext(ctx, query)
		if err != nil {
			errCh <- fmt.Errorf("sqlite: ReadFull query failed: %w", err)
			return
		}
		defer rows.Close()

		if err := s.streamRows(ctx, rows, rowCh); err != nil {
			errCh <- err
		}
	}()

	return rowCh, errCh
}

// streamRows reads columns from a sql.Rows result and sends them as types.Row maps.
func (s *SQLiteSource) streamRows(ctx context.Context, rows *sql.Rows, rowCh chan<- types.Row) error {
	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("sqlite: streamRows columns failed: %w", err)
	}

	for rows.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return fmt.Errorf("sqlite: streamRows scan failed: %w", err)
		}

		row := make(types.Row, len(cols))
		for i, col := range cols {
			row[col] = vals[i]
		}
		rowCh <- row
	}
	return rows.Err()
}

// Close closes the SQLite database connection.
func (s *SQLiteSource) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
