package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rootwit/rootwit/config"
	"github.com/rootwit/rootwit/logger"
	"github.com/rootwit/rootwit/types"
)

// PostgresSource implements sources.SourceConnector for Postgres databases.
type PostgresSource struct {
	cfg  config.SourceConfig
	pool *pgxpool.Pool
}

// NewPostgresSource creates a new PostgresSource from the given config.
func NewPostgresSource(cfg config.SourceConfig) *PostgresSource {
	return &PostgresSource{cfg: cfg}
}

// Connect establishes a connection pool to the Postgres database.
func (p *PostgresSource) Connect() error {
	connStr := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s connect_timeout=%d pool_max_conns=%d",
		p.cfg.Host,
		p.cfg.Port,
		p.cfg.Database,
		p.cfg.Username,
		p.cfg.Password,
		p.cfg.SSLMode,
		p.cfg.ConnectionTimeoutSeconds,
		p.cfg.MaxConnections,
	)

	pool, err := pgxpool.New(context.Background(), connStr)
	if err != nil {
		return fmt.Errorf("postgres: failed to connect: %w", err)
	}

	p.pool = pool
	return nil
}

// GetTables returns all user tables in the public schema.
func (p *PostgresSource) GetTables() ([]string, error) {
	query := `
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = 'public' 
		  AND table_type = 'BASE TABLE'
		ORDER BY table_name`

	rows, err := p.pool.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("postgres: failed to query tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("postgres: failed to scan table name: %w", err)
		}
		tables = append(tables, name)
	}

	return tables, rows.Err()
}

// GetSchema returns the current schema for a table, fetched live from Postgres.
func (p *PostgresSource) GetSchema(table string) (types.Schema, error) {
	// Query column info with OIDs from pg_attribute + pg_type for accurate type mapping.
	query := `
		SELECT 
			a.attname AS column_name,
			t.oid AS type_oid,
			a.attnotnull AS not_null
		FROM pg_attribute a
		JOIN pg_class c ON a.attrelid = c.oid
		JOIN pg_namespace n ON c.relnamespace = n.oid
		JOIN pg_type t ON a.atttypid = t.oid
		WHERE n.nspname = 'public'
		  AND c.relname = $1
		  AND a.attnum > 0
		  AND NOT a.attisdropped
		ORDER BY a.attnum`

	rows, err := p.pool.Query(context.Background(), query, table)
	if err != nil {
		return types.Schema{}, fmt.Errorf("postgres: failed to query schema for %s: %w", table, err)
	}
	defer rows.Close()

	schema := types.Schema{TableName: table}
	for rows.Next() {
		var colName string
		var typeOID uint32
		var notNull bool

		if err := rows.Scan(&colName, &typeOID, &notNull); err != nil {
			return types.Schema{}, fmt.Errorf("postgres: failed to scan column: %w", err)
		}

		fieldType, itemType, known := MapOIDToFieldType(typeOID)
		if !known {
			// Never crash on unknown type — map to STRING and warn.
			logger.L.Warnw("unknown Postgres OID, mapping to STRING", "oid", typeOID, "table", table, "column", colName)
		}

		field := types.Field{
			Name:     colName,
			Type:     fieldType,
			Nullable: !notNull,
			ItemType: itemType,
		}
		schema.Fields = append(schema.Fields, field)
	}

	return schema, rows.Err()
}

// ReadIncremental streams rows where cursorField > cursorValue, ordered by
// cursorField. Returns a row channel and a one-shot error channel.
// Read rows until the row channel is closed, then check the error channel.
func (p *PostgresSource) ReadIncremental(ctx context.Context, table string, cursorField string, cursorValue any, batchSize int) (<-chan types.Row, <-chan error) {
	rowCh := make(chan types.Row, batchSize)
	errCh := make(chan error, 1)

	go func() {
		defer close(rowCh)
		defer close(errCh)

		// Sanitize table and cursor field names to prevent SQL injection.
		// pgx uses parameterized queries for values, but identifiers need quoting.
		safeTable := pgx.Identifier{table}.Sanitize()
		safeCursor := pgx.Identifier{cursorField}.Sanitize()

		var query string
		var args []any

		if cursorValue == nil {
			// First run — no cursor yet, sync everything ordered by cursor field.
			query = fmt.Sprintf("SELECT * FROM %s ORDER BY %s", safeTable, safeCursor)
		} else {
			// Subsequent runs — sync only new/updated rows.
			query = fmt.Sprintf("SELECT * FROM %s WHERE %s > $1 ORDER BY %s", safeTable, safeCursor, safeCursor)
			args = append(args, cursorValue)
		}

		rows, err := p.pool.Query(ctx, query, args...)
		if err != nil {
			errCh <- fmt.Errorf("postgres: ReadIncremental query failed: %w", err)
			return
		}
		defer rows.Close()

		if err := p.streamRows(ctx, rows, rowCh); err != nil {
			errCh <- err
		}
	}()

	return rowCh, errCh
}

// ReadFull streams all rows from the table. Same channel pattern as ReadIncremental.
func (p *PostgresSource) ReadFull(ctx context.Context, table string, batchSize int) (<-chan types.Row, <-chan error) {
	rowCh := make(chan types.Row, batchSize)
	errCh := make(chan error, 1)

	go func() {
		defer close(rowCh)
		defer close(errCh)

		safeTable := pgx.Identifier{table}.Sanitize()

		// Use ctid ordering for consistent full scans.
		query := fmt.Sprintf("SELECT * FROM %s ORDER BY ctid", safeTable)

		rows, err := p.pool.Query(ctx, query)
		if err != nil {
			errCh <- fmt.Errorf("postgres: ReadFull query failed: %w", err)
			return
		}
		defer rows.Close()

		if err := p.streamRows(ctx, rows, rowCh); err != nil {
			errCh <- err
		}
	}()

	return rowCh, errCh
}

// streamRows reads all rows from a pgx.Rows result set and sends them to the
// row channel as types.Row maps. Checks ctx between rows so a timeout cancels
// mid-stream rather than blocking until the query finishes naturally.
func (p *PostgresSource) streamRows(ctx context.Context, rows pgx.Rows, rowCh chan<- types.Row) error {
	fieldDescs := rows.FieldDescriptions()

	for rows.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		values, err := rows.Values()
		if err != nil {
			return fmt.Errorf("postgres: failed to scan row values: %w", err)
		}

		row := make(types.Row, len(fieldDescs))
		for i, fd := range fieldDescs {
			row[fd.Name] = values[i]
		}
		rowCh <- row
	}

	return rows.Err()
}

// Close closes the Postgres connection pool.
func (p *PostgresSource) Close() error {
	if p.pool != nil {
		p.pool.Close()
	}
	return nil
}

// IsConnectionError checks if an error is a Postgres connection-level error
// (useful for retry classification).
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if ok := isPgError(err, &pgErr); ok {
		// Connection exception class = "08"
		return strings.HasPrefix(pgErr.Code, "08")
	}
	return false
}

// isPgError attempts to unwrap a *pgconn.PgError from the error chain.
func isPgError(err error, target **pgconn.PgError) bool {
	for err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			*target = pgErr
			return true
		}
		if unwrapper, ok := err.(interface{ Unwrap() error }); ok {
			err = unwrapper.Unwrap()
		} else {
			return false
		}
	}
	return false
}
