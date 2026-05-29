package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/rootwit/rootwit/config"
	"github.com/rootwit/rootwit/types"
)

// MySQLSource implements sources.SourceConnector for MySQL databases.
type MySQLSource struct {
	cfg config.SourceConfig
	db  *sql.DB
}

// NewMySQLSource creates a new MySQLSource from the given config.
func NewMySQLSource(cfg config.SourceConfig) *MySQLSource {
	return &MySQLSource{cfg: cfg}
}

// Connect opens a connection pool to the MySQL database.
func (m *MySQLSource) Connect() error {
	cfg := mysql.NewConfig()
	cfg.User = m.cfg.Username
	cfg.Passwd = m.cfg.Password
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.Port)
	cfg.DBName = m.cfg.Database
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	cfg.Timeout = time.Duration(m.cfg.ConnectionTimeoutSeconds) * time.Second
	cfg.ReadTimeout = 0  // no read timeout — long queries allowed
	cfg.WriteTimeout = 0

	switch m.cfg.SSLMode {
	case "require", "verify-full":
		cfg.TLSConfig = "true"
	default:
		cfg.TLSConfig = "false"
	}

	dsn := cfg.FormatDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("mysql: failed to open connection: %w", err)
	}

	db.SetMaxOpenConns(m.cfg.MaxConnections)
	db.SetMaxIdleConns(m.cfg.MaxConnections)
	db.SetConnMaxLifetime(10 * time.Minute)

	if err := db.PingContext(context.Background()); err != nil {
		db.Close()
		return fmt.Errorf("mysql: ping failed: %w", err)
	}

	m.db = db
	return nil
}

// GetTables returns all base tables in the configured database.
func (m *MySQLSource) GetTables() ([]string, error) {
	query := `
		SELECT TABLE_NAME
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'
		ORDER BY TABLE_NAME`

	rows, err := m.db.QueryContext(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("mysql: GetTables failed: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("mysql: GetTables scan failed: %w", err)
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

// GetSchema returns the schema of a table by querying information_schema.
func (m *MySQLSource) GetSchema(table string) (types.Schema, error) {
	query := `
		SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION`

	rows, err := m.db.QueryContext(context.Background(), query, table)
	if err != nil {
		return types.Schema{}, fmt.Errorf("mysql: GetSchema failed for %s: %w", table, err)
	}
	defer rows.Close()

	schema := types.Schema{TableName: table}
	for rows.Next() {
		var colName, dataType, columnType, isNullable string
		if err := rows.Scan(&colName, &dataType, &columnType, &isNullable); err != nil {
			return types.Schema{}, fmt.Errorf("mysql: GetSchema scan failed: %w", err)
		}
		schema.Fields = append(schema.Fields, types.Field{
			Name:     colName,
			Type:     fromMySQLType(dataType, columnType),
			Nullable: strings.EqualFold(isNullable, "YES"),
		})
	}
	return schema, rows.Err()
}

// ReadIncremental streams rows where cursorField > cursorValue, ordered by cursorField.
func (m *MySQLSource) ReadIncremental(ctx context.Context, table string, cursorField string, cursorValue any, batchSize int) (<-chan types.Row, <-chan error) {
	rowCh := make(chan types.Row, batchSize)
	errCh := make(chan error, 1)

	go func() {
		defer close(rowCh)
		defer close(errCh)

		safeTable := "`" + strings.ReplaceAll(table, "`", "``") + "`"
		safeCursor := "`" + strings.ReplaceAll(cursorField, "`", "``") + "`"

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

		rows, err := m.db.QueryContext(ctx, query, args...)
		if err != nil {
			errCh <- fmt.Errorf("mysql: ReadIncremental query failed: %w", err)
			return
		}
		defer rows.Close()

		if err := m.streamRows(ctx, rows, rowCh); err != nil {
			errCh <- err
		}
	}()

	return rowCh, errCh
}

// ReadFull streams all rows from the table.
func (m *MySQLSource) ReadFull(ctx context.Context, table string, batchSize int) (<-chan types.Row, <-chan error) {
	rowCh := make(chan types.Row, batchSize)
	errCh := make(chan error, 1)

	go func() {
		defer close(rowCh)
		defer close(errCh)

		safeTable := "`" + strings.ReplaceAll(table, "`", "``") + "`"
		query := fmt.Sprintf("SELECT * FROM %s", safeTable)

		rows, err := m.db.QueryContext(ctx, query)
		if err != nil {
			errCh <- fmt.Errorf("mysql: ReadFull query failed: %w", err)
			return
		}
		defer rows.Close()

		if err := m.streamRows(ctx, rows, rowCh); err != nil {
			errCh <- err
		}
	}()

	return rowCh, errCh
}

// streamRows reads from sql.Rows and sends each row as a types.Row map.
func (m *MySQLSource) streamRows(ctx context.Context, rows *sql.Rows, rowCh chan<- types.Row) error {
	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("mysql: streamRows columns failed: %w", err)
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
			return fmt.Errorf("mysql: streamRows scan failed: %w", err)
		}

		row := make(types.Row, len(cols))
		for i, col := range cols {
			// go-sql-driver returns []byte for string columns when not using NullString.
			// Convert []byte → string so downstream sees consistent Go string values.
			if b, ok := vals[i].([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = vals[i]
			}
		}
		rowCh <- row
	}
	return rows.Err()
}

// Close closes the MySQL connection pool.
func (m *MySQLSource) Close() error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}
