package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jpconstantineau/gorchata/internal/platform"
	_ "modernc.org/sqlite"
)

// SQLiteAdapter implements the DatabaseAdapter interface for SQLite
type SQLiteAdapter struct {
	config *platform.ConnectionConfig
	db     *sql.DB
}

// NewSQLiteAdapter creates a new SQLite database adapter
func NewSQLiteAdapter(config *platform.ConnectionConfig) *SQLiteAdapter {
	return &SQLiteAdapter{
		config: config,
	}
}

// Connect establishes a connection to the SQLite database
func (a *SQLiteAdapter) Connect(ctx context.Context) error {
	connStr := buildConnectionString(a.config.DatabasePath)

	db, err := sql.Open("sqlite", connStr)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping database: %w", err)
	}

	// Configure connection pool for SQLite
	// SQLite works best with limited connections
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	a.db = db

	// Apply default pragmas
	for _, pragma := range defaultPragmas() {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			db.Close()
			return fmt.Errorf("failed to execute pragma: %w", err)
		}
	}

	return nil
}

// Close closes the database connection
func (a *SQLiteAdapter) Close() error {
	if a.db != nil {
		return a.db.Close()
	}
	return nil
}

// ExecuteQuery executes a SELECT query and returns results
func (a *SQLiteAdapter) ExecuteQuery(ctx context.Context, sql string, args ...interface{}) (*platform.QueryResult, error) {
	rows, err := a.db.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Prepare result
	result := &platform.QueryResult{
		Columns: columns,
		Rows:    make([][]interface{}, 0),
	}

	// Scan rows
	for rows.Next() {
		// Create a slice of interface{} for scanning
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		result.Rows = append(result.Rows, values)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	result.RowsAffected = int64(len(result.Rows))

	return result, nil
}

// ExecuteDDL executes a DDL statement (CREATE, ALTER, DROP, INSERT, UPDATE, DELETE)
func (a *SQLiteAdapter) ExecuteDDL(ctx context.Context, sql string) error {
	_, err := a.db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to execute DDL: %w", err)
	}
	return nil
}

// TableExists checks if a table exists in the database
func (a *SQLiteAdapter) TableExists(ctx context.Context, table string) (bool, error) {
	query := "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
	var name string
	err := a.db.QueryRowContext(ctx, query, table).Scan(&name)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to check table existence: %w", err)
	}
	return true, nil
}

// GetTableSchema retrieves the schema information for a table
func (a *SQLiteAdapter) GetTableSchema(ctx context.Context, table string) (*platform.Schema, error) {
	// First check if table exists
	exists, err := a.TableExists(ctx, table)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("table %q does not exist", table)
	}

	// Query table info
	query := fmt.Sprintf("PRAGMA table_info(%s)", table)
	rows, err := a.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %w", err)
	}
	defer rows.Close()

	schema := &platform.Schema{
		TableName: table,
		Columns:   make([]platform.Column, 0),
	}

	for rows.Next() {
		var cid int
		var name, colType string
		var notNull, pk int
		var dfltValue sql.NullString

		if err := rows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}

		column := platform.Column{
			Name:       name,
			Type:       colType,
			Nullable:   notNull == 0,
			PrimaryKey: pk > 0,
		}
		schema.Columns = append(schema.Columns, column)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating column info: %w", err)
	}

	return schema, nil
}

// CreateTableAs creates a new table from a SELECT query
func (a *SQLiteAdapter) CreateTableAs(ctx context.Context, table, selectSQL string) error {
	sql := fmt.Sprintf("CREATE TABLE %s AS %s", table, selectSQL)
	return a.ExecuteDDL(ctx, sql)
}

// CreateView creates a view from a SELECT query
func (a *SQLiteAdapter) CreateView(ctx context.Context, view, selectSQL string) error {
	sql := fmt.Sprintf("CREATE VIEW %s AS %s", view, selectSQL)
	return a.ExecuteDDL(ctx, sql)
}

// BeginTransaction starts a new transaction
func (a *SQLiteAdapter) BeginTransaction(ctx context.Context) (platform.Transaction, error) {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	return &sqliteTransaction{tx: tx}, nil
}
