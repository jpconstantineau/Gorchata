package platform

import "context"

// DatabaseAdapter defines the interface for database operations
type DatabaseAdapter interface {
	// Connect establishes a connection to the database
	Connect(ctx context.Context) error

	// Close closes the database connection
	Close() error

	// ExecuteQuery executes a SELECT query and returns results
	ExecuteQuery(ctx context.Context, sql string, args ...interface{}) (*QueryResult, error)

	// ExecuteDDL executes a DDL statement (CREATE, ALTER, DROP)
	ExecuteDDL(ctx context.Context, sql string) error

	// TableExists checks if a table exists in the database
	TableExists(ctx context.Context, table string) (bool, error)

	// GetTableSchema retrieves the schema information for a table
	GetTableSchema(ctx context.Context, table string) (*Schema, error)

	// CreateTableAs creates a new table from a SELECT query
	CreateTableAs(ctx context.Context, table, selectSQL string) error

	// CreateView creates a view from a SELECT query
	CreateView(ctx context.Context, view, selectSQL string) error

	// BeginTransaction starts a new transaction
	BeginTransaction(ctx context.Context) (Transaction, error)
}

// Transaction defines the interface for database transactions
type Transaction interface {
	// Commit commits the transaction
	Commit() error

	// Rollback rolls back the transaction
	Rollback() error

	// Exec executes a statement within the transaction
	Exec(ctx context.Context, sql string, args ...interface{}) error
}
