package platform

// ConnectionConfig holds database connection configuration
type ConnectionConfig struct {
	DatabasePath string
	Options      map[string]string
}

// QueryResult represents the result of a database query
type QueryResult struct {
	Columns      []string
	Rows         [][]interface{}
	RowsAffected int64
}

// Schema represents the structure of a database table
type Schema struct {
	TableName string
	Columns   []Column
}

// Column represents a single column in a database table
type Column struct {
	Name       string
	Type       string
	Nullable   bool
	PrimaryKey bool
}
