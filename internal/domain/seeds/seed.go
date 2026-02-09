package seeds

// SeedType represents the type of seed data source
type SeedType string

const (
	// SeedTypeCSV represents a CSV file seed
	SeedTypeCSV SeedType = "csv"
	// SeedTypeSQL represents a SQL file seed
	SeedTypeSQL SeedType = "sql"
)

// SeedColumn represents a single column in a seed schema
type SeedColumn struct {
	Name string // Column name
	Type string // SQL type (e.g., "INTEGER", "TEXT", "TIMESTAMP")
}

// SeedSchema represents the schema definition for a seed
type SeedSchema struct {
	Columns []SeedColumn // List of columns
}

// Seed represents a data seed with its metadata and configuration
type Seed struct {
	ID                string                 // Unique identifier for the seed
	Path              string                 // File path to the seed data
	Type              SeedType               // Type of seed (CSV, SQL, etc.)
	Schema            *SeedSchema            // Optional schema definition
	Config            map[string]interface{} // Optional configuration for future use
	ResolvedTableName string                 // Fully qualified table name (e.g., "schema.table_name")
}
