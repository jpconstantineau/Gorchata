package materialization

import (
	"fmt"
	"strings"
)

// TableStrategy implements materialization as a full-refresh table
type TableStrategy struct{}

// Materialize generates SQL to create a table with full refresh
func (t *TableStrategy) Materialize(modelName string, compiledSQL string, config MaterializationConfig) ([]string, error) {
	// Validate inputs
	if strings.TrimSpace(modelName) == "" {
		return nil, fmt.Errorf("model name cannot be empty")
	}

	if strings.TrimSpace(compiledSQL) == "" {
		return nil, fmt.Errorf("compiled SQL cannot be empty")
	}

	// Generate SQL statements for full refresh
	statements := make([]string, 0, 2)

	// Drop existing table if it exists
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", modelName)
	statements = append(statements, dropSQL)

	// Create new table from SELECT query
	createSQL := fmt.Sprintf("CREATE TABLE %s AS %s", modelName, compiledSQL)
	statements = append(statements, createSQL)

	return statements, nil
}

// Name returns the strategy name
func (t *TableStrategy) Name() string {
	return "table"
}
