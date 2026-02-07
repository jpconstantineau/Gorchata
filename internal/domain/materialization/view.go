package materialization

import (
	"fmt"
	"strings"
)

// ViewStrategy implements materialization as a SQL view
type ViewStrategy struct{}

// Materialize generates SQL to create a view
func (v *ViewStrategy) Materialize(modelName string, compiledSQL string, config MaterializationConfig) ([]string, error) {
	// Validate inputs
	if strings.TrimSpace(modelName) == "" {
		return nil, fmt.Errorf("model name cannot be empty")
	}

	if strings.TrimSpace(compiledSQL) == "" {
		return nil, fmt.Errorf("compiled SQL cannot be empty")
	}

	// Generate SQL statements
	statements := make([]string, 0, 2)

	// Drop existing view if it exists
	dropSQL := fmt.Sprintf("DROP VIEW IF EXISTS %s", modelName)
	statements = append(statements, dropSQL)

	// Create the view
	createSQL := fmt.Sprintf("CREATE VIEW %s AS %s", modelName, compiledSQL)
	statements = append(statements, createSQL)

	return statements, nil
}

// Name returns the strategy name
func (v *ViewStrategy) Name() string {
	return "view"
}
