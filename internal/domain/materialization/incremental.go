package materialization

import (
	"fmt"
	"strings"
)

// IncrementalStrategy implements materialization with incremental updates
type IncrementalStrategy struct{}

// Materialize generates SQL for incremental table updates using merge logic
func (i *IncrementalStrategy) Materialize(modelName string, compiledSQL string, config MaterializationConfig) ([]string, error) {
	// Validate inputs
	if strings.TrimSpace(modelName) == "" {
		return nil, fmt.Errorf("model name cannot be empty")
	}

	if strings.TrimSpace(compiledSQL) == "" {
		return nil, fmt.Errorf("compiled SQL cannot be empty")
	}

	if len(config.UniqueKey) == 0 {
		return nil, fmt.Errorf("unique key is required for incremental materialization")
	}

	// If full refresh is requested, use simple drop and create
	if config.FullRefresh {
		return i.fullRefresh(modelName, compiledSQL)
	}

	// Otherwise, use incremental merge logic
	return i.incrementalMerge(modelName, compiledSQL, config.UniqueKey)
}

// fullRefresh performs a full table refresh (drop and recreate)
func (i *IncrementalStrategy) fullRefresh(modelName string, compiledSQL string) ([]string, error) {
	statements := make([]string, 0, 2)

	// Drop existing table
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", modelName)
	statements = append(statements, dropSQL)

	// Create new table from SELECT
	createSQL := fmt.Sprintf("CREATE TABLE %s AS %s", modelName, compiledSQL)
	statements = append(statements, createSQL)

	return statements, nil
}

// incrementalMerge performs incremental updates using temp table and merge logic
func (i *IncrementalStrategy) incrementalMerge(modelName string, compiledSQL string, uniqueKey []string) ([]string, error) {
	statements := make([]string, 0, 5)
	tempTableName := modelName + "__tmp"

	// Step 1: Create temporary table with new data
	createTempSQL := fmt.Sprintf("CREATE TEMP TABLE %s AS %s", tempTableName, compiledSQL)
	statements = append(statements, createTempSQL)

	// Step 2: Create target table if it doesn't exist (first run scenario)
	createTargetSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM %s WHERE 1=0", modelName, tempTableName)
	statements = append(statements, createTargetSQL)

	// Step 3: Delete rows from target that match keys in temp table (update scenario)
	whereClause := i.buildWhereClause(modelName, tempTableName, uniqueKey)
	deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE EXISTS (SELECT 1 FROM %s WHERE %s)",
		modelName, tempTableName, whereClause)
	statements = append(statements, deleteSQL)

	// Step 4: Insert all rows from temp table (includes new and updated)
	insertSQL := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", modelName, tempTableName)
	statements = append(statements, insertSQL)

	// Step 5: Drop temporary table
	dropTempSQL := fmt.Sprintf("DROP TABLE %s", tempTableName)
	statements = append(statements, dropTempSQL)

	return statements, nil
}

// buildWhereClause creates a WHERE clause for matching unique keys
func (i *IncrementalStrategy) buildWhereClause(targetTable, tempTable string, uniqueKey []string) string {
	conditions := make([]string, 0, len(uniqueKey))

	for _, key := range uniqueKey {
		condition := fmt.Sprintf("%s.%s = %s.%s", targetTable, key, tempTable, key)
		conditions = append(conditions, condition)
	}

	return strings.Join(conditions, " AND ")
}

// Name returns the strategy name
func (i *IncrementalStrategy) Name() string {
	return "incremental"
}
