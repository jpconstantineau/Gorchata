package generic

import (
	"fmt"
	"strings"
)

// UniqueCombinationTest checks that a combination of columns is unique
type UniqueCombinationTest struct{}

// Name returns the test identifier
func (t *UniqueCombinationTest) Name() string {
	return "unique_combination_of_columns"
}

// Validate checks if the test arguments are valid
func (t *UniqueCombinationTest) Validate(model, column string, args map[string]interface{}) error {
	if model == "" {
		return fmt.Errorf("model name cannot be empty")
	}

	if err := ValidateRequired(args, []string{"columns"}); err != nil {
		return err
	}

	// Validate that columns is actually an array
	columns, ok := args["columns"]
	if !ok {
		return fmt.Errorf("missing required argument: columns")
	}

	columnsSlice, ok := columns.([]interface{})
	if !ok {
		return fmt.Errorf("columns must be an array")
	}

	if len(columnsSlice) == 0 {
		return fmt.Errorf("columns array cannot be empty")
	}

	if len(columnsSlice) < 2 {
		return fmt.Errorf("columns array must contain at least 2 columns (use unique test for single column)")
	}

	return nil
}

// GenerateSQL generates SQL that returns duplicate combinations
func (t *UniqueCombinationTest) GenerateSQL(model, column string, args map[string]interface{}) (string, error) {
	if err := t.Validate(model, column, args); err != nil {
		return "", err
	}

	columnsSlice := args["columns"].([]interface{})

	// Build column list
	var columnNames []string
	for _, col := range columnsSlice {
		columnNames = append(columnNames, fmt.Sprintf("%v", col))
	}

	columnList := strings.Join(columnNames, ", ")
	whereClause := BuildWhereClause(args)

	var sqlBuilder strings.Builder
	sqlBuilder.WriteString(fmt.Sprintf("SELECT %s, COUNT(*) as duplicate_count\n", columnList))
	sqlBuilder.WriteString(fmt.Sprintf("FROM %s\n", model))

	if whereClause != "" {
		// Remove leading " AND " from whereClause and use WHERE
		cleanWhere := strings.TrimPrefix(whereClause, " AND ")
		sqlBuilder.WriteString(fmt.Sprintf("WHERE %s\n", cleanWhere))
	}

	sqlBuilder.WriteString(fmt.Sprintf("GROUP BY %s\n", columnList))
	sqlBuilder.WriteString("HAVING COUNT(*) > 1")

	return sqlBuilder.String(), nil
}
