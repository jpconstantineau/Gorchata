package generic

import (
	"fmt"
	"strings"
)

// UniqueTest checks that a column contains only unique values
type UniqueTest struct{}

// Name returns the test identifier
func (t *UniqueTest) Name() string {
	return "unique"
}

// Validate checks if the test arguments are valid
func (t *UniqueTest) Validate(model, column string, args map[string]interface{}) error {
	return ValidateModelColumn(model, column)
}

// GenerateSQL generates SQL that returns duplicate values
func (t *UniqueTest) GenerateSQL(model, column string, args map[string]interface{}) (string, error) {
	if err := t.Validate(model, column, args); err != nil {
		return "", err
	}

	whereClause := BuildWhereClause(args)

	var sqlBuilder strings.Builder
	sqlBuilder.WriteString(fmt.Sprintf("SELECT %s, COUNT(*) as duplicate_count\n", column))
	sqlBuilder.WriteString(fmt.Sprintf("FROM %s\n", model))

	if whereClause != "" {
		// Remove leading " AND " from whereClause and use WHERE
		cleanWhere := strings.TrimPrefix(whereClause, " AND ")
		sqlBuilder.WriteString(fmt.Sprintf("WHERE %s\n", cleanWhere))
	}

	sqlBuilder.WriteString(fmt.Sprintf("GROUP BY %s\n", column))
	sqlBuilder.WriteString("HAVING COUNT(*) > 1")

	return sqlBuilder.String(), nil
}
