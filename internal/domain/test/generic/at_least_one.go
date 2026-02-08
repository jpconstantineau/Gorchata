package generic

import (
	"fmt"
	"strings"
)

// AtLeastOneTest checks that at least one non-NULL value exists in a column
type AtLeastOneTest struct{}

// Name returns the test identifier
func (t *AtLeastOneTest) Name() string {
	return "at_least_one"
}

// Validate checks if the test arguments are valid
func (t *AtLeastOneTest) Validate(model, column string, args map[string]interface{}) error {
	return ValidateModelColumn(model, column)
}

// GenerateSQL generates SQL that returns a row if no non-NULL values exist
func (t *AtLeastOneTest) GenerateSQL(model, column string, args map[string]interface{}) (string, error) {
	if err := t.Validate(model, column, args); err != nil {
		return "", err
	}

	whereClause := BuildWhereClause(args)

	var sqlBuilder strings.Builder
	sqlBuilder.WriteString("SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END as failure\n")
	sqlBuilder.WriteString(fmt.Sprintf("FROM %s\n", model))
	sqlBuilder.WriteString(fmt.Sprintf("WHERE %s IS NOT NULL", column))

	if whereClause != "" {
		sqlBuilder.WriteString(whereClause)
	}

	return sqlBuilder.String(), nil
}
