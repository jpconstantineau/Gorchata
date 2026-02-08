package generic

import (
	"fmt"
)

// NotNullTest checks that a column contains no NULL values
type NotNullTest struct{}

// Name returns the test identifier
func (t *NotNullTest) Name() string {
	return "not_null"
}

// Validate checks if the test arguments are valid
func (t *NotNullTest) Validate(model, column string, args map[string]interface{}) error {
	return ValidateModelColumn(model, column)
}

// GenerateSQL generates SQL that returns rows where the column is NULL
func (t *NotNullTest) GenerateSQL(model, column string, args map[string]interface{}) (string, error) {
	if err := t.Validate(model, column, args); err != nil {
		return "", err
	}

	whereClause := BuildWhereClause(args)

	sql := fmt.Sprintf(
		"SELECT * FROM %s WHERE %s IS NULL%s",
		model,
		column,
		whereClause,
	)

	return sql, nil
}
