package generic

import (
	"fmt"
)

// NotEmptyStringTest checks that string columns are not empty (after trimming whitespace)
type NotEmptyStringTest struct{}

// Name returns the test identifier
func (t *NotEmptyStringTest) Name() string {
	return "not_empty_string"
}

// Validate checks if the test arguments are valid
func (t *NotEmptyStringTest) Validate(model, column string, args map[string]interface{}) error {
	return ValidateModelColumn(model, column)
}

// GenerateSQL generates SQL that returns rows with empty strings
func (t *NotEmptyStringTest) GenerateSQL(model, column string, args map[string]interface{}) (string, error) {
	if err := t.Validate(model, column, args); err != nil {
		return "", err
	}

	whereClause := BuildWhereClause(args)

	sql := fmt.Sprintf(
		"SELECT * FROM %s WHERE %s IS NOT NULL AND TRIM(%s) = ''%s",
		model,
		column,
		column,
		whereClause,
	)

	return sql, nil
}
