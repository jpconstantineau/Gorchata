package generic

import (
	"fmt"
)

// AcceptedRangeTest checks that numeric values fall within a specified range
type AcceptedRangeTest struct{}

// Name returns the test identifier
func (t *AcceptedRangeTest) Name() string {
	return "accepted_range"
}

// Validate checks if the test arguments are valid
func (t *AcceptedRangeTest) Validate(model, column string, args map[string]interface{}) error {
	if err := ValidateModelColumn(model, column); err != nil {
		return err
	}
	
	return ValidateRequired(args, []string{"min_value", "max_value"})
}

// GenerateSQL generates SQL that returns rows outside the accepted range
func (t *AcceptedRangeTest) GenerateSQL(model, column string, args map[string]interface{}) (string, error) {
	if err := t.Validate(model, column, args); err != nil {
		return "", err
	}
	
	minValue := args["min_value"]
	maxValue := args["max_value"]
	whereClause := BuildWhereClause(args)
	
	sql := fmt.Sprintf(
		"SELECT * FROM %s WHERE %s NOT BETWEEN %v AND %v%s",
		model,
		column,
		minValue,
		maxValue,
		whereClause,
	)
	
	return sql, nil
}
