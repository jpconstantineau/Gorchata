package generic

import (
	"fmt"
	"strings"
)

// AcceptedValuesTest checks that a column contains only values from an allowed list
type AcceptedValuesTest struct{}

// Name returns the test identifier
func (t *AcceptedValuesTest) Name() string {
	return "accepted_values"
}

// Validate checks if the test arguments are valid
func (t *AcceptedValuesTest) Validate(model, column string, args map[string]interface{}) error {
	if err := ValidateModelColumn(model, column); err != nil {
		return err
	}
	
	if err := ValidateRequired(args, []string{"values"}); err != nil {
		return err
	}
	
	// Validate that values is actually an array
	values, ok := args["values"]
	if !ok {
		return fmt.Errorf("missing required argument: values")
	}
	
	valuesSlice, ok := values.([]interface{})
	if !ok {
		return fmt.Errorf("values must be an array")
	}
	
	if len(valuesSlice) == 0 {
		return fmt.Errorf("values array cannot be empty")
	}
	
	return nil
}

// GenerateSQL generates SQL that returns rows with values not in the accepted list
func (t *AcceptedValuesTest) GenerateSQL(model, column string, args map[string]interface{}) (string, error) {
	if err := t.Validate(model, column, args); err != nil {
		return "", err
	}
	
	valuesSlice := args["values"].([]interface{})
	
	// Build the IN clause
	var valueStrings []string
	for _, v := range valuesSlice {
		switch val := v.(type) {
		case string:
			valueStrings = append(valueStrings, fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "''")))
		case int, int64, float64, bool:
			valueStrings = append(valueStrings, fmt.Sprintf("%v", val))
		default:
			valueStrings = append(valueStrings, fmt.Sprintf("'%v'", val))
		}
	}
	
	inClause := strings.Join(valueStrings, ", ")
	whereClause := BuildWhereClause(args)
	
	sql := fmt.Sprintf(
		"SELECT * FROM %s WHERE %s NOT IN (%s) AND %s IS NOT NULL%s",
		model,
		column,
		inClause,
		column,
		whereClause,
	)
	
	return sql, nil
}
