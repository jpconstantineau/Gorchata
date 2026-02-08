package generic

import (
	"fmt"
)

// GenericTest defines the interface for all generic data quality tests
type GenericTest interface {
	// Name returns the unique identifier for this test type
	Name() string
	
	// GenerateSQL generates the SQL query for this test
	// Returns SQL that selects failing rows (0 rows = test passes)
	GenerateSQL(model, column string, args map[string]interface{}) (string, error)
	
	// Validate checks if the test arguments are valid
	Validate(model, column string, args map[string]interface{}) error
}

// BuildWhereClause constructs an optional WHERE clause from args
func BuildWhereClause(args map[string]interface{}) string {
	if args == nil {
		return ""
	}
	
	whereStr, ok := args["where"]
	if !ok {
		return ""
	}
	
	whereClause, ok := whereStr.(string)
	if !ok || whereClause == "" {
		return ""
	}
	
	return fmt.Sprintf(" AND (%s)", whereClause)
}

// ValidateRequired checks if all required arguments are present and non-empty
func ValidateRequired(args map[string]interface{}, required []string) error {
	if len(required) == 0 {
		return nil
	}
	
	if args == nil {
		return fmt.Errorf("missing required arguments: %v", required)
	}
	
	for _, field := range required {
		val, ok := args[field]
		if !ok {
			return fmt.Errorf("missing required argument: %s", field)
		}
		
		// Check for empty strings
		if str, ok := val.(string); ok && str == "" {
			return fmt.Errorf("required argument %s cannot be empty", field)
		}
	}
	
	return nil
}

// ValidateModelColumn checks if model and column names are non-empty
func ValidateModelColumn(model, column string) error {
	if model == "" {
		return fmt.Errorf("model name cannot be empty")
	}
	if column == "" {
		return fmt.Errorf("column name cannot be empty")
	}
	return nil
}
