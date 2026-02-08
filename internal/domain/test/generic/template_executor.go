package generic

import (
	"fmt"
	"strings"
)

// TemplateTest represents a custom SQL template-based generic test
type TemplateTest struct {
	testName    string
	params      []string
	sqlTemplate string
}

// NewTemplateTest creates a new TemplateTest instance
func NewTemplateTest(testName string, params []string, sqlTemplate string) *TemplateTest {
	return &TemplateTest{
		testName:    testName,
		params:      params,
		sqlTemplate: sqlTemplate,
	}
}

// Name returns the test identifier (implements GenericTest interface)
func (t *TemplateTest) Name() string {
	return t.testName
}

// Validate checks if the test arguments are valid (implements GenericTest interface)
func (t *TemplateTest) Validate(model, column string, args map[string]interface{}) error {
	// Validate model and column
	if model == "" {
		return fmt.Errorf("model name cannot be empty")
	}
	if column == "" {
		return fmt.Errorf("column name cannot be empty")
	}

	// Validate that all required parameters (beyond model and column_name) are provided
	// Skip "model" and "column_name" as they're always provided
	for _, param := range t.params {
		if param == "model" || param == "column_name" {
			continue
		}

		// Check if this parameter is in args
		if args == nil {
			return fmt.Errorf("missing required parameter: %s", param)
		}

		if _, ok := args[param]; !ok {
			return fmt.Errorf("missing required parameter: %s", param)
		}
	}

	return nil
}

// GenerateSQL generates SQL by substituting template parameters (implements GenericTest interface)
func (t *TemplateTest) GenerateSQL(model, column string, args map[string]interface{}) (string, error) {
	// Validate first
	if err := t.Validate(model, column, args); err != nil {
		return "", err
	}

	// Start with the template
	sql := t.sqlTemplate

	// Replace {{ model }} with actual model name
	sql = strings.ReplaceAll(sql, "{{ model }}", model)

	// Replace {{ column_name }} with actual column name
	sql = strings.ReplaceAll(sql, "{{ column_name }}", column)

	// Replace other parameters from args
	if args != nil {
		for key, value := range args {
			// Skip special parameters
			if key == "where" {
				continue
			}

			placeholder := fmt.Sprintf("{{ %s }}", key)
			sql = strings.ReplaceAll(sql, placeholder, fmt.Sprintf("%v", value))
		}
	}

	// Append WHERE clause if provided (like other generic tests)
	whereClause := BuildWhereClause(args)
	if whereClause != "" {
		sql = sql + whereClause
	}

	return sql, nil
}
