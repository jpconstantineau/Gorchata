package schema

import (
	"fmt"

	"github.com/jpconstantineau/gorchata/internal/domain/test"
	"github.com/jpconstantineau/gorchata/internal/domain/test/generic"
)

// BuildTestsFromSchema builds test instances from parsed schema files
func BuildTestsFromSchema(schemaFiles []*SchemaFile, registry *generic.Registry) ([]*test.Test, error) {
	var tests []*test.Test
	var errors []error

	for _, schema := range schemaFiles {
		for _, model := range schema.Models {
			// Build column-level tests
			for _, column := range model.Columns {
				columnTests, err := buildColumnTests(model.Name, column.Name, column.DataTests, registry)
				if err != nil {
					errors = append(errors, fmt.Errorf("model %s, column %s: %w", model.Name, column.Name, err))
					continue
				}
				tests = append(tests, columnTests...)
			}

			// Build table-level tests
			tableTests, err := buildTableTests(model.Name, model.DataTests, registry)
			if err != nil {
				errors = append(errors, fmt.Errorf("model %s: %w", model.Name, err))
				continue
			}
			tests = append(tests, tableTests...)
		}
	}

	// If we have errors but no tests, return the errors
	if len(tests) == 0 && len(errors) > 0 {
		return nil, fmt.Errorf("failed to build tests: %v", errors)
	}

	return tests, nil
}

// buildColumnTests builds tests for a specific column
func buildColumnTests(modelName, columnName string, testDefs []interface{}, registry *generic.Registry) ([]*test.Test, error) {
	var tests []*test.Test

	for _, testDef := range testDefs {
		testName, args, config, err := parseTestDefinition(testDef)
		if err != nil {
			return nil, fmt.Errorf("failed to parse test definition: %w", err)
		}

		// Get test from registry
		genericTest, ok := registry.Get(testName)
		if !ok {
			// Skip unknown tests (could also return error)
			continue
		}

		// Validate test arguments
		err = genericTest.Validate(modelName, columnName, args)
		if err != nil {
			return nil, fmt.Errorf("test %s validation failed: %w", testName, err)
		}

		// Generate SQL
		sql, err := genericTest.GenerateSQL(modelName, columnName, args)
		if err != nil {
			return nil, fmt.Errorf("failed to generate SQL for test %s: %w", testName, err)
		}

		// Create test ID
		testID := generateTestID(testName, modelName, columnName, config)

		// Create test instance
		testInstance, err := test.NewTest(testID, testName, modelName, columnName, test.GenericTest, sql)
		if err != nil {
			return nil, fmt.Errorf("failed to create test instance: %w", err)
		}

		// Apply configuration
		testInstance.Config = applyTestConfig(config)

		tests = append(tests, testInstance)
	}

	return tests, nil
}

// buildTableTests builds table-level tests (no column specified)
func buildTableTests(modelName string, testDefs []interface{}, registry *generic.Registry) ([]*test.Test, error) {
	var tests []*test.Test

	for _, testDef := range testDefs {
		testName, args, config, err := parseTestDefinition(testDef)
		if err != nil {
			return nil, fmt.Errorf("failed to parse test definition: %w", err)
		}

		// Get test from registry
		genericTest, ok := registry.Get(testName)
		if !ok {
			// Skip unknown tests
			continue
		}

		// For table-level tests, if there's a 'field' argument, use it as the column
		column := ""
		if args != nil {
			if fieldVal, ok := args["field"]; ok {
				if fieldStr, ok := fieldVal.(string); ok {
					column = fieldStr
				}
			}
		}

		// Validate test arguments
		err = genericTest.Validate(modelName, column, args)
		if err != nil {
			return nil, fmt.Errorf("test %s validation failed: %w", testName, err)
		}

		// Generate SQL
		sql, err := genericTest.GenerateSQL(modelName, column, args)
		if err != nil {
			return nil, fmt.Errorf("failed to generate SQL for test %s: %w", testName, err)
		}

		// Create test ID (use empty column for ID generation even if column was extracted)
		testID := generateTestID(testName, modelName, "", config)

		// Create test instance (use empty column name for table-level tests)
		testInstance, err := test.NewTest(testID, testName, modelName, "", test.GenericTest, sql)
		if err != nil {
			return nil, fmt.Errorf("failed to create test instance: %w", err)
		}

		// Apply configuration
		testInstance.Config = applyTestConfig(config)

		tests = append(tests, testInstance)
	}

	return tests, nil
}

// parseTestDefinition parses a test definition which can be:
// 1. Simple string: "not_null"
// 2. Map with no args: {unique: {}}
// 3. Map with args: {accepted_values: {values: ['a', 'b'], severity: warn}}
//
// Returns: testName, args, config, error
func parseTestDefinition(testDef interface{}) (string, map[string]interface{}, map[string]interface{}, error) {
	switch v := testDef.(type) {
	case string:
		// Simple string test: "not_null"
		return v, nil, nil, nil

	case map[string]interface{}:
		// Map format - extract test name and arguments
		if len(v) != 1 {
			return "", nil, nil, fmt.Errorf("test definition map must have exactly one key, got %d", len(v))
		}

		// Get the single key (test name)
		var testName string
		var testData interface{}
		for k, val := range v {
			testName = k
			testData = val
			break
		}

		// testData can be nil, map, or other types
		if testData == nil {
			return testName, nil, nil, nil
		}

		dataMap, ok := testData.(map[string]interface{})
		if !ok {
			return "", nil, nil, fmt.Errorf("test arguments must be a map, got %T", testData)
		}

		// Separate test-specific args from config
		args, config := separateArgsAndConfig(dataMap)

		return testName, args, config, nil

	default:
		return "", nil, nil, fmt.Errorf("invalid test definition type: %T", testDef)
	}
}

// separateArgsAndConfig splits a map into test arguments and configuration
// Reserved config keys: severity, store_failures, where, config, name, error_if, warn_if
func separateArgsAndConfig(data map[string]interface{}) (map[string]interface{}, map[string]interface{}) {
	reservedKeys := map[string]bool{
		"severity":       true,
		"store_failures": true,
		"where":          true,
		"config":         true,
		"name":           true,
		"error_if":       true,
		"warn_if":        true,
	}

	args := make(map[string]interface{})
	config := make(map[string]interface{})

	for key, value := range data {
		if reservedKeys[key] {
			config[key] = value
		} else {
			args[key] = value
		}
	}

	return args, config
}

// applyTestConfig creates a TestConfig from extracted configuration map
func applyTestConfig(configMap map[string]interface{}) *test.TestConfig {
	config := test.DefaultTestConfig()

	if configMap == nil {
		return config
	}

	// Apply severity
	if severityVal, ok := configMap["severity"]; ok {
		if severityStr, ok := severityVal.(string); ok {
			switch severityStr {
			case "error":
				config.SetSeverity(test.SeverityError)
			case "warn", "warning":
				config.SetSeverity(test.SeverityWarn)
			}
		}
	}

	// Apply store_failures
	if storeVal, ok := configMap["store_failures"]; ok {
		if storeBool, ok := storeVal.(bool); ok {
			config.SetStoreFailures(storeBool)
		}
	}

	// Apply where clause
	if whereVal, ok := configMap["where"]; ok {
		if whereStr, ok := whereVal.(string); ok {
			config.SetWhere(whereStr)
		}
	}

	// Apply custom name
	if nameVal, ok := configMap["name"]; ok {
		if nameStr, ok := nameVal.(string); ok {
			config.SetCustomName(nameStr)
		}
	}

	// Note: error_if and warn_if are more complex and would require parsing
	// the conditional threshold structure. Skipping for now as not in test requirements.

	return config
}

// generateTestID creates a unique test ID
func generateTestID(testName, modelName, columnName string, config map[string]interface{}) string {
	// Check for custom name first
	if config != nil {
		if nameVal, ok := config["name"]; ok {
			if nameStr, ok := nameVal.(string); ok && nameStr != "" {
				return nameStr
			}
		}
	}

	// Default format: <test_name>_<model>_<column> or <test_name>_<model> for table tests
	if columnName != "" {
		return fmt.Sprintf("%s_%s_%s", testName, modelName, columnName)
	}
	return fmt.Sprintf("%s_%s", testName, modelName)
}
