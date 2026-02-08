package schema

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestSchemaFile_Unmarshal(t *testing.T) {
	yamlContent := `version: 2
models:
  - name: test_model
    description: "Test description"
    columns:
      - name: test_column
        description: "Column description"
`

	var schema SchemaFile
	err := yaml.Unmarshal([]byte(yamlContent), &schema)

	if err != nil {
		t.Fatalf("Failed to unmarshal YAML: %v", err)
	}

	if schema.Version != 2 {
		t.Errorf("Expected version 2, got %d", schema.Version)
	}

	if len(schema.Models) != 1 {
		t.Fatalf("Expected 1 model, got %d", len(schema.Models))
	}

	model := schema.Models[0]
	if model.Name != "test_model" {
		t.Errorf("Expected model name 'test_model', got '%s'", model.Name)
	}

	if model.Description != "Test description" {
		t.Errorf("Expected description 'Test description', got '%s'", model.Description)
	}

	if len(model.Columns) != 1 {
		t.Fatalf("Expected 1 column, got %d", len(model.Columns))
	}

	column := model.Columns[0]
	if column.Name != "test_column" {
		t.Errorf("Expected column name 'test_column', got '%s'", column.Name)
	}

	if column.Description != "Column description" {
		t.Errorf("Expected column description 'Column description', got '%s'", column.Description)
	}
}

func TestModelSchema_WithColumns(t *testing.T) {
	yamlContent := `name: users
description: "User table"
columns:
  - name: user_id
    description: "Primary key"
  - name: email
    description: "Email address"
`

	var model ModelSchema
	err := yaml.Unmarshal([]byte(yamlContent), &model)

	if err != nil {
		t.Fatalf("Failed to unmarshal model YAML: %v", err)
	}

	if model.Name != "users" {
		t.Errorf("Expected model name 'users', got '%s'", model.Name)
	}

	if len(model.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(model.Columns))
	}

	if model.Columns[0].Name != "user_id" {
		t.Errorf("Expected first column 'user_id', got '%s'", model.Columns[0].Name)
	}

	if model.Columns[1].Name != "email" {
		t.Errorf("Expected second column 'email', got '%s'", model.Columns[1].Name)
	}
}

func TestColumnSchema_WithTests(t *testing.T) {
	yamlContent := `name: email
description: "Email address"
data_tests:
  - not_null
  - unique
`

	var column ColumnSchema
	err := yaml.Unmarshal([]byte(yamlContent), &column)

	if err != nil {
		t.Fatalf("Failed to unmarshal column YAML: %v", err)
	}

	if column.Name != "email" {
		t.Errorf("Expected column name 'email', got '%s'", column.Name)
	}

	if len(column.DataTests) != 2 {
		t.Fatalf("Expected 2 tests, got %d", len(column.DataTests))
	}

	// Tests should be parseable (we'll validate the structure later)
	if column.DataTests[0] == nil {
		t.Error("First test should not be nil")
	}

	if column.DataTests[1] == nil {
		t.Error("Second test should not be nil")
	}
}

func TestTestDefinition_SimpleString(t *testing.T) {
	yamlContent := `data_tests:
  - not_null
  - unique
`

	var data struct {
		DataTests []interface{} `yaml:"data_tests"`
	}

	err := yaml.Unmarshal([]byte(yamlContent), &data)

	if err != nil {
		t.Fatalf("Failed to unmarshal test definitions: %v", err)
	}

	if len(data.DataTests) != 2 {
		t.Fatalf("Expected 2 tests, got %d", len(data.DataTests))
	}

	// First test should be a string
	testStr, ok := data.DataTests[0].(string)
	if !ok {
		t.Errorf("Expected first test to be string, got %T", data.DataTests[0])
	}

	if testStr != "not_null" {
		t.Errorf("Expected 'not_null', got '%s'", testStr)
	}
}

func TestTestDefinition_MapWithArgs(t *testing.T) {
	yamlContent := `data_tests:
  - accepted_values:
      values: ['active', 'inactive']
      severity: warn
`

	var data struct {
		DataTests []interface{} `yaml:"data_tests"`
	}

	err := yaml.Unmarshal([]byte(yamlContent), &data)

	if err != nil {
		t.Fatalf("Failed to unmarshal test definitions: %v", err)
	}

	if len(data.DataTests) != 1 {
		t.Fatalf("Expected 1 test, got %d", len(data.DataTests))
	}

	// Test should be a map
	testMap, ok := data.DataTests[0].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected test to be map, got %T", data.DataTests[0])
	}

	// Should have 'accepted_values' key
	acceptedValuesData, ok := testMap["accepted_values"]
	if !ok {
		t.Fatal("Expected 'accepted_values' key in test map")
	}

	// accepted_values should be a map with 'values' and 'severity'
	avMap, ok := acceptedValuesData.(map[string]interface{})
	if !ok {
		t.Fatalf("Expected accepted_values to be map, got %T", acceptedValuesData)
	}

	if _, ok := avMap["values"]; !ok {
		t.Error("Expected 'values' key in accepted_values")
	}

	if _, ok := avMap["severity"]; !ok {
		t.Error("Expected 'severity' key in accepted_values")
	}
}

func TestModelSchema_WithTableLevelTests(t *testing.T) {
	yamlContent := `name: orders
columns:
  - name: order_id
    data_tests:
      - unique
data_tests:
  - recency:
      datepart: day
      field: created_at
      interval: 7
`

	var model ModelSchema
	err := yaml.Unmarshal([]byte(yamlContent), &model)

	if err != nil {
		t.Fatalf("Failed to unmarshal model YAML: %v", err)
	}

	if model.Name != "orders" {
		t.Errorf("Expected model name 'orders', got '%s'", model.Name)
	}

	// Should have model-level tests
	if len(model.DataTests) != 1 {
		t.Fatalf("Expected 1 model-level test, got %d", len(model.DataTests))
	}

	// Should also have column with tests
	if len(model.Columns) != 1 {
		t.Fatalf("Expected 1 column, got %d", len(model.Columns))
	}

	if len(model.Columns[0].DataTests) != 1 {
		t.Fatalf("Expected 1 column-level test, got %d", len(model.Columns[0].DataTests))
	}
}
