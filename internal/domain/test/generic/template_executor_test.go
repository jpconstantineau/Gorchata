package generic

import (
	"strings"
	"testing"
)

func TestTemplateTest_Name(t *testing.T) {
	tt := &TemplateTest{
		testName:    "custom_test",
		params:      []string{"model", "column_name"},
		sqlTemplate: "SELECT * FROM {{ model }}",
	}

	if tt.Name() != "custom_test" {
		t.Errorf("Expected name 'custom_test', got '%s'", tt.Name())
	}
}

func TestTemplateTest_GenerateSQL_Simple(t *testing.T) {
	tt := &TemplateTest{
		testName:    "positive_test",
		params:      []string{"model", "column_name"},
		sqlTemplate: "SELECT * FROM {{ model }} WHERE {{ column_name }} <= 0",
	}

	sql, err := tt.GenerateSQL("my_table", "my_column", nil)
	if err != nil {
		t.Fatalf("GenerateSQL failed: %v", err)
	}

	expected := "SELECT * FROM my_table WHERE my_column <= 0"
	if sql != expected {
		t.Errorf("SQL mismatch.\nExpected: %s\nGot: %s", expected, sql)
	}
}

func TestTemplateTest_GenerateSQL_WithArguments(t *testing.T) {
	tt := &TemplateTest{
		testName:    "range_test",
		params:      []string{"model", "column_name", "min_value", "max_value"},
		sqlTemplate: "SELECT * FROM {{ model }} WHERE {{ column_name }} < {{ min_value }} OR {{ column_name }} > {{ max_value }}",
	}

	args := map[string]interface{}{
		"min_value": 0,
		"max_value": 100,
	}

	sql, err := tt.GenerateSQL("data_table", "value", args)
	if err != nil {
		t.Fatalf("GenerateSQL failed: %v", err)
	}

	expected := "SELECT * FROM data_table WHERE value < 0 OR value > 100"
	if sql != expected {
		t.Errorf("SQL mismatch.\nExpected: %s\nGot: %s", expected, sql)
	}
}

func TestTemplateTest_GenerateSQL_MultipleOccurrences(t *testing.T) {
	tt := &TemplateTest{
		testName:    "duplicate_test",
		params:      []string{"model", "column_name"},
		sqlTemplate: "SELECT {{ column_name }}, COUNT(*) FROM {{ model }} GROUP BY {{ column_name }} HAVING COUNT(*) > 1",
	}

	sql, err := tt.GenerateSQL("users", "email", nil)
	if err != nil {
		t.Fatalf("GenerateSQL failed: %v", err)
	}

	// Should replace all occurrences
	if strings.Count(sql, "users") != 1 {
		t.Errorf("Expected 1 occurrence of 'users', got %d", strings.Count(sql, "users"))
	}
	if strings.Count(sql, "email") != 2 {
		t.Errorf("Expected 2 occurrences of 'email', got %d", strings.Count(sql, "email"))
	}
}

func TestTemplateTest_Validate_ValidArgs(t *testing.T) {
	tt := &TemplateTest{
		testName:    "range_test",
		params:      []string{"model", "column_name", "min_value", "max_value"},
		sqlTemplate: "SELECT * FROM {{ model }}",
	}

	args := map[string]interface{}{
		"min_value": 0,
		"max_value": 100,
	}

	err := tt.Validate("my_table", "my_column", args)
	if err != nil {
		t.Errorf("Validate failed: %v", err)
	}
}

func TestTemplateTest_Validate_MissingArgs(t *testing.T) {
	tt := &TemplateTest{
		testName:    "range_test",
		params:      []string{"model", "column_name", "min_value", "max_value"},
		sqlTemplate: "SELECT * FROM {{ model }}",
	}

	args := map[string]interface{}{
		"min_value": 0,
		// missing max_value
	}

	err := tt.Validate("my_table", "my_column", args)
	if err == nil {
		t.Errorf("Expected validation error for missing args, got nil")
	}
}

func TestTemplateTest_Validate_EmptyModel(t *testing.T) {
	tt := &TemplateTest{
		testName:    "simple_test",
		params:      []string{"model", "column_name"},
		sqlTemplate: "SELECT * FROM {{ model }}",
	}

	err := tt.Validate("", "my_column", nil)
	if err == nil {
		t.Errorf("Expected validation error for empty model, got nil")
	}
}

func TestTemplateTest_Validate_EmptyColumn(t *testing.T) {
	tt := &TemplateTest{
		testName:    "simple_test",
		params:      []string{"model", "column_name"},
		sqlTemplate: "SELECT * FROM {{ model }}",
	}

	err := tt.Validate("my_table", "", nil)
	if err == nil {
		t.Errorf("Expected validation error for empty column, got nil")
	}
}

func TestTemplateTest_GenerateSQL_WithWhereClause(t *testing.T) {
	tt := &TemplateTest{
		testName:    "positive_test",
		params:      []string{"model", "column_name"},
		sqlTemplate: "SELECT * FROM {{ model }} WHERE {{ column_name }} <= 0",
	}

	args := map[string]interface{}{
		"where": "active = 1",
	}

	sql, err := tt.GenerateSQL("my_table", "my_column", args)
	if err != nil {
		t.Fatalf("GenerateSQL failed: %v", err)
	}

	// Should include WHERE clause
	if !strings.Contains(sql, "active = 1") {
		t.Errorf("Expected WHERE clause to be appended, got: %s", sql)
	}
}

func TestTemplateTest_GenerateSQL_NilArgs(t *testing.T) {
	tt := &TemplateTest{
		testName:    "simple_test",
		params:      []string{"model", "column_name"},
		sqlTemplate: "SELECT * FROM {{ model }} WHERE {{ column_name }} IS NULL",
	}

	sql, err := tt.GenerateSQL("my_table", "my_column", nil)
	if err != nil {
		t.Fatalf("GenerateSQL failed: %v", err)
	}

	expected := "SELECT * FROM my_table WHERE my_column IS NULL"
	if sql != expected {
		t.Errorf("SQL mismatch.\nExpected: %s\nGot: %s", expected, sql)
	}
}

func TestTemplateTest_GenerateSQL_ComplexTemplate(t *testing.T) {
	tt := &TemplateTest{
		testName: "complex_test",
		params:   []string{"model", "column_name", "threshold", "status"},
		sqlTemplate: `SELECT 
    {{ column_name }},
    COUNT(*) as count
FROM {{ model }}
WHERE {{ column_name }} > {{ threshold }}
  AND status = '{{ status }}'
GROUP BY {{ column_name }}`,
	}

	args := map[string]interface{}{
		"threshold": 100,
		"status":    "active",
	}

	sql, err := tt.GenerateSQL("events", "event_count", args)
	if err != nil {
		t.Fatalf("GenerateSQL failed: %v", err)
	}

	// Verify all substitutions
	if !strings.Contains(sql, "events") {
		t.Errorf("Expected 'events' in SQL")
	}
	if !strings.Contains(sql, "event_count") {
		t.Errorf("Expected 'event_count' in SQL")
	}
	if !strings.Contains(sql, "100") {
		t.Errorf("Expected '100' in SQL")
	}
	if !strings.Contains(sql, "active") {
		t.Errorf("Expected 'active' in SQL")
	}
}
