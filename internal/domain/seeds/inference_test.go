package seeds

import (
	"testing"
)

// Test Set 1 - Column Type Inference

func TestInferColumnType_AllIntegers(t *testing.T) {
	values := []string{"1", "2", "3", "100"}
	expected := "INTEGER"

	result := inferColumnType(values)

	if result != expected {
		t.Errorf("inferColumnType(%v) = %s, expected %s", values, result, expected)
	}
}

func TestInferColumnType_Decimals(t *testing.T) {
	values := []string{"1.5", "2.0", "3.14"}
	expected := "REAL"

	result := inferColumnType(values)

	if result != expected {
		t.Errorf("inferColumnType(%v) = %s, expected %s", values, result, expected)
	}
}

func TestInferColumnType_Mixed(t *testing.T) {
	values := []string{"1", "hello", "3"}
	expected := "TEXT"

	result := inferColumnType(values)

	if result != expected {
		t.Errorf("inferColumnType(%v) = %s, expected %s", values, result, expected)
	}
}

func TestInferColumnType_WithNulls(t *testing.T) {
	values := []string{"1", "", "3", "  "}
	expected := "INTEGER"

	result := inferColumnType(values)

	if result != expected {
		t.Errorf("inferColumnType(%v) = %s, expected %s", values, result, expected)
	}
}

func TestInferColumnType_AllNulls(t *testing.T) {
	values := []string{"", "  ", ""}
	expected := "TEXT"

	result := inferColumnType(values)

	if result != expected {
		t.Errorf("inferColumnType(%v) = %s, expected %s", values, result, expected)
	}
}

func TestInferColumnType_Priority(t *testing.T) {
	tests := []struct {
		name     string
		values   []string
		expected string
	}{
		{
			name:     "integers only",
			values:   []string{"1", "2", "3"},
			expected: "INTEGER",
		},
		{
			name:     "mix of int and float",
			values:   []string{"1", "2.5", "3"},
			expected: "REAL",
		},
		{
			name:     "mix of float and text",
			values:   []string{"1.5", "hello"},
			expected: "TEXT",
		},
		{
			name:     "mix of int and text",
			values:   []string{"1", "hello"},
			expected: "TEXT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := inferColumnType(tt.values)
			if result != tt.expected {
				t.Errorf("inferColumnType(%v) = %s, expected %s", tt.values, result, tt.expected)
			}
		})
	}
}

func TestInferColumnType_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		values   []string
		expected string
	}{
		{
			name:     "leading zeros",
			values:   []string{"007", "008", "009"},
			expected: "TEXT",
		},
		{
			name:     "scientific notation",
			values:   []string{"1.5e10", "2.3e-5"},
			expected: "REAL",
		},
		{
			name:     "negative integers",
			values:   []string{"-123", "-456", "-789"},
			expected: "INTEGER",
		},
		{
			name:     "negative floats",
			values:   []string{"-1.5", "-2.3"},
			expected: "REAL",
		},
		{
			name:     "zero is integer",
			values:   []string{"0", "0", "0"},
			expected: "INTEGER",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := inferColumnType(tt.values)
			if result != tt.expected {
				t.Errorf("inferColumnType(%v) = %s, expected %s", tt.values, result, tt.expected)
			}
		})
	}
}

// Test Set 2 - Schema Inference

func TestInferSchema_BasicCSV(t *testing.T) {
	// CSV data: headers in first row, data in subsequent rows
	rows := [][]string{
		{"id", "name", "score"},
		{"1", "Alice", "95.5"},
		{"2", "Bob", "88.0"},
		{"3", "Charlie", "92.3"},
	}

	schema, err := InferSchema(rows, 0, nil)
	if err != nil {
		t.Fatalf("InferSchema() error = %v", err)
	}

	if len(schema.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(schema.Columns))
	}

	// Check column 0: id (INTEGER)
	if schema.Columns[0].Name != "id" {
		t.Errorf("column 0 name = %s, expected 'id'", schema.Columns[0].Name)
	}
	if schema.Columns[0].Type != "INTEGER" {
		t.Errorf("column 0 type = %s, expected 'INTEGER'", schema.Columns[0].Type)
	}

	// Check column 1: name (TEXT)
	if schema.Columns[1].Name != "name" {
		t.Errorf("column 1 name = %s, expected 'name'", schema.Columns[1].Name)
	}
	if schema.Columns[1].Type != "TEXT" {
		t.Errorf("column 1 type = %s, expected 'TEXT'", schema.Columns[1].Type)
	}

	// Check column 2: score (REAL)
	if schema.Columns[2].Name != "score" {
		t.Errorf("column 2 name = %s, expected 'score'", schema.Columns[2].Name)
	}
	if schema.Columns[2].Type != "REAL" {
		t.Errorf("column 2 type = %s, expected 'REAL'", schema.Columns[2].Type)
	}
}

func TestInferSchema_MixedTypes(t *testing.T) {
	rows := [][]string{
		{"id", "name", "age", "balance", "active"},
		{"1", "Alice", "25", "1500.50", "true"},
		{"2", "Bob", "30", "2400.00", "false"},
		{"3", "Charlie", "28", "3200.75", "true"},
	}

	schema, err := InferSchema(rows, 0, nil)
	if err != nil {
		t.Fatalf("InferSchema() error = %v", err)
	}

	expected := []struct {
		name string
		typ  string
	}{
		{"id", "INTEGER"},
		{"name", "TEXT"},
		{"age", "INTEGER"},
		{"balance", "REAL"},
		{"active", "TEXT"},
	}

	if len(schema.Columns) != len(expected) {
		t.Fatalf("expected %d columns, got %d", len(expected), len(schema.Columns))
	}

	for i, exp := range expected {
		if schema.Columns[i].Name != exp.name {
			t.Errorf("column %d name = %s, expected %s", i, schema.Columns[i].Name, exp.name)
		}
		if schema.Columns[i].Type != exp.typ {
			t.Errorf("column %d type = %s, expected %s", i, schema.Columns[i].Type, exp.typ)
		}
	}
}

func TestInferSchema_WithHeaders(t *testing.T) {
	// Ensure first row is treated as headers, not data
	rows := [][]string{
		{"100", "200", "300"}, // These are headers (even though they look numeric)
		{"1", "2", "3"},       // Data row 1
		{"4", "5", "6"},       // Data row 2
	}

	schema, err := InferSchema(rows, 0, nil)
	if err != nil {
		t.Fatalf("InferSchema() error = %v", err)
	}

	// Headers should be used as column names
	if schema.Columns[0].Name != "100" {
		t.Errorf("column 0 name = %s, expected '100'", schema.Columns[0].Name)
	}

	// Data rows should be all integers
	for i := 0; i < 3; i++ {
		if schema.Columns[i].Type != "INTEGER" {
			t.Errorf("column %d type = %s, expected 'INTEGER'", i, schema.Columns[i].Type)
		}
	}
}

func TestInferSchema_SampleSize(t *testing.T) {
	rows := [][]string{
		{"value"},
		{"1"},     // Row 1
		{"2"},     // Row 2
		{"3"},     // Row 3
		{"hello"}, // Row 4 - if sampled, would change type to TEXT
	}

	// Sample only first 3 data rows (integers)
	schema, err := InferSchema(rows, 3, nil)
	if err != nil {
		t.Fatalf("InferSchema() error = %v", err)
	}

	if schema.Columns[0].Type != "INTEGER" {
		t.Errorf("with sampleSize=3, expected INTEGER, got %s", schema.Columns[0].Type)
	}

	// Sample all rows (includes "hello")
	schema, err = InferSchema(rows, 0, nil)
	if err != nil {
		t.Fatalf("InferSchema() error = %v", err)
	}

	if schema.Columns[0].Type != "TEXT" {
		t.Errorf("with sampleSize=0, expected TEXT, got %s", schema.Columns[0].Type)
	}
}

func TestInferSchema_EmptyData(t *testing.T) {
	// Only headers, no data rows
	rows := [][]string{
		{"id", "name", "score"},
	}

	_, err := InferSchema(rows, 0, nil)
	if err == nil {
		t.Error("expected error for empty data, got nil")
	}
}

func TestInferSchema_NoHeaders(t *testing.T) {
	// Empty first row (no headers)
	rows := [][]string{
		{},
		{"1", "2", "3"},
	}

	_, err := InferSchema(rows, 0, nil)
	if err == nil {
		t.Error("expected error for no headers, got nil")
	}
}

// TestInferSchema_WithOverrides tests applying manual column type overrides
func TestInferSchema_WithOverrides(t *testing.T) {
	rows := [][]string{
		{"id", "code", "amount", "name"},
		{"1", "ABC123", "100.50", "Alice"},
		{"2", "DEF456", "250.75", "Bob"},
	}

	// Without overrides, 'code' would be TEXT (correct), 'id' and 'amount' would be inferred
	// Let's force 'id' to be TEXT and keep amount as REAL
	overrides := map[string]string{
		"id":   "TEXT", // Force ID to be TEXT even though it looks like INTEGER
		"code": "TEXT", // Explicitly set (would be inferred anyway)
	}

	schema, err := InferSchema(rows, 0, overrides)
	if err != nil {
		t.Fatalf("InferSchema failed: %v", err)
	}

	if len(schema.Columns) != 4 {
		t.Fatalf("expected 4 columns, got %d", len(schema.Columns))
	}

	// Check id - should be TEXT due to override
	if schema.Columns[0].Name != "id" {
		t.Errorf("column 0: expected name 'id', got '%s'", schema.Columns[0].Name)
	}
	if schema.Columns[0].Type != "TEXT" {
		t.Errorf("column 0: expected type 'TEXT' (overridden), got '%s'", schema.Columns[0].Type)
	}

	// Check code - should be TEXT
	if schema.Columns[1].Name != "code" {
		t.Errorf("column 1: expected name 'code', got '%s'", schema.Columns[1].Name)
	}
	if schema.Columns[1].Type != "TEXT" {
		t.Errorf("column 1: expected type 'TEXT', got '%s'", schema.Columns[1].Type)
	}

	// Check amount - should be REAL (inferred, no override)
	if schema.Columns[2].Name != "amount" {
		t.Errorf("column 2: expected name 'amount', got '%s'", schema.Columns[2].Name)
	}
	if schema.Columns[2].Type != "REAL" {
		t.Errorf("column 2: expected type 'REAL' (inferred), got '%s'", schema.Columns[2].Type)
	}

	// Check name - should be TEXT (inferred, no override)
	if schema.Columns[3].Name != "name" {
		t.Errorf("column 3: expected name 'name', got '%s'", schema.Columns[3].Name)
	}
	if schema.Columns[3].Type != "TEXT" {
		t.Errorf("column 3: expected type 'TEXT' (inferred), got '%s'", schema.Columns[3].Type)
	}
}

// TestInferSchema_OverrideAll tests applying overrides to all columns
func TestInferSchema_OverrideAll(t *testing.T) {
	rows := [][]string{
		{"col1", "col2"},
		{"1", "2.5"},
		{"3", "4.5"},
	}

	// Override everything
	overrides := map[string]string{
		"col1": "TEXT",
		"col2": "TEXT",
	}

	schema, err := InferSchema(rows, 0, overrides)
	if err != nil {
		t.Fatalf("InferSchema failed: %v", err)
	}

	// Both columns should be TEXT due to overrides
	if schema.Columns[0].Type != "TEXT" {
		t.Errorf("col1: expected TEXT, got %s", schema.Columns[0].Type)
	}
	if schema.Columns[1].Type != "TEXT" {
		t.Errorf("col2: expected TEXT, got %s", schema.Columns[1].Type)
	}
}
