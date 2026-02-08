package generic

import (
	"strings"
	"testing"
)

func TestSequentialValuesTest_Name(t *testing.T) {
	test := &SequentialValuesTest{}
	if test.Name() != "sequential_values" {
		t.Errorf("Name() = %q, want %q", test.Name(), "sequential_values")
	}
}

func TestSequentialValuesTest_Validate_ValidInput(t *testing.T) {
	test := &SequentialValuesTest{}
	args := map[string]interface{}{
		"interval": 1,
	}

	err := test.Validate("events", "sequence_number", args)
	if err != nil {
		t.Errorf("Validate() with valid input returned error: %v", err)
	}
}

func TestSequentialValuesTest_Validate_DefaultInterval(t *testing.T) {
	test := &SequentialValuesTest{}

	// Should use default interval of 1 if not provided
	err := test.Validate("events", "sequence_number", nil)
	if err != nil {
		t.Errorf("Validate() without interval should use default: %v", err)
	}
}

func TestSequentialValuesTest_GenerateSQL_ValidInput(t *testing.T) {
	test := &SequentialValuesTest{}
	args := map[string]interface{}{
		"interval": 1,
	}

	sql, err := test.GenerateSQL("events", "sequence_number", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	sql = strings.ToLower(sql)
	if !strings.Contains(sql, "select") {
		t.Error("GenerateSQL() missing SELECT")
	}
	if !strings.Contains(sql, "from events") {
		t.Error("GenerateSQL() missing FROM events")
	}
	// Should use window function or self-join to detect gaps
	if !strings.Contains(sql, "lag(") && !strings.Contains(sql, "lead(") && !strings.Contains(sql, "row_number()") {
		t.Error("GenerateSQL() should use window functions for gap detection")
	}
}

func TestSequentialValuesTest_GenerateSQL_DefaultInterval(t *testing.T) {
	test := &SequentialValuesTest{}

	sql, err := test.GenerateSQL("events", "sequence_number", nil)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	// Should work with default interval
	if sql == "" {
		t.Error("GenerateSQL() returned empty SQL")
	}
}

func TestSequentialValuesTest_GenerateSQL_WithWhereClause(t *testing.T) {
	test := &SequentialValuesTest{}
	args := map[string]interface{}{
		"interval": 1,
		"where":    "event_type = 'audit'",
	}

	sql, err := test.GenerateSQL("events", "sequence_number", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	if !strings.Contains(sql, "event_type = 'audit'") {
		t.Error("GenerateSQL() missing WHERE clause")
	}
}
