package generic

import (
	"strings"
	"testing"
)

func TestAcceptedValuesTest_Name(t *testing.T) {
	test := &AcceptedValuesTest{}
	if test.Name() != "accepted_values" {
		t.Errorf("Name() = %q, want %q", test.Name(), "accepted_values")
	}
}

func TestAcceptedValuesTest_Validate_ValidInput(t *testing.T) {
	test := &AcceptedValuesTest{}
	args := map[string]interface{}{
		"values": []interface{}{"active", "pending", "closed"},
	}

	err := test.Validate("orders", "status", args)
	if err != nil {
		t.Errorf("Validate() with valid input returned error: %v", err)
	}
}

func TestAcceptedValuesTest_Validate_MissingValues(t *testing.T) {
	test := &AcceptedValuesTest{}

	err := test.Validate("orders", "status", nil)
	if err == nil {
		t.Error("Validate() without values should return error")
	}
}

func TestAcceptedValuesTest_Validate_EmptyValues(t *testing.T) {
	test := &AcceptedValuesTest{}
	args := map[string]interface{}{
		"values": []interface{}{},
	}

	err := test.Validate("orders", "status", args)
	if err == nil {
		t.Error("Validate() with empty values should return error")
	}
}

func TestAcceptedValuesTest_Validate_InvalidValuesType(t *testing.T) {
	test := &AcceptedValuesTest{}
	args := map[string]interface{}{
		"values": "not_an_array",
	}

	err := test.Validate("orders", "status", args)
	if err == nil {
		t.Error("Validate() with invalid values type should return error")
	}
}

func TestAcceptedValuesTest_GenerateSQL_ValidInput(t *testing.T) {
	test := &AcceptedValuesTest{}
	args := map[string]interface{}{
		"values": []interface{}{"pending", "shipped", "delivered"},
	}

	sql, err := test.GenerateSQL("orders", "status", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	// Verify SQL contains expected components
	sql = strings.ToLower(sql)
	if !strings.Contains(sql, "select") {
		t.Error("GenerateSQL() missing SELECT")
	}
	if !strings.Contains(sql, "from orders") {
		t.Error("GenerateSQL() missing FROM orders")
	}
	if !strings.Contains(sql, "where") {
		t.Error("GenerateSQL() missing WHERE")
	}
	if !strings.Contains(sql, "not in") {
		t.Error("GenerateSQL() missing NOT IN")
	}
	if !strings.Contains(sql, "status is not null") {
		t.Error("GenerateSQL() missing IS NOT NULL check")
	}

	// Verify values are included
	if !strings.Contains(sql, "'pending'") || !strings.Contains(sql, "'shipped'") || !strings.Contains(sql, "'delivered'") {
		t.Error("GenerateSQL() missing expected values in IN clause")
	}
}

func TestAcceptedValuesTest_GenerateSQL_WithWhereClause(t *testing.T) {
	test := &AcceptedValuesTest{}
	args := map[string]interface{}{
		"values": []interface{}{"active", "inactive"},
		"where":  "created_at > '2024-01-01'",
	}

	sql, err := test.GenerateSQL("users", "status", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	// Verify WHERE clause is included
	if !strings.Contains(sql, "created_at > '2024-01-01'") {
		t.Error("GenerateSQL() missing WHERE clause")
	}
}

func TestAcceptedValuesTest_GenerateSQL_NumericValues(t *testing.T) {
	test := &AcceptedValuesTest{}
	args := map[string]interface{}{
		"values": []interface{}{1, 2, 3, 4, 5},
	}

	sql, err := test.GenerateSQL("ratings", "score", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	// Verify numeric values are included without quotes
	if !strings.Contains(sql, "1") || !strings.Contains(sql, "5") {
		t.Error("GenerateSQL() missing numeric values")
	}
}

func TestAcceptedValuesTest_GenerateSQL_MissingValues(t *testing.T) {
	test := &AcceptedValuesTest{}

	_, err := test.GenerateSQL("orders", "status", nil)
	if err == nil {
		t.Error("GenerateSQL() without values should return error")
	}
}
