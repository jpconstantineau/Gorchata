package generic

import (
	"strings"
	"testing"
)

func TestNotNullTest_Name(t *testing.T) {
	test := &NotNullTest{}
	if test.Name() != "not_null" {
		t.Errorf("Name() = %q, want %q", test.Name(), "not_null")
	}
}

func TestNotNullTest_Validate_ValidInput(t *testing.T) {
	test := &NotNullTest{}

	err := test.Validate("users", "email", nil)
	if err != nil {
		t.Errorf("Validate() with valid input returned error: %v", err)
	}
}

func TestNotNullTest_Validate_EmptyModel(t *testing.T) {
	test := &NotNullTest{}

	err := test.Validate("", "email", nil)
	if err == nil {
		t.Error("Validate() with empty model should return error")
	}
}

func TestNotNullTest_Validate_EmptyColumn(t *testing.T) {
	test := &NotNullTest{}

	err := test.Validate("users", "", nil)
	if err == nil {
		t.Error("Validate() with empty column should return error")
	}
}

func TestNotNullTest_GenerateSQL_ValidInput(t *testing.T) {
	test := &NotNullTest{}

	sql, err := test.GenerateSQL("users", "email", nil)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	// Verify SQL contains expected components
	sql = strings.ToLower(sql)
	if !strings.Contains(sql, "select") {
		t.Error("GenerateSQL() missing SELECT")
	}
	if !strings.Contains(sql, "from users") {
		t.Error("GenerateSQL() missing FROM users")
	}
	if !strings.Contains(sql, "where") {
		t.Error("GenerateSQL() missing WHERE")
	}
	if !strings.Contains(sql, "email is null") {
		t.Error("GenerateSQL() missing 'email IS NULL' condition")
	}
}

func TestNotNullTest_GenerateSQL_WithWhereClause(t *testing.T) {
	test := &NotNullTest{}
	args := map[string]interface{}{
		"where": "status = 'active'",
	}

	sql, err := test.GenerateSQL("users", "email", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	// Verify SQL contains both NULL check and WHERE clause
	sql = strings.ToLower(sql)
	if !strings.Contains(sql, "email is null") {
		t.Error("GenerateSQL() missing NULL check")
	}
	if !strings.Contains(sql, "status = 'active'") {
		t.Error("GenerateSQL() missing WHERE clause")
	}
	if !strings.Contains(sql, "and") {
		t.Error("GenerateSQL() should combine conditions with AND")
	}
}

func TestNotNullTest_GenerateSQL_InvalidModel(t *testing.T) {
	test := &NotNullTest{}

	_, err := test.GenerateSQL("", "email", nil)
	if err == nil {
		t.Error("GenerateSQL() with empty model should return error")
	}
}

func TestNotNullTest_GenerateSQL_InvalidColumn(t *testing.T) {
	test := &NotNullTest{}

	_, err := test.GenerateSQL("users", "", nil)
	if err == nil {
		t.Error("GenerateSQL() with empty column should return error")
	}
}
