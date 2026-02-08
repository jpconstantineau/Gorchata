package generic

import (
	"strings"
	"testing"
)

func TestNotEmptyStringTest_Name(t *testing.T) {
	test := &NotEmptyStringTest{}
	if test.Name() != "not_empty_string" {
		t.Errorf("Name() = %q, want %q", test.Name(), "not_empty_string")
	}
}

func TestNotEmptyStringTest_Validate_ValidInput(t *testing.T) {
	test := &NotEmptyStringTest{}

	err := test.Validate("users", "name", nil)
	if err != nil {
		t.Errorf("Validate() with valid input returned error: %v", err)
	}
}

func TestNotEmptyStringTest_GenerateSQL_ValidInput(t *testing.T) {
	test := &NotEmptyStringTest{}

	sql, err := test.GenerateSQL("users", "name", nil)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	sql = strings.ToLower(sql)
	if !strings.Contains(sql, "select") {
		t.Error("GenerateSQL() missing SELECT")
	}
	if !strings.Contains(sql, "from users") {
		t.Error("GenerateSQL() missing FROM users")
	}
	if !strings.Contains(sql, "name is not null") {
		t.Error("GenerateSQL() missing IS NOT NULL check")
	}
	if !strings.Contains(sql, "trim") {
		t.Error("GenerateSQL() missing TRIM function")
	}
}

func TestNotEmptyStringTest_GenerateSQL_WithWhereClause(t *testing.T) {
	test := &NotEmptyStringTest{}
	args := map[string]interface{}{
		"where": "active = 1",
	}

	sql, err := test.GenerateSQL("users", "name", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	if !strings.Contains(sql, "active = 1") {
		t.Error("GenerateSQL() missing WHERE clause")
	}
}
