package generic

import (
	"strings"
	"testing"
)

func TestNotConstantTest_Name(t *testing.T) {
	test := &NotConstantTest{}
	if test.Name() != "not_constant" {
		t.Errorf("Name() = %q, want %q", test.Name(), "not_constant")
	}
}

func TestNotConstantTest_Validate_ValidInput(t *testing.T) {
	test := &NotConstantTest{}

	err := test.Validate("users", "status", nil)
	if err != nil {
		t.Errorf("Validate() with valid input returned error: %v", err)
	}
}

func TestNotConstantTest_GenerateSQL_ValidInput(t *testing.T) {
	test := &NotConstantTest{}

	sql, err := test.GenerateSQL("users", "status", nil)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	sql = strings.ToLower(sql)
	if !strings.Contains(sql, "select") {
		t.Error("GenerateSQL() missing SELECT")
	}
	if !strings.Contains(sql, "count(distinct") {
		t.Error("GenerateSQL() missing COUNT(DISTINCT ...)")
	}
	if !strings.Contains(sql, "from users") {
		t.Error("GenerateSQL() missing FROM users")
	}
	if !strings.Contains(sql, "having") {
		t.Error("GenerateSQL() missing HAVING clause")
	}
}

func TestNotConstantTest_GenerateSQL_WithWhereClause(t *testing.T) {
	test := &NotConstantTest{}
	args := map[string]interface{}{
		"where": "created_at > '2024-01-01'",
	}

	sql, err := test.GenerateSQL("users", "status", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	if !strings.Contains(sql, "created_at > '2024-01-01'") {
		t.Error("GenerateSQL() missing WHERE clause")
	}
}
