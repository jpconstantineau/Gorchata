package generic

import (
	"strings"
	"testing"
)

func TestAtLeastOneTest_Name(t *testing.T) {
	test := &AtLeastOneTest{}
	if test.Name() != "at_least_one" {
		t.Errorf("Name() = %q, want %q", test.Name(), "at_least_one")
	}
}

func TestAtLeastOneTest_Validate_ValidInput(t *testing.T) {
	test := &AtLeastOneTest{}

	err := test.Validate("users", "email", nil)
	if err != nil {
		t.Errorf("Validate() with valid input returned error: %v", err)
	}
}

func TestAtLeastOneTest_GenerateSQL_ValidInput(t *testing.T) {
	test := &AtLeastOneTest{}

	sql, err := test.GenerateSQL("users", "email", nil)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	sql = strings.ToLower(sql)
	if !strings.Contains(sql, "select") {
		t.Error("GenerateSQL() missing SELECT")
	}
	if !strings.Contains(sql, "case") {
		t.Error("GenerateSQL() missing CASE expression")
	}
	if !strings.Contains(sql, "count(*)") {
		t.Error("GenerateSQL() missing COUNT(*)")
	}
	if !strings.Contains(sql, "from users") {
		t.Error("GenerateSQL() missing FROM users")
	}
}

func TestAtLeastOneTest_GenerateSQL_WithWhereClause(t *testing.T) {
	test := &AtLeastOneTest{}
	args := map[string]interface{}{
		"where": "deleted_at IS NULL",
	}

	sql, err := test.GenerateSQL("users", "email", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	if !strings.Contains(sql, "deleted_at IS NULL") {
		t.Error("GenerateSQL() missing WHERE clause")
	}
}
