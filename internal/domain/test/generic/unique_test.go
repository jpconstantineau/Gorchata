package generic

import (
	"strings"
	"testing"
)

func TestUniqueTest_Name(t *testing.T) {
	test := &UniqueTest{}
	if test.Name() != "unique" {
		t.Errorf("Name() = %q, want %q", test.Name(), "unique")
	}
}

func TestUniqueTest_Validate_ValidInput(t *testing.T) {
	test := &UniqueTest{}

	err := test.Validate("users", "email", nil)
	if err != nil {
		t.Errorf("Validate() with valid input returned error: %v", err)
	}
}

func TestUniqueTest_Validate_EmptyModel(t *testing.T) {
	test := &UniqueTest{}

	err := test.Validate("", "email", nil)
	if err == nil {
		t.Error("Validate() with empty model should return error")
	}
}

func TestUniqueTest_Validate_EmptyColumn(t *testing.T) {
	test := &UniqueTest{}

	err := test.Validate("users", "", nil)
	if err == nil {
		t.Error("Validate() with empty column should return error")
	}
}

func TestUniqueTest_GenerateSQL_ValidInput(t *testing.T) {
	test := &UniqueTest{}

	sql, err := test.GenerateSQL("users", "user_id", nil)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	// Verify SQL contains expected components
	sql = strings.ToLower(sql)
	if !strings.Contains(sql, "select") {
		t.Error("GenerateSQL() missing SELECT")
	}
	if !strings.Contains(sql, "user_id") {
		t.Error("GenerateSQL() missing column in SELECT")
	}
	if !strings.Contains(sql, "count(*)") {
		t.Error("GenerateSQL() missing COUNT(*)")
	}
	if !strings.Contains(sql, "from users") {
		t.Error("GenerateSQL() missing FROM users")
	}
	if !strings.Contains(sql, "group by") {
		t.Error("GenerateSQL() missing GROUP BY")
	}
	if !strings.Contains(sql, "having") {
		t.Error("GenerateSQL() missing HAVING")
	}
}

func TestUniqueTest_GenerateSQL_WithWhereClause(t *testing.T) {
	test := &UniqueTest{}
	args := map[string]interface{}{
		"where": "created_at > '2024-01-01'",
	}

	sql, err := test.GenerateSQL("users", "email", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	// Verify SQL contains WHERE clause
	if !strings.Contains(sql, "created_at > '2024-01-01'") {
		t.Error("GenerateSQL() missing WHERE clause")
	}
	if !strings.Contains(strings.ToLower(sql), "where") {
		t.Error("GenerateSQL() missing WHERE keyword")
	}
}

func TestUniqueTest_GenerateSQL_InvalidModel(t *testing.T) {
	test := &UniqueTest{}

	_, err := test.GenerateSQL("", "email", nil)
	if err == nil {
		t.Error("GenerateSQL() with empty model should return error")
	}
}

func TestUniqueTest_GenerateSQL_InvalidColumn(t *testing.T) {
	test := &UniqueTest{}

	_, err := test.GenerateSQL("users", "", nil)
	if err == nil {
		t.Error("GenerateSQL() with empty column should return error")
	}
}
