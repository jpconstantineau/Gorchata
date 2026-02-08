package generic

import (
	"strings"
	"testing"
)

func TestEqualRowcountTest_Name(t *testing.T) {
	test := &EqualRowcountTest{}
	if test.Name() != "equal_rowcount" {
		t.Errorf("Name() = %q, want %q", test.Name(), "equal_rowcount")
	}
}

func TestEqualRowcountTest_Validate_ValidInput(t *testing.T) {
	test := &EqualRowcountTest{}
	args := map[string]interface{}{
		"compare_model": "users_backup",
	}

	err := test.Validate("users", "", args)
	if err != nil {
		t.Errorf("Validate() with valid input returned error: %v", err)
	}
}

func TestEqualRowcountTest_Validate_MissingCompareModel(t *testing.T) {
	test := &EqualRowcountTest{}

	err := test.Validate("users", "", nil)
	if err == nil {
		t.Error("Validate() without compare_model should return error")
	}
}

func TestEqualRowcountTest_Validate_EmptyCompareModel(t *testing.T) {
	test := &EqualRowcountTest{}
	args := map[string]interface{}{
		"compare_model": "",
	}

	err := test.Validate("users", "", args)
	if err == nil {
		t.Error("Validate() with empty compare_model should return error")
	}
}

func TestEqualRowcountTest_GenerateSQL_ValidInput(t *testing.T) {
	test := &EqualRowcountTest{}
	args := map[string]interface{}{
		"compare_model": "users_backup",
	}

	sql, err := test.GenerateSQL("users", "", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	sql = strings.ToLower(sql)
	if !strings.Contains(sql, "select") {
		t.Error("GenerateSQL() missing SELECT")
	}
	if !strings.Contains(sql, "count(*)") {
		t.Error("GenerateSQL() missing COUNT(*)")
	}
	if !strings.Contains(sql, "from users") {
		t.Error("GenerateSQL() missing FROM users")
	}
	if !strings.Contains(sql, "from users_backup") {
		t.Error("GenerateSQL() missing FROM users_backup")
	}
}

func TestEqualRowcountTest_GenerateSQL_WithWhereClause(t *testing.T) {
	test := &EqualRowcountTest{}
	args := map[string]interface{}{
		"compare_model": "users_backup",
		"where":         "active = 1",
	}

	sql, err := test.GenerateSQL("users", "", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	// WHERE clause should apply to both tables
	occurrences := strings.Count(sql, "active = 1")
	if occurrences < 2 {
		t.Errorf("GenerateSQL() should apply WHERE clause to both tables, found %d occurrences", occurrences)
	}
}
