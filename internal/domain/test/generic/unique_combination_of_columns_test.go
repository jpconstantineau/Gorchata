package generic

import (
	"strings"
	"testing"
)

func TestUniqueCombinationTest_Name(t *testing.T) {
	test := &UniqueCombinationTest{}
	if test.Name() != "unique_combination_of_columns" {
		t.Errorf("Name() = %q, want %q", test.Name(), "unique_combination_of_columns")
	}
}

func TestUniqueCombinationTest_Validate_ValidInput(t *testing.T) {
	test := &UniqueCombinationTest{}
	args := map[string]interface{}{
		"columns": []interface{}{"col1", "col2", "col3"},
	}

	err := test.Validate("users", "", args)
	if err != nil {
		t.Errorf("Validate() with valid input returned error: %v", err)
	}
}

func TestUniqueCombinationTest_Validate_MissingColumns(t *testing.T) {
	test := &UniqueCombinationTest{}

	err := test.Validate("users", "", nil)
	if err == nil {
		t.Error("Validate() without columns should return error")
	}
}

func TestUniqueCombinationTest_Validate_EmptyColumns(t *testing.T) {
	test := &UniqueCombinationTest{}
	args := map[string]interface{}{
		"columns": []interface{}{},
	}

	err := test.Validate("users", "", args)
	if err == nil {
		t.Error("Validate() with empty columns should return error")
	}
}

func TestUniqueCombinationTest_Validate_SingleColumn(t *testing.T) {
	test := &UniqueCombinationTest{}
	args := map[string]interface{}{
		"columns": []interface{}{"col1"},
	}

	err := test.Validate("users", "", args)
	if err == nil {
		t.Error("Validate() with single column should return error (use unique test instead)")
	}
}

func TestUniqueCombinationTest_GenerateSQL_ValidInput(t *testing.T) {
	test := &UniqueCombinationTest{}
	args := map[string]interface{}{
		"columns": []interface{}{"first_name", "last_name", "birthdate"},
	}

	sql, err := test.GenerateSQL("users", "", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	sql = strings.ToLower(sql)
	if !strings.Contains(sql, "select") {
		t.Error("GenerateSQL() missing SELECT")
	}
	if !strings.Contains(sql, "first_name") || !strings.Contains(sql, "last_name") || !strings.Contains(sql, "birthdate") {
		t.Error("GenerateSQL() missing column names")
	}
	if !strings.Contains(sql, "count(*)") {
		t.Error("GenerateSQL() missing COUNT(*)")
	}
	if !strings.Contains(sql, "group by") {
		t.Error("GenerateSQL() missing GROUP BY")
	}
	if !strings.Contains(sql, "having") {
		t.Error("GenerateSQL() missing HAVING")
	}
}

func TestUniqueCombinationTest_GenerateSQL_WithWhereClause(t *testing.T) {
	test := &UniqueCombinationTest{}
	args := map[string]interface{}{
		"columns": []interface{}{"col1", "col2"},
		"where":   "active = 1",
	}

	sql, err := test.GenerateSQL("users", "", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	if !strings.Contains(sql, "active = 1") {
		t.Error("GenerateSQL() missing WHERE clause")
	}
}
