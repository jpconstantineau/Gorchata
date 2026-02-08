package generic

import (
	"strings"
	"testing"
)

func TestRelationshipsWhereTest_Name(t *testing.T) {
	test := &RelationshipsWhereTest{}
	if test.Name() != "relationships_where" {
		t.Errorf("Name() = %q, want %q", test.Name(), "relationships_where")
	}
}

func TestRelationshipsWhereTest_Validate_ValidInput(t *testing.T) {
	test := &RelationshipsWhereTest{}
	args := map[string]interface{}{
		"to":             "users",
		"field":          "id",
		"from_condition": "deleted_at IS NULL",
		"to_condition":   "active = 1",
	}

	err := test.Validate("orders", "user_id", args)
	if err != nil {
		t.Errorf("Validate() with valid input returned error: %v", err)
	}
}

func TestRelationshipsWhereTest_Validate_MissingTo(t *testing.T) {
	test := &RelationshipsWhereTest{}
	args := map[string]interface{}{
		"field": "id",
	}

	err := test.Validate("orders", "user_id", args)
	if err == nil {
		t.Error("Validate() without 'to' should return error")
	}
}

func TestRelationshipsWhereTest_Validate_MissingField(t *testing.T) {
	test := &RelationshipsWhereTest{}
	args := map[string]interface{}{
		"to": "users",
	}

	err := test.Validate("orders", "user_id", args)
	if err == nil {
		t.Error("Validate() without 'field' should return error")
	}
}

func TestRelationshipsWhereTest_GenerateSQL_ValidInput(t *testing.T) {
	test := &RelationshipsWhereTest{}
	args := map[string]interface{}{
		"to":    "users",
		"field": "id",
	}

	sql, err := test.GenerateSQL("orders", "user_id", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	sql = strings.ToLower(sql)
	if !strings.Contains(sql, "select") {
		t.Error("GenerateSQL() missing SELECT")
	}
	if !strings.Contains(sql, "from orders") {
		t.Error("GenerateSQL() missing FROM orders")
	}
	if !strings.Contains(sql, "not in") {
		t.Error("GenerateSQL() missing NOT IN")
	}
}

func TestRelationshipsWhereTest_GenerateSQL_WithConditions(t *testing.T) {
	test := &RelationshipsWhereTest{}
	args := map[string]interface{}{
		"to":             "users",
		"field":          "id",
		"from_condition": "deleted_at IS NULL",
		"to_condition":   "active = 1",
	}

	sql, err := test.GenerateSQL("orders", "user_id", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	if !strings.Contains(sql, "deleted_at IS NULL") {
		t.Error("GenerateSQL() missing from_condition")
	}
	if !strings.Contains(sql, "active = 1") {
		t.Error("GenerateSQL() missing to_condition")
	}
}
