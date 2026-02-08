package generic

import (
	"strings"
	"testing"
)

func TestRelationshipsTest_Name(t *testing.T) {
	test := &RelationshipsTest{}
	if test.Name() != "relationships" {
		t.Errorf("Name() = %q, want %q", test.Name(), "relationships")
	}
}

func TestRelationshipsTest_Validate_ValidInput(t *testing.T) {
	test := &RelationshipsTest{}
	args := map[string]interface{}{
		"to":    "users",
		"field": "id",
	}

	err := test.Validate("orders", "user_id", args)
	if err != nil {
		t.Errorf("Validate() with valid input returned error: %v", err)
	}
}

func TestRelationshipsTest_Validate_MissingTo(t *testing.T) {
	test := &RelationshipsTest{}
	args := map[string]interface{}{
		"field": "id",
	}

	err := test.Validate("orders", "user_id", args)
	if err == nil {
		t.Error("Validate() without 'to' should return error")
	}
}

func TestRelationshipsTest_Validate_MissingField(t *testing.T) {
	test := &RelationshipsTest{}
	args := map[string]interface{}{
		"to": "users",
	}

	err := test.Validate("orders", "user_id", args)
	if err == nil {
		t.Error("Validate() without 'field' should return error")
	}
}

func TestRelationshipsTest_Validate_EmptyTo(t *testing.T) {
	test := &RelationshipsTest{}
	args := map[string]interface{}{
		"to":    "",
		"field": "id",
	}

	err := test.Validate("orders", "user_id", args)
	if err == nil {
		t.Error("Validate() with empty 'to' should return error")
	}
}

func TestRelationshipsTest_Validate_EmptyField(t *testing.T) {
	test := &RelationshipsTest{}
	args := map[string]interface{}{
		"to":    "users",
		"field": "",
	}

	err := test.Validate("orders", "user_id", args)
	if err == nil {
		t.Error("Validate() with empty 'field' should return error")
	}
}

func TestRelationshipsTest_GenerateSQL_ValidInput(t *testing.T) {
	test := &RelationshipsTest{}
	args := map[string]interface{}{
		"to":    "users",
		"field": "id",
	}

	sql, err := test.GenerateSQL("orders", "user_id", args)
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
	if !strings.Contains(sql, "select") && !strings.Contains(sql, "from users") {
		t.Error("GenerateSQL() missing subquery FROM users")
	}
	if !strings.Contains(sql, "user_id is not null") {
		t.Error("GenerateSQL() missing IS NOT NULL check")
	}
}

func TestRelationshipsTest_GenerateSQL_WithWhereClause(t *testing.T) {
	test := &RelationshipsTest{}
	args := map[string]interface{}{
		"to":    "users",
		"field": "id",
		"where": "deleted_at IS NULL",
	}

	sql, err := test.GenerateSQL("orders", "user_id", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	// Verify WHERE clause is included
	if !strings.Contains(sql, "deleted_at IS NULL") {
		t.Error("GenerateSQL() missing WHERE clause")
	}
}

func TestRelationshipsTest_GenerateSQL_MissingArgs(t *testing.T) {
	test := &RelationshipsTest{}

	_, err := test.GenerateSQL("orders", "user_id", nil)
	if err == nil {
		t.Error("GenerateSQL() without args should return error")
	}
}
