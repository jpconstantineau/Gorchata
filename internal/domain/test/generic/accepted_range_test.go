package generic

import (
	"strings"
	"testing"
)

func TestAcceptedRangeTest_Name(t *testing.T) {
	test := &AcceptedRangeTest{}
	if test.Name() != "accepted_range" {
		t.Errorf("Name() = %q, want %q", test.Name(), "accepted_range")
	}
}

func TestAcceptedRangeTest_Validate_ValidInput(t *testing.T) {
	test := &AcceptedRangeTest{}
	args := map[string]interface{}{
		"min_value": 0,
		"max_value": 100,
	}

	err := test.Validate("scores", "value", args)
	if err != nil {
		t.Errorf("Validate() with valid input returned error: %v", err)
	}
}

func TestAcceptedRangeTest_Validate_MissingMin(t *testing.T) {
	test := &AcceptedRangeTest{}
	args := map[string]interface{}{
		"max_value": 100,
	}

	err := test.Validate("scores", "value", args)
	if err == nil {
		t.Error("Validate() without min_value should return error")
	}
}

func TestAcceptedRangeTest_Validate_MissingMax(t *testing.T) {
	test := &AcceptedRangeTest{}
	args := map[string]interface{}{
		"min_value": 0,
	}

	err := test.Validate("scores", "value", args)
	if err == nil {
		t.Error("Validate() without max_value should return error")
	}
}

func TestAcceptedRangeTest_GenerateSQL_ValidInput(t *testing.T) {
	test := &AcceptedRangeTest{}
	args := map[string]interface{}{
		"min_value": 0,
		"max_value": 100,
	}

	sql, err := test.GenerateSQL("scores", "value", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	sql = strings.ToLower(sql)
	if !strings.Contains(sql, "select") {
		t.Error("GenerateSQL() missing SELECT")
	}
	if !strings.Contains(sql, "from scores") {
		t.Error("GenerateSQL() missing FROM scores")
	}
	if !strings.Contains(sql, "not between") {
		t.Error("GenerateSQL() missing NOT BETWEEN")
	}
}

func TestAcceptedRangeTest_GenerateSQL_WithWhereClause(t *testing.T) {
	test := &AcceptedRangeTest{}
	args := map[string]interface{}{
		"min_value": 0,
		"max_value": 100,
		"where":     "active = 1",
	}

	sql, err := test.GenerateSQL("scores", "value", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	if !strings.Contains(sql, "active = 1") {
		t.Error("GenerateSQL() missing WHERE clause")
	}
}
