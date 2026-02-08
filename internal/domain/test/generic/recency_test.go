package generic

import (
	"strings"
	"testing"
)

func TestRecencyTest_Name(t *testing.T) {
	test := &RecencyTest{}
	if test.Name() != "recency" {
		t.Errorf("Name() = %q, want %q", test.Name(), "recency")
	}
}

func TestRecencyTest_Validate_ValidInput(t *testing.T) {
	test := &RecencyTest{}
	args := map[string]interface{}{
		"datepart": "day",
		"interval": 7,
	}

	err := test.Validate("events", "created_at", args)
	if err != nil {
		t.Errorf("Validate() with valid input returned error: %v", err)
	}
}

func TestRecencyTest_Validate_MissingDatepart(t *testing.T) {
	test := &RecencyTest{}
	args := map[string]interface{}{
		"interval": 7,
	}

	err := test.Validate("events", "created_at", args)
	if err == nil {
		t.Error("Validate() without datepart should return error")
	}
}

func TestRecencyTest_Validate_MissingInterval(t *testing.T) {
	test := &RecencyTest{}
	args := map[string]interface{}{
		"datepart": "day",
	}

	err := test.Validate("events", "created_at", args)
	if err == nil {
		t.Error("Validate() without interval should return error")
	}
}

func TestRecencyTest_Validate_InvalidDatepart(t *testing.T) {
	test := &RecencyTest{}
	args := map[string]interface{}{
		"datepart": "invalid",
		"interval": 7,
	}

	err := test.Validate("events", "created_at", args)
	if err == nil {
		t.Error("Validate() with invalid datepart should return error")
	}
}

func TestRecencyTest_GenerateSQL_ValidInput(t *testing.T) {
	test := &RecencyTest{}
	args := map[string]interface{}{
		"datepart": "day",
		"interval": 7,
	}

	sql, err := test.GenerateSQL("events", "created_at", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	sql = strings.ToLower(sql)
	if !strings.Contains(sql, "select") {
		t.Error("GenerateSQL() missing SELECT")
	}
	if !strings.Contains(sql, "max(") {
		t.Error("GenerateSQL() missing MAX function")
	}
	if !strings.Contains(sql, "julianday") {
		t.Error("GenerateSQL() missing JULIANDAY function")
	}
	if !strings.Contains(sql, "from events") {
		t.Error("GenerateSQL() missing FROM events")
	}
	if !strings.Contains(sql, "having") {
		t.Error("GenerateSQL() missing HAVING clause")
	}
}

func TestRecencyTest_GenerateSQL_HourDatepart(t *testing.T) {
	test := &RecencyTest{}
	args := map[string]interface{}{
		"datepart": "hour",
		"interval": 24,
	}

	sql, err := test.GenerateSQL("events", "created_at", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	if !strings.Contains(sql, "* 24") {
		t.Error("GenerateSQL() should multiply by 24 for hour datepart")
	}
}

func TestRecencyTest_GenerateSQL_WithWhereClause(t *testing.T) {
	test := &RecencyTest{}
	args := map[string]interface{}{
		"datepart": "day",
		"interval": 7,
		"where":    "event_type = 'login'",
	}

	sql, err := test.GenerateSQL("events", "created_at", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	if !strings.Contains(sql, "event_type = 'login'") {
		t.Error("GenerateSQL() missing WHERE clause")
	}
}
