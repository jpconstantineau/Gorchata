package generic

import (
	"strings"
	"testing"
)

func TestMutuallyExclusiveRangesTest_Name(t *testing.T) {
	test := &MutuallyExclusiveRangesTest{}
	if test.Name() != "mutually_exclusive_ranges" {
		t.Errorf("Name() = %q, want %q", test.Name(), "mutually_exclusive_ranges")
	}
}

func TestMutuallyExclusiveRangesTest_Validate_ValidInput(t *testing.T) {
	test := &MutuallyExclusiveRangesTest{}
	args := map[string]interface{}{
		"lower_bound_column": "start_date",
		"upper_bound_column": "end_date",
		"partition_by":       "user_id",
	}

	err := test.Validate("subscriptions", "", args)
	if err != nil {
		t.Errorf("Validate() with valid input returned error: %v", err)
	}
}

func TestMutuallyExclusiveRangesTest_Validate_MissingLowerBound(t *testing.T) {
	test := &MutuallyExclusiveRangesTest{}
	args := map[string]interface{}{
		"upper_bound_column": "end_date",
	}

	err := test.Validate("subscriptions", "", args)
	if err == nil {
		t.Error("Validate() without lower_bound_column should return error")
	}
}

func TestMutuallyExclusiveRangesTest_Validate_MissingUpperBound(t *testing.T) {
	test := &MutuallyExclusiveRangesTest{}
	args := map[string]interface{}{
		"lower_bound_column": "start_date",
	}

	err := test.Validate("subscriptions", "", args)
	if err == nil {
		t.Error("Validate() without upper_bound_column should return error")
	}
}

func TestMutuallyExclusiveRangesTest_Validate_WithoutPartitionBy(t *testing.T) {
	test := &MutuallyExclusiveRangesTest{}
	args := map[string]interface{}{
		"lower_bound_column": "start_date",
		"upper_bound_column": "end_date",
	}

	// partition_by is optional
	err := test.Validate("subscriptions", "", args)
	if err != nil {
		t.Errorf("Validate() without partition_by should succeed: %v", err)
	}
}

func TestMutuallyExclusiveRangesTest_GenerateSQL_ValidInput(t *testing.T) {
	test := &MutuallyExclusiveRangesTest{}
	args := map[string]interface{}{
		"lower_bound_column": "start_date",
		"upper_bound_column": "end_date",
	}

	sql, err := test.GenerateSQL("subscriptions", "", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	sql = strings.ToLower(sql)
	if !strings.Contains(sql, "select") {
		t.Error("GenerateSQL() missing SELECT")
	}
	if !strings.Contains(sql, "from subscriptions") {
		t.Error("GenerateSQL() missing FROM subscriptions")
	}
	if !strings.Contains(sql, "start_date") || !strings.Contains(sql, "end_date") {
		t.Error("GenerateSQL() missing range columns")
	}
}

func TestMutuallyExclusiveRangesTest_GenerateSQL_WithPartitionBy(t *testing.T) {
	test := &MutuallyExclusiveRangesTest{}
	args := map[string]interface{}{
		"lower_bound_column": "start_date",
		"upper_bound_column": "end_date",
		"partition_by":       "user_id",
	}

	sql, err := test.GenerateSQL("subscriptions", "", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	if !strings.Contains(sql, "user_id") {
		t.Error("GenerateSQL() missing partition_by column")
	}
}

func TestMutuallyExclusiveRangesTest_GenerateSQL_WithWhereClause(t *testing.T) {
	test := &MutuallyExclusiveRangesTest{}
	args := map[string]interface{}{
		"lower_bound_column": "start_date",
		"upper_bound_column": "end_date",
		"where":              "status = 'active'",
	}

	sql, err := test.GenerateSQL("subscriptions", "", args)
	if err != nil {
		t.Fatalf("GenerateSQL() returned error: %v", err)
	}

	if !strings.Contains(sql, "status = 'active'") {
		t.Error("GenerateSQL() missing WHERE clause")
	}
}
