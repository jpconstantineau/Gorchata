package executor

import (
	"testing"

	"github.com/jpconstantineau/gorchata/internal/domain/test"
)

func TestNewTestSelector(t *testing.T) {
	selector := NewTestSelector([]string{"*"}, []string{}, []string{}, []string{})

	if selector == nil {
		t.Error("NewTestSelector() returned nil")
	}
}

func TestSelector_ByName_SinglePattern(t *testing.T) {
	selector := NewTestSelector([]string{"not_null_*"}, []string{}, []string{}, []string{})

	test1, _ := test.NewTest("not_null_users_email", "not_null", "users", "email", test.GenericTest, "SELECT 1")
	test2, _ := test.NewTest("unique_users_id", "unique", "users", "id", test.GenericTest, "SELECT 1")

	if !selector.Matches(test1) {
		t.Error("Selector should match not_null_users_email")
	}
	if selector.Matches(test2) {
		t.Error("Selector should not match unique_users_id")
	}
}

func TestSelector_ByName_Wildcard(t *testing.T) {
	selector := NewTestSelector([]string{"*"}, []string{}, []string{}, []string{})

	test1, _ := test.NewTest("not_null_users_email", "not_null", "users", "email", test.GenericTest, "SELECT 1")
	test2, _ := test.NewTest("unique_users_id", "unique", "users", "id", test.GenericTest, "SELECT 1")

	if !selector.Matches(test1) {
		t.Error("Selector with * should match all tests")
	}
	if !selector.Matches(test2) {
		t.Error("Selector with * should match all tests")
	}
}

func TestSelector_ByName_Exclude(t *testing.T) {
	selector := NewTestSelector([]string{"*"}, []string{"*_email"}, []string{}, []string{})

	test1, _ := test.NewTest("not_null_users_email", "not_null", "users", "email", test.GenericTest, "SELECT 1")
	test2, _ := test.NewTest("unique_users_id", "unique", "users", "id", test.GenericTest, "SELECT 1")

	if selector.Matches(test1) {
		t.Error("Selector should exclude tests matching *_email")
	}
	if !selector.Matches(test2) {
		t.Error("Selector should match unique_users_id")
	}
}

func TestSelector_ByTag(t *testing.T) {
	selector := NewTestSelector([]string{}, []string{}, []string{"critical"}, []string{})

	test1, _ := test.NewTest("test1", "not_null", "users", "email", test.GenericTest, "SELECT 1")
	test1.Config.AddTag("critical")

	test2, _ := test.NewTest("test2", "unique", "users", "id", test.GenericTest, "SELECT 1")
	test2.Config.AddTag("important")

	if !selector.Matches(test1) {
		t.Error("Selector should match test with 'critical' tag")
	}
	if selector.Matches(test2) {
		t.Error("Selector should not match test without 'critical' tag")
	}
}

func TestSelector_ByTag_Multiple(t *testing.T) {
	selector := NewTestSelector([]string{}, []string{}, []string{"critical", "important"}, []string{})

	test1, _ := test.NewTest("test1", "not_null", "users", "email", test.GenericTest, "SELECT 1")
	test1.Config.AddTag("critical")

	test2, _ := test.NewTest("test2", "unique", "users", "id", test.GenericTest, "SELECT 1")
	test2.Config.AddTag("important")

	test3, _ := test.NewTest("test3", "not_null", "orders", "id", test.GenericTest, "SELECT 1")
	test3.Config.AddTag("optional")

	if !selector.Matches(test1) {
		t.Error("Selector should match test with 'critical' tag")
	}
	if !selector.Matches(test2) {
		t.Error("Selector should match test with 'important' tag")
	}
	if selector.Matches(test3) {
		t.Error("Selector should not match test without specified tags")
	}
}

func TestSelector_ByModel(t *testing.T) {
	selector := NewTestSelector([]string{}, []string{}, []string{}, []string{"users"})

	test1, _ := test.NewTest("test1", "not_null", "users", "email", test.GenericTest, "SELECT 1")
	test2, _ := test.NewTest("test2", "unique", "orders", "id", test.GenericTest, "SELECT 1")

	if !selector.Matches(test1) {
		t.Error("Selector should match test for 'users' model")
	}
	if selector.Matches(test2) {
		t.Error("Selector should not match test for 'orders' model")
	}
}

func TestSelector_ByModel_Wildcard(t *testing.T) {
	selector := NewTestSelector([]string{}, []string{}, []string{}, []string{"user*"})

	test1, _ := test.NewTest("test1", "not_null", "users", "email", test.GenericTest, "SELECT 1")
	test2, _ := test.NewTest("test2", "not_null", "user_profiles", "bio", test.GenericTest, "SELECT 1")
	test3, _ := test.NewTest("test3", "unique", "orders", "id", test.GenericTest, "SELECT 1")

	if !selector.Matches(test1) {
		t.Error("Selector should match 'users' model with user* pattern")
	}
	if !selector.Matches(test2) {
		t.Error("Selector should match 'user_profiles' model with user* pattern")
	}
	if selector.Matches(test3) {
		t.Error("Selector should not match 'orders' model")
	}
}

func TestSelector_CombinedFilters(t *testing.T) {
	selector := NewTestSelector([]string{"not_null_*"}, []string{}, []string{"critical"}, []string{"users"})

	test1, _ := test.NewTest("not_null_users_email", "not_null", "users", "email", test.GenericTest, "SELECT 1")
	test1.Config.AddTag("critical")

	test2, _ := test.NewTest("not_null_users_name", "not_null", "users", "name", test.GenericTest, "SELECT 1")
	test2.Config.AddTag("important")

	test3, _ := test.NewTest("unique_users_id", "unique", "users", "id", test.GenericTest, "SELECT 1")
	test3.Config.AddTag("critical")

	test4, _ := test.NewTest("not_null_orders_id", "not_null", "orders", "id", test.GenericTest, "SELECT 1")
	test4.Config.AddTag("critical")

	if !selector.Matches(test1) {
		t.Error("test1 should match all criteria")
	}
	if selector.Matches(test2) {
		t.Error("test2 should not match (wrong tag)")
	}
	if selector.Matches(test3) {
		t.Error("test3 should not match (wrong name)")
	}
	if selector.Matches(test4) {
		t.Error("test4 should not match (wrong model)")
	}
}

func TestSelector_NoFilters(t *testing.T) {
	selector := NewTestSelector([]string{}, []string{}, []string{}, []string{})

	test1, _ := test.NewTest("test1", "not_null", "users", "email", test.GenericTest, "SELECT 1")

	// No filters means match all
	if !selector.Matches(test1) {
		t.Error("Selector with no filters should match all tests")
	}
}

func TestSelector_Filter_EmptyList(t *testing.T) {
	selector := NewTestSelector([]string{"*"}, []string{}, []string{}, []string{})

	tests := []*test.Test{}
	filtered := selector.Filter(tests)

	if len(filtered) != 0 {
		t.Errorf("Filter() on empty list should return empty list, got %d", len(filtered))
	}
}

func TestSelector_Filter_AllMatch(t *testing.T) {
	selector := NewTestSelector([]string{"*"}, []string{}, []string{}, []string{})

	test1, _ := test.NewTest("test1", "not_null", "users", "email", test.GenericTest, "SELECT 1")
	test2, _ := test.NewTest("test2", "unique", "users", "id", test.GenericTest, "SELECT 1")
	tests := []*test.Test{test1, test2}

	filtered := selector.Filter(tests)

	if len(filtered) != 2 {
		t.Errorf("Filter() should return all matching tests, got %d, want 2", len(filtered))
	}
}

func TestSelector_Filter_PartialMatch(t *testing.T) {
	selector := NewTestSelector([]string{"not_null_*"}, []string{}, []string{}, []string{})

	test1, _ := test.NewTest("not_null_users_email", "not_null", "users", "email", test.GenericTest, "SELECT 1")
	test2, _ := test.NewTest("unique_users_id", "unique", "users", "id", test.GenericTest, "SELECT 1")
	tests := []*test.Test{test1, test2}

	filtered := selector.Filter(tests)

	if len(filtered) != 1 {
		t.Errorf("Filter() should return 1 matching test, got %d", len(filtered))
	}
	if len(filtered) > 0 && filtered[0].ID != "not_null_users_email" {
		t.Errorf("Filter() should return not_null_users_email, got %s", filtered[0].ID)
	}
}

func TestSelector_ExcludePriority(t *testing.T) {
	// Include all, but exclude specific pattern
	selector := NewTestSelector([]string{"*"}, []string{"*_staging"}, []string{}, []string{})

	test1, _ := test.NewTest("not_null_users_email", "not_null", "users", "email", test.GenericTest, "SELECT 1")
	test2, _ := test.NewTest("not_null_users_staging", "not_null", "users_staging", "email", test.GenericTest, "SELECT 1")

	if !selector.Matches(test1) {
		t.Error("Should match test not in exclude pattern")
	}
	if selector.Matches(test2) {
		t.Error("Should not match test in exclude pattern")
	}
}

func TestSelector_MultipleIncludes(t *testing.T) {
	selector := NewTestSelector([]string{"not_null_*", "unique_*"}, []string{}, []string{}, []string{})

	test1, _ := test.NewTest("not_null_users_email", "not_null", "users", "email", test.GenericTest, "SELECT 1")
	test2, _ := test.NewTest("unique_users_id", "unique", "users", "id", test.GenericTest, "SELECT 1")
	test3, _ := test.NewTest("accepted_values_status", "accepted_values", "orders", "status", test.GenericTest, "SELECT 1")

	if !selector.Matches(test1) {
		t.Error("Should match not_null pattern")
	}
	if !selector.Matches(test2) {
		t.Error("Should match unique pattern")
	}
	if selector.Matches(test3) {
		t.Error("Should not match accepted_values pattern")
	}
}
