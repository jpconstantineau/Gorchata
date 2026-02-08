package generic

import (
	"testing"
)

func TestNewRegistry(t *testing.T) {
	registry := NewRegistry()

	if registry == nil {
		t.Fatal("NewRegistry() returned nil")
	}

	list := registry.List()
	if len(list) != 0 {
		t.Errorf("NewRegistry() should start empty, got %d tests", len(list))
	}
}

func TestRegistry_Register(t *testing.T) {
	registry := NewRegistry()

	mockTest := &mockGenericTest{name: "test_mock"}
	registry.Register("test_mock", mockTest)

	// Verify registration
	test, ok := registry.Get("test_mock")
	if !ok {
		t.Error("Register() did not register test")
	}
	if test == nil {
		t.Error("Register() registered nil test")
	}
	if test.Name() != "test_mock" {
		t.Errorf("Register() test name = %q, want %q", test.Name(), "test_mock")
	}
}

func TestRegistry_RegisterMultiple(t *testing.T) {
	registry := NewRegistry()

	test1 := &mockGenericTest{name: "test_1"}
	test2 := &mockGenericTest{name: "test_2"}

	registry.Register("test_1", test1)
	registry.Register("test_2", test2)

	list := registry.List()
	if len(list) != 2 {
		t.Errorf("Register() registered %d tests, want 2", len(list))
	}
}

func TestRegistry_Get(t *testing.T) {
	registry := NewRegistry()
	mockTest := &mockGenericTest{name: "test_mock"}
	registry.Register("test_mock", mockTest)

	tests := []struct {
		name      string
		testName  string
		wantFound bool
	}{
		{
			name:      "existing test",
			testName:  "test_mock",
			wantFound: true,
		},
		{
			name:      "non-existing test",
			testName:  "non_existing",
			wantFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			test, ok := registry.Get(tt.testName)
			if ok != tt.wantFound {
				t.Errorf("Get(%q) found = %v, want %v", tt.testName, ok, tt.wantFound)
			}
			if tt.wantFound && test == nil {
				t.Error("Get() returned nil test when found = true")
			}
		})
	}
}

func TestRegistry_List(t *testing.T) {
	registry := NewRegistry()

	// Empty registry
	list := registry.List()
	if len(list) != 0 {
		t.Errorf("List() on empty registry = %d, want 0", len(list))
	}

	// Add tests
	registry.Register("test_1", &mockGenericTest{name: "test_1"})
	registry.Register("test_2", &mockGenericTest{name: "test_2"})
	registry.Register("test_3", &mockGenericTest{name: "test_3"})

	list = registry.List()
	if len(list) != 3 {
		t.Errorf("List() = %d, want 3", len(list))
	}

	// Verify all registered names are in the list
	expected := map[string]bool{"test_1": true, "test_2": true, "test_3": true}
	for _, name := range list {
		if !expected[name] {
			t.Errorf("List() contains unexpected name %q", name)
		}
		delete(expected, name)
	}
	if len(expected) > 0 {
		t.Errorf("List() missing names: %v", expected)
	}
}

func TestNewDefaultRegistry(t *testing.T) {
	registry := NewDefaultRegistry()

	if registry == nil {
		t.Fatal("NewDefaultRegistry() returned nil")
	}

	// Should contain all core tests
	coreTests := []string{
		"not_null",
		"unique",
		"accepted_values",
		"relationships",
	}

	for _, testName := range coreTests {
		test, ok := registry.Get(testName)
		if !ok {
			t.Errorf("NewDefaultRegistry() missing core test %q", testName)
		}
		if test == nil {
			t.Errorf("NewDefaultRegistry() has nil test for %q", testName)
		}
	}

	// Should also contain extended tests
	extendedTests := []string{
		"not_empty_string",
		"at_least_one",
		"not_constant",
		"unique_combination_of_columns",
		"relationships_where",
		"accepted_range",
		"recency",
		"equal_rowcount",
		"sequential_values",
		"mutually_exclusive_ranges",
	}

	for _, testName := range extendedTests {
		test, ok := registry.Get(testName)
		if !ok {
			t.Errorf("NewDefaultRegistry() missing extended test %q", testName)
		}
		if test == nil {
			t.Errorf("NewDefaultRegistry() has nil test for %q", testName)
		}
	}

	// Total should be 14 tests
	list := registry.List()
	if len(list) != 14 {
		t.Errorf("NewDefaultRegistry() has %d tests, want 14", len(list))
	}
}
