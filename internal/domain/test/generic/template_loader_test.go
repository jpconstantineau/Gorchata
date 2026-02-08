package generic

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadCustomGenericTests_FromDirectory(t *testing.T) {
	// Create temp directory
	tempDir := t.TempDir()

	// Create test file
	testFile := filepath.Join(tempDir, "test_positive_values.sql")
	content := `{% test positive_values(model, column_name) %}
SELECT * FROM {{ model }} WHERE {{ column_name }} <= 0
{% endtest %}`

	if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	// Create registry
	registry := NewRegistry()

	// Load custom tests
	err := LoadCustomGenericTests(tempDir, registry)
	if err != nil {
		t.Fatalf("LoadCustomGenericTests failed: %v", err)
	}

	// Verify test was registered
	test, ok := registry.Get("positive_values")
	if !ok {
		t.Errorf("Expected test 'positive_values' to be registered")
	}

	if test.Name() != "positive_values" {
		t.Errorf("Expected test name 'positive_values', got '%s'", test.Name())
	}
}

func TestLoadCustomGenericTests_EmptyDirectory(t *testing.T) {
	tempDir := t.TempDir()
	registry := NewRegistry()

	err := LoadCustomGenericTests(tempDir, registry)
	if err != nil {
		t.Fatalf("LoadCustomGenericTests failed: %v", err)
	}

	// Registry should be empty (or only have default tests)
	initialCount := len(registry.List())

	// Should not error on empty directory
	if initialCount > 0 {
		t.Logf("Registry has %d tests (expected behavior if defaults exist)", initialCount)
	}
}

func TestLoadCustomGenericTests_MultipleFiles(t *testing.T) {
	tempDir := t.TempDir()

	// Create multiple test files
	test1 := filepath.Join(tempDir, "test_one.sql")
	test2 := filepath.Join(tempDir, "test_two.sql")

	content1 := `{% test test_one(model, column_name) %}
SELECT * FROM {{ model }} WHERE {{ column_name }} IS NULL
{% endtest %}`

	content2 := `{% test test_two(model, column_name) %}
SELECT * FROM {{ model }} WHERE {{ column_name }} = ''
{% endtest %}`

	if err := os.WriteFile(test1, []byte(content1), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}
	if err := os.WriteFile(test2, []byte(content2), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	registry := NewRegistry()
	err := LoadCustomGenericTests(tempDir, registry)
	if err != nil {
		t.Fatalf("LoadCustomGenericTests failed: %v", err)
	}

	// Verify both tests were registered
	_, ok1 := registry.Get("test_one")
	_, ok2 := registry.Get("test_two")

	if !ok1 {
		t.Errorf("Expected test 'test_one' to be registered")
	}
	if !ok2 {
		t.Errorf("Expected test 'test_two' to be registered")
	}
}

func TestLoadCustomGenericTests_IgnoresNonSQLFiles(t *testing.T) {
	tempDir := t.TempDir()

	// Create SQL and non-SQL files
	sqlFile := filepath.Join(tempDir, "test_valid.sql")
	txtFile := filepath.Join(tempDir, "readme.txt")

	sqlContent := `{% test test_valid(model, column_name) %}
SELECT * FROM {{ model }}
{% endtest %}`

	if err := os.WriteFile(sqlFile, []byte(sqlContent), 0644); err != nil {
		t.Fatalf("Failed to write SQL file: %v", err)
	}
	if err := os.WriteFile(txtFile, []byte("Not a test"), 0644); err != nil {
		t.Fatalf("Failed to write TXT file: %v", err)
	}

	registry := NewRegistry()
	err := LoadCustomGenericTests(tempDir, registry)
	if err != nil {
		t.Fatalf("LoadCustomGenericTests failed: %v", err)
	}

	// Should only have the SQL test
	test, ok := registry.Get("test_valid")
	if !ok {
		t.Errorf("Expected test 'test_valid' to be registered")
	}

	if test.Name() != "test_valid" {
		t.Errorf("Expected test name 'test_valid', got '%s'", test.Name())
	}
}

func TestLoadCustomGenericTests_InvalidTemplate(t *testing.T) {
	tempDir := t.TempDir()

	// Create invalid test file (missing endtest tag)
	testFile := filepath.Join(tempDir, "test_invalid.sql")
	content := `{% test invalid_test(model, column_name) %}
SELECT * FROM {{ model }}
-- The endtest tag is missing here`

	if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	registry := NewRegistry()
	err := LoadCustomGenericTests(tempDir, registry)

	// Should return error for invalid template
	if err == nil {
		t.Errorf("Expected error for invalid template, got nil")
	}
}

func TestLoadCustomGenericTests_NonexistentDirectory(t *testing.T) {
	registry := NewRegistry()
	err := LoadCustomGenericTests("/nonexistent/directory", registry)

	// Should return error for nonexistent directory
	if err == nil {
		t.Errorf("Expected error for nonexistent directory, got nil")
	}
}

func TestLoadCustomGenericTests_RecursiveSearch(t *testing.T) {
	tempDir := t.TempDir()
	subDir := filepath.Join(tempDir, "subdir")

	if err := os.MkdirAll(subDir, 0755); err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}

	// Create test in subdirectory
	testFile := filepath.Join(subDir, "test_nested.sql")
	content := `{% test test_nested(model, column_name) %}
SELECT * FROM {{ model }} WHERE {{ column_name }} IS NULL
{% endtest %}`

	if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	registry := NewRegistry()
	err := LoadCustomGenericTests(tempDir, registry)
	if err != nil {
		t.Fatalf("LoadCustomGenericTests failed: %v", err)
	}

	// Verify nested test was found
	test, ok := registry.Get("test_nested")
	if !ok {
		t.Errorf("Expected nested test 'test_nested' to be registered")
	}

	if test.Name() != "test_nested" {
		t.Errorf("Expected test name 'test_nested', got '%s'", test.Name())
	}
}

func TestLoadCustomGenericTests_WithArguments(t *testing.T) {
	tempDir := t.TempDir()

	testFile := filepath.Join(tempDir, "test_range.sql")
	content := `{% test range_check(model, column_name, min_value, max_value) %}
SELECT * FROM {{ model }} 
WHERE {{ column_name }} < {{ min_value }} 
   OR {{ column_name }} > {{ max_value }}
{% endtest %}`

	if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	registry := NewRegistry()
	err := LoadCustomGenericTests(tempDir, registry)
	if err != nil {
		t.Fatalf("LoadCustomGenericTests failed: %v", err)
	}

	test, ok := registry.Get("range_check")
	if !ok {
		t.Errorf("Expected test 'range_check' to be registered")
	}

	// Try to use the test
	args := map[string]interface{}{
		"min_value": 0,
		"max_value": 100,
	}

	sql, err := test.GenerateSQL("my_table", "my_column", args)
	if err != nil {
		t.Errorf("GenerateSQL failed: %v", err)
	}

	if sql == "" {
		t.Errorf("Expected non-empty SQL")
	}
}
