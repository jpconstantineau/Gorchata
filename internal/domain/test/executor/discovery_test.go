package executor

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/config"
	"github.com/jpconstantineau/gorchata/internal/domain/test/generic"
)

func TestDiscoverAllTests_EmptyDirectory(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &config.Config{
		Project: &config.ProjectConfig{
			TestPaths:  []string{filepath.Join(tmpDir, "tests")},
			ModelPaths: []string{filepath.Join(tmpDir, "models")},
		},
	}

	// Create empty directories
	os.MkdirAll(filepath.Join(tmpDir, "tests"), 0755)
	os.MkdirAll(filepath.Join(tmpDir, "models"), 0755)

	registry := generic.NewDefaultRegistry()

	tests, err := DiscoverAllTests(cfg, registry)

	if err != nil {
		t.Errorf("DiscoverAllTests() error = %v, want nil", err)
	}
	if len(tests) != 0 {
		t.Errorf("DiscoverAllTests() found %d tests, want 0", len(tests))
	}
}

func TestDiscoverAllTests_WithSingularTests(t *testing.T) {
	tmpDir := t.TempDir()
	testsDir := filepath.Join(tmpDir, "tests")
	modelsDir := filepath.Join(tmpDir, "models")

	os.MkdirAll(testsDir, 0755)
	os.MkdirAll(modelsDir, 0755)

	// Create a singular test file
	testContent := `-- Test: check_active_users
-- Model: users
SELECT * FROM users WHERE active = 1 AND last_login IS NULL
`
	testFile := filepath.Join(testsDir, "check_active_users.sql")
	if err := os.WriteFile(testFile, []byte(testContent), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{
		Project: &config.ProjectConfig{
			TestPaths:  []string{testsDir},
			ModelPaths: []string{modelsDir},
		},
	}

	registry := generic.NewDefaultRegistry()

	tests, err := DiscoverAllTests(cfg, registry)

	if err != nil {
		t.Errorf("DiscoverAllTests() error = %v, want nil", err)
	}
	if len(tests) != 1 {
		t.Errorf("DiscoverAllTests() found %d tests, want 1", len(tests))
	}
}

func TestDiscoverAllTests_WithSchemaTests(t *testing.T) {
	tmpDir := t.TempDir()
	testsDir := filepath.Join(tmpDir, "tests")
	modelsDir := filepath.Join(tmpDir, "models")

	os.MkdirAll(testsDir, 0755)
	os.MkdirAll(modelsDir, 0755)

	// Create a schema file with tests
	schemaContent := `
version: 1
models:
  - name: users
    columns:
      - name: id
        data_tests:
          - unique
          - not_null
      - name: email
        data_tests:
          - not_null
`
	schemaFile := filepath.Join(modelsDir, "schema.yml")
	if err := os.WriteFile(schemaFile, []byte(schemaContent), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{
		Project: &config.ProjectConfig{
			TestPaths:  []string{testsDir},
			ModelPaths: []string{modelsDir},
		},
	}

	registry := generic.NewDefaultRegistry()

	tests, err := DiscoverAllTests(cfg, registry)

	if err != nil {
		t.Errorf("DiscoverAllTests() error = %v, want nil", err)
	}
	// Should find: unique_users_id, not_null_users_id, not_null_users_email  = 3 tests
	if len(tests) < 3 {
		t.Errorf("DiscoverAllTests() found %d tests, want at least 3", len(tests))
	}
}

func TestDiscoverAllTests_MixedTests(t *testing.T) {
	tmpDir := t.TempDir()
	testsDir := filepath.Join(tmpDir, "tests")
	modelsDir := filepath.Join(tmpDir, "models")

	os.MkdirAll(testsDir, 0755)
	os.MkdirAll(modelsDir, 0755)

	// Create a singular test
	singularContent := `-- Test: custom_check
-- Model: orders
SELECT * FROM orders WHERE total < 0
`
	if err := os.WriteFile(filepath.Join(testsDir, "custom_check.sql"), []byte(singularContent), 0644); err != nil {
		t.Fatal(err)
	}

	// Create a schema file
	schemaContent := `
version: 1
models:
  - name: users
    columns:
      - name: id
        data_tests:
          - not_null
`
	if err := os.WriteFile(filepath.Join(modelsDir, "schema.yml"), []byte(schemaContent), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{
		Project: &config.ProjectConfig{
			TestPaths:  []string{testsDir},
			ModelPaths: []string{modelsDir},
		},
	}

	registry := generic.NewDefaultRegistry()

	tests, err := DiscoverAllTests(cfg, registry)

	if err != nil {
		t.Errorf("DiscoverAllTests() error = %v, want nil", err)
	}
	// Should find: 1 singular test + 1 schema test = 2 tests
	if len(tests) < 2 {
		t.Errorf("DiscoverAllTests() found %d tests, want at least 2", len(tests))
	}
}

func TestDiscoverAllTests_NonExistentTestPath(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &config.Config{
		Project: &config.ProjectConfig{
			TestPaths:  []string{filepath.Join(tmpDir, "nonexistent")},
			ModelPaths: []string{filepath.Join(tmpDir, "models")},
		},
	}

	registry := generic.NewDefaultRegistry()

	tests, err := DiscoverAllTests(cfg, registry)

	// Should not error, just return empty list
	if err != nil {
		t.Errorf("DiscoverAllTests() error = %v, want nil", err)
	}
	if tests == nil {
		t.Error("DiscoverAllTests() should return empty slice, not nil")
	}
}

func TestDiscoverAllTests_MultipleTestPaths(t *testing.T) {
	tmpDir := t.TempDir()
	testsDir1 := filepath.Join(tmpDir, "tests1")
	testsDir2 := filepath.Join(tmpDir, "tests2")
	modelsDir := filepath.Join(tmpDir, "models")

	os.MkdirAll(testsDir1, 0755)
	os.MkdirAll(testsDir2, 0755)
	os.MkdirAll(modelsDir, 0755)

	// Create test in first directory
	test1Content := `-- Test: test1
-- Model: users
SELECT 1
`
	if err := os.WriteFile(filepath.Join(testsDir1, "test1.sql"), []byte(test1Content), 0644); err != nil {
		t.Fatal(err)
	}

	// Create test in second directory
	test2Content := `-- Test: test2
-- Model: orders
SELECT 1
`
	if err := os.WriteFile(filepath.Join(testsDir2, "test2.sql"), []byte(test2Content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{
		Project: &config.ProjectConfig{
			TestPaths:  []string{testsDir1, testsDir2},
			ModelPaths: []string{modelsDir},
		},
	}

	registry := generic.NewDefaultRegistry()

	tests, err := DiscoverAllTests(cfg, registry)

	if err != nil {
		t.Errorf("DiscoverAllTests() error = %v, want nil", err)
	}
	if len(tests) != 2 {
		t.Errorf("DiscoverAllTests() found %d tests, want 2", len(tests))
	}
}

func TestDiscoverAllTests_NilConfig(t *testing.T) {
	registry := generic.NewDefaultRegistry()

	tests, err := DiscoverAllTests(nil, registry)

	if err == nil {
		t.Error("DiscoverAllTests() with nil config should return error")
	}
	if tests != nil {
		t.Error("DiscoverAllTests() with nil config should return nil tests")
	}
}

func TestDiscoverAllTests_NilRegistry(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &config.Config{
		Project: &config.ProjectConfig{
			TestPaths:  []string{filepath.Join(tmpDir, "tests")},
			ModelPaths: []string{filepath.Join(tmpDir, "models")},
		},
	}

	tests, err := DiscoverAllTests(cfg, nil)

	if err == nil {
		t.Error("DiscoverAllTests() with nil registry should return error")
	}
	if tests != nil {
		t.Error("DiscoverAllTests() with nil registry should return nil tests")
	}
}

func TestDiscoverAllTests_NestedDirectories(t *testing.T) {
	tmpDir := t.TempDir()
	testsDir := filepath.Join(tmpDir, "tests")
	nestedDir := filepath.Join(testsDir, "nested", "deep")
	modelsDir := filepath.Join(tmpDir, "models")

	os.MkdirAll(nestedDir, 0755)
	os.MkdirAll(modelsDir, 0755)

	// Create test in nested directory
	testContent := `-- Test: nested_test
-- Model: users
SELECT 1
`
	if err := os.WriteFile(filepath.Join(nestedDir, "nested_test.sql"), []byte(testContent), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{
		Project: &config.ProjectConfig{
			TestPaths:  []string{testsDir},
			ModelPaths: []string{modelsDir},
		},
	}

	registry := generic.NewDefaultRegistry()

	tests, err := DiscoverAllTests(cfg, registry)

	if err != nil {
		t.Errorf("DiscoverAllTests() error = %v, want nil", err)
	}
	if len(tests) != 1 {
		t.Errorf("DiscoverAllTests() should find test in nested directory, found %d", len(tests))
	}
}
