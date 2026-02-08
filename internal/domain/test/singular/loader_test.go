package singular

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/domain/test"
)

func TestLoadTestFromFile_ValidSQL(t *testing.T) {
	// Create temp directory
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test_example.sql")

	// Write test SQL
	content := `-- config(severity='warn')
SELECT * FROM my_table WHERE value < 0
`
	if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	// Load test
	testResult, err := loadTestFromFile(testFile)
	if err != nil {
		t.Fatalf("loadTestFromFile failed: %v", err)
	}

	// Verify test properties
	if testResult.Name != "test_example" {
		t.Errorf("Expected name 'test_example', got '%s'", testResult.Name)
	}
	if testResult.Type != test.SingularTest {
		t.Errorf("Expected type 'singular', got '%s'", testResult.Type)
	}
	if testResult.SQLTemplate != content {
		t.Errorf("SQL content mismatch")
	}
	if testResult.Config.Severity != test.SeverityWarn {
		t.Errorf("Expected severity 'warn', got '%s'", testResult.Config.Severity)
	}
}

func TestLoadTestFromFile_InvalidPath(t *testing.T) {
	_, err := loadTestFromFile("/nonexistent/path/test.sql")
	if err == nil {
		t.Errorf("Expected error for nonexistent file, got nil")
	}
}

func TestLoadSingularTests_ValidDirectory(t *testing.T) {
	// Create temp directory with test files
	tempDir := t.TempDir()

	test1 := filepath.Join(tempDir, "test_one.sql")
	test2 := filepath.Join(tempDir, "test_two.sql")

	if err := os.WriteFile(test1, []byte("SELECT 1"), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}
	if err := os.WriteFile(test2, []byte("SELECT 2"), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	// Load tests
	tests, err := LoadSingularTests(tempDir)
	if err != nil {
		t.Fatalf("LoadSingularTests failed: %v", err)
	}

	if len(tests) != 2 {
		t.Errorf("Expected 2 tests, got %d", len(tests))
	}

	// Verify test names (could be in any order)
	names := make(map[string]bool)
	for _, test := range tests {
		names[test.Name] = true
	}

	if !names["test_one"] {
		t.Errorf("Expected test 'test_one' not found")
	}
	if !names["test_two"] {
		t.Errorf("Expected test 'test_two' not found")
	}
}

func TestLoadSingularTests_EmptyDirectory(t *testing.T) {
	tempDir := t.TempDir()

	tests, err := LoadSingularTests(tempDir)
	if err != nil {
		t.Fatalf("LoadSingularTests failed: %v", err)
	}

	if len(tests) != 0 {
		t.Errorf("Expected 0 tests, got %d", len(tests))
	}
}

func TestLoadSingularTests_RecursiveSearch(t *testing.T) {
	// Create temp directory structure
	tempDir := t.TempDir()
	subDir := filepath.Join(tempDir, "subdir")

	if err := os.MkdirAll(subDir, 0755); err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}

	test1 := filepath.Join(tempDir, "test_root.sql")
	test2 := filepath.Join(subDir, "test_nested.sql")

	if err := os.WriteFile(test1, []byte("SELECT 1"), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}
	if err := os.WriteFile(test2, []byte("SELECT 2"), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	// Load tests
	tests, err := LoadSingularTests(tempDir)
	if err != nil {
		t.Fatalf("LoadSingularTests failed: %v", err)
	}

	if len(tests) != 2 {
		t.Errorf("Expected 2 tests (recursive), got %d", len(tests))
	}

	// Verify both tests were found
	names := make(map[string]bool)
	for _, test := range tests {
		names[test.Name] = true
	}

	if !names["test_root"] {
		t.Errorf("Expected test 'test_root' not found")
	}
	if !names["test_nested"] {
		t.Errorf("Expected test 'test_nested' not found")
	}
}

func TestLoadSingularTests_IgnoresNonSQLFiles(t *testing.T) {
	tempDir := t.TempDir()

	// Create both .sql and non-.sql files
	sqlFile := filepath.Join(tempDir, "test_valid.sql")
	txtFile := filepath.Join(tempDir, "readme.txt")

	if err := os.WriteFile(sqlFile, []byte("SELECT 1"), 0644); err != nil {
		t.Fatalf("Failed to write SQL file: %v", err)
	}
	if err := os.WriteFile(txtFile, []byte("Not SQL"), 0644); err != nil {
		t.Fatalf("Failed to write TXT file: %v", err)
	}

	// Load tests
	tests, err := LoadSingularTests(tempDir)
	if err != nil {
		t.Fatalf("LoadSingularTests failed: %v", err)
	}

	if len(tests) != 1 {
		t.Errorf("Expected 1 test (ignoring .txt), got %d", len(tests))
	}

	if tests[0].Name != "test_valid" {
		t.Errorf("Expected test 'test_valid', got '%s'", tests[0].Name)
	}
}

func TestLoadSingularTests_NonexistentDirectory(t *testing.T) {
	_, err := LoadSingularTests("/nonexistent/directory")
	if err == nil {
		t.Errorf("Expected error for nonexistent directory, got nil")
	}
}
