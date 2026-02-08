package cli

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBuildCommand_Basic(t *testing.T) {
	// Create temp directory for test project
	tmpDir := t.TempDir()

	// Create test database
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create project config
	projectConfig := `
name: test_project
version: 1.0.0
model_paths:
  - models
test_paths:
  - tests
`
	if err := os.WriteFile(filepath.Join(tmpDir, "gorchata_project.yml"), []byte(projectConfig), 0644); err != nil {
		t.Fatal(err)
	}

	// Create profiles config
	profilesConfig := `
default:
  target: dev
  outputs:
    dev:
      type: sqlite
      database: ` + dbPath + `
`
	if err := os.WriteFile(filepath.Join(tmpDir, "profiles.yml"), []byte(profilesConfig), 0644); err != nil {
		t.Fatal(err)
	}

	// Create models directory with a simple model
	modelsDir := filepath.Join(tmpDir, "models")
	if err := os.MkdirAll(modelsDir, 0755); err != nil {
		t.Fatal(err)
	}

	modelContent := `
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL
);

INSERT INTO users (id, email) VALUES (1, 'test@example.com');
`
	if err := os.WriteFile(filepath.Join(modelsDir, "users.sql"), []byte(modelContent), 0644); err != nil {
		t.Fatal(err)
	}

	// Create tests directory (empty for now)
	testsDir := filepath.Join(tmpDir, "tests")
	if err := os.MkdirAll(testsDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Change to temp directory
	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(oldDir)

	if err := os.Chdir(tmpDir); err != nil {
		t.Fatal(err)
	}

	// Run build command (should run models and tests)
	// Note: This will pass even without tests since we have no tests defined
	err = BuildCommand([]string{})
	if err != nil {
		t.Errorf("BuildCommand() error = %v, want nil", err)
	}
}

func TestBuildCommand_WithTests(t *testing.T) {
	// Create temp directory for test project
	tmpDir := t.TempDir()

	// Create test database
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create project config
	projectConfig := `
name: test_project
version: 1.0.0
model_paths:
  - models
test_paths:
  - tests
`
	if err := os.WriteFile(filepath.Join(tmpDir, "gorchata_project.yml"), []byte(projectConfig), 0644); err != nil {
		t.Fatal(err)
	}

	// Create profiles config
	profilesConfig := `
default:
  target: dev
  outputs:
    dev:
      type: sqlite
      database: ` + dbPath + `
`
	if err := os.WriteFile(filepath.Join(tmpDir, "profiles.yml"), []byte(profilesConfig), 0644); err != nil {
		t.Fatal(err)
	}

	// Create models directory with a model
	modelsDir := filepath.Join(tmpDir, "models")
	if err := os.MkdirAll(modelsDir, 0755); err != nil {
		t.Fatal(err)
	}

	modelContent := `
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL
);

INSERT INTO users (id, email) VALUES (1, 'test@example.com');
`
	if err := os.WriteFile(filepath.Join(modelsDir, "users.sql"), []byte(modelContent), 0644); err != nil {
		t.Fatal(err)
	}

	// Create tests directory with a passing test
	testsDir := filepath.Join(tmpDir, "tests")
	if err := os.MkdirAll(testsDir, 0755); err != nil {
		t.Fatal(err)
	}

	testContent := `-- Test: check_users_not_empty
-- Model: users
SELECT * FROM users WHERE id IS NULL
`
	if err := os.WriteFile(filepath.Join(testsDir, "check_users.sql"), []byte(testContent), 0644); err != nil {
		t.Fatal(err)
	}

	// Change to temp directory
	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(oldDir)

	if err := os.Chdir(tmpDir); err != nil {
		t.Fatal(err)
	}

	// Run build command
	err = BuildCommand([]string{})
	if err != nil {
		t.Errorf("BuildCommand() error = %v, want nil", err)
	}
}
