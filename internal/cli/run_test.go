package cli

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

// TestRunCommand tests basic run workflow with test database
func TestRunCommand(t *testing.T) {
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
`
	if err := os.WriteFile(filepath.Join(tmpDir, "gorchata_project.yml"), []byte(projectConfig), 0644); err != nil {
		t.Fatal(err)
	}

	// Create profiles config with database path
	profilesConfig := fmt.Sprintf(`
default:
  target: dev
  outputs:
    dev:
      type: sqlite
      database: %s
`, dbPath)
	if err := os.WriteFile(filepath.Join(tmpDir, "profiles.yml"), []byte(profilesConfig), 0644); err != nil {
		t.Fatal(err)
	}

	// Create models directory
	modelsDir := filepath.Join(tmpDir, "models")
	if err := os.MkdirAll(modelsDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create a simple model that creates a table
	modelContent := `
-- Materialization: table
CREATE TABLE test_table (
  id INTEGER PRIMARY KEY,
  value TEXT
);

INSERT INTO test_table (id, value) VALUES (1, 'test');
`
	if err := os.WriteFile(filepath.Join(modelsDir, "test_model.sql"), []byte(modelContent), 0644); err != nil {
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

	// Run command
	err = RunCommand([]string{})
	if err != nil {
		t.Errorf("RunCommand() error = %v, want nil", err)
	}

	// Verify table was created
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var name string
	err = db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name='test_table'").Scan(&name)
	if err != nil {
		t.Errorf("Table test_table not found: %v", err)
	}
	if name != "test_table" {
		t.Errorf("Expected table 'test_table', got '%s'", name)
	}
}

// TestRunWithTarget tests run with --target flag
func TestRunWithTarget(t *testing.T) {
	// Create temp directory for test project
	tmpDir := t.TempDir()

	// Create test databases
	devDB := filepath.Join(tmpDir, "dev.db")
	prodDB := filepath.Join(tmpDir, "prod.db")

	// Create project config
	projectConfig := `
name: test_project
version: 1.0.0
model_paths:
  - models
`
	if err := os.WriteFile(filepath.Join(tmpDir, "gorchata_project.yml"), []byte(projectConfig), 0644); err != nil {
		t.Fatal(err)
	}

	// Create profiles config with multiple targets
	profilesConfig := fmt.Sprintf(`
default:
  target: dev
  outputs:
    dev:
      type: sqlite
      database: %s
    prod:
      type: sqlite
      database: %s
`, devDB, prodDB)
	if err := os.WriteFile(filepath.Join(tmpDir, "profiles.yml"), []byte(profilesConfig), 0644); err != nil {
		t.Fatal(err)
	}

	// Create models directory
	modelsDir := filepath.Join(tmpDir, "models")
	if err := os.MkdirAll(modelsDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create a simple model
	modelContent := `CREATE TABLE target_test (id INTEGER)`
	if err := os.WriteFile(filepath.Join(modelsDir, "target_test.sql"), []byte(modelContent), 0644); err != nil {
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

	// Run with --target prod
	err = RunCommand([]string{"--target", "prod"})
	if err != nil {
		t.Errorf("RunCommand() error = %v, want nil", err)
	}

	// Verify table was created in prod database, not dev
	db, err := sql.Open("sqlite", prodDB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var name string
	err = db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name='target_test'").Scan(&name)
	if err != nil {
		t.Errorf("Table target_test not found in prod database: %v", err)
	}
}

// TestRunWithModels tests run with --models flag
func TestRunWithModels(t *testing.T) {
	// Create temp directory
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create project config
	projectConfig := `
name: test_project
version: 1.0.0
model_paths:
  - models
`
	if err := os.WriteFile(filepath.Join(tmpDir, "gorchata_project.yml"), []byte(projectConfig), 0644); err != nil {
		t.Fatal(err)
	}

	// Create profiles config
	profilesConfig := fmt.Sprintf(`
default:
  target: dev
  outputs:
    dev:
      type: sqlite
      database: %s
`, dbPath)
	if err := os.WriteFile(filepath.Join(tmpDir, "profiles.yml"), []byte(profilesConfig), 0644); err != nil {
		t.Fatal(err)
	}

	// Create models directory
	modelsDir := filepath.Join(tmpDir, "models")
	if err := os.MkdirAll(modelsDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create two models
	model1 := `CREATE TABLE model1 (id INTEGER)`
	model2 := `CREATE TABLE model2 (id INTEGER)`

	if err := os.WriteFile(filepath.Join(modelsDir, "model1.sql"), []byte(model1), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(modelsDir, "model2.sql"), []byte(model2), 0644); err != nil {
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

	// Run only model1
	err = RunCommand([]string{"--models", "model1"})
	if err != nil {
		t.Errorf("RunCommand() error = %v, want nil", err)
	}

	// Verify only model1 table was created
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Check model1 exists
	var name1 string
	err = db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name='model1'").Scan(&name1)
	if err != nil {
		t.Errorf("Table model1 not found: %v", err)
	}

	// Check model2 does NOT exist
	var name2 string
	err = db.QueryRowContext(context.Background(), "SELECT name FROM sqlite_master WHERE type='table' AND name='model2'").Scan(&name2)
	if err == nil {
		t.Error("Table model2 should not have been created")
	}
}

// TestRunFailFast tests --fail-fast flag stops on first error
func TestRunFailFast(t *testing.T) {
	// Create temp directory
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create project config
	projectConfig := `
name: test_project
version: 1.0.0
model_paths:
  - models
`
	if err := os.WriteFile(filepath.Join(tmpDir, "gorchata_project.yml"), []byte(projectConfig), 0644); err != nil {
		t.Fatal(err)
	}

	// Create profiles config
	profilesConfig := fmt.Sprintf(`
default:
  target: dev
  outputs:
    dev:
      type: sqlite
      database: %s
`, dbPath)
	if err := os.WriteFile(filepath.Join(tmpDir, "profiles.yml"), []byte(profilesConfig), 0644); err != nil {
		t.Fatal(err)
	}

	// Create models directory
	modelsDir := filepath.Join(tmpDir, "models")
	if err := os.MkdirAll(modelsDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create models - first one will fail
	badModel := `SELECT * FROM nonexistent_table`
	goodModel := `CREATE TABLE should_not_run (id INTEGER)`

	if err := os.WriteFile(filepath.Join(modelsDir, "bad_model.sql"), []byte(badModel), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(modelsDir, "good_model.sql"), []byte(goodModel), 0644); err != nil {
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

	// Run with --fail-fast - should fail on bad_model
	err = RunCommand([]string{"--fail-fast"})
	if err == nil {
		t.Error("RunCommand() should return error when model fails with --fail-fast")
	}
}

// TestRunDependencyOrder tests models run in correct dependency order
func TestRunDependencyOrder(t *testing.T) {
	// Create temp directory
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create project config
	projectConfig := `
name: test_project
version: 1.0.0
model_paths:
  - models
`
	if err := os.WriteFile(filepath.Join(tmpDir, "gorchata_project.yml"), []byte(projectConfig), 0644); err != nil {
		t.Fatal(err)
	}

	// Create profiles config
	profilesConfig := fmt.Sprintf(`
default:
  target: dev
  outputs:
    dev:
      type: sqlite
      database: %s
`, dbPath)
	if err := os.WriteFile(filepath.Join(tmpDir, "profiles.yml"), []byte(profilesConfig), 0644); err != nil {
		t.Fatal(err)
	}

	// Create models directory
	modelsDir := filepath.Join(tmpDir, "models")
	if err := os.MkdirAll(modelsDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create models with dependencies
	// base model - creates a table
	baseModel := `CREATE TABLE base (id INTEGER)`

	// dependent model - references base
	dependentModel := `
-- This depends on base
-- {{ ref "base" }}
CREATE TABLE dependent AS SELECT * FROM base
`

	if err := os.WriteFile(filepath.Join(modelsDir, "base.sql"), []byte(baseModel), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(modelsDir, "dependent.sql"), []byte(dependentModel), 0644); err != nil {
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

	// Run - should execute base before dependent
	err = RunCommand([]string{})
	if err != nil {
		t.Errorf("RunCommand() error = %v, want nil", err)
	}

	// Verify both tables were created
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Check both tables exist
	var count int
	err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('base', 'dependent')").Scan(&count)
	if err != nil {
		t.Errorf("Failed to query tables: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 tables, got %d", count)
	}
}

// TestRunMissingConfig tests error when config is missing
func TestRunMissingConfig(t *testing.T) {
	// Create temp directory
	tmpDir := t.TempDir()

	// Change to temp directory (no config files)
	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(oldDir)

	if err := os.Chdir(tmpDir); err != nil {
		t.Fatal(err)
	}

	// Run command - should fail
	err = RunCommand([]string{})
	if err == nil {
		t.Error("RunCommand() should return error when config is missing")
	}
	if !strings.Contains(err.Error(), "config") && !strings.Contains(err.Error(), "not found") {
		t.Errorf("RunCommand() error = %v, want error about missing config", err)
	}
}
