package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCompileCommand tests basic compile workflow
func TestCompileCommand(t *testing.T) {
	// Create temp directory for test project
	tmpDir := t.TempDir()

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
	profilesConfig := `
default:
  target: dev
  outputs:
    dev:
      type: sqlite
      database: test.db
`
	if err := os.WriteFile(filepath.Join(tmpDir, "profiles.yml"), []byte(profilesConfig), 0644); err != nil {
		t.Fatal(err)
	}

	// Create models directory
	modelsDir := filepath.Join(tmpDir, "models")
	if err := os.MkdirAll(modelsDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create a simple model
	modelContent := `
-- Model: simple_model
-- Materialization: table

SELECT 1 as id, 'test' as value
`
	if err := os.WriteFile(filepath.Join(modelsDir, "simple_model.sql"), []byte(modelContent), 0644); err != nil {
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

	// Run compile command
	err = CompileCommand([]string{})
	if err != nil {
		t.Errorf("CompileCommand() error = %v, want nil", err)
	}
}

// TestCompileWithModels tests compile with --models flag
func TestCompileWithModels(t *testing.T) {
	// Create temp directory for test project
	tmpDir := t.TempDir()

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
	profilesConfig := `
default:
  target: dev
  outputs:
    dev:
      type: sqlite
      database: test.db
`
	if err := os.WriteFile(filepath.Join(tmpDir, "profiles.yml"), []byte(profilesConfig), 0644); err != nil {
		t.Fatal(err)
	}

	// Create models directory
	modelsDir := filepath.Join(tmpDir, "models")
	if err := os.MkdirAll(modelsDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create multiple models
	model1 := `SELECT 1 as id`
	model2 := `SELECT 2 as id`

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

	// Run compile command with specific model
	err = CompileCommand([]string{"--models", "model1"})
	if err != nil {
		t.Errorf("CompileCommand() error = %v, want nil", err)
	}
}

// TestCompileOutputDir tests compile with --output-dir flag
func TestCompileOutputDir(t *testing.T) {
	// Create temp directory for test project
	tmpDir := t.TempDir()

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
	profilesConfig := `
default:
  target: dev
  outputs:
    dev:
      type: sqlite
      database: test.db
`
	if err := os.WriteFile(filepath.Join(tmpDir, "profiles.yml"), []byte(profilesConfig), 0644); err != nil {
		t.Fatal(err)
	}

	// Create models directory
	modelsDir := filepath.Join(tmpDir, "models")
	if err := os.MkdirAll(modelsDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create a simple model
	modelContent := `SELECT 1 as id`
	if err := os.WriteFile(filepath.Join(modelsDir, "test_model.sql"), []byte(modelContent), 0644); err != nil {
		t.Fatal(err)
	}

	// Create output directory
	outputDir := filepath.Join(tmpDir, "compiled")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
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

	// Run compile command with output directory
	err = CompileCommand([]string{"--output-dir", outputDir})
	if err != nil {
		t.Errorf("CompileCommand() error = %v, want nil", err)
	}

	// Check if output file was created
	outputFile := filepath.Join(outputDir, "test_model.sql")
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Errorf("Expected output file %s to exist", outputFile)
	}
}

// TestCompileMissingConfig tests error when config files are missing
func TestCompileMissingConfig(t *testing.T) {
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

	// Run compile command - should fail
	err = CompileCommand([]string{})
	if err == nil {
		t.Error("CompileCommand() should return error when config is missing")
	}
	if !strings.Contains(err.Error(), "config") && !strings.Contains(err.Error(), "not found") {
		t.Errorf("CompileCommand() error = %v, want error about missing config", err)
	}
}

// TestCompileInvalidSQL tests handling of template syntax errors
func TestCompileInvalidSQL(t *testing.T) {
	// Create temp directory for test project
	tmpDir := t.TempDir()

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
	profilesConfig := `
default:
  target: dev
  outputs:
    dev:
      type: sqlite
      database: test.db
`
	if err := os.WriteFile(filepath.Join(tmpDir, "profiles.yml"), []byte(profilesConfig), 0644); err != nil {
		t.Fatal(err)
	}

	// Create models directory
	modelsDir := filepath.Join(tmpDir, "models")
	if err := os.MkdirAll(modelsDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create a model with invalid template syntax
	modelContent := `SELECT {{ invalid template }} FROM table`
	if err := os.WriteFile(filepath.Join(modelsDir, "bad_model.sql"), []byte(modelContent), 0644); err != nil {
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

	// Run compile command - should handle error gracefully
	err = CompileCommand([]string{})
	if err == nil {
		t.Error("CompileCommand() should return error for invalid template")
	}
}
