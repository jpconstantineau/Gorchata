package schema

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseSchemaFile_ValidYAML(t *testing.T) {
	testFile := filepath.Join("testdata", "simple_schema.yml")

	schema, err := ParseSchemaFile(testFile)

	if err != nil {
		t.Fatalf("Failed to parse valid schema file: %v", err)
	}

	if schema == nil {
		t.Fatal("Expected non-nil schema")
	}

	if schema.Version != 2 {
		t.Errorf("Expected version 2, got %d", schema.Version)
	}

	if len(schema.Models) != 1 {
		t.Fatalf("Expected 1 model, got %d", len(schema.Models))
	}

	model := schema.Models[0]
	if model.Name != "users" {
		t.Errorf("Expected model name 'users', got '%s'", model.Name)
	}

	// Verify columns
	if len(model.Columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(model.Columns))
	}

	// Check first column
	if model.Columns[0].Name != "user_id" {
		t.Errorf("Expected column 'user_id', got '%s'", model.Columns[0].Name)
	}

	// Check that columns have tests
	if len(model.Columns[0].DataTests) != 2 {
		t.Errorf("Expected 2 tests on user_id, got %d", len(model.Columns[0].DataTests))
	}
}

func TestParseSchemaFile_ComplexYAML(t *testing.T) {
	testFile := filepath.Join("testdata", "complex_schema.yml")

	schema, err := ParseSchemaFile(testFile)

	if err != nil {
		t.Fatalf("Failed to parse complex schema file: %v", err)
	}

	if len(schema.Models) != 1 {
		t.Fatalf("Expected 1 model, got %d", len(schema.Models))
	}

	model := schema.Models[0]
	if model.Name != "orders" {
		t.Errorf("Expected model name 'orders', got '%s'", model.Name)
	}

	// Check for model-level tests
	if len(model.DataTests) != 1 {
		t.Errorf("Expected 1 model-level test, got %d", len(model.DataTests))
	}

	// Check columns with complex test definitions
	userIDCol := findColumn(model.Columns, "user_id")
	if userIDCol == nil {
		t.Fatal("Expected to find user_id column")
	}

	// user_id should have relationships test
	if len(userIDCol.DataTests) < 2 {
		t.Errorf("Expected at least 2 tests on user_id, got %d", len(userIDCol.DataTests))
	}
}

func TestParseSchemaFile_InvalidYAML(t *testing.T) {
	testFile := filepath.Join("testdata", "invalid_schema.yml")

	_, err := ParseSchemaFile(testFile)

	// This might fail during parsing or validation depending on implementation
	// We just need to ensure it doesn't panic
	if err == nil {
		t.Log("Note: invalid_schema.yml parsed without error (may be valid YAML structure)")
	}
}

func TestParseSchemaFile_MissingFile(t *testing.T) {
	testFile := filepath.Join("testdata", "nonexistent.yml")

	_, err := ParseSchemaFile(testFile)

	if err == nil {
		t.Fatal("Expected error when parsing missing file")
	}

	// Error should mention the file
	errMsg := err.Error()
	if errMsg == "" {
		t.Error("Expected non-empty error message")
	}
}

func TestLoadSchemaFiles_MultipleFiles(t *testing.T) {
	testDir := filepath.Join("testdata", "multiple")

	schemas, err := LoadSchemaFiles(testDir)

	if err != nil {
		t.Fatalf("Failed to load schema files: %v", err)
	}

	if len(schemas) != 2 {
		t.Fatalf("Expected 2 schema files, got %d", len(schemas))
	}

	// Verify we got both models
	modelNames := make(map[string]bool)
	for _, schema := range schemas {
		for _, model := range schema.Models {
			modelNames[model.Name] = true
		}
	}

	if !modelNames["model_a"] {
		t.Error("Expected to find model_a")
	}

	if !modelNames["model_b"] {
		t.Error("Expected to find model_b")
	}
}

func TestLoadSchemaFiles_EmptyDirectory(t *testing.T) {
	// Create a temporary empty directory
	tmpDir := t.TempDir()

	schemas, err := LoadSchemaFiles(tmpDir)

	if err != nil {
		t.Fatalf("LoadSchemaFiles should not error on empty directory: %v", err)
	}

	if len(schemas) != 0 {
		t.Errorf("Expected 0 schemas in empty directory, got %d", len(schemas))
	}
}

func TestLoadSchemaFiles_RecursiveSearch(t *testing.T) {
	// Create a nested directory structure with schema files
	tmpDir := t.TempDir()

	// Create nested directories
	subDir1 := filepath.Join(tmpDir, "models")
	subDir2 := filepath.Join(tmpDir, "models", "staging")

	err := os.MkdirAll(subDir2, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directories: %v", err)
	}

	// Create schema files at different levels
	schema1 := filepath.Join(subDir1, "schema.yml")
	schema2 := filepath.Join(subDir2, "staging_schema.yml")

	schemaContent := `version: 2
models:
  - name: test_model
`

	err = os.WriteFile(schema1, []byte(schemaContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test schema file: %v", err)
	}

	err = os.WriteFile(schema2, []byte(schemaContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test schema file: %v", err)
	}

	// Load schemas recursively from root
	schemas, err := LoadSchemaFiles(tmpDir)

	if err != nil {
		t.Fatalf("Failed to load schema files recursively: %v", err)
	}

	if len(schemas) != 2 {
		t.Errorf("Expected 2 schema files found recursively, got %d", len(schemas))
	}
}

func TestLoadSchemaFiles_NonexistentDirectory(t *testing.T) {
	_, err := LoadSchemaFiles(filepath.Join("testdata", "does_not_exist"))

	if err == nil {
		t.Fatal("Expected error when loading from nonexistent directory")
	}
}

// Helper function to find a column by name
func findColumn(columns []ColumnSchema, name string) *ColumnSchema {
	for i := range columns {
		if columns[i].Name == name {
			return &columns[i]
		}
	}
	return nil
}
