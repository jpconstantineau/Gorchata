package schema

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/domain/test"
	"github.com/jpconstantineau/gorchata/internal/domain/test/generic"
)

func TestBuildTestsFromSchema_SimpleColumnTest(t *testing.T) {
	// Load a simple schema
	schemaFile := filepath.Join("testdata", "simple_schema.yml")
	schema, err := ParseSchemaFile(schemaFile)
	if err != nil {
		t.Fatalf("Failed to parse schema: %v", err)
	}

	registry := generic.NewDefaultRegistry()

	tests, err := BuildTestsFromSchema([]*SchemaFile{schema}, registry)

	if err != nil {
		t.Fatalf("Failed to build tests from schema: %v", err)
	}

	if len(tests) == 0 {
		t.Fatal("Expected at least one test to be built")
	}

	// Find a specific test (e.g., not_null on users.user_id)
	var notNullTest *test.Test
	for _, tst := range tests {
		if tst.Name == "not_null" && tst.ModelName == "users" && tst.ColumnName == "user_id" {
			notNullTest = tst
			break
		}
	}

	if notNullTest == nil {
		t.Fatal("Expected to find not_null test on users.user_id")
	}

	// Verify test properties
	if notNullTest.Type != test.GenericTest {
		t.Errorf("Expected test type GenericTest, got %v", notNullTest.Type)
	}

	if notNullTest.SQLTemplate == "" {
		t.Error("Expected non-empty SQL template")
	}
}

func TestBuildTestsFromSchema_TestWithArguments(t *testing.T) {
	schemaFile := filepath.Join("testdata", "simple_schema.yml")
	schema, err := ParseSchemaFile(schemaFile)
	if err != nil {
		t.Fatalf("Failed to parse schema: %v", err)
	}

	registry := generic.NewDefaultRegistry()

	tests, err := BuildTestsFromSchema([]*SchemaFile{schema}, registry)

	if err != nil {
		t.Fatalf("Failed to build tests from schema: %v", err)
	}

	// Find accepted_values test on users.status
	var acceptedValuesTest *test.Test
	for _, tst := range tests {
		if tst.Name == "accepted_values" && tst.ModelName == "users" && tst.ColumnName == "status" {
			acceptedValuesTest = tst
			break
		}
	}

	if acceptedValuesTest == nil {
		t.Fatal("Expected to find accepted_values test on users.status")
	}

	// SQL template should contain the values from the schema
	if acceptedValuesTest.SQLTemplate == "" {
		t.Error("Expected non-empty SQL template")
	}

	// The SQL should have been generated with the 'values' argument
	// We can't check exact SQL here without knowing implementation details,
	// but we verify it was created
}

func TestBuildTestsFromSchema_RelationshipsTest(t *testing.T) {
	schemaFile := filepath.Join("testdata", "complex_schema.yml")
	schema, err := ParseSchemaFile(schemaFile)
	if err != nil {
		t.Fatalf("Failed to parse schema: %v", err)
	}

	registry := generic.NewDefaultRegistry()

	tests, err := BuildTestsFromSchema([]*SchemaFile{schema}, registry)

	if err != nil {
		t.Fatalf("Failed to build tests from schema: %v", err)
	}

	// Find relationships test on orders.user_id
	var relationshipsTest *test.Test
	for _, tst := range tests {
		if tst.Name == "relationships" && tst.ModelName == "orders" && tst.ColumnName == "user_id" {
			relationshipsTest = tst
			break
		}
	}

	if relationshipsTest == nil {
		t.Fatal("Expected to find relationships test on orders.user_id")
	}

	if relationshipsTest.SQLTemplate == "" {
		t.Error("Expected non-empty SQL template")
	}
}

func TestBuildTestsFromSchema_TableLevelTest(t *testing.T) {
	schemaFile := filepath.Join("testdata", "complex_schema.yml")
	schema, err := ParseSchemaFile(schemaFile)
	if err != nil {
		t.Fatalf("Failed to parse schema: %v", err)
	}

	registry := generic.NewDefaultRegistry()

	tests, err := BuildTestsFromSchema([]*SchemaFile{schema}, registry)

	if err != nil {
		t.Fatalf("Failed to build tests from schema: %v", err)
	}

	// Find recency test on orders (table-level test)
	var recencyTest *test.Test
	for _, tst := range tests {
		if tst.Name == "recency" && tst.ModelName == "orders" && tst.ColumnName == "" {
			recencyTest = tst
			break
		}
	}

	if recencyTest == nil {
		t.Fatal("Expected to find recency test on orders (table-level)")
	}

	if recencyTest.SQLTemplate == "" {
		t.Error("Expected non-empty SQL template")
	}

	// Table-level tests should have empty column name
	if recencyTest.ColumnName != "" {
		t.Errorf("Expected empty column name for table-level test, got '%s'", recencyTest.ColumnName)
	}
}

func TestBuildTestsFromSchema_WithSeverity(t *testing.T) {
	schemaFile := filepath.Join("testdata", "simple_schema.yml")
	schema, err := ParseSchemaFile(schemaFile)
	if err != nil {
		t.Fatalf("Failed to parse schema: %v", err)
	}

	registry := generic.NewDefaultRegistry()

	tests, err := BuildTestsFromSchema([]*SchemaFile{schema}, registry)

	if err != nil {
		t.Fatalf("Failed to build tests from schema: %v", err)
	}

	// Find accepted_values test with severity: warn
	var avTest *test.Test
	for _, tst := range tests {
		if tst.Name == "accepted_values" && tst.ModelName == "users" && tst.ColumnName == "status" {
			avTest = tst
			break
		}
	}

	if avTest == nil {
		t.Fatal("Expected to find accepted_values test on users.status")
	}

	if avTest.Config == nil {
		t.Fatal("Expected test to have config")
	}

	if avTest.Config.Severity != test.SeverityWarn {
		t.Errorf("Expected severity 'warn', got '%s'", avTest.Config.Severity)
	}
}

func TestBuildTestsFromSchema_WithWhereClause(t *testing.T) {
	// Create a schema with a where clause
	yamlContent := `version: 2
models:
  - name: users
    columns:
      - name: email
        data_tests:
          - not_null:
              where: "status = 'active'"
`

	tmpFile := filepath.Join(t.TempDir(), "schema_with_where.yml")
	err := os.WriteFile(tmpFile, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	schema, err := ParseSchemaFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to parse schema: %v", err)
	}

	registry := generic.NewDefaultRegistry()

	tests, err := BuildTestsFromSchema([]*SchemaFile{schema}, registry)

	if err != nil {
		t.Fatalf("Failed to build tests from schema: %v", err)
	}

	if len(tests) == 0 {
		t.Fatal("Expected at least one test")
	}

	testFound := tests[0]
	if testFound.Config == nil {
		t.Fatal("Expected test to have config")
	}

	if testFound.Config.Where != "status = 'active'" {
		t.Errorf("Expected where clause 'status = 'active'', got '%s'", testFound.Config.Where)
	}
}

func TestBuildTestsFromSchema_MultipleModels(t *testing.T) {
	testDir := filepath.Join("testdata", "multiple")
	schemas, err := LoadSchemaFiles(testDir)
	if err != nil {
		t.Fatalf("Failed to load schemas: %v", err)
	}

	registry := generic.NewDefaultRegistry()

	tests, err := BuildTestsFromSchema(schemas, registry)

	if err != nil {
		t.Fatalf("Failed to build tests from schemas: %v", err)
	}

	// Should have tests from both schema files
	modelNames := make(map[string]bool)
	for _, tst := range tests {
		modelNames[tst.ModelName] = true
	}

	if !modelNames["model_a"] {
		t.Error("Expected tests for model_a")
	}

	if !modelNames["model_b"] {
		t.Error("Expected tests for model_b")
	}
}

func TestBuildTestsFromSchema_TestNotInRegistry(t *testing.T) {
	yamlContent := `version: 2
models:
  - name: users
    columns:
      - name: email
        data_tests:
          - nonexistent_test
`

	tmpFile := filepath.Join(t.TempDir(), "schema_unknown_test.yml")
	err := os.WriteFile(tmpFile, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	schema, err := ParseSchemaFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to parse schema: %v", err)
	}

	registry := generic.NewDefaultRegistry()

	tests, err := BuildTestsFromSchema([]*SchemaFile{schema}, registry)

	// Should either return error or skip unknown tests
	if err != nil {
		// Error is acceptable - validates test exists
		t.Logf("Builder returned error for unknown test (expected): %v", err)
	} else {
		// Or it skipped the unknown test
		if len(tests) > 0 {
			t.Error("Expected no tests to be built for unknown test type")
		}
	}
}

func TestBuildTestsFromSchema_CustomTestName(t *testing.T) {
	yamlContent := `version: 2
models:
  - name: users
    columns:
      - name: email
        data_tests:
          - unique:
              name: email_must_be_unique
`

	tmpFile := filepath.Join(t.TempDir(), "schema_custom_name.yml")
	err := os.WriteFile(tmpFile, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	schema, err := ParseSchemaFile(tmpFile)
	if err != nil {
		t.Fatalf("Failed to parse schema: %v", err)
	}

	registry := generic.NewDefaultRegistry()

	tests, err := BuildTestsFromSchema([]*SchemaFile{schema}, registry)

	if err != nil {
		t.Fatalf("Failed to build tests from schema: %v", err)
	}

	if len(tests) == 0 {
		t.Fatal("Expected at least one test")
	}

	// Check if custom name was applied
	testFound := tests[0]
	if testFound.Config == nil {
		t.Fatal("Expected test to have config")
	}

	if testFound.Config.CustomName != "email_must_be_unique" {
		t.Errorf("Expected custom name 'email_must_be_unique', got '%s'", testFound.Config.CustomName)
	}
}

func TestBuildTestsFromSchema_EmptySchema(t *testing.T) {
	registry := generic.NewDefaultRegistry()

	tests, err := BuildTestsFromSchema([]*SchemaFile{}, registry)

	if err != nil {
		t.Fatalf("Should not error on empty schema list: %v", err)
	}

	if len(tests) != 0 {
		t.Errorf("Expected 0 tests from empty schema, got %d", len(tests))
	}
}
