package config

import (
	"os"
	"path/filepath"
	"testing"
)

// TestParseSchemaYAML_SeedsSection tests parsing of seeds section with column_types
func TestParseSchemaYAML_SeedsSection(t *testing.T) {
	yamlContent := `
version: 2

seeds:
  - name: raw_sales
    config:
      column_types:
        sale_id: INTEGER
        sale_date: TEXT
        amount: REAL
        customer_id: INTEGER
  - name: raw_orders
    config:
      column_types:
        order_id: INTEGER
        status: TEXT
`

	// Write to temp file
	tmpFile := tempFilePath(t, "schema_test.yml")
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}
	defer os.Remove(tmpFile)

	// Parse schema
	schema, err := ParseSchemaYAML(tmpFile)
	if err != nil {
		t.Fatalf("ParseSchemaYAML failed: %v", err)
	}

	// Verify version
	if schema.Version != 2 {
		t.Errorf("Expected version 2, got %d", schema.Version)
	}

	// Verify seeds
	if len(schema.Seeds) != 2 {
		t.Fatalf("Expected 2 seeds, got %d", len(schema.Seeds))
	}

	// Verify first seed
	seed1 := schema.Seeds[0]
	if seed1.Name != "raw_sales" {
		t.Errorf("Expected name 'raw_sales', got '%s'", seed1.Name)
	}
	if len(seed1.Config.ColumnTypes) != 4 {
		t.Errorf("Expected 4 column types, got %d", len(seed1.Config.ColumnTypes))
	}
	if seed1.Config.ColumnTypes["sale_id"] != "INTEGER" {
		t.Errorf("Expected sale_id type 'INTEGER', got '%s'", seed1.Config.ColumnTypes["sale_id"])
	}
	if seed1.Config.ColumnTypes["sale_date"] != "TEXT" {
		t.Errorf("Expected sale_date type 'TEXT', got '%s'", seed1.Config.ColumnTypes["sale_date"])
	}
	if seed1.Config.ColumnTypes["amount"] != "REAL" {
		t.Errorf("Expected amount type 'REAL', got '%s'", seed1.Config.ColumnTypes["amount"])
	}

	// Verify second seed
	seed2 := schema.Seeds[1]
	if seed2.Name != "raw_orders" {
		t.Errorf("Expected name 'raw_orders', got '%s'", seed2.Name)
	}
	if len(seed2.Config.ColumnTypes) != 2 {
		t.Errorf("Expected 2 column types, got %d", len(seed2.Config.ColumnTypes))
	}
}

// TestParseSchemaYAML_EmptySeedsSection tests schema with no seeds
func TestParseSchemaYAML_EmptySeedsSection(t *testing.T) {
	yamlContent := `
version: 2
`

	tmpFile := tempFilePath(t, "schema_empty.yml")
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}
	defer os.Remove(tmpFile)

	schema, err := ParseSchemaYAML(tmpFile)
	if err != nil {
		t.Fatalf("ParseSchemaYAML failed: %v", err)
	}

	if schema.Version != 2 {
		t.Errorf("Expected version 2, got %d", schema.Version)
	}

	if len(schema.Seeds) != 0 {
		t.Errorf("Expected 0 seeds, got %d", len(schema.Seeds))
	}
}

// TestParseSchemaYAML_SeedWithoutColumnTypes tests seed without column_types
func TestParseSchemaYAML_SeedWithoutColumnTypes(t *testing.T) {
	yamlContent := `
version: 2

seeds:
  - name: simple_seed
`

	tmpFile := tempFilePath(t, "schema_no_types.yml")
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}
	defer os.Remove(tmpFile)

	schema, err := ParseSchemaYAML(tmpFile)
	if err != nil {
		t.Fatalf("ParseSchemaYAML failed: %v", err)
	}

	if len(schema.Seeds) != 1 {
		t.Fatalf("Expected 1 seed, got %d", len(schema.Seeds))
	}

	seed := schema.Seeds[0]
	if seed.Name != "simple_seed" {
		t.Errorf("Expected name 'simple_seed', got '%s'", seed.Name)
	}

	// Should have empty column types, not nil
	if seed.Config.ColumnTypes == nil {
		t.Error("Expected ColumnTypes map to be initialized, got nil")
	}
	if len(seed.Config.ColumnTypes) != 0 {
		t.Errorf("Expected 0 column types, got %d", len(seed.Config.ColumnTypes))
	}
}

// TestGetSeedConfig tests retrieving configuration for a specific seed
func TestGetSeedConfig(t *testing.T) {
	schema := &SchemaYAML{
		Version: 2,
		Seeds: []SeedSchema{
			{
				Name: "seed1",
				Config: SeedSchemaConfig{
					ColumnTypes: map[string]string{
						"col1": "INTEGER",
					},
				},
			},
			{
				Name: "seed2",
				Config: SeedSchemaConfig{
					ColumnTypes: map[string]string{
						"col2": "TEXT",
					},
				},
			},
		},
	}

	// Test finding existing seed
	config, found := schema.GetSeedConfig("seed1")
	if !found {
		t.Error("Expected to find seed1")
	}
	if config.ColumnTypes["col1"] != "INTEGER" {
		t.Errorf("Expected col1 type 'INTEGER', got '%s'", config.ColumnTypes["col1"])
	}

	// Test not finding non-existent seed
	_, found = schema.GetSeedConfig("nonexistent")
	if found {
		t.Error("Expected not to find nonexistent seed")
	}
}

// tempFilePath creates a temporary file path for testing
func tempFilePath(t *testing.T, name string) string {
	return filepath.Join(t.TempDir(), name)
}
