package test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/config"
	"github.com/jpconstantineau/gorchata/internal/domain/test/executor"
	"github.com/jpconstantineau/gorchata/internal/domain/test/generic"
	"github.com/jpconstantineau/gorchata/internal/domain/test/schema"
)

// TestUnitTrainSchemaValidation validates schema YAML structure
func TestUnitTrainSchemaValidation(t *testing.T) {
	repoRoot := getRepoRoot(t)
	schemaPath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "schema.yml")

	// Verify schema file exists
	if _, err := os.Stat(schemaPath); os.IsNotExist(err) {
		t.Fatal("Schema file does not exist: models/schema.yml")
	}

	// Parse schema
	schemaObj, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema: %v", err)
	}

	// Verify version
	if schemaObj.Version != 2 {
		t.Errorf("Expected schema version 2, got %d", schemaObj.Version)
	}

	// Verify we have models defined
	if len(schemaObj.Models) == 0 {
		t.Fatal("Schema has no models defined")
	}
}

// TestUnitTrainSchemaParsing ensures schema parses correctly
func TestUnitTrainSchemaParsing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	repoRoot := getRepoRoot(t)
	exampleDir := filepath.Join(repoRoot, "examples", "unit_train_analytics")
	schemaPath := filepath.Join(exampleDir, "models", "schema.yml")

	// Parse schema
	schemaObj, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema: %v", err)
	}

	// Verify models are defined
	if len(schemaObj.Models) == 0 {
		t.Fatal("No models found in schema")
	}

	// Build model map for easy lookup
	modelMap := make(map[string]*schema.ModelSchema)
	for i, model := range schemaObj.Models {
		modelMap[model.Name] = &schemaObj.Models[i]
	}

	// Verify all models have descriptions
	for _, model := range schemaObj.Models {
		if model.Description == "" {
			t.Errorf("Model %s is missing description", model.Name)
		}

		// Verify all columns have descriptions
		for _, col := range model.Columns {
			if col.Description == "" {
				t.Errorf("Column %s.%s is missing description", model.Name, col.Name)
			}
		}
	}
}

// TestUnitTrainDimensionTables verifies all required dimension tables exist
func TestUnitTrainDimensionTables(t *testing.T) {
	repoRoot := getRepoRoot(t)
	schemaPath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "schema.yml")

	schemaObj, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema: %v", err)
	}

	// Build model map
	modelMap := make(map[string]*schema.ModelSchema)
	for i, model := range schemaObj.Models {
		modelMap[model.Name] = &schemaObj.Models[i]
	}

	// Required dimension tables
	requiredDims := []string{
		"dim_train",
		"dim_car",
		"dim_location",
		"dim_corridor",
		"dim_date",
	}

	for _, dimName := range requiredDims {
		model, exists := modelMap[dimName]
		if !exists {
			t.Errorf("Required dimension table %s not found in schema", dimName)
			continue
		}

		// Verify dimension has columns
		if len(model.Columns) == 0 {
			t.Errorf("Dimension table %s has no columns defined", dimName)
		}
	}

	// Verify dim_train structure
	if train, ok := modelMap["dim_train"]; ok {
		expectedCols := []string{"train_id", "train_name", "num_cars", "formed_at", "completed_at"}
		verifyColumns(t, train, expectedCols)
	}

	// Verify dim_car structure
	if car, ok := modelMap["dim_car"]; ok {
		expectedCols := []string{"car_id", "car_type", "capacity_tons"}
		verifyColumns(t, car, expectedCols)
	}

	// Verify dim_location structure with origin/destination/station types
	if location, ok := modelMap["dim_location"]; ok {
		expectedCols := []string{"location_id", "location_name", "location_type", "avg_queue_hours"}
		verifyColumns(t, location, expectedCols)
	}

	// Verify dim_corridor structure
	if corridor, ok := modelMap["dim_corridor"]; ok {
		expectedCols := []string{"corridor_id", "origin_location_id", "destination_location_id", "transit_time_class"}
		verifyColumns(t, corridor, expectedCols)
	}

	// Verify dim_date structure
	if date, ok := modelMap["dim_date"]; ok {
		expectedCols := []string{"date_key", "full_date", "year", "quarter", "month", "week", "day_of_week"}
		verifyColumns(t, date, expectedCols)
	}
}

// TestUnitTrainFactTables verifies fact table structure including power inference table
func TestUnitTrainFactTables(t *testing.T) {
	repoRoot := getRepoRoot(t)
	schemaPath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "schema.yml")

	schemaObj, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema: %v", err)
	}

	// Build model map
	modelMap := make(map[string]*schema.ModelSchema)
	for i, model := range schemaObj.Models {
		modelMap[model.Name] = &schemaObj.Models[i]
	}

	// Required fact tables
	requiredFacts := []string{
		"fact_car_location_event",
		"fact_train_trip",
		"fact_straggler",
		"fact_inferred_power_transfer",
	}

	for _, factName := range requiredFacts {
		model, exists := modelMap[factName]
		if !exists {
			t.Errorf("Required fact table %s not found in schema", factName)
			continue
		}

		// Verify fact has columns
		if len(model.Columns) == 0 {
			t.Errorf("Fact table %s has no columns defined", factName)
		}
	}

	// Verify fact_car_location_event structure (CLM input)
	if carEvent, ok := modelMap["fact_car_location_event"]; ok {
		expectedCols := []string{"event_id", "event_timestamp", "car_id", "train_id", "location_id", "event_type"}
		verifyColumns(t, carEvent, expectedCols)

		// Verify event_type has accepted_values test for CLM event types
		hasEventTypeValidation := false
		for _, col := range carEvent.Columns {
			if col.Name == "event_type" {
				for _, test := range col.DataTests {
					if testMap, ok := test.(map[string]interface{}); ok {
						if _, hasAccepted := testMap["accepted_values"]; hasAccepted {
							hasEventTypeValidation = true
							break
						}
					}
				}
			}
		}
		if !hasEventTypeValidation {
			t.Error("fact_car_location_event.event_type should have accepted_values validation")
		}
	}

	// Verify fact_train_trip structure
	if trip, ok := modelMap["fact_train_trip"]; ok {
		expectedCols := []string{"trip_id", "train_id", "corridor_id", "origin_queue_hours", "destination_queue_hours", "transit_hours"}
		verifyColumns(t, trip, expectedCols)
	}

	// Verify fact_straggler structure
	if straggler, ok := modelMap["fact_straggler"]; ok {
		expectedCols := []string{"straggler_id", "car_id", "train_id", "set_out_timestamp", "picked_up_timestamp", "delay_hours"}
		verifyColumns(t, straggler, expectedCols)
	}

	// Verify fact_inferred_power_transfer structure
	if power, ok := modelMap["fact_inferred_power_transfer"]; ok {
		expectedCols := []string{"transfer_id", "train_id", "location_id", "transfer_timestamp", "gap_hours", "inferred_same_power"}
		verifyColumns(t, power, expectedCols)
	}
}

// TestUnitTrainCorridorLogic validates corridor combination logic
func TestUnitTrainCorridorLogic(t *testing.T) {
	repoRoot := getRepoRoot(t)
	schemaPath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "schema.yml")

	schemaObj, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema: %v", err)
	}

	// Build model map
	modelMap := make(map[string]*schema.ModelSchema)
	for i, model := range schemaObj.Models {
		modelMap[model.Name] = &schemaObj.Models[i]
	}

	// Verify dim_corridor exists
	corridor, exists := modelMap["dim_corridor"]
	if !exists {
		t.Fatal("dim_corridor not found in schema")
	}

	// Verify corridor defines origin + destination + transit_time_class
	requiredCorridorCols := []string{
		"corridor_id",
		"origin_location_id",
		"destination_location_id",
		"transit_time_class",
	}

	colMap := make(map[string]bool)
	for _, col := range corridor.Columns {
		colMap[col.Name] = true
	}

	for _, requiredCol := range requiredCorridorCols {
		if !colMap[requiredCol] {
			t.Errorf("dim_corridor missing required column: %s", requiredCol)
		}
	}

	// Verify transit_time_class has accepted_values validation
	hasTransitTimeValidation := false
	for _, col := range corridor.Columns {
		if col.Name == "transit_time_class" {
			for _, test := range col.DataTests {
				if testMap, ok := test.(map[string]interface{}); ok {
					if _, hasAccepted := testMap["accepted_values"]; hasAccepted {
						hasTransitTimeValidation = true
						break
					}
				}
			}
		}
	}

	if !hasTransitTimeValidation {
		t.Error("dim_corridor.transit_time_class should have accepted_values validation (2-day, 3-day, 4-day)")
	}

	// Verify corridor should have description mentioning it combines origin+destination+time
	if corridor.Description == "" {
		t.Error("dim_corridor missing description explaining combination logic")
	}
}

// TestUnitTrainTestDiscovery verifies that data quality tests can be discovered from schema
func TestUnitTrainTestDiscovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	repoRoot := getRepoRoot(t)
	exampleDir := filepath.Join(repoRoot, "examples", "unit_train_analytics")

	// Change to example directory
	originalDir, _ := os.Getwd()
	defer os.Chdir(originalDir)
	if err := os.Chdir(exampleDir); err != nil {
		t.Fatalf("Failed to change to example directory: %v", err)
	}

	// Load config
	cfg, err := config.Discover("dev")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Initialize registry
	registry := generic.NewDefaultRegistry()

	// Discover all tests
	tests, err := executor.DiscoverAllTests(cfg, registry)
	if err != nil {
		t.Fatalf("Failed to discover tests: %v", err)
	}

	// Verify we have some tests discovered
	if len(tests) == 0 {
		t.Error("No tests discovered from schema")
	}

	// Verify we have generic tests from schema
	hasGeneric := false
	for _, test := range tests {
		if test.Type == "generic" {
			hasGeneric = true
			break
		}
	}

	if !hasGeneric {
		t.Error("No generic tests discovered from schema")
	}
}

// Helper function to verify columns exist in a model
func verifyColumns(t *testing.T, model *schema.ModelSchema, expectedCols []string) {
	t.Helper()

	colMap := make(map[string]bool)
	for _, col := range model.Columns {
		colMap[col.Name] = true
	}

	for _, expectedCol := range expectedCols {
		if !colMap[expectedCol] {
			t.Errorf("Model %s missing expected column: %s", model.Name, expectedCol)
		}
	}
}

// Helper function to get repository root
func getRepoRoot(t *testing.T) string {
	t.Helper()

	// Start from test directory and walk up to find go.mod
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working directory: %v", err)
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("Could not find repository root (go.mod not found)")
		}
		dir = parent
	}
}
