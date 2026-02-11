package test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/domain/test/schema"
)

// TestHaulTruckSchemaValidation validates schema YAML structure
func TestHaulTruckSchemaValidation(t *testing.T) {
	repoRoot := getRepoRoot(t)
	schemaPath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "schema.yml")

	// Verify schema file exists
	if _, err := os.Stat(schemaPath); os.IsNotExist(err) {
		t.Fatal("Schema file does not exist: schema.yml")
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

// TestHaulTruckSchemaParsing ensures schema parses correctly
func TestHaulTruckSchemaParsing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	repoRoot := getRepoRoot(t)
	schemaPath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "schema.yml")

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

// TestHaulTruckDimensionTables verifies all required dimensions exist
func TestHaulTruckDimensionTables(t *testing.T) {
	repoRoot := getRepoRoot(t)
	schemaPath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "schema.yml")

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
		"dim_truck",
		"dim_shovel",
		"dim_crusher",
		"dim_operator",
		"dim_shift",
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

	// Verify dim_truck structure
	if truck, ok := modelMap["dim_truck"]; ok {
		expectedCols := []string{"truck_id", "model", "payload_capacity_tons", "fleet_class"}
		verifyColumns(t, truck, expectedCols)
	}

	// Verify dim_shovel structure
	if shovel, ok := modelMap["dim_shovel"]; ok {
		expectedCols := []string{"shovel_id", "bucket_size_m3", "pit_zone"}
		verifyColumns(t, shovel, expectedCols)
	}

	// Verify dim_crusher structure
	if crusher, ok := modelMap["dim_crusher"]; ok {
		expectedCols := []string{"crusher_id", "capacity_tph"}
		verifyColumns(t, crusher, expectedCols)
	}

	// Verify dim_operator structure
	if operator, ok := modelMap["dim_operator"]; ok {
		expectedCols := []string{"operator_id", "experience_level"}
		verifyColumns(t, operator, expectedCols)
	}

	// Verify dim_shift structure
	if shift, ok := modelMap["dim_shift"]; ok {
		expectedCols := []string{"shift_id", "shift_name", "start_time", "end_time"}
		verifyColumns(t, shift, expectedCols)
	}

	// Verify dim_date structure
	if date, ok := modelMap["dim_date"]; ok {
		expectedCols := []string{"date_key", "full_date", "year", "quarter", "month", "week", "day_of_week"}
		verifyColumns(t, date, expectedCols)
	}
}

// TestHaulTruckStagingTables validates raw telemetry staging structure
func TestHaulTruckStagingTables(t *testing.T) {
	repoRoot := getRepoRoot(t)
	schemaPath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "schema.yml")

	schemaObj, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema: %v", err)
	}

	// Build model map
	modelMap := make(map[string]*schema.ModelSchema)
	for i, model := range schemaObj.Models {
		modelMap[model.Name] = &schemaObj.Models[i]
	}

	// Required staging tables
	requiredStaging := []string{
		"stg_telemetry_events",
		"stg_truck_states",
	}

	for _, stagingName := range requiredStaging {
		model, exists := modelMap[stagingName]
		if !exists {
			t.Errorf("Required staging table %s not found in schema", stagingName)
			continue
		}

		// Verify staging has columns
		if len(model.Columns) == 0 {
			t.Errorf("Staging table %s has no columns defined", stagingName)
		}
	}

	// Verify stg_telemetry_events structure
	if telemetry, ok := modelMap["stg_telemetry_events"]; ok {
		expectedCols := []string{
			"truck_id",
			"timestamp",
			"gps_lat",
			"gps_lon",
			"speed_kmh",
			"payload_tons",
			"suspension_pressure_psi",
			"engine_rpm",
			"fuel_level_liters",
			"engine_hours",
			"geofence_zone",
		}
		verifyColumns(t, telemetry, expectedCols)
	}

	// Verify stg_truck_states structure
	if states, ok := modelMap["stg_truck_states"]; ok {
		expectedCols := []string{
			"truck_id",
			"state_start",
			"state_end",
			"operational_state",
			"location_zone",
			"payload_at_start",
			"payload_at_end",
		}
		verifyColumns(t, states, expectedCols)
	}
}

// TestHaulTruckFactTables verifies haul cycle fact table and metrics structure
func TestHaulTruckFactTables(t *testing.T) {
	repoRoot := getRepoRoot(t)
	schemaPath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "schema.yml")

	schemaObj, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema: %v", err)
	}

	// Build model map
	modelMap := make(map[string]*schema.ModelSchema)
	for i, model := range schemaObj.Models {
		modelMap[model.Name] = &schemaObj.Models[i]
	}

	// Required fact table
	factName := "fact_haul_cycle"
	model, exists := modelMap[factName]
	if !exists {
		t.Fatalf("Required fact table %s not found in schema", factName)
	}

	// Verify fact has columns
	if len(model.Columns) == 0 {
		t.Errorf("Fact table %s has no columns defined", factName)
	}

	// Verify fact_haul_cycle structure
	expectedCols := []string{
		"cycle_id",
		"truck_id",
		"shovel_id",
		"crusher_id",
		"operator_id",
		"shift_id",
		"date_id",
		"cycle_start",
		"cycle_end",
		"payload_tons",
		"distance_loaded_km",
		"distance_empty_km",
		"duration_loading_min",
		"duration_hauling_loaded_min",
		"duration_queue_crusher_min",
		"duration_dumping_min",
		"duration_returning_min",
		"duration_queue_shovel_min",
		"duration_spot_delays_min",
		"fuel_consumed_liters",
		"speed_avg_loaded_kmh",
		"speed_avg_empty_kmh",
	}
	verifyColumns(t, model, expectedCols)
}

// TestPayloadThresholdLogic validates loaded/empty state thresholds
func TestPayloadThresholdLogic(t *testing.T) {
	repoRoot := getRepoRoot(t)
	schemaPath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "schema.yml")

	schemaObj, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema: %v", err)
	}

	// Build model map
	modelMap := make(map[string]*schema.ModelSchema)
	for i, model := range schemaObj.Models {
		modelMap[model.Name] = &schemaObj.Models[i]
	}

	// Verify stg_truck_states exists and has operational_state column
	states, exists := modelMap["stg_truck_states"]
	if !exists {
		t.Fatal("stg_truck_states not found in schema")
	}

	// Verify operational_state column exists
	hasOperationalState := false
	for _, col := range states.Columns {
		if col.Name == "operational_state" {
			hasOperationalState = true

			// Verify accepted_values test exists for operational_state
			hasAcceptedValues := false
			for _, test := range col.DataTests {
				if testMap, ok := test.(map[string]interface{}); ok {
					if valuesObj, hasAccepted := testMap["accepted_values"]; hasAccepted {
						hasAcceptedValues = true

						// Verify all required operational states are present
						if valuesMap, ok := valuesObj.(map[string]interface{}); ok {
							if valuesList, ok := valuesMap["values"].([]interface{}); ok {
								requiredStates := []string{
									"queued_at_shovel",
									"loading",
									"hauling_loaded",
									"queued_at_crusher",
									"dumping",
									"returning_empty",
									"spot_delay",
									"idle",
								}

								stateSet := make(map[string]bool)
								for _, val := range valuesList {
									if strVal, ok := val.(string); ok {
										stateSet[strVal] = true
									}
								}

								for _, requiredState := range requiredStates {
									if !stateSet[requiredState] {
										t.Errorf("Missing required operational_state: %s", requiredState)
									}
								}
							}
						}
						break
					}
				}
			}

			if !hasAcceptedValues {
				t.Error("operational_state should have accepted_values validation")
			}
			break
		}
	}

	if !hasOperationalState {
		t.Error("stg_truck_states missing operational_state column")
	}
}
