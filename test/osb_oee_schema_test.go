package test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/domain/test/schema"
)

// TestOSBSchemaValidation validates schema YAML structure
func TestOSBSchemaValidation(t *testing.T) {
	repoRoot := getRepoRoot(t)
	schemaPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "schema.yml")

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

// TestOSBSchemaParsing ensures schema parses correctly
func TestOSBSchemaParsing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	repoRoot := getRepoRoot(t)
	schemaPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "schema.yml")

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

// TestOSBDimensionTables verifies all required dimensions exist
func TestOSBDimensionTables(t *testing.T) {
	repoRoot := getRepoRoot(t)
	schemaPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "schema.yml")

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
		"dim_equipment",
		"dim_production_area",
		"dim_reason_code",
		"dim_shift",
		"dim_date",
		"dim_product_spec",
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

	// Verify dim_equipment structure
	if equipment, ok := modelMap["dim_equipment"]; ok {
		expectedCols := []string{
			"equipment_id", "equipment_name", "equipment_type", "production_area",
			"ideal_cycle_time_sec", "rated_capacity_units_hr", "installation_date",
			"criticality_level",
		}
		verifyColumns(t, equipment, expectedCols)
	}

	// Verify dim_production_area structure
	if area, ok := modelMap["dim_production_area"]; ok {
		expectedCols := []string{
			"area_id", "area_name", "sequence_order", "upstream_area_id",
			"downstream_area_id", "buffer_capacity_hours",
		}
		verifyColumns(t, area, expectedCols)
	}

	// Verify dim_reason_code structure
	if reasonCode, ok := modelMap["dim_reason_code"]; ok {
		expectedCols := []string{
			"reason_code_id", "reason_code", "reason_category", "oee_time_model_class",
			"oee_loss_type", "equipment_type", "typical_duration_min", "maintenance_action_required",
		}
		verifyColumns(t, reasonCode, expectedCols)
	}

	// Verify dim_shift structure
	if shift, ok := modelMap["dim_shift"]; ok {
		expectedCols := []string{"shift_id", "shift_name", "shift_start_time", "shift_end_time", "crew_size"}
		verifyColumns(t, shift, expectedCols)
	}

	// Verify dim_date structure
	if date, ok := modelMap["dim_date"]; ok {
		expectedCols := []string{"date_key", "full_date", "year", "quarter", "month", "week", "day_of_week"}
		verifyColumns(t, date, expectedCols)
	}

	// Verify dim_product_spec structure
	if product, ok := modelMap["dim_product_spec"]; ok {
		expectedCols := []string{
			"product_id", "thickness_inches", "density_lbft3", "grade",
			"target_thickness", "thickness_tolerance_plus", "thickness_tolerance_minus",
			"target_density", "density_tolerance",
		}
		verifyColumns(t, product, expectedCols)
	}
}

// TestOSBStagingTables validates raw event staging structure
func TestOSBStagingTables(t *testing.T) {
	repoRoot := getRepoRoot(t)
	schemaPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "schema.yml")

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
		"stg_machine_events",
		"stg_equipment_state_history",
		"stg_production_output",
		"stg_quality_tests",
		"stg_buffer_levels",
	}

	for _, stagingName := range requiredStaging {
		model, exists := modelMap[stagingName]
		if !exists {
			t.Errorf("Required staging table %s not found in schema", stagingName)
			continue
		}

		// Verify staging table has columns
		if len(model.Columns) == 0 {
			t.Errorf("Staging table %s has no columns defined", stagingName)
		}
	}

	// Verify stg_machine_events structure
	if events, ok := modelMap["stg_machine_events"]; ok {
		expectedCols := []string{"event_id", "equipment_id", "event_timestamp", "state", "reason_code_id", "operator_notes"}
		verifyColumns(t, events, expectedCols)
	}

	// Verify stg_equipment_state_history structure
	if stateHistory, ok := modelMap["stg_equipment_state_history"]; ok {
		expectedCols := []string{
			"equipment_id", "state_start_timestamp", "state_end_timestamp",
			"state_duration_min", "machine_state", "reason_code_id", "shift_id",
		}
		verifyColumns(t, stateHistory, expectedCols)
	}

	// Verify stg_production_output structure
	if production, ok := modelMap["stg_production_output"]; ok {
		expectedCols := []string{"equipment_id", "production_timestamp", "output_quantity", "product_id", "batch_id"}
		verifyColumns(t, production, expectedCols)
	}

	// Verify stg_quality_tests structure
	if quality, ok := modelMap["stg_quality_tests"]; ok {
		expectedCols := []string{
			"test_id", "batch_id", "test_timestamp", "test_type", "measured_thickness",
			"measured_density", "pass_fail", "defect_type",
		}
		verifyColumns(t, quality, expectedCols)
	}

	// Verify stg_buffer_levels structure
	if buffer, ok := modelMap["stg_buffer_levels"]; ok {
		expectedCols := []string{"buffer_name", "timestamp", "inventory_level_tons", "capacity_utilization_pct", "hours_of_supply"}
		verifyColumns(t, buffer, expectedCols)
	}
}

// TestOSBFactTables verifies equipment state facts, production facts, and quality facts
func TestOSBFactTables(t *testing.T) {
	repoRoot := getRepoRoot(t)
	schemaPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "schema.yml")

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
		"fact_equipment_state",
		"fact_production_output",
		"fact_quality_results",
	}

	for _, factName := range requiredFacts {
		model, exists := modelMap[factName]
		if !exists {
			t.Errorf("Required fact table %s not found in schema", factName)
			continue
		}

		// Verify fact table has columns
		if len(model.Columns) == 0 {
			t.Errorf("Fact table %s has no columns defined", factName)
		}
	}

	// Verify fact_equipment_state structure
	if equipState, ok := modelMap["fact_equipment_state"]; ok {
		expectedCols := []string{
			"equipment_id", "shift_id", "date_id", "state_start", "state_end",
			"duration_min", "machine_state", "reason_code_id", "oee_loss_category",
		}
		verifyColumns(t, equipState, expectedCols)
	}

	// Verify fact_production_output structure
	if prodOutput, ok := modelMap["fact_production_output"]; ok {
		expectedCols := []string{
			"production_id", "equipment_id", "shift_id", "date_id", "timestamp",
			"quantity", "product_id", "batch_id", "cycle_time_sec", "performance_pct",
		}
		verifyColumns(t, prodOutput, expectedCols)
	}

	// Verify fact_quality_results structure
	if qualResults, ok := modelMap["fact_quality_results"]; ok {
		expectedCols := []string{
			"quality_id", "batch_id", "product_id", "date_id", "panels_produced",
			"panels_tested", "panels_passed", "panels_downgraded", "panels_scrapped",
			"thickness_avg", "thickness_stdev", "density_avg", "density_stdev",
		}
		verifyColumns(t, qualResults, expectedCols)
	}
}

// TestReasonCodeOEEMapping validates reason codes correctly map to Planned/Unplanned and OEE loss categories
func TestReasonCodeOEEMapping(t *testing.T) {
	repoRoot := getRepoRoot(t)
	schemaPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "schema.yml")

	schemaObj, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema: %v", err)
	}

	// Find dim_reason_code model
	var reasonCodeModel *schema.ModelSchema
	for i, model := range schemaObj.Models {
		if model.Name == "dim_reason_code" {
			reasonCodeModel = &schemaObj.Models[i]
			break
		}
	}

	if reasonCodeModel == nil {
		t.Fatal("dim_reason_code table not found")
	}

	// Verify oee_time_model_class column has accepted_values constraint
	var oeeTMCCol *schema.ColumnSchema
	for i, col := range reasonCodeModel.Columns {
		if col.Name == "oee_time_model_class" {
			oeeTMCCol = &reasonCodeModel.Columns[i]
			break
		}
	}

	if oeeTMCCol == nil {
		t.Fatal("oee_time_model_class column not found in dim_reason_code")
	}

	// Check for accepted_values test
	hasAcceptedValues := false
	for _, test := range oeeTMCCol.DataTests {
		if testMap, ok := test.(map[string]interface{}); ok {
			if valuesObj, hasAccepted := testMap["accepted_values"]; hasAccepted {
				hasAcceptedValues = true
				// Verify it includes 'Planned' and 'Unplanned'
				if valuesMap, ok := valuesObj.(map[string]interface{}); ok {
					if valuesList, ok := valuesMap["values"].([]interface{}); ok {
						hasPlanned := false
						hasUnplanned := false
						for _, val := range valuesList {
							if strVal, ok := val.(string); ok {
								if strVal == "Planned" {
									hasPlanned = true
								}
								if strVal == "Unplanned" {
									hasUnplanned = true
								}
							}
						}
						if !hasPlanned || !hasUnplanned {
							t.Error("oee_time_model_class accepted_values must include 'Planned' and 'Unplanned'")
						}
					}
				}
				break
			}
		}
	}

	if !hasAcceptedValues {
		t.Error("oee_time_model_class should have accepted_values data test")
	}

	// Verify oee_loss_type column has accepted_values constraint
	var oeeLossCol *schema.ColumnSchema
	for i, col := range reasonCodeModel.Columns {
		if col.Name == "oee_loss_type" {
			oeeLossCol = &reasonCodeModel.Columns[i]
			break
		}
	}

	if oeeLossCol == nil {
		t.Fatal("oee_loss_type column not found in dim_reason_code")
	}

	// Check for accepted_values test
	hasAcceptedValues = false
	for _, test := range oeeLossCol.DataTests {
		if testMap, ok := test.(map[string]interface{}); ok {
			if valuesObj, hasAccepted := testMap["accepted_values"]; hasAccepted {
				hasAcceptedValues = true
				// Verify it includes 'Availability', 'Performance', 'Quality'
				if valuesMap, ok := valuesObj.(map[string]interface{}); ok {
					if valuesList, ok := valuesMap["values"].([]interface{}); ok {
						expectedLossTypes := []string{"Availability", "Performance", "Quality"}
						for _, expectedType := range expectedLossTypes {
							found := false
							for _, val := range valuesList {
								if strVal, ok := val.(string); ok {
									if strVal == expectedType {
										found = true
										break
									}
								}
							}
							if !found {
								t.Errorf("oee_loss_type accepted_values should include '%s'", expectedType)
							}
						}
					}
				}
				break
			}
		}
	}

	if !hasAcceptedValues {
		t.Error("oee_loss_type should have accepted_values data test")
	}
}

// TestBufferInventoryTracking validates buffer level tracking structure
func TestBufferInventoryTracking(t *testing.T) {
	repoRoot := getRepoRoot(t)
	schemaPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "schema.yml")

	schemaObj, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema: %v", err)
	}

	// Build model map
	modelMap := make(map[string]*schema.ModelSchema)
	for i, model := range schemaObj.Models {
		modelMap[model.Name] = &schemaObj.Models[i]
	}

	// Verify dim_production_area has buffer_capacity_hours
	if area, ok := modelMap["dim_production_area"]; ok {
		hasBufferCapacity := false
		for _, col := range area.Columns {
			if col.Name == "buffer_capacity_hours" {
				hasBufferCapacity = true
				break
			}
		}
		if !hasBufferCapacity {
			t.Error("dim_production_area should have buffer_capacity_hours column")
		}
	}

	// Verify stg_buffer_levels has necessary tracking columns
	if buffer, ok := modelMap["stg_buffer_levels"]; ok {
		requiredCols := []string{"inventory_level_tons", "capacity_utilization_pct", "hours_of_supply"}
		for _, reqCol := range requiredCols {
			found := false
			for _, col := range buffer.Columns {
				if col.Name == reqCol {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("stg_buffer_levels should have %s column", reqCol)
			}
		}
	}
}
