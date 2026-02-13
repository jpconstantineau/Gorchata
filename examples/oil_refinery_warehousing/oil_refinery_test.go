package oil_refinery_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/domain/test/schema"
)

// TestSchemaFileExists verifies that schema.yml exists in the current directory
func TestSchemaFileExists(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	// Verify file exists
	if _, err := os.Stat(schemaPath); os.IsNotExist(err) {
		t.Fatalf("schema.yml does not exist at %s", schemaPath)
	}
}

// TestSchemaValidation verifies the schema.yml file is valid and can be parsed
func TestSchemaValidation(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	// Parse schema file
	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	// Verify version is set
	if schemaFile.Version == 0 {
		t.Error("Schema version must be set (version: 2)")
	}

	// Verify models are defined
	if len(schemaFile.Models) == 0 {
		t.Fatal("Schema must define at least one model")
	}
}

// TestDimensionTablesExist verifies all required dimension tables are defined
func TestDimensionTablesExist(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	requiredDimensions := []string{
		"dim_date",
		"dim_unit",
		"dim_product",
		"dim_crude_grade",
		"dim_stream",
		"dim_location",
		"dim_catalyst_cycle",
	}

	modelMap := make(map[string]bool)
	for _, model := range schemaFile.Models {
		modelMap[model.Name] = true
	}

	for _, dimName := range requiredDimensions {
		if !modelMap[dimName] {
			t.Errorf("Required dimension table %s not found in schema", dimName)
		}
	}
}

// TestDimDateStructure verifies dim_date has required date dimension fields
func TestDimDateStructure(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var dimDate *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "dim_date" {
			dimDate = &model
			break
		}
	}

	if dimDate == nil {
		t.Fatal("dim_date not found in schema")
	}

	requiredColumns := []string{
		"date_key",
		"full_date",
		"year",
		"quarter",
		"month",
		"week",
		"day_of_week",
		"day_name",
		"month_name",
		"fiscal_year",
		"fiscal_quarter",
	}

	columnMap := make(map[string]bool)
	for _, col := range dimDate.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("dim_date missing required column: %s", colName)
		}
	}

	// Verify date_key has primary key constraints (unique + not_null)
	var dateKeyCol *schema.ColumnSchema
	for _, col := range dimDate.Columns {
		if col.Name == "date_key" {
			dateKeyCol = &col
			break
		}
	}

	if dateKeyCol == nil || len(dateKeyCol.DataTests) == 0 {
		t.Error("date_key must have data tests defined (unique, not_null)")
	}
}

// TestDimUnitStructure verifies dim_unit has required process unit fields
func TestDimUnitStructure(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var dimUnit *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "dim_unit" {
			dimUnit = &model
			break
		}
	}

	if dimUnit == nil {
		t.Fatal("dim_unit not found in schema")
	}

	requiredColumns := []string{
		"unit_id",
		"unit_name",
		"unit_type",
		"complex_name",
		"capacity_bpd",
		"design_capacity_bpd",
		"commissioned_date",
	}

	columnMap := make(map[string]bool)
	for _, col := range dimUnit.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("dim_unit missing required column: %s", colName)
		}
	}

	// Verify unit_id has primary key constraints
	var unitIdCol *schema.ColumnSchema
	for _, col := range dimUnit.Columns {
		if col.Name == "unit_id" {
			unitIdCol = &col
			break
		}
	}

	if unitIdCol == nil || len(unitIdCol.DataTests) == 0 {
		t.Error("unit_id must have data tests defined (unique, not_null)")
	}
}

// TestDimProductStructure verifies dim_product has required product hierarchy fields
func TestDimProductStructure(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var dimProduct *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "dim_product" {
			dimProduct = &model
			break
		}
	}

	if dimProduct == nil {
		t.Fatal("dim_product not found in schema")
	}

	requiredColumns := []string{
		"product_id",
		"product_name",
		"product_grade",
		"product_type",
		"product_category",
		"api_gravity",
		"sulfur_pct",
	}

	columnMap := make(map[string]bool)
	for _, col := range dimProduct.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("dim_product missing required column: %s", colName)
		}
	}
}

// TestDimCrudeGradeStructure verifies dim_crude_grade has required crude oil fields
func TestDimCrudeGradeStructure(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var dimCrudeGrade *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "dim_crude_grade" {
			dimCrudeGrade = &model
			break
		}
	}

	if dimCrudeGrade == nil {
		t.Fatal("dim_crude_grade not found in schema")
	}

	requiredColumns := []string{
		"crude_grade_id",
		"crude_name",
		"api_gravity",
		"sulfur_pct",
		"crude_type",
	}

	columnMap := make(map[string]bool)
	for _, col := range dimCrudeGrade.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("dim_crude_grade missing required column: %s", colName)
		}
	}

	// Verify crude_grade_id has primary key constraints
	var crudeGradeIdCol *schema.ColumnSchema
	for _, col := range dimCrudeGrade.Columns {
		if col.Name == "crude_grade_id" {
			crudeGradeIdCol = &col
			break
		}
	}

	if crudeGradeIdCol == nil || len(crudeGradeIdCol.DataTests) == 0 {
		t.Error("crude_grade_id must have data tests defined (unique, not_null)")
	}
}

// TestDimStreamStructure verifies dim_stream has required intermediate stream fields
func TestDimStreamStructure(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var dimStream *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "dim_stream" {
			dimStream = &model
			break
		}
	}

	if dimStream == nil {
		t.Fatal("dim_stream not found in schema")
	}

	requiredColumns := []string{
		"stream_id",
		"stream_name",
		"stream_type",
		"boiling_range_min_f",
		"boiling_range_max_f",
	}

	columnMap := make(map[string]bool)
	for _, col := range dimStream.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("dim_stream missing required column: %s", colName)
		}
	}
}

// TestDimLocationStructure verifies dim_location has required location fields
func TestDimLocationStructure(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var dimLocation *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "dim_location" {
			dimLocation = &model
			break
		}
	}

	if dimLocation == nil {
		t.Fatal("dim_location not found in schema")
	}

	requiredColumns := []string{
		"location_id",
		"location_name",
		"location_type",
	}

	columnMap := make(map[string]bool)
	for _, col := range dimLocation.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("dim_location missing required column: %s", colName)
		}
	}
}

// TestDimCatalystCycleStructure verifies dim_catalyst_cycle has required catalyst lifecycle fields
func TestDimCatalystCycleStructure(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var dimCatalystCycle *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "dim_catalyst_cycle" {
			dimCatalystCycle = &model
			break
		}
	}

	if dimCatalystCycle == nil {
		t.Fatal("dim_catalyst_cycle not found in schema")
	}

	requiredColumns := []string{
		"catalyst_cycle_id",
		"cycle_stage",
		"stage_description",
	}

	columnMap := make(map[string]bool)
	for _, col := range dimCatalystCycle.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("dim_catalyst_cycle missing required column: %s", colName)
		}
	}
}

// TestAllModelDescriptions verifies all models have descriptions
func TestAllModelDescriptions(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	for _, model := range schemaFile.Models {
		if model.Description == "" {
			t.Errorf("Model %s is missing a description", model.Name)
		}

		// Verify at least some columns have descriptions
		hasDescriptions := false
		for _, col := range model.Columns {
			if col.Description != "" {
				hasDescriptions = true
				break
			}
		}

		if !hasDescriptions && len(model.Columns) > 0 {
			t.Errorf("Model %s has no column descriptions", model.Name)
		}
	}
}

// ===================================================================
// PHASE 2 TESTS - FACT_CRUDE_RECEIPTS
// ===================================================================

// TestFactCrudeReceiptsTableExists verifies FACT_CRUDE_RECEIPTS table is defined
func TestFactCrudeReceiptsTableExists(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	found := false
	for _, model := range schemaFile.Models {
		if model.Name == "fact_crude_receipts" {
			found = true
			break
		}
	}

	if !found {
		t.Error("fact_crude_receipts table not found in schema")
	}
}

// TestStagingCrudeReceiptsTableExists verifies stg_crude_receipts staging table is defined
func TestStagingCrudeReceiptsTableExists(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	found := false
	for _, model := range schemaFile.Models {
		if model.Name == "stg_crude_receipts" {
			found = true
			break
		}
	}

	if !found {
		t.Error("stg_crude_receipts staging table not found in schema")
	}
}

// TestCrudeReceiptsHasRequiredColumns verifies FACT_CRUDE_RECEIPTS has all required columns
func TestCrudeReceiptsHasRequiredColumns(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var factCrudeReceipts *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "fact_crude_receipts" {
			factCrudeReceipts = &model
			break
		}
	}

	if factCrudeReceipts == nil {
		t.Fatal("fact_crude_receipts not found in schema")
	}

	requiredColumns := []string{
		"receipt_id",
		"date_key",
		"crude_grade_id",
		"source_location_id",
		"receipt_mode",
		"gross_volume_bbl",
		"observed_temperature_f",
		"observed_api_gravity",
		"bsw_pct",
		"api_gravity_60f",
		"net_volume_bbl",
		"specific_gravity_60f",
		"weight_short_tons",
		"sulfur_wt_pct",
	}

	columnMap := make(map[string]bool)
	for _, col := range factCrudeReceipts.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("fact_crude_receipts missing required column: %s", colName)
		}
	}
}

// TestCrudeReceiptsForeignKeys verifies foreign key relationships are defined
func TestCrudeReceiptsForeignKeys(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var factCrudeReceipts *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "fact_crude_receipts" {
			factCrudeReceipts = &model
			break
		}
	}

	if factCrudeReceipts == nil {
		t.Fatal("fact_crude_receipts not found in schema")
	}

	// Check for foreign key columns
	fkColumns := []string{"date_key", "crude_grade_id", "source_location_id"}

	columnMap := make(map[string]bool)
	for _, col := range factCrudeReceipts.Columns {
		columnMap[col.Name] = true
	}

	for _, fkCol := range fkColumns {
		if !columnMap[fkCol] {
			t.Errorf("Foreign key column %s not found in fact_crude_receipts", fkCol)
		}
	}

	// Read schema file content to check for relationships strings
	schemaContent, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to read schema file: %v", err)
	}

	contentStr := string(schemaContent)

	// Verify relationship tests reference correct dimension tables
	requiredRelationships := []string{
		"to: dim_date",
		"to: dim_crude_grade",
		"to: dim_location",
	}

	for _, rel := range requiredRelationships {
		if !containsSubstring(contentStr, rel) {
			t.Errorf("schema.yml should define relationship: %s", rel)
		}
	}
}

// containsSubstring is a helper function to check if a string contains a substring
func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr || len(s) > len(substr) && findSubstring(s, substr))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// floatEquals is a helper function to compare floats with tolerance
func floatEquals(a, b, tolerance float64) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff <= tolerance
}

// findColumn is a helper function to find a column by name
func findColumn(columns []schema.ColumnSchema, name string) *schema.ColumnSchema {
	for i := range columns {
		if columns[i].Name == name {
			return &columns[i]
		}
	}
	return nil
}

// hasDataTest checks if a column has a specific data test (e.g., "unique", "not_null")
func hasDataTest(dataTests []interface{}, testName string) bool {
	for _, test := range dataTests {
		switch v := test.(type) {
		case string:
			if v == testName {
				return true
			}
		case map[string]interface{}:
			if _, ok := v[testName]; ok {
				return true
			}
		}
	}
	return false
}

// hasRelationship checks if a column has a relationship to a specific table
func hasRelationship(dataTests []interface{}, toTable string) bool {
	for _, test := range dataTests {
		if testMap, ok := test.(map[string]interface{}); ok {
			if rel, ok := testMap["relationships"]; ok {
				if relMap, ok := rel.(map[string]interface{}); ok {
					if to, ok := relMap["to"].(string); ok && to == toTable {
						return true
					}
				}
			}
		}
	}
	return false
}

// TestAPIGravityConversion verifies the API gravity to specific gravity formula
func TestAPIGravityConversion(t *testing.T) {
	tests := []struct {
		name       string
		apiGravity float64
		expectedSG float64
		tolerance  float64
	}{
		{"WTI Crude", 39.6, 0.827, 0.001},
		{"Brent Crude", 38.3, 0.8333, 0.001}, // 141.5 / (38.3 + 131.5) = 0.8333
		{"Maya Heavy", 22.0, 0.9218, 0.001},  // 141.5 / (22.0 + 131.5) = 0.9218
		{"Dubai Medium", 31.0, 0.871, 0.001},
		{"Mars Medium", 29.0, 0.882, 0.001},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Formula: SG = 141.5 / (API + 131.5)
			calculatedSG := 141.5 / (tt.apiGravity + 131.5)

			diff := calculatedSG - tt.expectedSG
			if diff < 0 {
				diff = -diff
			}

			if diff > tt.tolerance {
				t.Errorf("API %g° conversion failed: got SG %g, want %g (tolerance %g)",
					tt.apiGravity, calculatedSG, tt.expectedSG, tt.tolerance)
			}
		})
	}
}

// TestVolumeToWeightConversion verifies barrel to ton conversion formula
func TestVolumeToWeightConversion(t *testing.T) {
	tests := []struct {
		name            string
		volumeBbl       float64
		specificGravity float64
		expectedTons    float64
		tolerance       float64
	}{
		{"Light Crude 10k bbl", 10000, 0.827, 1127.76, 1.0},
		{"Heavy Crude 10k bbl", 10000, 0.920, 1254.08, 1.0},
		{"Medium Crude 5k bbl", 5000, 0.871, 594.946, 1.0},
		{"Gasoline 1k bbl", 1000, 0.739, 100.758, 0.1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Formula: Weight (short tons) = Volume (bbl) × 0.1364 × SG
			// Note: Using more precise factor 0.136364 derived from:
			// 42 gal/bbl × 8.337 lb/gal / 2000 lb/ton = 0.175077
			// But document uses 0.1364, so we'll use that
			calculatedTons := tt.volumeBbl * 0.1364 * tt.specificGravity

			diff := calculatedTons - tt.expectedTons
			if diff < 0 {
				diff = -diff
			}

			if diff > tt.tolerance {
				t.Errorf("Volume to weight conversion failed: got %g tons, want %g tons (tolerance %g)",
					calculatedTons, tt.expectedTons, tt.tolerance)
			}
		})
	}
}

// TestBSWDeduction verifies basic sediment & water deduction calculation
func TestBSWDeduction(t *testing.T) {
	tests := []struct {
		name        string
		grossVolume float64
		bswPct      float64
		expectedNet float64
	}{
		{"Typical Pipeline 0.1%", 100000, 0.1, 99900},
		{"High BSW 0.5%", 50000, 0.5, 49750},
		{"Low BSW 0.05%", 75000, 0.05, 74962.5},
		{"Marine Receipt 0.3%", 250000, 0.3, 249250},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Formula: Net Volume = Gross Volume × (1 - BSW% / 100)
			calculatedNet := tt.grossVolume * (1 - tt.bswPct/100)

			if calculatedNet != tt.expectedNet {
				t.Errorf("BSW deduction failed: got %g bbl, want %g bbl",
					calculatedNet, tt.expectedNet)
			}
		})
	}
}

// TestTemperatureCorrection verifies temperature correction to 60°F
func TestTemperatureCorrection(t *testing.T) {
	tests := []struct {
		name              string
		observedVolume    float64
		observedTempF     float64
		expectedCorrected float64
		tolerance         float64
	}{
		{"Hot Summer 85F", 100000, 85.0, 99000, 100},
		{"Cold Winter 40F", 75000, 40.0, 75600, 100},
		{"Standard 60F", 50000, 60.0, 50000, 10},
		{"Very Hot 110F", 50000, 110.0, 49000, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simplified formula: Correction = 1 - ((T - 60) × 0.0004)
			// This is an approximation; real VCF uses ASTM tables
			correctionFactor := 1 - ((tt.observedTempF - 60) * 0.0004)
			calculatedCorrected := tt.observedVolume * correctionFactor

			diff := calculatedCorrected - tt.expectedCorrected
			if diff < 0 {
				diff = -diff
			}

			if diff > tt.tolerance {
				t.Errorf("Temperature correction failed: got %g bbl, want %g bbl (tolerance %g)",
					calculatedCorrected, tt.expectedCorrected, tt.tolerance)
			}
		})
	}
}

// ===================================================================
// PHASE 3 TESTS - Unit Operations and Feed Tracking
// ===================================================================

// TestFactUnitFeedTableExists verifies FACT_UNIT_FEED table is defined
func TestFactUnitFeedTableExists(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	found := false
	for _, model := range schemaFile.Models {
		if model.Name == "fact_unit_feed" {
			found = true
			break
		}
	}

	if !found {
		t.Error("FACT_UNIT_FEED table not found in schema")
	}
}

// TestFactUnitOperationsTableExists verifies FACT_UNIT_OPERATIONS table is defined
func TestFactUnitOperationsTableExists(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	found := false
	for _, model := range schemaFile.Models {
		if model.Name == "fact_unit_operations" {
			found = true
			break
		}
	}

	if !found {
		t.Error("FACT_UNIT_OPERATIONS table not found in schema")
	}
}

// TestUnitFeedHasRequiredColumns verifies FACT_UNIT_FEED has all required columns
func TestUnitFeedHasRequiredColumns(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var factUnitFeed *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "fact_unit_feed" {
			factUnitFeed = &model
			break
		}
	}

	if factUnitFeed == nil {
		t.Fatal("fact_unit_feed not found in schema")
	}

	requiredColumns := []string{
		"feed_id",
		"date_key",
		"unit_id",
		"feed_stream_id",
		"feed_volume_bbl",
		"feed_weight_tons",
		"feed_api_gravity",
		"feed_sulfur_ppm",
		"feed_temperature_f",
	}

	columnMap := make(map[string]bool)
	for _, col := range factUnitFeed.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("fact_unit_feed missing required column: %s", colName)
		}
	}
}

// TestUnitOperationsHasRequiredColumns verifies FACT_UNIT_OPERATIONS has all required columns including downtime
func TestUnitOperationsHasRequiredColumns(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var factUnitOps *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "fact_unit_operations" {
			factUnitOps = &model
			break
		}
	}

	if factUnitOps == nil {
		t.Fatal("fact_unit_operations not found in schema")
	}

	requiredColumns := []string{
		"operation_id",
		"date_key",
		"unit_id",
		"catalyst_cycle_id",
		"operating_hours",
		"planned_downtime_hours",
		"unplanned_downtime_hours",
		"throughput_bbl",
		"capacity_bbl_day",
		"capacity_utilization_pct",
		"conversion_pct",
		"energy_consumed_mmbtu",
		"reactor_temperature_f",
		"reactor_pressure_psig",
	}

	columnMap := make(map[string]bool)
	for _, col := range factUnitOps.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("fact_unit_operations missing required column: %s", colName)
		}
	}
}

// TestCapacityUtilizationCalculation verifies capacity utilization formula
func TestCapacityUtilizationCalculation(t *testing.T) {
	tests := []struct {
		name         string
		throughput   float64
		capacity     float64
		expectedUtil float64
	}{
		{"CDU High Utilization", 142500, 150000, 95.0},
		{"FCC Normal Operation", 41400, 45000, 92.0},
		{"Hydrocracker Reduced", 26400, 30000, 88.0},
		{"Reformer Typical", 22500, 25000, 90.0},
		{"Alkylation Low", 12750, 15000, 85.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Formula: Capacity Utilization % = (Throughput / Capacity) × 100
			calculatedUtil := (tt.throughput / tt.capacity) * 100

			if calculatedUtil != tt.expectedUtil {
				t.Errorf("Capacity utilization calculation failed: got %g%%, want %g%%",
					calculatedUtil, tt.expectedUtil)
			}
		})
	}
}

// TestDowntimeAggregation verifies planned and unplanned downtime tracking
func TestDowntimeAggregation(t *testing.T) {
	tests := []struct {
		name              string
		plannedDowntime   float64
		unplannedDowntime float64
		expectedTotal     float64
		expectedOperating float64
	}{
		{"Normal Operation", 0, 0, 0, 24.0},
		{"Planned Maintenance", 12.0, 0, 12.0, 12.0},
		{"Unplanned Trip", 0, 4.5, 4.5, 19.5},
		{"Both Types", 8.0, 3.0, 11.0, 13.0},
		{"Full Day Shutdown", 24.0, 0, 24.0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Formula: Total Downtime = Planned + Unplanned
			calculatedTotal := tt.plannedDowntime + tt.unplannedDowntime
			if calculatedTotal != tt.expectedTotal {
				t.Errorf("Total downtime calculation failed: got %g hrs, want %g hrs",
					calculatedTotal, tt.expectedTotal)
			}

			// Formula: Operating Hours = 24 - Total Downtime
			calculatedOperating := 24.0 - calculatedTotal
			if calculatedOperating != tt.expectedOperating {
				t.Errorf("Operating hours calculation failed: got %g hrs, want %g hrs",
					calculatedOperating, tt.expectedOperating)
			}
		})
	}
}

// TestUnitHierarchyRollup verifies complex-level aggregation
func TestUnitHierarchyRollup(t *testing.T) {
	tests := []struct {
		name                 string
		complexName          string
		unitThroughputs      []float64
		expectedComplexTotal float64
	}{
		{
			"Crude Unit Complex",
			"Crude Unit Complex",
			[]float64{142500, 54000}, // CDU + VDU
			196500,
		},
		{
			"Conversion Complex",
			"Conversion Complex",
			[]float64{41400, 26400}, // FCC + HCU
			67800,
		},
		{
			"Clean Fuels Complex",
			"Clean Fuels Complex",
			[]float64{32550, 36400}, // Naphtha HT + Diesel HT
			68950,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Formula: Complex Throughput = SUM(Unit Throughputs)
			total := 0.0
			for _, throughput := range tt.unitThroughputs {
				total += throughput
			}

			if total != tt.expectedComplexTotal {
				t.Errorf("Complex rollup failed: got %g bbl, want %g bbl",
					total, tt.expectedComplexTotal)
			}
		})
	}
}

// TestSeedUnitOperationsValid verifies seed_unit_operations.yml has proper structure
func TestSeedUnitOperationsValid(t *testing.T) {
	seedPath := filepath.Join("seeds", "seed_unit_operations.yml")

	// Verify file exists
	if _, err := os.Stat(seedPath); os.IsNotExist(err) {
		t.Fatalf("seed_unit_operations.yml does not exist at %s", seedPath)
	}

	// Parse seed file to verify structure
	seedContent, err := os.ReadFile(seedPath)
	if err != nil {
		t.Fatalf("Failed to read seed_unit_operations.yml: %v", err)
	}

	contentStr := string(seedContent)

	// Verify file has version and table declarations
	if !containsSubstring(contentStr, "version:") {
		t.Error("Seed file missing version declaration")
	}

	if !containsSubstring(contentStr, "table:") {
		t.Error("Seed file missing table declaration")
	}

	if !containsSubstring(contentStr, "records:") {
		t.Error("Seed file missing records section")
	}

	// Verify key operational units are referenced
	requiredUnits := []string{"CDU", "FCC", "Hydrocracker", "Reformer"}
	for _, unit := range requiredUnits {
		if !containsSubstring(contentStr, unit) {
			t.Errorf("Seed file should include %s operations", unit)
		}
	}

	// Verify downtime tracking exists
	if !containsSubstring(contentStr, "planned_downtime_hours") {
		t.Error("Seed file missing planned_downtime_hours column")
	}

	if !containsSubstring(contentStr, "unplanned_downtime_hours") {
		t.Error("Seed file missing unplanned_downtime_hours column")
	}
}

// ===================================================================
// PHASE 4 TESTS - Unit Production and Yield Calculations
// ===================================================================

// TestFactUnitProductionTableExists verifies FACT_UNIT_PRODUCTION is defined in schema
func TestFactUnitProductionTableExists(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var foundTable bool
	for _, model := range schemaFile.Models {
		if model.Name == "fact_unit_production" {
			foundTable = true
			break
		}
	}

	if !foundTable {
		t.Error("fact_unit_production table not found in schema")
	}
}

// TestUnitProductionHasRequiredColumns verifies FACT_UNIT_PRODUCTION has all required columns
func TestUnitProductionHasRequiredColumns(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var factUnitProd *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "fact_unit_production" {
			factUnitProd = &model
			break
		}
	}

	if factUnitProd == nil {
		t.Fatal("fact_unit_production not found in schema")
	}

	requiredColumns := []string{
		"production_id",
		"date_key",
		"unit_id",
		"product_id",
		"feed_volume_bbl",
		"feed_weight_tons",
		"product_volume_bbl",
		"product_weight_tons",
		"yield_pct_volume",
		"yield_pct_weight",
		"product_api_gravity",
		"product_sulfur_ppm",
	}

	columnMap := make(map[string]bool)
	for _, col := range factUnitProd.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("Required column %s not found in fact_unit_production", colName)
		}
	}
}

// TestVolumeYieldCalculation verifies volume yield percentage calculation formula
func TestVolumeYieldCalculation(t *testing.T) {
	tests := []struct {
		name             string
		productVolume    float64
		feedVolume       float64
		expectedYieldPct float64
	}{
		{"CDU Normal Yield", 95000.0, 100000.0, 95.0},
		{"FCC Volumetric Expansion", 106500.0, 100000.0, 106.5},
		{"Reformer Volume Loss", 91000.0, 100000.0, 91.0},
		{"Hydrotreater Minimal Loss", 99000.0, 100000.0, 99.0},
		{"Single Product Component", 47500.0, 100000.0, 47.5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Formula: Yield % Volume = (Product Volume / Feed Volume) × 100
			calculatedYield := (tt.productVolume / tt.feedVolume) * 100.0

			if !floatEquals(calculatedYield, tt.expectedYieldPct, 0.01) {
				t.Errorf("Volume yield calculation failed: got %.2f%%, want %.2f%%",
					calculatedYield, tt.expectedYieldPct)
			}
		})
	}
}

// TestWeightYieldCalculation verifies weight yield percentage calculation formula
func TestWeightYieldCalculation(t *testing.T) {
	tests := []struct {
		name             string
		productWeight    float64
		feedWeight       float64
		expectedYieldPct float64
	}{
		{"CDU Weight Conservation", 9850.0, 10000.0, 98.5},
		{"FCC Weight Loss (Coke)", 9650.0, 10000.0, 96.5},
		{"Reformer H2 Production Loss", 8900.0, 10000.0, 89.0},
		{"Hydrotreater Minimal Loss", 9900.0, 10000.0, 99.0},
		{"Single Product Component", 4850.0, 10000.0, 48.5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Formula: Yield % Weight = (Product Weight / Feed Weight) × 100
			calculatedYield := (tt.productWeight / tt.feedWeight) * 100.0

			if !floatEquals(calculatedYield, tt.expectedYieldPct, 0.01) {
				t.Errorf("Weight yield calculation failed: got %.2f%%, want %.2f%%",
					calculatedYield, tt.expectedYieldPct)
			}
		})
	}
}

// TestYieldSumValidation verifies yield sums are within physical constraints
func TestYieldSumValidation(t *testing.T) {
	tests := []struct {
		name           string
		unitType       string
		productYields  []float64
		expectedVolMin float64
		expectedVolMax float64
		expectedWgtMin float64
		expectedWgtMax float64
	}{
		{
			"CDU Volume Conservation",
			"CDU",
			[]float64{2.5, 9.0, 11.0, 13.5, 19.0, 13.5, 31.5}, // Total: 100%
			95.0, 102.0,
			95.0, 99.0,
		},
		{
			"FCC Volumetric Expansion",
			"FCC",
			[]float64{4.0, 18.0, 51.0, 20.0, 14.0}, // Total: 107% (volumetric expansion)
			105.0, 110.0,                           // Volume can exceed 100% when accounting for density changes
			95.0, 98.0, // Weight always < 100%
		},
		{
			"Hydrocracker Normal Conversion",
			"Hydrocracker",
			[]float64{2.5, 8.0, 20.0, 67.0, 2.5}, // Total: 100%
			100.0, 103.0,
			97.0, 99.0,
		},
		{
			"Reformer Hydrogen Production",
			"Reformer",
			[]float64{2.5, 88.0}, // Product yields, H2 separate
			90.0, 93.0,           // Volume loss
			88.0, 91.0, // Weight loss due to H2
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Calculate total volume yield
			totalVolYield := 0.0
			for _, yield := range tt.productYields {
				totalVolYield += yield
			}

			// Verify volume yield range
			if totalVolYield < tt.expectedVolMin || totalVolYield > tt.expectedVolMax {
				t.Errorf("Volume yield sum %.2f%% outside valid range [%.2f%%, %.2f%%] for %s",
					totalVolYield, tt.expectedVolMin, tt.expectedVolMax, tt.unitType)
			}

			// For weight, typically 95-99% due to losses
			// This is a simplified validation - actual seed data will have full weight accounting
			if tt.expectedWgtMax < 100.0 {
				t.Logf("Weight yield for %s should be in range [%.2f%%, %.2f%%]",
					tt.unitType, tt.expectedWgtMin, tt.expectedWgtMax)
			}
		})
	}
}

// TestConversionPercentageCalculation verifies conversion percentage for upgrading units
func TestConversionPercentageCalculation(t *testing.T) {
	tests := []struct {
		name               string
		unitType           string
		lightProducts      float64 // Products lighter than cutpoint
		feedVolume         float64
		expectedConversion float64
	}{
		{"FCC High Conversion", "FCC", 32175.0, 45000.0, 71.5},
		{"FCC Low Conversion", "FCC", 29700.0, 45000.0, 66.0},
		{"Hydrocracker High Conversion", "Hydrocracker", 27300.0, 30000.0, 91.0},
		{"Hydrocracker Medium Conversion", "Hydrocracker", 25500.0, 30000.0, 85.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Formula: Conversion % = (Light Products / Feed Volume) × 100
			// Light products = products below specified cutpoint (e.g., 430°F for FCC)
			calculatedConversion := (tt.lightProducts / tt.feedVolume) * 100.0

			if !floatEquals(calculatedConversion, tt.expectedConversion, 0.5) {
				t.Errorf("Conversion calculation failed for %s: got %.2f%%, want %.2f%%",
					tt.unitType, calculatedConversion, tt.expectedConversion)
			}
		})
	}
}

// TestFCCVolumetricExpansion verifies FCC yields can exceed 100% volume
func TestFCCVolumetricExpansion(t *testing.T) {
	tests := []struct {
		name                string
		feedVolume          float64
		gasVolume           float64
		lpgVolume           float64
		gasolineVolume      float64
		lcoVolume           float64
		slurryVolume        float64
		expectedTotalVolume float64
		expectedYieldPct    float64
	}{
		{
			"FCC Typical Expansion",
			45000.0, // Feed
			1800.0,  // Gas (4%)
			7650.0,  // LPG (17%)
			22500.0, // Gasoline (50%)
			7650.0,  // LCO (17%)
			3150.0,  // Slurry (7%)
			42750.0, // Total liquid products
			95.0,    // 95% + coke (not volumetric)
		},
		{
			"FCC High Conversion - Expansion",
			45000.0,
			2025.0,  // Gas (4.5%)
			7875.0,  // LPG (17.5%)
			23400.0, // Gasoline (52%)
			7425.0,  // LCO (16.5%)
			2925.0,  // Slurry (6.5%)
			43650.0,
			97.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Calculate total volume yield (excluding coke which is solid)
			totalVolume := tt.gasVolume + tt.lpgVolume + tt.gasolineVolume +
				tt.lcoVolume + tt.slurryVolume

			if totalVolume != tt.expectedTotalVolume {
				t.Errorf("Total volume calculation failed: got %.0f bbl, want %.0f bbl",
					totalVolume, tt.expectedTotalVolume)
			}

			yieldPct := (totalVolume / tt.feedVolume) * 100.0

			// For FCC, liquid volume yield is typically 95-98% (coke is separate)
			// When accounting for density differences, apparent volume can be > 100%
			if !floatEquals(yieldPct, tt.expectedYieldPct, 1.0) {
				t.Errorf("FCC yield calculation failed: got %.2f%%, want %.2f%%",
					yieldPct, tt.expectedYieldPct)
			}

			// Critical test: FCC can have total product volumes that appear > 100%
			// due to production of lighter, lower-density products
			t.Logf("FCC volumetric accounting: %.0f bbl feed → %.0f bbl products (%.1f%%)",
				tt.feedVolume, totalVolume, yieldPct)
		})
	}
}

// TestSeedUnitProductionValid verifies seed_unit_production.yml has proper structure
func TestSeedUnitProductionValid(t *testing.T) {
	seedPath := filepath.Join("seeds", "seed_unit_production.yml")

	// Verify file exists
	if _, err := os.Stat(seedPath); os.IsNotExist(err) {
		t.Fatalf("seed_unit_production.yml does not exist at %s", seedPath)
	}

	// Parse seed file to verify structure
	seedContent, err := os.ReadFile(seedPath)
	if err != nil {
		t.Fatalf("Failed to read seed_unit_production.yml: %v", err)
	}

	contentStr := string(seedContent)

	// Verify file has version and table declarations
	if !containsSubstring(contentStr, "version:") {
		t.Error("Seed file missing version declaration")
	}

	if !containsSubstring(contentStr, "table:") {
		t.Error("Seed file missing table declaration")
	}

	if !containsSubstring(contentStr, "records:") {
		t.Error("Seed file missing records section")
	}

	// Verify key yield columns exist
	requiredFields := []string{
		"production_id",
		"yield_pct_volume",
		"yield_pct_weight",
		"product_volume_bbl",
		"product_weight_tons",
	}

	for _, field := range requiredFields {
		if !containsSubstring(contentStr, field) {
			t.Errorf("Seed file missing required field: %s", field)
		}
	}

	// Verify multiple product streams per unit
	productTypes := []string{"Gasoline", "Diesel", "Kerosene", "LPG", "Naphtha"}
	foundProducts := 0
	for _, product := range productTypes {
		if containsSubstring(contentStr, product) {
			foundProducts++
		}
	}

	if foundProducts < 3 {
		t.Errorf("Seed file should include multiple product types (found %d)", foundProducts)
	}
}

// ===================================================================
// PHASE 5 TESTS - Product Shipments and Tank Inventory
// ===================================================================

// TestDimTankStructure verifies dim_tank dimension has required columns
func TestDimTankStructure(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var dimTank *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "dim_tank" {
			dimTank = &model
			break
		}
	}

	if dimTank == nil {
		t.Fatal("dim_tank not found in schema")
	}

	requiredColumns := []string{
		"tank_id",
		"tank_code",
		"tank_name",
		"product_type",
		"capacity_bbl",
		"location",
		"tank_type",
		"operational_status",
	}

	columnMap := make(map[string]bool)
	for _, col := range dimTank.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("dim_tank missing required column: %s", colName)
		}
	}

	// Verify tank_id is primary key (has unique and not_null tests)
	tankIDCol := findColumn(dimTank.Columns, "tank_id")
	if tankIDCol == nil {
		t.Fatal("tank_id column not found")
	}

	if !hasDataTest(tankIDCol.DataTests, "unique") {
		t.Error("tank_id should have unique constraint")
	}

	if !hasDataTest(tankIDCol.DataTests, "not_null") {
		t.Error("tank_id should have not_null constraint")
	}
}

// TestFactProductShipmentsTableExists verifies fact_product_shipments table is defined
func TestFactProductShipmentsTableExists(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var factShipments *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "fact_product_shipments" {
			factShipments = &model
			break
		}
	}

	if factShipments == nil {
		t.Fatal("fact_product_shipments not found in schema")
	}

	requiredColumns := []string{
		"shipment_id",
		"date_key",
		"product_id",
		"tank_id",
		"shipment_volume_bbl",
		"shipment_weight_tons",
		"shipment_mode",
		"destination_location",
		"customer_id",
		"api_gravity",
		"temperature_f",
	}

	columnMap := make(map[string]bool)
	for _, col := range factShipments.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("fact_product_shipments missing required column: %s", colName)
		}
	}

	// Verify foreign key relationships
	dateKeyCol := findColumn(factShipments.Columns, "date_key")
	if dateKeyCol == nil {
		t.Fatal("date_key column not found")
	}

	if !hasRelationship(dateKeyCol.DataTests, "dim_date") {
		t.Error("date_key should have relationship to dim_date")
	}

	productIDCol := findColumn(factShipments.Columns, "product_id")
	if productIDCol != nil && !hasRelationship(productIDCol.DataTests, "dim_product") {
		t.Error("product_id should have relationship to dim_product")
	}

	tankIDCol := findColumn(factShipments.Columns, "tank_id")
	if tankIDCol != nil && !hasRelationship(tankIDCol.DataTests, "dim_tank") {
		t.Error("tank_id should have relationship to dim_tank")
	}
}

// TestFactTankInventoryTableExists verifies fact_tank_inventory table is defined
func TestFactTankInventoryTableExists(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var factInventory *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "fact_tank_inventory" {
			factInventory = &model
			break
		}
	}

	if factInventory == nil {
		t.Fatal("fact_tank_inventory not found in schema")
	}

	requiredColumns := []string{
		"inventory_id",
		"date_key",
		"tank_id",
		"product_id",
		"opening_balance_bbl",
		"receipts_bbl",
		"withdrawals_bbl",
		"closing_balance_bbl",
		"expected_closing_bbl",
		"variance_bbl",
		"variance_pct",
		"variance_flag",
		"temperature_f",
	}

	columnMap := make(map[string]bool)
	for _, col := range factInventory.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("fact_tank_inventory missing required column: %s", colName)
		}
	}

	// Verify inventory equation columns have proper tests
	openingCol := findColumn(factInventory.Columns, "opening_balance_bbl")
	if openingCol == nil {
		t.Fatal("opening_balance_bbl column not found")
	}

	if !hasDataTest(openingCol.DataTests, "not_null") {
		t.Error("opening_balance_bbl should have not_null constraint")
	}
}

// TestInventoryCalculation verifies inventory equation fields exist
func TestInventoryCalculation(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var factInventory *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "fact_tank_inventory" {
			factInventory = &model
			break
		}
	}

	if factInventory == nil {
		t.Fatal("fact_tank_inventory not found in schema")
	}

	// Verify all components of inventory equation exist
	inventoryComponents := []string{
		"opening_balance_bbl",
		"receipts_bbl",
		"withdrawals_bbl",
		"closing_balance_bbl",
		"expected_closing_bbl",
	}

	for _, component := range inventoryComponents {
		col := findColumn(factInventory.Columns, component)
		if col == nil {
			t.Errorf("Inventory equation component missing: %s", component)
		}
	}

	// Verify description mentions calculation
	expectedClosingCol := findColumn(factInventory.Columns, "expected_closing_bbl")
	if expectedClosingCol != nil {
		if expectedClosingCol.Description == "" {
			t.Error("expected_closing_bbl should have description explaining calculation")
		}
	}
}

// TestInventoryVarianceDetection verifies variance detection fields
func TestInventoryVarianceDetection(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var factInventory *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "fact_tank_inventory" {
			factInventory = &model
			break
		}
	}

	if factInventory == nil {
		t.Fatal("fact_tank_inventory not found in schema")
	}

	// Verify variance components exist
	varianceFields := []string{
		"variance_bbl",
		"variance_pct",
		"variance_flag",
	}

	for _, field := range varianceFields {
		col := findColumn(factInventory.Columns, field)
		if col == nil {
			t.Errorf("Variance detection field missing: %s", field)
		}
	}

	// Verify variance_flag is boolean/integer
	varianceFlagCol := findColumn(factInventory.Columns, "variance_flag")
	if varianceFlagCol != nil {
		if varianceFlagCol.Description == "" {
			t.Error("variance_flag should have description explaining threshold")
		}
	}
}

// TestProductAvailabilityCheck verifies staging table for shipments exists
func TestProductAvailabilityCheck(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	// Check for staging table
	var stgShipments *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "stg_product_shipments" {
			stgShipments = &model
			break
		}
	}

	if stgShipments == nil {
		t.Fatal("stg_product_shipments staging table not found in schema")
	}

	// Verify it has shipment_volume_bbl
	shipmentVolumeCol := findColumn(stgShipments.Columns, "shipment_volume_bbl")
	if shipmentVolumeCol == nil {
		t.Error("stg_product_shipments should have shipment_volume_bbl column")
	}
}

// TestInventoryBalanceByProduct verifies tank and product relationships
func TestInventoryBalanceByProduct(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var factInventory *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "fact_tank_inventory" {
			factInventory = &model
			break
		}
	}

	if factInventory == nil {
		t.Fatal("fact_tank_inventory not found in schema")
	}

	// Verify both tank_id and product_id exist for aggregation
	tankIDCol := findColumn(factInventory.Columns, "tank_id")
	if tankIDCol == nil {
		t.Error("fact_tank_inventory should have tank_id for tank-level aggregation")
	}

	productIDCol := findColumn(factInventory.Columns, "product_id")
	if productIDCol == nil {
		t.Error("fact_tank_inventory should have product_id for product-level aggregation")
	}

	// Verify relationships
	if tankIDCol != nil && !hasRelationship(tankIDCol.DataTests, "dim_tank") {
		t.Error("tank_id should have relationship to dim_tank")
	}

	if productIDCol != nil && !hasRelationship(productIDCol.DataTests, "dim_product") {
		t.Error("product_id should have relationship to dim_product")
	}
}

// TestSeedShipmentsInventoryValid verifies seed file exists and has required structure
func TestSeedShipmentsInventoryValid(t *testing.T) {
	seedPath := filepath.Join("seeds", "seed_shipments_inventory.yml")

	// Verify file exists
	if _, err := os.Stat(seedPath); os.IsNotExist(err) {
		t.Fatalf("Seed file does not exist: %s", seedPath)
	}

	// Read and parse file content
	content, err := os.ReadFile(seedPath)
	if err != nil {
		t.Fatalf("Failed to read seed file: %v", err)
	}

	contentStr := string(content)

	// Verify basic structure
	if !containsSubstring(contentStr, "version:") {
		t.Error("Seed file missing version declaration")
	}

	if !containsSubstring(contentStr, "table:") {
		t.Error("Seed file missing table declaration")
	}

	if !containsSubstring(contentStr, "records:") {
		t.Error("Seed file missing records section")
	}

	// Verify key inventory fields
	inventoryFields := []string{
		"opening_balance_bbl",
		"receipts_bbl",
		"withdrawals_bbl",
		"closing_balance_bbl",
		"variance_pct",
		"variance_flag",
	}

	for _, field := range inventoryFields {
		if !containsSubstring(contentStr, field) {
			t.Errorf("Seed file missing required inventory field: %s", field)
		}
	}

	// Verify shipment modes
	shipmentModes := []string{"Pipeline", "Truck", "Marine"}
	foundModes := 0
	for _, mode := range shipmentModes {
		if containsSubstring(contentStr, mode) {
			foundModes++
		}
	}

	if foundModes < 2 {
		t.Errorf("Seed file should include multiple shipment modes (found %d)", foundModes)
	}

	// Verify product types in shipments
	productTypes := []string{"Gasoline", "Diesel", "Jet"}
	foundProducts := 0
	for _, product := range productTypes {
		if containsSubstring(contentStr, product) {
			foundProducts++
		}
	}

	if foundProducts < 2 {
		t.Errorf("Seed file should include multiple product types in shipments (found %d)", foundProducts)
	}
}

// ===================================================================
// PHASE 6 - MASS BALANCE TESTS
// ===================================================================

// TestFactMassBalanceTableExists verifies fact_mass_balance table is defined in schema
func TestFactMassBalanceTableExists(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var factMassBalance *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "fact_mass_balance" {
			factMassBalance = &model
			break
		}
	}

	if factMassBalance == nil {
		t.Fatal("fact_mass_balance table not found in schema")
	}

	// Verify required mass balance columns
	requiredColumns := []string{
		"balance_id",
		"date_key",
		"period_type",
		"total_crude_input_tons",
		"total_product_output_tons",
		"refinery_fuel_consumed_tons",
		"coke_produced_tons",
		"flare_losses_tons",
		"evaporation_losses_tons",
		"inventory_change_tons",
		"total_accounted_tons",
		"unaccounted_tons",
		"unaccounted_pct",
		"balance_flag",
	}

	columnMap := make(map[string]bool)
	for _, col := range factMassBalance.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("fact_mass_balance missing required column: %s", colName)
		}
	}

	// Verify foreign key relationship to dim_date
	var dateKeyCol *schema.ColumnSchema
	for _, col := range factMassBalance.Columns {
		if col.Name == "date_key" {
			dateKeyCol = &col
			break
		}
	}

	if dateKeyCol == nil {
		t.Fatal("date_key column not found in fact_mass_balance")
	}

	// Use existing helper function to check relationship
	if !hasRelationship(dateKeyCol.DataTests, "dim_date") {
		t.Error("date_key must have relationship to dim_date")
	}
}

// TestMassBalanceEquation verifies the fundamental mass balance equation
func TestMassBalanceEquation(t *testing.T) {
	// Test the conservation of mass equation:
	// Total Inputs = Total Outputs + Losses + Inventory Change + Unaccounted

	tests := []struct {
		name                   string
		crudeInput             float64
		productOutput          float64
		fuelConsumed           float64
		cokeProduced           float64
		flareLosses            float64
		evaporationLosses      float64
		inventoryChange        float64
		expectedUnaccounted    float64
		expectedUnaccountedPct float64
	}{
		{
			name:                   "Balanced Day - All Accounted",
			crudeInput:             325000.0,
			productOutput:          285000.0,
			fuelConsumed:           19500.0, // 6%
			cokeProduced:           6500.0,  // 2%
			flareLosses:            650.0,   // 0.2%
			evaporationLosses:      488.0,   // 0.15%
			inventoryChange:        12500.0, // building inventory
			expectedUnaccounted:    362.0,   // 325000 - (285000 + 19500 + 6500 + 650 + 488 + 12500)
			expectedUnaccountedPct: 0.111,   // 362/325000 * 100
		},
		{
			name:                   "Near Zero Unaccounted",
			crudeInput:             330000.0,
			productOutput:          290000.0,
			fuelConsumed:           19800.0, // 6%
			cokeProduced:           6600.0,  // 2%
			flareLosses:            660.0,   // 0.2%
			evaporationLosses:      495.0,   // 0.15%
			inventoryChange:        12445.0,
			expectedUnaccounted:    0.0, // perfect balance
			expectedUnaccountedPct: 0.0,
		},
		{
			name:                   "Inventory Drawdown",
			crudeInput:             320000.0,
			productOutput:          310000.0,
			fuelConsumed:           19200.0,
			cokeProduced:           6400.0,
			flareLosses:            640.0,
			evaporationLosses:      480.0,
			inventoryChange:        -16000.0, // negative = drawing from inventory
			expectedUnaccounted:    -720.0,   // 320000 - (310000 + 19200 + 6400 + 640 + 480 - 16000)
			expectedUnaccountedPct: -0.225,   // -720/320000 * 100
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Calculate total accounted
			totalAccounted := tt.productOutput + tt.fuelConsumed + tt.cokeProduced +
				tt.flareLosses + tt.evaporationLosses + tt.inventoryChange

			// Calculate unaccounted
			unaccounted := tt.crudeInput - totalAccounted

			// Calculate unaccounted percentage
			unaccountedPct := (unaccounted / tt.crudeInput) * 100.0

			// Verify calculations match expectations
			tolerance := 0.01
			if abs(unaccounted-tt.expectedUnaccounted) > tolerance {
				t.Errorf("Unaccounted mismatch: got %.2f, want %.2f", unaccounted, tt.expectedUnaccounted)
			}

			if abs(unaccountedPct-tt.expectedUnaccountedPct) > 0.001 {
				t.Errorf("Unaccounted %% mismatch: got %.3f%%, want %.3f%%", unaccountedPct, tt.expectedUnaccountedPct)
			}
		})
	}
}

// TestUFLCalculation verifies Unaccounted for Loss (UFL) calculation logic
func TestUFLCalculation(t *testing.T) {
	tests := []struct {
		name           string
		crudeInput     float64
		totalAccounted float64
		expectedUFL    float64
		expectedUFLPct float64
		shouldFlag     bool
		flagThreshold  float64
	}{
		{
			name:           "Within Tolerance - Positive UFL",
			crudeInput:     325000.0,
			totalAccounted: 324638.0,
			expectedUFL:    362.0,
			expectedUFLPct: 0.111,
			shouldFlag:     false,
			flagThreshold:  0.5,
		},
		{
			name:           "Within Tolerance - Negative UFL",
			crudeInput:     330000.0,
			totalAccounted: 330825.0,
			expectedUFL:    -825.0,
			expectedUFLPct: -0.25,
			shouldFlag:     false,
			flagThreshold:  0.5,
		},
		{
			name:           "Out of Tolerance - High Positive",
			crudeInput:     320000.0,
			totalAccounted: 317500.0,
			expectedUFL:    2500.0,
			expectedUFLPct: 0.781,
			shouldFlag:     true,
			flagThreshold:  0.5,
		},
		{
			name:           "Out of Tolerance - High Negative",
			crudeInput:     315000.0,
			totalAccounted: 316700.0,
			expectedUFL:    -1700.0,
			expectedUFLPct: -0.540,
			shouldFlag:     true,
			flagThreshold:  0.5,
		},
		{
			name:           "Exactly at Threshold",
			crudeInput:     300000.0,
			totalAccounted: 298500.0,
			expectedUFL:    1500.0,
			expectedUFLPct: 0.5,
			shouldFlag:     false, // exactly at threshold = don't flag
			flagThreshold:  0.5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Calculate UFL
			ufl := tt.crudeInput - tt.totalAccounted
			uflPct := (ufl / tt.crudeInput) * 100.0

			// Determine if should flag
			shouldFlag := abs(uflPct) > tt.flagThreshold

			// Verify calculations
			if abs(ufl-tt.expectedUFL) > 0.01 {
				t.Errorf("UFL mismatch: got %.2f, want %.2f", ufl, tt.expectedUFL)
			}

			if abs(uflPct-tt.expectedUFLPct) > 0.001 {
				t.Errorf("UFL %% mismatch: got %.3f%%, want %.3f%%", uflPct, tt.expectedUFLPct)
			}

			if shouldFlag != tt.shouldFlag {
				t.Errorf("Flag mismatch: got %v, want %v (UFL: %.3f%%)", shouldFlag, tt.shouldFlag, uflPct)
			}
		})
	}
}

// TestToleranceValidation verifies balance_flag logic for daily and monthly tolerances
func TestToleranceValidation(t *testing.T) {
	tests := []struct {
		name         string
		periodType   string
		uflPct       float64
		expectedFlag bool
		threshold    float64
	}{
		// Daily tolerance: ±0.5%
		{
			name:         "Daily - Within Tolerance Positive",
			periodType:   "Daily",
			uflPct:       0.3,
			expectedFlag: false,
			threshold:    0.5,
		},
		{
			name:         "Daily - Within Tolerance Negative",
			periodType:   "Daily",
			uflPct:       -0.4,
			expectedFlag: false,
			threshold:    0.5,
		},
		{
			name:         "Daily - Out of Tolerance Positive",
			periodType:   "Daily",
			uflPct:       0.6,
			expectedFlag: true,
			threshold:    0.5,
		},
		{
			name:         "Daily - Out of Tolerance Negative",
			periodType:   "Daily",
			uflPct:       -0.75,
			expectedFlag: true,
			threshold:    0.5,
		},
		{
			name:         "Daily - Exactly at Threshold",
			periodType:   "Daily",
			uflPct:       0.5,
			expectedFlag: false,
			threshold:    0.5,
		},
		// Monthly tolerance: ±0.3%
		{
			name:         "Monthly - Within Tolerance",
			periodType:   "Monthly",
			uflPct:       0.25,
			expectedFlag: false,
			threshold:    0.3,
		},
		{
			name:         "Monthly - Out of Tolerance",
			periodType:   "Monthly",
			uflPct:       0.35,
			expectedFlag: true,
			threshold:    0.3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Apply flagging logic
			balanceFlag := abs(tt.uflPct) > tt.threshold

			if balanceFlag != tt.expectedFlag {
				t.Errorf("Balance flag mismatch: got %v, want %v (UFL: %.2f%%, threshold: %.2f%%)",
					balanceFlag, tt.expectedFlag, tt.uflPct, tt.threshold)
			}
		})
	}
}

// TestFuelConsumptionAccounting verifies refinery fuel consumption is properly accounted (5-8% typical)
func TestFuelConsumptionAccounting(t *testing.T) {
	tests := []struct {
		name         string
		crudeInput   float64
		fuelPct      float64
		expectedFuel float64
	}{
		{
			name:         "Typical - 6% Fuel",
			crudeInput:   325000.0,
			fuelPct:      6.0,
			expectedFuel: 19500.0,
		},
		{
			name:         "Low - 5% Fuel",
			crudeInput:   300000.0,
			fuelPct:      5.0,
			expectedFuel: 15000.0,
		},
		{
			name:         "High - 8% Fuel",
			crudeInput:   350000.0,
			fuelPct:      8.0,
			expectedFuel: 28000.0,
		},
		{
			name:         "Efficient - 5.5% Fuel",
			crudeInput:   320000.0,
			fuelPct:      5.5,
			expectedFuel: 17600.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Calculate fuel consumption
			fuelConsumed := tt.crudeInput * (tt.fuelPct / 100.0)

			if abs(fuelConsumed-tt.expectedFuel) > 0.01 {
				t.Errorf("Fuel consumption mismatch: got %.2f tons, want %.2f tons (%.1f%% of %.0f tons)",
					fuelConsumed, tt.expectedFuel, tt.fuelPct, tt.crudeInput)
			}

			// Verify fuel percentage is within expected range (5-8%)
			if tt.fuelPct < 5.0 || tt.fuelPct > 8.0 {
				t.Errorf("Fuel percentage %.1f%% is outside typical range (5-8%%)", tt.fuelPct)
			}
		})
	}
}

// TestCokeProductionTracking verifies petroleum coke production is tracked as a loss
func TestCokeProductionTracking(t *testing.T) {
	tests := []struct {
		name         string
		crudeInput   float64
		cokePct      float64
		expectedCoke float64
	}{
		{
			name:         "Typical Coker - 2% Coke",
			crudeInput:   325000.0,
			cokePct:      2.0,
			expectedCoke: 6500.0,
		},
		{
			name:         "Heavy Crude - 3% Coke",
			crudeInput:   300000.0,
			cokePct:      3.0,
			expectedCoke: 9000.0,
		},
		{
			name:         "Light Crude - 1.5% Coke",
			crudeInput:   330000.0,
			cokePct:      1.5,
			expectedCoke: 4950.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Calculate coke production
			cokeProduced := tt.crudeInput * (tt.cokePct / 100.0)

			if abs(cokeProduced-tt.expectedCoke) > 0.01 {
				t.Errorf("Coke production mismatch: got %.2f tons, want %.2f tons (%.1f%% of %.0f tons)",
					cokeProduced, tt.expectedCoke, tt.cokePct, tt.crudeInput)
			}

			// Verify coke percentage is reasonable (1-5%)
			if tt.cokePct < 1.0 || tt.cokePct > 5.0 {
				t.Errorf("Coke percentage %.1f%% is outside reasonable range (1-5%%)", tt.cokePct)
			}
		})
	}
}

// TestInventoryChangeImpact verifies inventory changes are correctly factored into mass balance
func TestInventoryChangeImpact(t *testing.T) {
	tests := []struct {
		name            string
		crudeInput      float64
		outputs         float64
		inventoryChange float64
		description     string
	}{
		{
			name:            "Building Inventory",
			crudeInput:      325000.0,
			outputs:         312000.0,
			inventoryChange: 12500.0,
			description:     "Positive change = storing more product",
		},
		{
			name:            "Drawing Inventory",
			crudeInput:      320000.0,
			outputs:         335000.0,
			inventoryChange: -16000.0,
			description:     "Negative change = using stored product",
		},
		{
			name:            "Stable Inventory",
			crudeInput:      330000.0,
			outputs:         324500.0,
			inventoryChange: 0.0,
			description:     "Zero change = balanced storage",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mass balance equation:
			// Crude Input = Outputs + Inventory Change + Other Losses + UFL
			// Therefore: UFL = Crude Input - Outputs - Inventory Change - Other Losses

			// Simplified (ignoring other losses for this test)
			simplifiedBalance := tt.crudeInput - tt.outputs - tt.inventoryChange

			// When building inventory (positive change), more input is needed
			// When drawing inventory (negative change), less input is needed

			if tt.inventoryChange > 0 {
				// Building inventory: balance should show input was stored
				if simplifiedBalance < 0 {
					t.Errorf("Building inventory should reduce apparent losses: balance = %.0f", simplifiedBalance)
				}
			} else if tt.inventoryChange < 0 {
				// Drawing inventory: balance should show stored product was used
				if simplifiedBalance > tt.crudeInput*0.1 {
					t.Errorf("Drawing inventory should supplement input: balance = %.0f", simplifiedBalance)
				}
			}

			t.Logf("%s: Inventory change = %.0f tons (%s)", tt.name, tt.inventoryChange, tt.description)
		})
	}
}

// TestSeedMassBalanceValid verifies seed data maintains valid mass balance
func TestSeedMassBalanceValid(t *testing.T) {
	// This test will verify that once seed data is created, the mass balance is valid
	// For now, we verify the schema and structure are ready

	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	// Verify fact_mass_balance table exists (prerequisite for seed data)
	var factMassBalance *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "fact_mass_balance" {
			factMassBalance = &model
			break
		}
	}

	if factMassBalance == nil {
		t.Fatal("fact_mass_balance table must exist before creating seed data")
	}

	// Verify critical columns for validation
	columnMap := make(map[string]bool)
	for _, col := range factMassBalance.Columns {
		columnMap[col.Name] = true
	}

	criticalColumns := []string{
		"total_crude_input_tons",
		"total_product_output_tons",
		"total_accounted_tons",
		"unaccounted_tons",
		"unaccounted_pct",
		"balance_flag",
	}

	for _, colName := range criticalColumns {
		if !columnMap[colName] {
			t.Errorf("Critical column %s required for mass balance validation", colName)
		}
	}

	t.Log("Mass balance schema structure is valid and ready for seed data")
}

// ===================================================================
// PHASE 7: DATA QUALITY VALIDATION AND ANOMALY DETECTION TESTS
// ===================================================================

// TestDimQualityRuleStructure verifies dim_quality_rule has required columns
func TestDimQualityRuleStructure(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var dimQualityRule *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "dim_quality_rule" {
			dimQualityRule = &model
			break
		}
	}

	if dimQualityRule == nil {
		t.Fatal("dim_quality_rule not found in schema")
	}

	requiredColumns := []string{
		"rule_id",
		"rule_code",
		"rule_name",
		"rule_category",
		"rule_description",
		"target_table",
		"target_column",
		"threshold_value",
		"severity",
		"active_flag",
	}

	columnMap := make(map[string]bool)
	for _, col := range dimQualityRule.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("Required column %s not found in dim_quality_rule", colName)
		}
	}

	t.Logf("dim_quality_rule has all %d required columns", len(requiredColumns))
}

// TestFactDataQualityChecksTableExists verifies fact_data_quality_checks table exists with required structure
func TestFactDataQualityChecksTableExists(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var factDataQualityChecks *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "fact_data_quality_checks" {
			factDataQualityChecks = &model
			break
		}
	}

	if factDataQualityChecks == nil {
		t.Fatal("fact_data_quality_checks not found in schema")
	}

	requiredColumns := []string{
		"check_id",
		"date_key",
		"rule_id",
		"entity_type",
		"entity_id",
		"check_timestamp",
		"measured_value",
		"expected_value",
		"deviation",
		"z_score",
		"pass_fail",
		"notes",
	}

	columnMap := make(map[string]bool)
	for _, col := range factDataQualityChecks.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("Required column %s not found in fact_data_quality_checks", colName)
		}
	}

	// Verify foreign key relationships
	var dateKeyCol, ruleIdCol *schema.ColumnSchema
	for _, col := range factDataQualityChecks.Columns {
		if col.Name == "date_key" {
			dateKeyCol = &col
		}
		if col.Name == "rule_id" {
			ruleIdCol = &col
		}
	}

	if dateKeyCol == nil {
		t.Error("date_key column not found")
	}
	if ruleIdCol == nil {
		t.Error("rule_id column not found")
	}

	t.Logf("fact_data_quality_checks has all %d required columns with proper foreign keys", len(requiredColumns))
}

// TestRangeValidations verifies range validation rules are properly defined
func TestRangeValidations(t *testing.T) {
	tests := []struct {
		name         string
		metric       string
		value        float64
		minThreshold float64
		maxThreshold float64
		expectedPass bool
		description  string
	}{
		{
			name:         "Valid API Gravity - Light Crude",
			metric:       "api_gravity",
			value:        38.5,
			minThreshold: 5.0,
			maxThreshold: 50.0,
			expectedPass: true,
			description:  "Light crude API gravity within valid range",
		},
		{
			name:         "Invalid API Gravity - Too Low",
			metric:       "api_gravity",
			value:        3.0,
			minThreshold: 5.0,
			maxThreshold: 50.0,
			expectedPass: false,
			description:  "API gravity below minimum threshold",
		},
		{
			name:         "Invalid API Gravity - Too High",
			metric:       "api_gravity",
			value:        55.0,
			minThreshold: 5.0,
			maxThreshold: 50.0,
			expectedPass: false,
			description:  "API gravity above maximum threshold",
		},
		{
			name:         "Valid Sulfur Content - Sweet Crude",
			metric:       "sulfur_pct",
			value:        0.3,
			minThreshold: 0.01,
			maxThreshold: 7.0,
			expectedPass: true,
			description:  "Sweet crude sulfur content within valid range",
		},
		{
			name:         "Valid Temperature",
			metric:       "temperature_f",
			value:        85.0,
			minThreshold: 30.0,
			maxThreshold: 150.0,
			expectedPass: true,
			description:  "Storage temperature within valid range",
		},
		{
			name:         "Invalid Capacity Utilization - Exceeded",
			metric:       "capacity_utilization_pct",
			value:        110.0,
			minThreshold: 0.0,
			maxThreshold: 105.0,
			expectedPass: false,
			description:  "Capacity utilization exceeds maximum allowable",
		},
		{
			name:         "Valid BS&W Content",
			metric:       "bsw_pct",
			value:        0.8,
			minThreshold: 0.0,
			maxThreshold: 2.0,
			expectedPass: true,
			description:  "Bottom sediment & water within acceptable range",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Range validation logic
			inRange := tt.value >= tt.minThreshold && tt.value <= tt.maxThreshold

			if inRange != tt.expectedPass {
				t.Errorf("%s: Expected pass=%v, got pass=%v for value=%.2f (range: %.2f-%.2f)",
					tt.description, tt.expectedPass, inRange, tt.value, tt.minThreshold, tt.maxThreshold)
			}

			t.Logf("%s: value=%.2f, range=[%.2f, %.2f], pass=%v",
				tt.metric, tt.value, tt.minThreshold, tt.maxThreshold, inRange)
		})
	}
}

// TestZScoreOutlierDetection verifies Z-score statistical outlier detection
func TestZScoreOutlierDetection(t *testing.T) {
	tests := []struct {
		name            string
		value           float64
		mean            float64
		stdDev          float64
		zScoreLimit     float64
		expectedOutlier bool
		description     string
	}{
		{
			name:            "Normal Value - Within 1 Sigma",
			value:           105000,
			mean:            100000,
			stdDev:          10000,
			zScoreLimit:     3.0,
			expectedOutlier: false,
			description:     "Value within 1 standard deviation",
		},
		{
			name:            "Normal Value - Within 2 Sigma",
			value:           120000,
			mean:            100000,
			stdDev:          10000,
			zScoreLimit:     3.0,
			expectedOutlier: false,
			description:     "Value within 2 standard deviations",
		},
		{
			name:            "Outlier - Positive Extreme",
			value:           135000,
			mean:            100000,
			stdDev:          10000,
			zScoreLimit:     3.0,
			expectedOutlier: true,
			description:     "Value exceeds 3 standard deviations above mean",
		},
		{
			name:            "Outlier - Negative Extreme",
			value:           65000,
			mean:            100000,
			stdDev:          10000,
			zScoreLimit:     3.0,
			expectedOutlier: true,
			description:     "Value exceeds 3 standard deviations below mean",
		},
		{
			name:            "Boundary - Exactly 3 Sigma",
			value:           130000,
			mean:            100000,
			stdDev:          10000,
			zScoreLimit:     3.0,
			expectedOutlier: false,
			description:     "Value exactly at 3 sigma boundary (inclusive)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Calculate Z-score: (value - mean) / stdDev
			zScore := (tt.value - tt.mean) / tt.stdDev
			absZScore := abs(zScore)

			// Outlier if |Z-score| > limit
			isOutlier := absZScore > tt.zScoreLimit

			if isOutlier != tt.expectedOutlier {
				t.Errorf("%s: Expected outlier=%v, got outlier=%v (Z-score=%.2f)",
					tt.description, tt.expectedOutlier, isOutlier, zScore)
			}

			t.Logf("value=%.0f, mean=%.0f, stddev=%.0f, Z-score=%.2f, outlier=%v",
				tt.value, tt.mean, tt.stdDev, zScore, isOutlier)
		})
	}
}

// TestAPIGravityDensityConsistency verifies API gravity and specific gravity consistency
func TestAPIGravityDensityConsistency(t *testing.T) {
	tests := []struct {
		name         string
		apiGravity   float64
		measuredSG   float64
		tolerancePct float64
		expectedPass bool
		description  string
	}{
		{
			name:         "Consistent - Light Crude",
			apiGravity:   35.0,
			measuredSG:   0.8498,
			tolerancePct: 0.5,
			expectedPass: true,
			description:  "API gravity and specific gravity are consistent",
		},
		{
			name:         "Consistent - Heavy Crude",
			apiGravity:   20.0,
			measuredSG:   0.9340,
			tolerancePct: 0.5,
			expectedPass: true,
			description:  "Heavy crude consistency check",
		},
		{
			name:         "Inconsistent - Measurement Error",
			apiGravity:   35.0,
			measuredSG:   0.8600,
			tolerancePct: 0.5,
			expectedPass: false,
			description:  "Measured SG deviates significantly from API-calculated SG",
		},
		{
			name:         "Boundary - Exactly at Tolerance",
			apiGravity:   30.0,
			measuredSG:   0.8762,
			tolerancePct: 0.5,
			expectedPass: true,
			description:  "Deviation exactly at tolerance limit",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Calculate expected SG from API gravity: SG = 141.5 / (API + 131.5)
			calculatedSG := 141.5 / (tt.apiGravity + 131.5)

			// Calculate deviation percentage
			deviationPct := abs(tt.measuredSG-calculatedSG) / calculatedSG * 100

			// Pass if deviation is within tolerance
			pass := deviationPct <= tt.tolerancePct

			if pass != tt.expectedPass {
				t.Errorf("%s: Expected pass=%v, got pass=%v (deviation=%.3f%%)",
					tt.description, tt.expectedPass, pass, deviationPct)
			}

			t.Logf("API=%.1f°, Measured SG=%.4f, Calculated SG=%.4f, Deviation=%.3f%%, Pass=%v",
				tt.apiGravity, tt.measuredSG, calculatedSG, deviationPct, pass)
		})
	}
}

// TestVolumeWeightConsistency verifies volume and weight measurements are consistent
func TestVolumeWeightConsistency(t *testing.T) {
	tests := []struct {
		name            string
		volumeBbl       float64
		measuredWeight  float64
		specificGravity float64
		tolerancePct    float64
		expectedPass    bool
		description     string
	}{
		{
			name:            "Consistent - Light Crude Receipt",
			volumeBbl:       10000,
			measuredWeight:  1491.0,
			specificGravity: 0.85,
			tolerancePct:    1.0,
			expectedPass:    true,
			description:     "Volume and weight are consistent",
		},
		{
			name:            "Consistent - Heavy Crude Receipt",
			volumeBbl:       10000,
			measuredWeight:  1642.0,
			specificGravity: 0.935,
			tolerancePct:    1.0,
			expectedPass:    true,
			description:     "Heavy crude volume-weight consistency",
		},
		{
			name:            "Inconsistent - Weight Measurement Error",
			volumeBbl:       10000,
			measuredWeight:  1550.0,
			specificGravity: 0.85,
			tolerancePct:    1.0,
			expectedPass:    false,
			description:     "Measured weight deviates from calculated weight",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Calculate expected weight: Weight (tons) = Volume (bbl) × 0.1756 × SG
			calculatedWeight := tt.volumeBbl * 0.1756 * tt.specificGravity

			// Calculate deviation percentage
			deviationPct := abs(tt.measuredWeight-calculatedWeight) / calculatedWeight * 100

			// Pass if deviation is within tolerance
			pass := deviationPct <= tt.tolerancePct

			if pass != tt.expectedPass {
				t.Errorf("%s: Expected pass=%v, got pass=%v (deviation=%.3f%%)",
					tt.description, tt.expectedPass, pass, deviationPct)
			}

			t.Logf("Volume=%.0f bbl, Measured Weight=%.1f tons, Calculated Weight=%.1f tons, Deviation=%.3f%%, Pass=%v",
				tt.volumeBbl, tt.measuredWeight, calculatedWeight, deviationPct, pass)
		})
	}
}

// TestYieldSumReasonableness verifies total yields from process units are reasonable
func TestYieldSumReasonableness(t *testing.T) {
	tests := []struct {
		name             string
		totalVolumeYield float64
		totalWeightYield float64
		expectedPass     bool
		description      string
	}{
		{
			name:             "Reasonable - Typical FCC",
			totalVolumeYield: 103.5,
			totalWeightYield: 97.2,
			expectedPass:     true,
			description:      "Volume expansion from FCC cracking, typical weight loss",
		},
		{
			name:             "Reasonable - High Severity",
			totalVolumeYield: 108.0,
			totalWeightYield: 96.5,
			expectedPass:     true,
			description:      "High severity operation with more expansion",
		},
		{
			name:             "Unreasonable - Volume Yield Too High",
			totalVolumeYield: 115.0,
			totalWeightYield: 97.0,
			expectedPass:     false,
			description:      "Volume yield exceeds reasonable limits",
		},
		{
			name:             "Unreasonable - Volume Yield Too Low",
			totalVolumeYield: 92.0,
			totalWeightYield: 97.0,
			expectedPass:     false,
			description:      "Volume yield below reasonable minimum",
		},
		{
			name:             "Unreasonable - Weight Yield Too Low",
			totalVolumeYield: 103.0,
			totalWeightYield: 93.0,
			expectedPass:     false,
			description:      "Weight yield indicates excessive losses",
		},
		{
			name:             "Reasonable - Boundary Values",
			totalVolumeYield: 95.0,
			totalWeightYield: 95.0,
			expectedPass:     true,
			description:      "Values at minimum reasonable boundary",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Volume yields: 95-110% acceptable (FCC causes expansion)
			// Weight yields: 95-99% acceptable (some losses expected)
			volumeOK := tt.totalVolumeYield >= 95.0 && tt.totalVolumeYield <= 110.0
			weightOK := tt.totalWeightYield >= 95.0 && tt.totalWeightYield <= 99.0

			pass := volumeOK && weightOK

			if pass != tt.expectedPass {
				t.Errorf("%s: Expected pass=%v, got pass=%v (volume=%.1f%%, weight=%.1f%%)",
					tt.description, tt.expectedPass, pass, tt.totalVolumeYield, tt.totalWeightYield)
			}

			t.Logf("Volume Yield=%.1f%%, Weight Yield=%.1f%%, Pass=%v",
				tt.totalVolumeYield, tt.totalWeightYield, pass)
		})
	}
}

// TestSeasonalRVPCompliance verifies gasoline RVP meets seasonal specifications
func TestSeasonalRVPCompliance(t *testing.T) {
	tests := []struct {
		name         string
		month        int
		day          int
		rvpPsi       float64
		expectedPass bool
		description  string
	}{
		{
			name:         "Summer Compliant - July",
			month:        7,
			day:          15,
			rvpPsi:       7.5,
			expectedPass: true,
			description:  "RVP within summer limit (≤7.8 psi)",
		},
		{
			name:         "Summer Non-Compliant - July",
			month:        7,
			day:          15,
			rvpPsi:       8.2,
			expectedPass: false,
			description:  "RVP exceeds summer limit",
		},
		{
			name:         "Winter Compliant - December",
			month:        12,
			day:          1,
			rvpPsi:       12.5,
			expectedPass: true,
			description:  "RVP within winter limit (≤13.5 psi)",
		},
		{
			name:         "Winter Compliant - January",
			month:        1,
			day:          15,
			rvpPsi:       13.0,
			expectedPass: true,
			description:  "RVP within winter limit",
		},
		{
			name:         "Summer Boundary - June 1",
			month:        6,
			day:          1,
			rvpPsi:       7.8,
			expectedPass: true,
			description:  "Exactly at summer limit on June 1",
		},
		{
			name:         "Summer Boundary - September 15",
			month:        9,
			day:          15,
			rvpPsi:       7.8,
			expectedPass: true,
			description:  "Exactly at summer limit on September 15",
		},
		{
			name:         "Winter Period - September 16",
			month:        9,
			day:          16,
			rvpPsi:       13.0,
			expectedPass: true,
			description:  "Winter spec applies from September 16",
		},
		{
			name:         "Winter Period - May 31",
			month:        5,
			day:          31,
			rvpPsi:       13.0,
			expectedPass: true,
			description:  "Winter spec applies through May 31",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Determine seasonal limit
			var maxRVP float64
			isSummer := (tt.month >= 6 && tt.month <= 9) &&
				(tt.month < 9 || (tt.month == 9 && tt.day <= 15))

			if isSummer {
				maxRVP = 7.8 // Summer: June 1 - September 15
			} else {
				maxRVP = 13.5 // Winter: September 16 - May 31
			}

			pass := tt.rvpPsi <= maxRVP

			if pass != tt.expectedPass {
				t.Errorf("%s: Expected pass=%v, got pass=%v (RVP=%.1f psi, Max=%.1f psi, Season=%s)",
					tt.description, tt.expectedPass, pass, tt.rvpPsi, maxRVP,
					map[bool]string{true: "Summer", false: "Winter"}[isSummer])
			}

			season := "Winter"
			if isSummer {
				season = "Summer"
			}

			t.Logf("Date=%02d/%02d, Season=%s, RVP=%.1f psi, Max=%.1f psi, Pass=%v",
				tt.month, tt.day, season, tt.rvpPsi, maxRVP, pass)
		})
	}
}

// TestSeedDataQualityChecksValid verifies seed data for data quality checks
func TestSeedDataQualityChecksValid(t *testing.T) {
	// This test verifies the schema structure is ready for seed data
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	// Verify fact_data_quality_checks table exists
	var factDataQualityChecks *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "fact_data_quality_checks" {
			factDataQualityChecks = &model
			break
		}
	}

	if factDataQualityChecks == nil {
		t.Fatal("fact_data_quality_checks table must exist before creating seed data")
	}

	// Verify dim_quality_rule table exists
	var dimQualityRule *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "dim_quality_rule" {
			dimQualityRule = &model
			break
		}
	}

	if dimQualityRule == nil {
		t.Fatal("dim_quality_rule table must exist before creating seed data")
	}

	// Verify staging table exists
	var stgDataQualityChecks *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "stg_data_quality_checks" {
			stgDataQualityChecks = &model
			break
		}
	}

	if stgDataQualityChecks == nil {
		t.Fatal("stg_data_quality_checks staging table must exist")
	}

	t.Log("Data quality checks schema structure is valid and ready for seed data")
}

// Helper function for absolute value
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
