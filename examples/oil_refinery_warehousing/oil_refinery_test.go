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
