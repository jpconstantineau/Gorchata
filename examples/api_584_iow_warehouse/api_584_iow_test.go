package api_584_iow_test

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jpconstantineau/gorchata/internal/domain/test/schema"
)

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
		"dim_asset",
		"dim_iow_limit",
		"dim_parameter_type",
		"dim_criticality_level",
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

// TestDimDateSchema validates dim_date has required columns and proper data tests
func TestDimDateSchema(t *testing.T) {
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
		"day_of_month",
		"day_of_week",
		"day_name",
		"month_name",
		"is_turnaround",
		"is_summer_spec",
		"is_winter_spec",
	}

	columnMap := make(map[string]bool)
	for _, col := range dimDate.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("Required column %s not found in dim_date", colName)
		}
	}

	// Verify date_key has unique and not_null tests
	var dateKeyCol *schema.ColumnSchema
	for _, col := range dimDate.Columns {
		if col.Name == "date_key" {
			dateKeyCol = &col
			break
		}
	}

	if dateKeyCol != nil {
		if !hasDataTest(dateKeyCol.DataTests, "unique") {
			t.Error("date_key must have unique test")
		}
		if !hasDataTest(dateKeyCol.DataTests, "not_null") {
			t.Error("date_key must have not_null test")
		}
	}
}

// TestDimAssetSchema validates dim_asset has required columns for 100 assets across 4 units
func TestDimAssetSchema(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var dimAsset *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "dim_asset" {
			dimAsset = &model
			break
		}
	}

	if dimAsset == nil {
		t.Fatal("dim_asset not found in schema")
	}

	requiredColumns := []string{
		"asset_key",
		"tag_id",
		"equipment_name",
		"system_name",
		"unit_name",
		"design_life_years",
		"install_date",
		"material_grade",
		"damage_mechanism_primary",
		"damage_mechanism_secondary",
	}

	columnMap := make(map[string]bool)
	for _, col := range dimAsset.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("Required column %s not found in dim_asset", colName)
		}
	}

	// Verify asset_key has unique and not_null tests
	var assetKeyCol *schema.ColumnSchema
	for _, col := range dimAsset.Columns {
		if col.Name == "asset_key" {
			assetKeyCol = &col
			break
		}
	}

	if assetKeyCol != nil {
		if !hasDataTest(assetKeyCol.DataTests, "unique") {
			t.Error("asset_key must have unique test")
		}
		if !hasDataTest(assetKeyCol.DataTests, "not_null") {
			t.Error("asset_key must have not_null test")
		}
	}

	// Verify unit_name has accepted_values test with correct units
	var unitNameCol *schema.ColumnSchema
	for _, col := range dimAsset.Columns {
		if col.Name == "unit_name" {
			unitNameCol = &col
			break
		}
	}

	if unitNameCol != nil {
		if !hasDataTest(unitNameCol.DataTests, "accepted_values") {
			t.Error("unit_name must have accepted_values test")
		}
	}
}

// TestDimIOWLimitSchema validates dim_iow_limit has three-tier limits structure
func TestDimIOWLimitSchema(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var dimIOWLimit *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "dim_iow_limit" {
			dimIOWLimit = &model
			break
		}
	}

	if dimIOWLimit == nil {
		t.Fatal("dim_iow_limit not found in schema")
	}

	requiredColumns := []string{
		"limit_key",
		"parameter_type",
		"criticality_level",
		"lower_limit",
		"upper_limit",
		"consequence_description",
	}

	columnMap := make(map[string]bool)
	for _, col := range dimIOWLimit.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("Required column %s not found in dim_iow_limit", colName)
		}
	}

	// Verify criticality_level has accepted_values test
	var criticalityCol *schema.ColumnSchema
	for _, col := range dimIOWLimit.Columns {
		if col.Name == "criticality_level" {
			criticalityCol = &col
			break
		}
	}

	if criticalityCol != nil {
		if !hasDataTest(criticalityCol.DataTests, "accepted_values") {
			t.Error("criticality_level must have accepted_values test")
		}
	}
}

// TestDimParameterTypeSchema validates dim_parameter_type has 4 sensor types
func TestDimParameterTypeSchema(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var dimParamType *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "dim_parameter_type" {
			dimParamType = &model
			break
		}
	}

	if dimParamType == nil {
		t.Fatal("dim_parameter_type not found in schema")
	}

	requiredColumns := []string{
		"parameter_type_key",
		"parameter_type",
		"units",
		"normal_range_min",
		"normal_range_max",
	}

	columnMap := make(map[string]bool)
	for _, col := range dimParamType.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("Required column %s not found in dim_parameter_type", colName)
		}
	}

	// Verify parameter_type has accepted_values test
	var paramTypeCol *schema.ColumnSchema
	for _, col := range dimParamType.Columns {
		if col.Name == "parameter_type" {
			paramTypeCol = &col
			break
		}
	}

	if paramTypeCol != nil {
		if !hasDataTest(paramTypeCol.DataTests, "accepted_values") {
			t.Error("parameter_type must have accepted_values test")
		}
	}
}

// TestDimCriticalitySchema validates dim_criticality_level has 3 levels
func TestDimCriticalitySchema(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var dimCriticality *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "dim_criticality_level" {
			dimCriticality = &model
			break
		}
	}

	if dimCriticality == nil {
		t.Fatal("dim_criticality_level not found in schema")
	}

	requiredColumns := []string{
		"criticality_key",
		"criticality_level",
		"description",
		"response_time_hours",
	}

	columnMap := make(map[string]bool)
	for _, col := range dimCriticality.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("Required column %s not found in dim_criticality_level", colName)
		}
	}

	// Verify criticality_key has unique and not_null tests
	var critKeyCol *schema.ColumnSchema
	for _, col := range dimCriticality.Columns {
		if col.Name == "criticality_key" {
			critKeyCol = &col
			break
		}
	}

	if critKeyCol != nil {
		if !hasDataTest(critKeyCol.DataTests, "unique") {
			t.Error("criticality_key must have unique test")
		}
		if !hasDataTest(critKeyCol.DataTests, "not_null") {
			t.Error("criticality_key must have not_null test")
		}
	}
}

// TestSeedFilesExist verifies all required seed CSV files exist
func TestSeedFilesExist(t *testing.T) {
	requiredSeeds := []string{
		"seeds/dim_date.csv",
		"seeds/dim_asset.csv",
		"seeds/dim_iow_limit.csv",
		"seeds/dim_parameter_type.csv",
		"seeds/dim_criticality_level.csv",
	}

	for _, seedPath := range requiredSeeds {
		if _, err := os.Stat(seedPath); os.IsNotExist(err) {
			t.Errorf("Required seed file %s does not exist", seedPath)
		}
	}
}

// ===================================================================
// PHASE 2 TESTS - STAGING LAYER (Raw Sensor Telemetry)
// ===================================================================

// TestStagingSensorReadingsSeedExists verifies raw_sensor_readings.csv seed file exists
func TestStagingSensorReadingsSeedExists(t *testing.T) {
	seedPath := filepath.Join("seeds", "raw_sensor_readings.csv")

	if _, err := os.Stat(seedPath); os.IsNotExist(err) {
		t.Fatalf("Required seed file %s does not exist", seedPath)
	}
}

// TestStagingSensorReadingsSchema validates stg_sensor_readings schema defined with required columns
func TestStagingSensorReadingsSchema(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var stgSensorReadings *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "stg_sensor_readings" {
			stgSensorReadings = &model
			break
		}
	}

	if stgSensorReadings == nil {
		t.Fatal("stg_sensor_readings not found in schema")
	}

	requiredColumns := []string{
		"reading_id",
		"timestamp",
		"tag_id",
		"parameter_type",
		"measured_value",
		"data_quality_flag",
		"reading_date_key",
		"hour_of_day",
		"is_excursion_candidate",
	}

	columnMap := make(map[string]bool)
	for _, col := range stgSensorReadings.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("Required column %s not found in stg_sensor_readings", colName)
		}
	}

	// Verify reading_id has unique and not_null tests
	var readingIDCol *schema.ColumnSchema
	for _, col := range stgSensorReadings.Columns {
		if col.Name == "reading_id" {
			readingIDCol = &col
			break
		}
	}

	if readingIDCol != nil {
		if !hasDataTest(readingIDCol.DataTests, "unique") {
			t.Error("reading_id must have unique test")
		}
		if !hasDataTest(readingIDCol.DataTests, "not_null") {
			t.Error("reading_id must have not_null test")
		}
	}

	// Verify data_quality_flag has accepted_values test
	var qualityFlagCol *schema.ColumnSchema
	for _, col := range stgSensorReadings.Columns {
		if col.Name == "data_quality_flag" {
			qualityFlagCol = &col
			break
		}
	}

	if qualityFlagCol != nil {
		if !hasDataTest(qualityFlagCol.DataTests, "accepted_values") {
			t.Error("data_quality_flag must have accepted_values test")
		}
	}
}

// TestSensorTimestampSequence validates 5-minute intervals in seed data
func TestSensorTimestampSequence(t *testing.T) {
	seedPath := filepath.Join("seeds", "raw_sensor_readings.csv")

	f, err := os.Open(seedPath)
	if err != nil {
		t.Fatalf("Failed to open seed file: %v", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read CSV: %v", err)
	}

	if len(records) < 2 {
		t.Fatal("Seed file is empty or missing header")
	}

	// Check first 100 records from same sensor to verify 5-minute intervals
	var prevTimestamp time.Time
	checkedCount := 0
	prevTagID := ""

	for i := 1; i < len(records) && checkedCount < 100; i++ {
		tagID := records[i][2]
		timestampStr := records[i][1]

		// Parse timestamp
		timestamp, err := time.Parse("2006-01-02 15:04:05", timestampStr)
		if err != nil {
			t.Errorf("Invalid timestamp format at row %d: %s", i, timestampStr)
			continue
		}

		// Check interval for same sensor
		if tagID == prevTagID && !prevTimestamp.IsZero() {
			interval := timestamp.Sub(prevTimestamp)
			expectedInterval := 5 * time.Minute

			if interval != expectedInterval {
				t.Errorf("Row %d: Expected 5-minute interval, got %v between %s and %s",
					i, interval, prevTimestamp.Format("15:04:05"), timestamp.Format("15:04:05"))
			}
			checkedCount++
		}

		prevTagID = tagID
		prevTimestamp = timestamp
	}

	if checkedCount == 0 {
		t.Error("No intervals checked - data may be insufficient")
	}
}

// TestSensorValueRanges validates physically realistic values
func TestSensorValueRanges(t *testing.T) {
	seedPath := filepath.Join("seeds", "raw_sensor_readings.csv")

	f, err := os.Open(seedPath)
	if err != nil {
		t.Fatalf("Failed to open seed file: %v", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read CSV: %v", err)
	}

	if len(records) < 2 {
		t.Fatal("Seed file is empty or missing header")
	}

	// Define valid ranges per parameter type
	validRanges := map[string]struct{ min, max float64 }{
		"Pressure":    {0, 3000},
		"Temperature": {32, 1400},
		"pH":          {0, 14},
		"Flow":        {0, 50000},
	}

	outOfRangeCount := 0
	maxErrors := 10 // Limit error output

	for i := 1; i < len(records); i++ {
		paramType := records[i][3]
		valueStr := records[i][4]

		value, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			t.Errorf("Row %d: Invalid value format: %s", i, valueStr)
			continue
		}

		if validRange, ok := validRanges[paramType]; ok {
			if value < validRange.min || value > validRange.max {
				outOfRangeCount++
				if outOfRangeCount <= maxErrors {
					t.Errorf("Row %d: %s value %.2f out of valid range [%.2f, %.2f]",
						i, paramType, value, validRange.min, validRange.max)
				}
			}
		} else {
			t.Errorf("Row %d: Unknown parameter type: %s", i, paramType)
		}
	}

	if outOfRangeCount > maxErrors {
		t.Errorf("Total of %d values out of range (showing first %d errors)", outOfRangeCount, maxErrors)
	}
}

// TestAssetTagJoin validates all tag_id values in readings exist in dim_asset
func TestAssetTagJoin(t *testing.T) {
	// Load valid tag IDs from dim_asset
	assetPath := filepath.Join("seeds", "dim_asset.csv")
	assetFile, err := os.Open(assetPath)
	if err != nil {
		t.Fatalf("Failed to open dim_asset.csv: %v", err)
	}
	defer assetFile.Close()

	assetReader := csv.NewReader(assetFile)
	assetRecords, err := assetReader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read dim_asset.csv: %v", err)
	}

	// Build set of valid tag IDs (column 1 is tag_id)
	validTags := make(map[string]bool)
	for i := 1; i < len(assetRecords); i++ {
		validTags[assetRecords[i][1]] = true
	}

	// Check sensor readings
	seedPath := filepath.Join("seeds", "raw_sensor_readings.csv")
	f, err := os.Open(seedPath)
	if err != nil {
		t.Fatalf("Failed to open raw_sensor_readings.csv: %v", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read CSV: %v", err)
	}

	invalidTags := make(map[string]int)
	maxErrors := 10

	for i := 1; i < len(records); i++ {
		tagID := records[i][2]

		if !validTags[tagID] {
			invalidTags[tagID]++
			if len(invalidTags) <= maxErrors {
				t.Errorf("Row %d: tag_id '%s' not found in dim_asset", i, tagID)
			}
		}
	}

	if len(invalidTags) > maxErrors {
		t.Errorf("Total of %d distinct invalid tag_ids found (showing first %d)", len(invalidTags), maxErrors)
	}
}

// TestDataQualityFlags validates data_quality_flag uses only valid values
func TestDataQualityFlags(t *testing.T) {
	seedPath := filepath.Join("seeds", "raw_sensor_readings.csv")

	f, err := os.Open(seedPath)
	if err != nil {
		t.Fatalf("Failed to open seed file: %v", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read CSV: %v", err)
	}

	if len(records) < 2 {
		t.Fatal("Seed file is empty or missing header")
	}

	validFlags := map[string]bool{
		"Good":         true,
		"Questionable": true,
		"Bad":          true,
		"Substituted":  true,
	}

	// Track distribution
	flagCounts := make(map[string]int)
	invalidCount := 0
	maxErrors := 10

	for i := 1; i < len(records); i++ {
		flag := records[i][5]
		flagCounts[flag]++

		if !validFlags[flag] {
			invalidCount++
			if invalidCount <= maxErrors {
				t.Errorf("Row %d: Invalid data_quality_flag: '%s'", i, flag)
			}
		}
	}

	if invalidCount > maxErrors {
		t.Errorf("Total of %d invalid quality flags (showing first %d)", invalidCount, maxErrors)
	}

	// Validate distribution (informational)
	totalRecords := len(records) - 1
	t.Logf("Data quality flag distribution:")
	for flag, count := range flagCounts {
		percentage := float64(count) / float64(totalRecords) * 100
		t.Logf("  %s: %d (%.1f%%)", flag, count, percentage)
	}
}

// TestOneMonthCoverage validates seed data covers approximately 1 month per asset
func TestOneMonthCoverage(t *testing.T) {
	seedPath := filepath.Join("seeds", "raw_sensor_readings.csv")

	f, err := os.Open(seedPath)
	if err != nil {
		t.Fatalf("Failed to open seed file: %v", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read CSV: %v", err)
	}

	if len(records) < 2 {
		t.Fatal("Seed file is empty or missing header")
	}

	// Count readings per tag and parameter combination
	sensorCounts := make(map[string]int)

	for i := 1; i < len(records); i++ {
		tagID := records[i][2]
		paramType := records[i][3]
		sensorKey := tagID + "|" + paramType
		sensorCounts[sensorKey]++
	}

	// Expected readings per sensor for 30 days:
	// 30 days × 24 hours × 12 readings/hour = 8,640
	expectedMin := 8000 // Allow some tolerance
	expectedMax := 9000

	underCount := 0
	overCount := 0

	for sensor, count := range sensorCounts {
		if count < expectedMin {
			underCount++
			if underCount <= 5 {
				parts := strings.Split(sensor, "|")
				t.Errorf("Sensor %s (%s): Only %d readings, expected ~8,640", parts[0], parts[1], count)
			}
		} else if count > expectedMax {
			overCount++
			if overCount <= 5 {
				parts := strings.Split(sensor, "|")
				t.Errorf("Sensor %s (%s): %d readings, expected ~8,640", parts[0], parts[1], count)
			}
		}
	}

	t.Logf("Total sensors: %d", len(sensorCounts))
	t.Logf("Average readings per sensor: %.0f", float64(len(records)-1)/float64(len(sensorCounts)))

	if underCount > 0 {
		t.Errorf("%d sensors have fewer readings than expected", underCount)
	}
	if overCount > 0 {
		t.Errorf("%d sensors have more readings than expected", overCount)
	}
}

// ===================================================================
// PHASE 3 TESTS - INTERMEDIATE LAYER (IOW Excursion Detection)
// ===================================================================

// TestIntIOWExcursionsModel validates int_iow_excursions.sql model file exists
func TestIntIOWExcursionsModel(t *testing.T) {
	modelPath := filepath.Join("models", "intermediate", "int_iow_excursions.sql")

	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		t.Fatalf("Required model file %s does not exist", modelPath)
	}

	// Read model file content
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read model file: %v", err)
	}

	// Verify model contains expected SQL patterns
	contentStr := string(content)
	requiredPatterns := []string{
		"stg_sensor_readings",
		"dim_iow_limit",
		"excursion_magnitude",
		"criticality_level",
		"breach_type",
	}

	for _, pattern := range requiredPatterns {
		if !strings.Contains(contentStr, pattern) {
			t.Errorf("Model file missing required pattern: %s", pattern)
		}
	}
}

// TestIntIOWExcursionsSchema validates int_iow_excursions has required columns
func TestIntIOWExcursionsSchema(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var intIOWExcursions *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "int_iow_excursions" {
			intIOWExcursions = &model
			break
		}
	}

	if intIOWExcursions == nil {
		t.Fatal("int_iow_excursions not found in schema")
	}

	requiredColumns := []string{
		"excursion_id",
		"reading_id",
		"timestamp",
		"tag_id",
		"asset_key",
		"parameter_type",
		"measured_value",
		"breached_limit_value",
		"breach_type",
		"excursion_magnitude",
		"criticality_level",
		"limit_key",
	}

	columnMap := make(map[string]bool)
	for _, col := range intIOWExcursions.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("Required column %s not found in int_iow_excursions", colName)
		}
	}

	// Verify excursion_id has unique and not_null tests
	var excursionIDCol *schema.ColumnSchema
	for _, col := range intIOWExcursions.Columns {
		if col.Name == "excursion_id" {
			excursionIDCol = &col
			break
		}
	}

	if excursionIDCol != nil {
		if !hasDataTest(excursionIDCol.DataTests, "unique") {
			t.Error("excursion_id must have unique test")
		}
		if !hasDataTest(excursionIDCol.DataTests, "not_null") {
			t.Error("excursion_id must have not_null test")
		}
	}

	// Verify breach_type has accepted_values
	var breachTypeCol *schema.ColumnSchema
	for _, col := range intIOWExcursions.Columns {
		if col.Name == "breach_type" {
			breachTypeCol = &col
			break
		}
	}

	if breachTypeCol != nil {
		if !hasDataTest(breachTypeCol.DataTests, "accepted_values") {
			t.Error("breach_type must have accepted_values test (High, Low)")
		}
	}
}

// TestIntExcursionWindowsModel validates int_excursion_windows.sql model file exists
func TestIntExcursionWindowsModel(t *testing.T) {
	modelPath := filepath.Join("models", "intermediate", "int_excursion_windows.sql")

	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		t.Fatalf("Required model file %s does not exist", modelPath)
	}

	// Read model file content
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read model file: %v", err)
	}

	// Verify model contains windowing logic
	contentStr := string(content)
	requiredPatterns := []string{
		"int_iow_excursions",
		"LAG",
		"PARTITION BY",
		"excursion_event_id",
		"duration_minutes",
	}

	for _, pattern := range requiredPatterns {
		if !strings.Contains(contentStr, pattern) {
			t.Errorf("Model file missing required windowing pattern: %s", pattern)
		}
	}
}

// TestIntExcursionWindowsSchema validates int_excursion_windows has required columns
func TestIntExcursionWindowsSchema(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var intExcursionWindows *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "int_excursion_windows" {
			intExcursionWindows = &model
			break
		}
	}

	if intExcursionWindows == nil {
		t.Fatal("int_excursion_windows not found in schema")
	}

	requiredColumns := []string{
		"excursion_event_id",
		"tag_id",
		"asset_key",
		"parameter_type",
		"criticality_level",
		"excursion_start_time",
		"excursion_end_time",
		"duration_minutes",
		"reading_count",
		"peak_magnitude",
		"average_magnitude",
		"breach_type",
		"limit_key",
	}

	columnMap := make(map[string]bool)
	for _, col := range intExcursionWindows.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("Required column %s not found in int_excursion_windows", colName)
		}
	}

	// Verify excursion_event_id has unique and not_null tests
	var eventIDCol *schema.ColumnSchema
	for _, col := range intExcursionWindows.Columns {
		if col.Name == "excursion_event_id" {
			eventIDCol = &col
			break
		}
	}

	if eventIDCol != nil {
		if !hasDataTest(eventIDCol.DataTests, "unique") {
			t.Error("excursion_event_id must have unique test")
		}
		if !hasDataTest(eventIDCol.DataTests, "not_null") {
			t.Error("excursion_event_id must have not_null test")
		}
	}
}

// TestIntExcursionSeverityModel validates int_excursion_severity.sql model file exists
func TestIntExcursionSeverityModel(t *testing.T) {
	modelPath := filepath.Join("models", "intermediate", "int_excursion_severity.sql")

	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		t.Fatalf("Required model file %s does not exist", modelPath)
	}

	// Read model file content
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read model file: %v", err)
	}

	// Verify model contains severity calculation logic
	contentStr := string(content)
	requiredPatterns := []string{
		"int_excursion_windows",
		"severity_score",
		"severity_category",
		"peak_magnitude",
		"duration_minutes",
	}

	for _, pattern := range requiredPatterns {
		if !strings.Contains(contentStr, pattern) {
			t.Errorf("Model file missing required severity pattern: %s", pattern)
		}
	}
}

// TestIntExcursionSeveritySchema validates int_excursion_severity has required columns
func TestIntExcursionSeveritySchema(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var intExcursionSeverity *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "int_excursion_severity" {
			intExcursionSeverity = &model
			break
		}
	}

	if intExcursionSeverity == nil {
		t.Fatal("int_excursion_severity not found in schema")
	}

	requiredColumns := []string{
		"excursion_event_id",
		"tag_id",
		"asset_key",
		"parameter_type",
		"criticality_level",
		"excursion_start_time",
		"excursion_end_time",
		"duration_minutes",
		"reading_count",
		"peak_magnitude",
		"average_magnitude",
		"breach_type",
		"limit_key",
		"severity_score",
		"severity_category",
	}

	columnMap := make(map[string]bool)
	for _, col := range intExcursionSeverity.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("Required column %s not found in int_excursion_severity", colName)
		}
	}

	// Verify severity_category has accepted_values test
	var severityCatCol *schema.ColumnSchema
	for _, col := range intExcursionSeverity.Columns {
		if col.Name == "severity_category" {
			severityCatCol = &col
			break
		}
	}

	if severityCatCol != nil {
		if !hasDataTest(severityCatCol.DataTests, "accepted_values") {
			t.Error("severity_category must have accepted_values test (Extreme, High, Moderate, Low)")
		}
	}
}

// TestExcursionDetection validates excursions are detected at all 3 criticality levels
func TestExcursionDetection(t *testing.T) {
	// This is a placeholder test that will validate data once models are created
	// For now, we just verify the test structure is in place
	t.Log("Test placeholder: Will validate excursions detected at Critical/Standard/Informational levels after model execution")

	// This test would:
	// 1. Query int_iow_excursions table
	// 2. Verify records exist for each criticality_level: Critical, Standard, Informational
	// 3. Verify excursion_magnitude is calculated correctly (> 0)
	// 4. Verify breach_type is either 'High' or 'Low'

	t.Skip("Skipping integration test - requires database execution")
}

// TestExcursionCriticality validates highest criticality assigned when multiple limits breached
func TestExcursionCriticality(t *testing.T) {
	// This is a placeholder test that will validate data once models are created
	t.Log("Test placeholder: Will validate most restrictive criticality level selected when multiple limits breached")

	// This test would:
	// 1. Find readings that breach multiple limits (e.g., both Standard AND Critical)
	// 2. Verify only ONE excursion record created per reading
	// 3. Verify criticality_level = 'Critical' (most restrictive)
	// 4. Verify limit_key points to the Critical limit

	t.Skip("Skipping integration test - requires database execution")
}

// TestNoFalsePositives validates in-limit readings don't create excursion records
func TestNoFalsePositives(t *testing.T) {
	// This is a placeholder test that will validate data once models are created
	t.Log("Test placeholder: Will validate in-limit readings are excluded from int_iow_excursions")

	// This test would:
	// 1. Query stg_sensor_readings for readings within all IOW limits
	// 2. Query int_iow_excursions for those same reading_ids
	// 3. Verify NO records found (no false positives)
	// 4. Calculate false positive rate = 0%

	t.Skip("Skipping integration test - requires database execution")
}

// TestExcursionWindowing validates consecutive excursions grouped with 15-minute gap tolerance
func TestExcursionWindowing(t *testing.T) {
	// This is a placeholder test that will validate data once models are created
	t.Log("Test placeholder: Will validate windowing logic groups consecutive excursions (15-min gap tolerance)")

	// This test would:
	// 1. Query int_excursion_windows to get event windows
	// 2. For each event, verify:
	//    - duration_minutes = excursion_end_time - excursion_start_time
	//    - reading_count matches excursion records in window
	//    - No gap > 15 minutes between consecutive readings within event
	// 3. Verify events separated by > 15 minutes have different excursion_event_ids

	t.Skip("Skipping integration test - requires database execution")
}

// TestSeverityCalculation validates severity scoring based on magnitude and duration
func TestSeverityCalculation(t *testing.T) {
	// This is a placeholder test that will validate data once models are created
	t.Log("Test placeholder: Will validate severity_score calculation formula")

	// This test would:
	// 1. Query int_excursion_severity
	// 2. For sample events, verify:
	//    - severity_score = (peak_magnitude × 0.4) + (duration_minutes × 0.3) + (criticality_weight × 0.3)
	//    - criticality_weight: Critical=3.0, Standard=2.0, Informational=1.0
	//    - severity_category matches score: Extreme (>8), High (5-8), Moderate (2-5), Low (<2)
	// 3. Verify all scores are >= 0

	t.Skip("Skipping integration test - requires database execution")
}

// ===================================================================
// PHASE 4 TESTS: FACT TABLES (Excursion Events with Damage Metrics)
// ===================================================================

// TestFactExcursionEventsModel validates fact_excursion_events.sql model file exists
func TestFactExcursionEventsModel(t *testing.T) {
	modelPath := filepath.Join("models", "marts", "fact_excursion_events.sql")

	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		t.Fatalf("Required model file %s does not exist", modelPath)
	}

	// Read model file content
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read model file: %v", err)
	}

	// Verify model contains fact table logic
	contentStr := string(content)
	requiredPatterns := []string{
		"int_excursion_severity",
		"ROW_NUMBER",
		"cumulative_damage_index",
		"excursion_event_key",
		"asset_key",
		"date_key",
		"parameter_type_key",
		"limit_key",
		"criticality_key",
		"excursion_type",
	}

	for _, pattern := range requiredPatterns {
		if !strings.Contains(contentStr, pattern) {
			t.Errorf("Model file missing required fact table pattern: %s", pattern)
		}
	}
}

// TestFactExcursionEventsSchema validates fact_excursion_events has required columns
func TestFactExcursionEventsSchema(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var factExcursionEvents *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "fact_excursion_events" {
			factExcursionEvents = &model
			break
		}
	}

	if factExcursionEvents == nil {
		t.Fatal("fact_excursion_events not found in schema")
	}

	requiredColumns := []string{
		"excursion_event_key",
		"excursion_event_id",
		"asset_key",
		"date_key",
		"parameter_type_key",
		"limit_key",
		"criticality_key",
		"excursion_start_timestamp",
		"excursion_end_timestamp",
		"duration_minutes",
		"reading_count",
		"peak_magnitude",
		"average_magnitude",
		"cumulative_damage_index",
		"severity_score",
		"severity_category",
		"breach_type",
		"excursion_type",
	}

	columnMap := make(map[string]bool)
	for _, col := range factExcursionEvents.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("Required column %s not found in fact_excursion_events", colName)
		}
	}

	// Verify excursion_event_key has unique and not_null tests
	var eventKeyCol *schema.ColumnSchema
	for _, col := range factExcursionEvents.Columns {
		if col.Name == "excursion_event_key" {
			eventKeyCol = &col
			break
		}
	}

	if eventKeyCol != nil {
		if !hasDataTest(eventKeyCol.DataTests, "unique") {
			t.Error("excursion_event_key must have unique test")
		}
		if !hasDataTest(eventKeyCol.DataTests, "not_null") {
			t.Error("excursion_event_key must have not_null test")
		}
	}

	// Verify excursion_type has accepted_values
	var excursionTypeCol *schema.ColumnSchema
	for _, col := range factExcursionEvents.Columns {
		if col.Name == "excursion_type" {
			excursionTypeCol = &col
			break
		}
	}

	if excursionTypeCol != nil {
		if !hasDataTest(excursionTypeCol.DataTests, "accepted_values") {
			t.Error("excursion_type must have accepted_values test")
		}
	}

	// Verify cumulative_damage_index has not_null test
	var damageCol *schema.ColumnSchema
	for _, col := range factExcursionEvents.Columns {
		if col.Name == "cumulative_damage_index" {
			damageCol = &col
			break
		}
	}

	if damageCol != nil {
		if !hasDataTest(damageCol.DataTests, "not_null") {
			t.Error("cumulative_damage_index must have not_null test")
		}
	}
}

// TestFactAssetDamageAccumulationModel validates fact_asset_damage_accumulation.sql model file exists
func TestFactAssetDamageAccumulationModel(t *testing.T) {
	modelPath := filepath.Join("models", "marts", "fact_asset_damage_accumulation.sql")

	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		t.Fatalf("Required model file %s does not exist", modelPath)
	}

	// Read model file content
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read model file: %v", err)
	}

	// Verify model contains damage accumulation logic
	contentStr := string(content)
	requiredPatterns := []string{
		"fact_excursion_events",
		"cumulative_damage_to_date",
		"damage_last_30_days",
		"damage_last_90_days",
		"damage_last_365_days",
		"GROUP BY",
		"asset_key",
	}

	for _, pattern := range requiredPatterns {
		if !strings.Contains(contentStr, pattern) {
			t.Errorf("Model file missing required damage accumulation pattern: %s", pattern)
		}
	}
}

// TestFactAssetDamageAccumulationSchema validates fact_asset_damage_accumulation has required columns
func TestFactAssetDamageAccumulationSchema(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	var factAssetDamage *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "fact_asset_damage_accumulation" {
			factAssetDamage = &model
			break
		}
	}

	if factAssetDamage == nil {
		t.Fatal("fact_asset_damage_accumulation not found in schema")
	}

	requiredColumns := []string{
		"damage_record_key",
		"asset_key",
		"as_of_date_key",
		"cumulative_damage_to_date",
		"damage_last_30_days",
		"damage_last_90_days",
		"damage_last_365_days",
		"excursion_count_total",
		"critical_excursion_count",
		"standard_excursion_count",
		"informational_excursion_count",
		"days_since_last_critical_excursion",
		"last_excursion_date",
		"asset_install_date",
		"design_life_years",
	}

	columnMap := make(map[string]bool)
	for _, col := range factAssetDamage.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("Required column %s not found in fact_asset_damage_accumulation", colName)
		}
	}

	// Verify damage_record_key has unique and not_null tests
	var keyCol *schema.ColumnSchema
	for _, col := range factAssetDamage.Columns {
		if col.Name == "damage_record_key" {
			keyCol = &col
			break
		}
	}

	if keyCol != nil {
		if !hasDataTest(keyCol.DataTests, "unique") {
			t.Error("damage_record_key must have unique test")
		}
		if !hasDataTest(keyCol.DataTests, "not_null") {
			t.Error("damage_record_key must have not_null test")
		}
	}

	// Verify cumulative_damage_to_date has not_null test
	var cumulativeDamageCol *schema.ColumnSchema
	for _, col := range factAssetDamage.Columns {
		if col.Name == "cumulative_damage_to_date" {
			cumulativeDamageCol = &col
			break
		}
	}

	if cumulativeDamageCol != nil {
		if !hasDataTest(cumulativeDamageCol.DataTests, "not_null") {
			t.Error("cumulative_damage_to_date must have not_null test")
		}
	}
}

// TestFactGrainUniqueness validates one row per excursion event
func TestFactGrainUniqueness(t *testing.T) {
	// This is a placeholder test that will validate data once models are created
	t.Log("Test placeholder: Will validate fact grain - one row per excursion event")

	// This test would:
	// 1. Query fact_excursion_events
	// 2. Verify excursion_event_key is unique
	// 3. Verify COUNT(*) matches COUNT(DISTINCT excursion_event_id)
	// 4. Verify no duplicate rows for same excursion_event_id

	t.Skip("Skipping integration test - requires database execution")
}

// TestDamageCalculationAUC validates Area Under Curve calculation
func TestDamageCalculationAUC(t *testing.T) {
	// This is a placeholder test that will validate data once models are created
	t.Log("Test placeholder: Will validate cumulative_damage_index = average_magnitude × duration_minutes")

	// This test would:
	// 1. Query fact_excursion_events
	// 2. For each event, verify:
	//    - cumulative_damage_index = average_magnitude × duration_minutes (Area Under Curve)
	//    - cumulative_damage_index >= 0
	//    - If duration_minutes = 0, then cumulative_damage_index = 0

	t.Skip("Skipping integration test - requires database execution")
}

// TestReferentialIntegrity validates all FKs resolve
func TestReferentialIntegrity(t *testing.T) {
	// This is a placeholder test that will validate data once models are created
	t.Log("Test placeholder: Will validate all foreign keys resolve to dimension tables")

	// This test would:
	// 1. Query fact_excursion_events joined to dimensions
	// 2. Verify all asset_key values exist in dim_asset
	// 3. Verify all date_key values exist in dim_date
	// 4. Verify all parameter_type_key values exist in dim_parameter_type
	// 5. Verify all limit_key values exist in dim_iow_limit
	// 6. Verify all criticality_key values exist in dim_criticality_level
	// 7. Count orphaned records (should be 0)

	t.Skip("Skipping integration test - requires database execution")
}

// ===================================================================
// PHASE 5 TESTS: METRICS LAYER (Asset Integrity Health Indices & Bad Actors)
// ===================================================================

// TestMetricsAssetIntegrityIndexModel validates metrics_asset_integrity_index.sql model file exists
func TestMetricsAssetIntegrityIndexModel(t *testing.T) {
	modelPath := filepath.Join("models", "marts", "metrics_asset_integrity_index.sql")

	// Verify file exists
	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		t.Fatalf("metrics_asset_integrity_index.sql model does not exist at %s", modelPath)
	}

	// Verify it's a non-empty file
	info, err := os.Stat(modelPath)
	if err != nil {
		t.Fatalf("Failed to stat model file: %v", err)
	}
	if info.Size() == 0 {
		t.Error("metrics_asset_integrity_index.sql is empty - must contain SELECT logic")
	}

	// Verify it references fact_asset_damage_accumulation
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read model file: %v", err)
	}
	contentStr := string(content)
	if !strings.Contains(strings.ToLower(contentStr), "fact_asset_damage_accumulation") {
		t.Error("metrics_asset_integrity_index.sql must reference fact_asset_damage_accumulation")
	}
}

// TestMetricsAssetIntegrityIndexSchema validates metrics_asset_integrity_index has required columns
func TestMetricsAssetIntegrityIndexSchema(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	// Find metrics_asset_integrity_index model
	var metricsModel *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "metrics_asset_integrity_index" {
			metricsModel = &model
			break
		}
	}

	if metricsModel == nil {
		t.Fatal("metrics_asset_integrity_index not found in schema")
	}

	// Verify required columns exist
	requiredColumns := []string{
		"asset_key",
		"tag_id",
		"equipment_name",
		"unit_name",
		"integrity_health_index",
		"health_status",
		"health_trend_30d",
		"trend_direction",
		"total_excursion_count",
		"critical_excursion_count",
		"cumulative_damage_total",
		"damage_last_90_days",
		"days_since_last_critical",
		"last_excursion_date",
		"asset_install_date",
		"design_life_years",
		"years_in_service",
	}

	columnMap := make(map[string]bool)
	for _, col := range metricsModel.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("Required column %s not found in metrics_asset_integrity_index", colName)
		}
	}

	// Verify integrity_health_index has accepted_range test (0-100)
	var healthIndexCol *schema.ColumnSchema
	for _, col := range metricsModel.Columns {
		if col.Name == "integrity_health_index" {
			healthIndexCol = &col
			break
		}
	}

	if healthIndexCol != nil {
		if !hasDataTest(healthIndexCol.DataTests, "not_null") {
			t.Error("integrity_health_index must have not_null test")
		}
		// Check for accepted_range test (simplified check)
		foundRange := false
		for _, test := range healthIndexCol.DataTests {
			if testMap, ok := test.(map[string]interface{}); ok {
				if _, ok := testMap["accepted_range"]; ok {
					foundRange = true
					break
				}
			}
		}
		if !foundRange {
			t.Error("integrity_health_index must have accepted_range test (0-100)")
		}
	}

	// Verify health_status has accepted_values test
	var healthStatusCol *schema.ColumnSchema
	for _, col := range metricsModel.Columns {
		if col.Name == "health_status" {
			healthStatusCol = &col
			break
		}
	}

	if healthStatusCol != nil {
		foundAcceptedValues := false
		for _, test := range healthStatusCol.DataTests {
			if testMap, ok := test.(map[string]interface{}); ok {
				if _, ok := testMap["accepted_values"]; ok {
					foundAcceptedValues = true
					break
				}
			}
		}
		if !foundAcceptedValues {
			t.Error("health_status must have accepted_values test")
		}
	}
}

// TestMetricsBadActorsModel validates metrics_bad_actors.sql model file exists
func TestMetricsBadActorsModel(t *testing.T) {
	modelPath := filepath.Join("models", "marts", "metrics_bad_actors.sql")

	// Verify file exists
	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		t.Fatalf("metrics_bad_actors.sql model does not exist at %s", modelPath)
	}

	// Verify it's a non-empty file
	info, err := os.Stat(modelPath)
	if err != nil {
		t.Fatalf("Failed to stat model file: %v", err)
	}
	if info.Size() == 0 {
		t.Error("metrics_bad_actors.sql is empty - must contain SELECT logic")
	}

	// Verify it references metrics_asset_integrity_index
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read model file: %v", err)
	}
	contentStr := string(content)
	if !strings.Contains(strings.ToLower(contentStr), "metrics_asset_integrity_index") {
		t.Error("metrics_bad_actors.sql must reference metrics_asset_integrity_index")
	}
}

// TestMetricsBadActorsSchema validates metrics_bad_actors has required columns
func TestMetricsBadActorsSchema(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	// Find metrics_bad_actors model
	var badActorsModel *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "metrics_bad_actors" {
			badActorsModel = &model
			break
		}
	}

	if badActorsModel == nil {
		t.Fatal("metrics_bad_actors not found in schema")
	}

	// Verify required columns exist
	requiredColumns := []string{
		"bad_actor_rank",
		"asset_key",
		"tag_id",
		"equipment_name",
		"unit_name",
		"bad_actor_score",
		"integrity_health_index",
		"primary_reason_code",
		"secondary_reason_code",
		"total_excursion_count",
		"critical_excursion_count",
		"cumulative_damage_total",
		"damage_last_90_days",
		"days_since_last_critical",
		"health_trend_30d",
		"recommended_action",
	}

	columnMap := make(map[string]bool)
	for _, col := range badActorsModel.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("Required column %s not found in metrics_bad_actors", colName)
		}
	}

	// Verify bad_actor_rank has unique and not_null tests
	var rankCol *schema.ColumnSchema
	for _, col := range badActorsModel.Columns {
		if col.Name == "bad_actor_rank" {
			rankCol = &col
			break
		}
	}

	if rankCol != nil {
		if !hasDataTest(rankCol.DataTests, "unique") {
			t.Error("bad_actor_rank must have unique test")
		}
		if !hasDataTest(rankCol.DataTests, "not_null") {
			t.Error("bad_actor_rank must have not_null test")
		}
	}

	// Verify recommended_action has accepted_values test
	var actionCol *schema.ColumnSchema
	for _, col := range badActorsModel.Columns {
		if col.Name == "recommended_action" {
			actionCol = &col
			break
		}
	}

	if actionCol != nil {
		foundAcceptedValues := false
		for _, test := range actionCol.DataTests {
			if testMap, ok := test.(map[string]interface{}); ok {
				if _, ok := testMap["accepted_values"]; ok {
					foundAcceptedValues = true
					break
				}
			}
		}
		if !foundAcceptedValues {
			t.Error("recommended_action must have accepted_values test")
		}
	}
}

// TestMetricsUnitHealthSummaryModel validates metrics_unit_health_summary.sql model file exists
func TestMetricsUnitHealthSummaryModel(t *testing.T) {
	modelPath := filepath.Join("models", "marts", "metrics_unit_health_summary.sql")

	// Verify file exists
	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		t.Fatalf("metrics_unit_health_summary.sql model does not exist at %s", modelPath)
	}

	// Verify it's a non-empty file
	info, err := os.Stat(modelPath)
	if err != nil {
		t.Fatalf("Failed to stat model file: %v", err)
	}
	if info.Size() == 0 {
		t.Error("metrics_unit_health_summary.sql is empty - must contain SELECT logic")
	}

	// Verify it references metrics_asset_integrity_index
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read model file: %v", err)
	}
	contentStr := string(content)
	if !strings.Contains(strings.ToLower(contentStr), "metrics_asset_integrity_index") {
		t.Error("metrics_unit_health_summary.sql must reference metrics_asset_integrity_index")
	}

	// Verify it contains GROUP BY unit_name
	if !strings.Contains(strings.ToLower(contentStr), "group by") {
		t.Error("metrics_unit_health_summary.sql must contain GROUP BY clause for unit aggregation")
	}
}

// TestMetricsUnitHealthSummarySchema validates metrics_unit_health_summary has required columns
func TestMetricsUnitHealthSummarySchema(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	// Find metrics_unit_health_summary model
	var unitSummaryModel *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "metrics_unit_health_summary" {
			unitSummaryModel = &model
			break
		}
	}

	if unitSummaryModel == nil {
		t.Fatal("metrics_unit_health_summary not found in schema")
	}

	// Verify required columns exist
	requiredColumns := []string{
		"unit_name",
		"asset_count",
		"unit_avg_health_index",
		"unit_min_health_index",
		"unit_max_health_index",
		"assets_in_critical_status",
		"assets_in_poor_status",
		"unit_critical_excursion_count",
		"unit_total_excursion_count",
		"unit_damage_total",
		"unit_damage_last_90_days",
		"worst_asset_tag_id",
		"worst_asset_health_index",
		"unit_health_status",
	}

	columnMap := make(map[string]bool)
	for _, col := range unitSummaryModel.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("Required column %s not found in metrics_unit_health_summary", colName)
		}
	}

	// Verify unit_name has unique and not_null tests (PK)
	var unitNameCol *schema.ColumnSchema
	for _, col := range unitSummaryModel.Columns {
		if col.Name == "unit_name" {
			unitNameCol = &col
			break
		}
	}

	if unitNameCol != nil {
		if !hasDataTest(unitNameCol.DataTests, "unique") {
			t.Error("unit_name must have unique test (primary key)")
		}
		if !hasDataTest(unitNameCol.DataTests, "not_null") {
			t.Error("unit_name must have not_null test")
		}
	}

	// Verify unit_health_status has accepted_values test
	var unitHealthStatusCol *schema.ColumnSchema
	for _, col := range unitSummaryModel.Columns {
		if col.Name == "unit_health_status" {
			unitHealthStatusCol = &col
			break
		}
	}

	if unitHealthStatusCol != nil {
		foundAcceptedValues := false
		for _, test := range unitHealthStatusCol.DataTests {
			if testMap, ok := test.(map[string]interface{}); ok {
				if _, ok := testMap["accepted_values"]; ok {
					foundAcceptedValues = true
					break
				}
			}
		}
		if !foundAcceptedValues {
			t.Error("unit_health_status must have accepted_values test")
		}
	}
}

// ===================================================================
// PHASE 6: ALERT MODELS TESTS
// ===================================================================

// TestAlertsCriticalExcursionsModel validates alerts_critical_excursions.sql model file exists
func TestAlertsCriticalExcursionsModel(t *testing.T) {
	modelPath := filepath.Join("models", "marts", "alerts_critical_excursions.sql")

	// Verify file exists
	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		t.Fatalf("alerts_critical_excursions.sql model does not exist at %s", modelPath)
	}

	// Read model file content
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read model file: %v", err)
	}

	// Verify model references fact_excursion_events
	contentStr := string(content)
	requiredPatterns := []string{
		"fact_excursion_events",
		"criticality_level",
		"Critical",
		"alert_id",
		"alert_timestamp",
		"asset_key",
		"alert_type",
		"priority",
		"message",
		"acknowledged_flag",
	}

	for _, pattern := range requiredPatterns {
		if !strings.Contains(contentStr, pattern) {
			t.Errorf("alerts_critical_excursions.sql missing required pattern: %s", pattern)
		}
	}
}

// TestAlertsCriticalExcursionsSchema validates alerts_critical_excursions has required columns
func TestAlertsCriticalExcursionsSchema(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	// Find alerts_critical_excursions model
	var alertsModel *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "alerts_critical_excursions" {
			alertsModel = &model
			break
		}
	}

	if alertsModel == nil {
		t.Fatal("alerts_critical_excursions not found in schema")
	}

	// Verify required columns exist
	requiredColumns := []string{
		"alert_id",
		"alert_timestamp",
		"asset_key",
		"asset_name",
		"alert_type",
		"priority",
		"message",
		"acknowledged_flag",
		"excursion_event_id",
	}

	columnMap := make(map[string]bool)
	for _, col := range alertsModel.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("Required column %s not found in alerts_critical_excursions", colName)
		}
	}

	// Verify alert_id has unique and not_null tests
	var alertIdCol *schema.ColumnSchema
	for _, col := range alertsModel.Columns {
		if col.Name == "alert_id" {
			alertIdCol = &col
			break
		}
	}

	if alertIdCol != nil {
		if !hasDataTest(alertIdCol.DataTests, "unique") {
			t.Error("alert_id must have unique test")
		}
		if !hasDataTest(alertIdCol.DataTests, "not_null") {
			t.Error("alert_id must have not_null test")
		}
	}

	// Verify alert_type has accepted_values test
	var alertTypeCol *schema.ColumnSchema
	for _, col := range alertsModel.Columns {
		if col.Name == "alert_type" {
			alertTypeCol = &col
			break
		}
	}

	if alertTypeCol != nil {
		if !hasDataTest(alertTypeCol.DataTests, "accepted_values") {
			t.Error("alert_type must have accepted_values test")
		}
	}
}

// TestAlertsInspectionDueModel validates alerts_inspection_due.sql model file exists
func TestAlertsInspectionDueModel(t *testing.T) {
	modelPath := filepath.Join("models", "marts", "alerts_inspection_due.sql")

	// Verify file exists
	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		t.Fatalf("alerts_inspection_due.sql model does not exist at %s", modelPath)
	}

	// Read model file content
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read model file: %v", err)
	}

	// Verify model references required upstream models
	contentStr := string(content)
	requiredPatterns := []string{
		"fact_asset_damage_accumulation",
		"metrics_asset_integrity_index",
		"cumulative_damage",
		"health_index",
		"damage_pct_of_limit",
		"days_until_inspection",
		"recommended_action",
	}

	for _, pattern := range requiredPatterns {
		if !strings.Contains(contentStr, pattern) {
			t.Errorf("alerts_inspection_due.sql missing required pattern: %s", pattern)
		}
	}
}

// TestAlertsInspectionDueSchema validates alerts_inspection_due has required columns
func TestAlertsInspectionDueSchema(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	// Find alerts_inspection_due model
	var alertsModel *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "alerts_inspection_due" {
			alertsModel = &model
			break
		}
	}

	if alertsModel == nil {
		t.Fatal("alerts_inspection_due not found in schema")
	}

	// Verify required columns exist
	requiredColumns := []string{
		"alert_id",
		"alert_timestamp",
		"asset_key",
		"asset_name",
		"alert_type",
		"priority",
		"days_until_inspection",
		"damage_pct_of_limit",
		"recommended_action",
		"message",
		"acknowledged_flag",
	}

	columnMap := make(map[string]bool)
	for _, col := range alertsModel.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("Required column %s not found in alerts_inspection_due", colName)
		}
	}

	// Verify alert_id has unique and not_null tests
	var alertIdCol *schema.ColumnSchema
	for _, col := range alertsModel.Columns {
		if col.Name == "alert_id" {
			alertIdCol = &col
			break
		}
	}

	if alertIdCol != nil {
		if !hasDataTest(alertIdCol.DataTests, "unique") {
			t.Error("alert_id must have unique test")
		}
		if !hasDataTest(alertIdCol.DataTests, "not_null") {
			t.Error("alert_id must have not_null test")
		}
	}
}

// TestAlertsHealthDegradationModel validates alerts_health_degradation.sql model file exists
func TestAlertsHealthDegradationModel(t *testing.T) {
	modelPath := filepath.Join("models", "marts", "alerts_health_degradation.sql")

	// Verify file exists
	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		t.Fatalf("alerts_health_degradation.sql model does not exist at %s", modelPath)
	}

	// Read model file content
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read model file: %v", err)
	}

	// Verify model references metrics_asset_integrity_index and has 30-day comparison logic
	contentStr := string(content)
	requiredPatterns := []string{
		"metrics_asset_integrity_index",
		"health_index",
		"health_change",
		"degradation_severity",
		"health_index_current",
		"health_index_30d_ago",
	}

	for _, pattern := range requiredPatterns {
		if !strings.Contains(contentStr, pattern) {
			t.Errorf("alerts_health_degradation.sql missing required pattern: %s", pattern)
		}
	}
}

// TestAlertsHealthDegradationSchema validates alerts_health_degradation has required columns
func TestAlertsHealthDegradationSchema(t *testing.T) {
	schemaPath := filepath.Join("schema.yml")

	schemaFile, err := schema.ParseSchemaFile(schemaPath)
	if err != nil {
		t.Fatalf("Failed to parse schema.yml: %v", err)
	}

	// Find alerts_health_degradation model
	var alertsModel *schema.ModelSchema
	for _, model := range schemaFile.Models {
		if model.Name == "alerts_health_degradation" {
			alertsModel = &model
			break
		}
	}

	if alertsModel == nil {
		t.Fatal("alerts_health_degradation not found in schema")
	}

	// Verify required columns exist
	requiredColumns := []string{
		"alert_id",
		"alert_timestamp",
		"asset_key",
		"asset_name",
		"alert_type",
		"priority",
		"health_index_current",
		"health_index_30d_ago",
		"health_change",
		"degradation_severity",
		"recommended_action",
		"message",
		"acknowledged_flag",
	}

	columnMap := make(map[string]bool)
	for _, col := range alertsModel.Columns {
		columnMap[col.Name] = true
	}

	for _, colName := range requiredColumns {
		if !columnMap[colName] {
			t.Errorf("Required column %s not found in alerts_health_degradation", colName)
		}
	}

	// Verify alert_id has unique and not_null tests
	var alertIdCol *schema.ColumnSchema
	for _, col := range alertsModel.Columns {
		if col.Name == "alert_id" {
			alertIdCol = &col
			break
		}
	}

	if alertIdCol != nil {
		if !hasDataTest(alertIdCol.DataTests, "unique") {
			t.Error("alert_id must have unique test")
		}
		if !hasDataTest(alertIdCol.DataTests, "not_null") {
			t.Error("alert_id must have not_null test")
		}
	}

	// Verify degradation_severity has accepted_values test
	var degradationCol *schema.ColumnSchema
	for _, col := range alertsModel.Columns {
		if col.Name == "degradation_severity" {
			degradationCol = &col
			break
		}
	}

	if degradationCol != nil {
		if !hasDataTest(degradationCol.DataTests, "accepted_values") {
			t.Error("degradation_severity must have accepted_values test")
		}
	}
}
