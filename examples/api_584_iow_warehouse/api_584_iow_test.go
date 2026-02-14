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
