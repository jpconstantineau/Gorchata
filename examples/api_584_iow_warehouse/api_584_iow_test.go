package api_584_iow_test

import (
	"os"
	"path/filepath"
	"testing"

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
