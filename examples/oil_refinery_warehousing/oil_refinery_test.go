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
