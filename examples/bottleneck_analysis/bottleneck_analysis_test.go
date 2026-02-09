package bottleneck_analysis_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/config"
	_ "modernc.org/sqlite"
)

// TestProjectConfigExists verifies gorchata_project.yml exists and loads correctly
func TestProjectConfigExists(t *testing.T) {
	projectPath := filepath.Join("gorchata_project.yml")

	// Verify file exists
	if _, err := os.Stat(projectPath); os.IsNotExist(err) {
		t.Fatalf("gorchata_project.yml does not exist at %s", projectPath)
	}

	// Load and validate project config
	cfg, err := config.LoadProject(projectPath)
	if err != nil {
		t.Fatalf("LoadProject() error = %v, want nil", err)
	}

	// Verify project name
	if cfg.Name != "bottleneck_analysis" {
		t.Errorf("Name = %q, want %q", cfg.Name, "bottleneck_analysis")
	}

	// Verify version
	if cfg.Version != "1.0" {
		t.Errorf("Version = %q, want %q", cfg.Version, "1.0")
	}

	// Verify profile
	if cfg.Profile != "dev" {
		t.Errorf("Profile = %q, want %q", cfg.Profile, "dev")
	}

	// Verify model paths
	if len(cfg.ModelPaths) == 0 {
		t.Error("ModelPaths is empty, want at least 1 path")
	}
	if len(cfg.ModelPaths) > 0 && cfg.ModelPaths[0] != "models" {
		t.Errorf("ModelPaths[0] = %q, want %q", cfg.ModelPaths[0], "models")
	}

	// Verify vars exist
	if cfg.Vars == nil {
		t.Fatal("Vars is nil, want non-nil")
	}

	// Verify analysis_start_date var
	if _, ok := cfg.Vars["analysis_start_date"]; !ok {
		t.Error("Vars['analysis_start_date'] not found")
	}

	// Verify analysis_end_date var
	if _, ok := cfg.Vars["analysis_end_date"]; !ok {
		t.Error("Vars['analysis_end_date'] not found")
	}

	// Verify shift_hours var
	if _, ok := cfg.Vars["shift_hours"]; !ok {
		t.Error("Vars['shift_hours'] not found")
	}
}

// TestProfilesConfigExists verifies profiles.yml exists and has valid structure
func TestProfilesConfigExists(t *testing.T) {
	profilesPath := filepath.Join("profiles.yml")

	// Verify file exists
	if _, err := os.Stat(profilesPath); os.IsNotExist(err) {
		t.Fatalf("profiles.yml does not exist at %s", profilesPath)
	}

	// Load profiles config
	profilesCfg, err := config.LoadProfiles(profilesPath)
	if err != nil {
		t.Fatalf("LoadProfiles() error = %v, want nil", err)
	}

	// Verify dev output exists
	output, err := profilesCfg.GetOutput("dev")
	if err != nil {
		t.Fatalf("GetOutput('dev') error = %v, want nil", err)
	}

	// Verify database type is sqlite
	if output.Type != "sqlite" {
		t.Errorf("Type = %q, want %q", output.Type, "sqlite")
	}

	// Verify database path is set
	if output.Database == "" {
		t.Error("Database path is empty, want non-empty")
	}
}

// TestDirectoryStructure verifies all required directories exist
func TestDirectoryStructure(t *testing.T) {
	requiredDirs := []string{
		"seeds",
		"models",
		"models/sources",
		"models/dimensions",
		"models/facts",
		"models/rollups",
		"tests",
		"tests/generic",
		"docs",
	}

	for _, dir := range requiredDirs {
		dirPath := filepath.Join(dir)
		info, err := os.Stat(dirPath)
		if os.IsNotExist(err) {
			t.Errorf("Directory %s does not exist", dirPath)
			continue
		}
		if err != nil {
			t.Errorf("Error checking directory %s: %v", dirPath, err)
			continue
		}
		if !info.IsDir() {
			t.Errorf("%s exists but is not a directory", dirPath)
		}
	}
}

// TestREADMEExists verifies README.md exists
func TestREADMEExists(t *testing.T) {
	readmePath := filepath.Join("README.md")

	// Verify file exists
	if _, err := os.Stat(readmePath); os.IsNotExist(err) {
		t.Fatalf("README.md does not exist at %s", readmePath)
	}

	// Read file to verify it has content
	content, err := os.ReadFile(readmePath)
	if err != nil {
		t.Fatalf("Failed to read README.md: %v", err)
	}

	if len(content) == 0 {
		t.Error("README.md is empty, want non-empty")
	}
}

// ==================== Phase 2: Seed Data Tests ====================

// TestSeedConfigExists verifies seeds/seed.yml exists and has correct structure
func TestSeedConfigExists(t *testing.T) {
	seedPath := filepath.Join("seeds", "seed.yml")

	// Verify file exists
	if _, err := os.Stat(seedPath); os.IsNotExist(err) {
		t.Fatalf("seeds/seed.yml does not exist at %s", seedPath)
	}

	// Load and validate seed config
	seedCfg, err := config.ParseSeedConfig(seedPath)
	if err != nil {
		t.Fatalf("ParseSeedConfig() error = %v, want nil", err)
	}

	// Verify batch size
	if seedCfg.Import.BatchSize != 1000 {
		t.Errorf("Import.BatchSize = %d, want %d", seedCfg.Import.BatchSize, 1000)
	}

	// Verify naming strategy
	if seedCfg.Naming.Strategy != "filename" {
		t.Errorf("Naming.Strategy = %q, want %q", seedCfg.Naming.Strategy, "filename")
	}

	// Verify scope
	if seedCfg.Import.Scope != "folder" {
		t.Errorf("Import.Scope = %q, want %q", seedCfg.Import.Scope, "folder")
	}
}

// TestSeedCSVFilesExist verifies all required CSV seed files exist
func TestSeedCSVFilesExist(t *testing.T) {
	requiredFiles := []string{
		"seeds/raw_resources.csv",
		"seeds/raw_work_orders.csv",
		"seeds/raw_operations.csv",
		"seeds/raw_downtime.csv",
	}

	for _, file := range requiredFiles {
		filePath := filepath.Join(file)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			t.Errorf("Required seed file %s does not exist", filePath)
		}
	}
}

// TestResourcesCSVStructure verifies raw_resources.csv has correct headers and valid data
func TestResourcesCSVStructure(t *testing.T) {
	filePath := filepath.Join("seeds", "raw_resources.csv")

	// Read CSV file
	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read %s: %v", filePath, err)
	}

	lines := string(content)
	if lines == "" {
		t.Fatal("raw_resources.csv is empty")
	}

	// Verify header row
	expectedHeader := "resource_id,resource_name,resource_type,available_hours_per_shift,shifts_per_day,theoretical_capacity_per_hour"
	if len(lines) < len(expectedHeader) {
		t.Fatalf("File too short to contain header")
	}

	// Check if header is present (simple check - contains key columns)
	if !containsSubstring(lines, "resource_id") || !containsSubstring(lines, "resource_name") {
		t.Error("Missing expected header columns in raw_resources.csv")
	}

	// Verify at least some data rows exist
	lineCount := countLines(lines)
	if lineCount < 6 { // Header + at least 5 resources (NCX-10, Heat Treat, Milling, Assembly, Grinding)
		t.Errorf("raw_resources.csv has %d lines, want at least 6 (header + 5 resources)", lineCount)
	}

	// Verify NCX-10 and Heat Treat are present (bottleneck resources)
	if !containsSubstring(lines, "NCX-10") {
		t.Error("NCX-10 resource not found in raw_resources.csv")
	}
	if !containsSubstring(lines, "Heat Treat") {
		t.Error("Heat Treat resource not found in raw_resources.csv")
	}
}

// TestWorkOrdersCSVStructure verifies raw_work_orders.csv has correct headers and valid data
func TestWorkOrdersCSVStructure(t *testing.T) {
	filePath := filepath.Join("seeds", "raw_work_orders.csv")

	// Read CSV file
	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read %s: %v", filePath, err)
	}

	lines := string(content)
	if lines == "" {
		t.Fatal("raw_work_orders.csv is empty")
	}

	// Verify key header columns
	if !containsSubstring(lines, "work_order_id") || !containsSubstring(lines, "part_number") {
		t.Error("Missing expected header columns in raw_work_orders.csv")
	}

	// Verify at least ~50 work orders exist
	lineCount := countLines(lines)
	if lineCount < 40 { // Header + at least 39 work orders (some flexibility)
		t.Errorf("raw_work_orders.csv has %d lines, want at least 40 (header + ~50 orders)", lineCount)
	}

	// Verify part numbers are present
	if !containsSubstring(lines, "PART-") {
		t.Error("No PART- prefix found in work orders")
	}
}

// TestOperationsCSVStructure verifies raw_operations.csv has correct headers and valid data
func TestOperationsCSVStructure(t *testing.T) {
	filePath := filepath.Join("seeds", "raw_operations.csv")

	// Read CSV file
	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read %s: %v", filePath, err)
	}

	lines := string(content)
	if lines == "" {
		t.Fatal("raw_operations.csv is empty")
	}

	// Verify key header columns
	requiredColumns := []string{"operation_id", "work_order_id", "operation_seq", "resource_id", "start_timestamp", "end_timestamp"}
	for _, col := range requiredColumns {
		if !containsSubstring(lines, col) {
			t.Errorf("Missing expected header column %q in raw_operations.csv", col)
		}
	}

	// Verify at least ~300 operations exist
	lineCount := countLines(lines)
	if lineCount < 250 { // Header + at least 249 operations (some flexibility)
		t.Errorf("raw_operations.csv has %d lines, want at least 250 (header + ~300 operations)", lineCount)
	}

	// Verify operation types are present
	if !containsSubstring(lines, "SETUP") && !containsSubstring(lines, "PROCESSING") {
		t.Error("No operation types (SETUP/PROCESSING) found in raw_operations.csv")
	}
}

// TestDowntimeCSVStructure verifies raw_downtime.csv has correct headers and valid data
func TestDowntimeCSVStructure(t *testing.T) {
	filePath := filepath.Join("seeds", "raw_downtime.csv")

	// Read CSV file
	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read %s: %v", filePath, err)
	}

	lines := string(content)
	if lines == "" {
		t.Fatal("raw_downtime.csv is empty")
	}

	// Verify key header columns
	requiredColumns := []string{"downtime_id", "resource_id", "downtime_start", "downtime_end", "downtime_type", "reason_code"}
	for _, col := range requiredColumns {
		if !containsSubstring(lines, col) {
			t.Errorf("Missing expected header column %q in raw_downtime.csv", col)
		}
	}

	// Verify at least ~30 downtime events exist
	lineCount := countLines(lines)
	if lineCount < 25 { // Header + at least 24 events (some flexibility)
		t.Errorf("raw_downtime.csv has %d lines, want at least 25 (header + ~30 events)", lineCount)
	}

	// Verify downtime types are present
	if !containsSubstring(lines, "SCHEDULED") && !containsSubstring(lines, "UNSCHEDULED") {
		t.Error("No downtime types found in raw_downtime.csv")
	}

	// Verify reason codes are present
	hasReasonCode := containsSubstring(lines, "BREAKDOWN") ||
		containsSubstring(lines, "PREVENTIVE_MAINTENANCE") ||
		containsSubstring(lines, "CHANGEOVER")
	if !hasReasonCode {
		t.Error("No reason codes found in raw_downtime.csv")
	}
}

// Helper functions
func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && findSubstring(s, substr)
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func countLines(s string) int {
	if s == "" {
		return 0
	}
	count := 1
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			count++
		}
	}
	// Don't count trailing newline as an extra line
	if s[len(s)-1] == '\n' {
		count--
	}
	return count
}

// ==================== Phase 3: Dimension Table Tests ====================

// TestDimensionSQLFilesExist verifies all required dimension SQL files exist
func TestDimensionSQLFilesExist(t *testing.T) {
	requiredFiles := []string{
		"models/dimensions/dim_resource.sql",
		"models/dimensions/dim_work_order.sql",
		"models/dimensions/dim_part.sql",
		"models/dimensions/dim_date.sql",
	}

	for _, file := range requiredFiles {
		filePath := filepath.Join(file)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			t.Errorf("Required dimension file %s does not exist", filePath)
		}
	}
}

// TestDimensionSQLStructure verifies dimension SQL files have correct Gorchata template syntax
func TestDimensionSQLStructure(t *testing.T) {
	testCases := []struct {
		name        string
		filePath    string
		mustContain []string
		description string
	}{
		{
			name:     "dim_resource",
			filePath: "models/dimensions/dim_resource.sql",
			mustContain: []string{
				"{{ config",
				"resource_key",
				"resource_id",
				"resource_name",
				"{{ seed \"raw_resources\" }}",
			},
			description: "Resource dimension must have surrogate key and reference raw_resources seed",
		},
		{
			name:     "dim_work_order",
			filePath: "models/dimensions/dim_work_order.sql",
			mustContain: []string{
				"{{ config",
				"work_order_key",
				"work_order_id",
				"{{ seed \"raw_work_orders\" }}",
			},
			description: "Work order dimension must have surrogate key and reference raw_work_orders seed",
		},
		{
			name:     "dim_part",
			filePath: "models/dimensions/dim_part.sql",
			mustContain: []string{
				"{{ config",
				"part_key",
				"part_number",
			},
			description: "Part dimension must have surrogate key and natural key",
		},
		{
			name:     "dim_date",
			filePath: "models/dimensions/dim_date.sql",
			mustContain: []string{
				"{{ config",
				"date_key",
				"full_date",
				"year",
				"month",
			},
			description: "Date dimension must have date_key and temporal attributes",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			content, err := os.ReadFile(filepath.Join(tc.filePath))
			if err != nil {
				t.Fatalf("Failed to read %s: %v", tc.filePath, err)
			}

			contentStr := string(content)
			for _, mustHave := range tc.mustContain {
				if !containsSubstring(contentStr, mustHave) {
					t.Errorf("%s: missing required element %q\nDescription: %s", tc.filePath, mustHave, tc.description)
				}
			}
		})
	}
}

// TestDimensionDataQuality verifies dimensions meet data quality requirements
// Note: These tests assume Gorchata can build the dimensions. For now, we validate SQL structure.
// Full data quality tests will be added once Gorchata build pipeline is integrated.
func TestDimensionDataQuality(t *testing.T) {
	t.Run("ResourceDimension", func(t *testing.T) {
		// Verify SQL contains surrogate key uniqueness logic
		content, err := os.ReadFile(filepath.Join("models/dimensions/dim_resource.sql"))
		if err != nil {
			t.Fatalf("Failed to read dim_resource.sql: %v", err)
		}

		contentStr := string(content)

		// Verify ROW_NUMBER() is used for unique key generation
		if !containsSubstring(contentStr, "ROW_NUMBER()") {
			t.Error("dim_resource.sql should use ROW_NUMBER() for unique surrogate key generation")
		}

		// Verify calculated fields are present
		if !containsSubstring(contentStr, "daily_capacity") {
			t.Error("dim_resource.sql missing calculated field: daily_capacity")
		}

		if !containsSubstring(contentStr, "is_bottleneck_candidate") {
			t.Error("dim_resource.sql missing calculated field: is_bottleneck_candidate")
		}

		// Verify SCD Type 2 metadata fields
		if !containsSubstring(contentStr, "is_current") {
			t.Error("dim_resource.sql missing SCD Type 2 field: is_current")
		}
	})

	t.Run("WorkOrderDimension", func(t *testing.T) {
		content, err := os.ReadFile(filepath.Join("models/dimensions/dim_work_order.sql"))
		if err != nil {
			t.Fatalf("Failed to read dim_work_order.sql: %v", err)
		}

		contentStr := string(content)

		// Verify calculated date fields
		requiredFields := []string{"release_date", "due_date", "lead_time_days"}
		for _, field := range requiredFields {
			if !containsSubstring(contentStr, field) {
				t.Errorf("dim_work_order.sql missing calculated field: %s", field)
			}
		}

		// Verify JULIANDAY is used for date calculations
		if !containsSubstring(contentStr, "JULIANDAY") {
			t.Error("dim_work_order.sql should use JULIANDAY for lead time calculations")
		}
	})

	t.Run("PartDimension", func(t *testing.T) {
		content, err := os.ReadFile(filepath.Join("models/dimensions/dim_part.sql"))
		if err != nil {
			t.Fatalf("Failed to read dim_part.sql: %v", err)
		}

		contentStr := string(content)

		// Verify DISTINCT is used to extract unique parts
		if !containsSubstring(contentStr, "DISTINCT") {
			t.Error("dim_part.sql should use DISTINCT to extract unique part numbers")
		}

		// Verify classification fields
		requiredFields := []string{"part_family", "routing_complexity"}
		for _, field := range requiredFields {
			if !containsSubstring(contentStr, field) {
				t.Errorf("dim_part.sql missing classification field: %s", field)
			}
		}
	})

	t.Run("DateDimension", func(t *testing.T) {
		content, err := os.ReadFile(filepath.Join("models/dimensions/dim_date.sql"))
		if err != nil {
			t.Fatalf("Failed to read dim_date.sql: %v", err)
		}

		contentStr := string(content)

		// Verify recursive CTE is used for date series generation
		if !containsSubstring(contentStr, "RECURSIVE") {
			t.Error("dim_date.sql should use recursive CTE for date series generation")
		}

		// Verify date range covers analysis period
		if !containsSubstring(contentStr, "2024-01-01") {
			t.Error("dim_date.sql should start from 2024-01-01")
		}

		// Verify temporal attributes
		requiredFields := []string{
			"year", "month", "month_name", "quarter",
			"day_of_month", "day_of_week", "day_name",
			"week_of_year", "is_weekend",
		}
		for _, field := range requiredFields {
			if !containsSubstring(contentStr, field) {
				t.Errorf("dim_date.sql missing temporal attribute: %s", field)
			}
		}

		// Verify date_key is in YYYYMMDD format
		if !containsSubstring(contentStr, "STRFTIME('%Y%m%d'") {
			t.Error("dim_date.sql date_key should be in YYYYMMDD format")
		}
	})
}

// ==================== Phase 4: Fact Table Tests ====================

// TestFactTableSQLFilesExist verifies the fact table SQL file exists
func TestFactTableSQLFilesExist(t *testing.T) {
	requiredFiles := []string{
		"models/facts/fct_operation.sql",
	}

	for _, file := range requiredFiles {
		filePath := filepath.Join(file)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			t.Errorf("Required fact table file %s does not exist", filePath)
		}
	}
}

// TestFactOperationSQLStructure verifies fct_operation.sql has correct structure
func TestFactOperationSQLStructure(t *testing.T) {
	filePath := filepath.Join("models/facts/fct_operation.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read %s: %v", filePath, err)
	}

	contentStr := string(content)

	// Verify Gorchata template syntax
	requiredTemplates := []string{
		"{{ config",
		"{{ seed \"raw_operations\" }}",
		"{{ seed \"raw_work_orders\" }}",
		"{{ ref \"dim_resource\" }}",
		"{{ ref \"dim_work_order\" }}",
		"{{ ref \"dim_part\" }}",
		"{{ ref \"dim_date\" }}",
	}

	for _, template := range requiredTemplates {
		if !containsSubstring(contentStr, template) {
			t.Errorf("fct_operation.sql missing required template syntax: %s", template)
		}
	}

	// Verify grain: natural keys for operation-level detail
	requiredNaturalKeys := []string{
		"operation_id",
		"work_order_id",
		"operation_seq",
	}

	for _, key := range requiredNaturalKeys {
		if !containsSubstring(contentStr, key) {
			t.Errorf("fct_operation.sql missing grain key: %s", key)
		}
	}
}

// TestFactOperationForeignKeys verifies all foreign key references are present
func TestFactOperationForeignKeys(t *testing.T) {
	filePath := filepath.Join("models/facts/fct_operation.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read %s: %v", filePath, err)
	}

	contentStr := string(content)

	// Verify all required foreign keys (surrogate keys from dimensions)
	requiredForeignKeys := []string{
		"resource_key",
		"work_order_key",
		"part_key",
		"start_date_key",
		"end_date_key",
	}

	for _, fk := range requiredForeignKeys {
		if !containsSubstring(contentStr, fk) {
			t.Errorf("fct_operation.sql missing foreign key: %s", fk)
		}
	}

	// Verify joins to dimension tables
	requiredJoins := []string{
		"dim_resource",
		"dim_work_order",
		"dim_part",
		"dim_date",
	}

	for _, table := range requiredJoins {
		if !containsSubstring(contentStr, table) {
			t.Errorf("fct_operation.sql missing join to dimension: %s", table)
		}
	}
}

// TestFactOperationQueueTimeCalculation verifies queue time calculation logic
func TestFactOperationQueueTimeCalculation(t *testing.T) {
	filePath := filepath.Join("models/facts/fct_operation.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read %s: %v", filePath, err)
	}

	contentStr := string(content)

	// Verify LAG window function for previous operation end time
	if !containsSubstring(contentStr, "LAG(") {
		t.Error("fct_operation.sql should use LAG() window function for queue time calculation")
	}

	// Verify PARTITION BY work_order_id
	if !containsSubstring(contentStr, "PARTITION BY") {
		t.Error("fct_operation.sql should use PARTITION BY for window function")
	}

	// Verify queue_time_minutes calculation
	if !containsSubstring(contentStr, "queue_time_minutes") {
		t.Error("fct_operation.sql missing calculated measure: queue_time_minutes")
	}

	// Verify JULIANDAY for time arithmetic (SQLite pattern)
	if !containsSubstring(contentStr, "JULIANDAY") {
		t.Error("fct_operation.sql should use JULIANDAY for timestamp arithmetic")
	}

	// Verify COALESCE for handling first operation (no previous operation)
	if !containsSubstring(contentStr, "COALESCE") {
		t.Error("fct_operation.sql should use COALESCE to handle first operation queue time")
	}

	// Verify reference to work order release timestamp for first operation
	if !containsSubstring(contentStr, "release_timestamp") {
		t.Error("fct_operation.sql should use work order release_timestamp for first operation arrival time")
	}
}

// TestFactOperationCycleTimeCalculation verifies cycle time calculation logic
func TestFactOperationCycleTimeCalculation(t *testing.T) {
	filePath := filepath.Join("models/facts/fct_operation.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read %s: %v", filePath, err)
	}

	contentStr := string(content)

	// Verify cycle_time_minutes calculation
	if !containsSubstring(contentStr, "cycle_time_minutes") {
		t.Error("fct_operation.sql missing calculated measure: cycle_time_minutes")
	}

	// Verify calculation: (end_timestamp - start_timestamp) * 24 * 60
	if !containsSubstring(contentStr, "* 24 * 60") {
		t.Error("fct_operation.sql cycle time should convert days to minutes (* 24 * 60)")
	}

	// Verify CAST to INTEGER for minute values
	if !containsSubstring(contentStr, "CAST(") || !containsSubstring(contentStr, "AS INTEGER)") {
		t.Error("fct_operation.sql should CAST time calculations to INTEGER")
	}
}

// TestFactOperationMeasures verifies all required measures are present
func TestFactOperationMeasures(t *testing.T) {
	filePath := filepath.Join("models/facts/fct_operation.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read %s: %v", filePath, err)
	}

	contentStr := string(content)

	// Verify base measures from raw_operations
	requiredMeasures := []string{
		"quantity_completed",
		"quantity_scrapped",
		"cycle_time_minutes",
		"queue_time_minutes",
	}

	for _, measure := range requiredMeasures {
		if !containsSubstring(contentStr, measure) {
			t.Errorf("fct_operation.sql missing measure: %s", measure)
		}
	}

	// Verify timestamps are included
	requiredTimestamps := []string{
		"start_timestamp",
		"end_timestamp",
	}

	for _, ts := range requiredTimestamps {
		if !containsSubstring(contentStr, ts) {
			t.Errorf("fct_operation.sql missing timestamp: %s", ts)
		}
	}

	// Verify operation attributes
	requiredAttributes := []string{
		"resource_id",
		"operation_type",
	}

	for _, attr := range requiredAttributes {
		if !containsSubstring(contentStr, attr) {
			t.Errorf("fct_operation.sql missing attribute: %s", attr)
		}
	}
}
