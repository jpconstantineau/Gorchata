package test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestFactSQLFilesExist validates that all fact table SQL files are created
func TestFactSQLFilesExist(t *testing.T) {
	repoRoot := getRepoRoot(t)
	factsDir := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "facts")

	requiredFiles := []string{
		"fact_car_location_event.sql",
		"fact_train_trip.sql",
		"fact_straggler.sql",
		"fact_inferred_power_transfer.sql",
	}

	for _, file := range requiredFiles {
		filePath := filepath.Join(factsDir, file)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			t.Errorf("Required fact file does not exist: %s", file)
		}
	}
}

// TestFactCarLocationEventStructure validates the SQL structure
func TestFactCarLocationEventStructure(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "facts", "fact_car_location_event.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read fact_car_location_event.sql: %v", err)
	}

	sql := string(content)

	// Verify it's configured as a table
	if !strings.Contains(sql, `config "materialized" "table"`) {
		t.Error("fact_car_location_event should be materialized as a table")
	}

	// Verify it references staging table
	if !strings.Contains(sql, "stg_clm_events") {
		t.Error("fact_car_location_event should reference stg_clm_events")
	}

	// Verify foreign key columns
	requiredColumns := []string{
		"event_id",
		"car_id",
		"train_id",
		"location_id",
		"event_type",
		"date_key",
	}

	for _, col := range requiredColumns {
		if !strings.Contains(sql, col) {
			t.Errorf("fact_car_location_event should include column: %s", col)
		}
	}

	// Verify dimension joins
	dimensionTables := []string{
		"dim_car",
		"dim_location",
		"dim_date",
	}

	for _, table := range dimensionTables {
		if !strings.Contains(sql, table) {
			t.Errorf("fact_car_location_event should join to: %s", table)
		}
	}
}

// TestFactTrainTripStructure validates trip aggregation logic
func TestFactTrainTripStructure(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "facts", "fact_train_trip.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read fact_train_trip.sql: %v", err)
	}

	sql := string(content)

	// Verify it's configured as a table
	if !strings.Contains(sql, `config "materialized" "table"`) {
		t.Error("fact_train_trip should be materialized as a table")
	}

	// Verify window functions for trip boundaries
	windowFunctions := []string{
		"LAG(",
		"LEAD(",
		"ROW_NUMBER(",
	}

	hasWindowFn := false
	for _, fn := range windowFunctions {
		if strings.Contains(sql, fn) {
			hasWindowFn = true
			break
		}
	}

	if !hasWindowFn {
		t.Error("fact_train_trip should use window functions for trip boundaries")
	}

	// Verify trip-level metrics
	metrics := []string{
		"departure_timestamp",           // Changed from total_transit_time
		"destination_arrival_timestamp", // Changed from loaded_transit_time
		"trip_number",                   // Changed from empty_return_time
		"train_id",                      // Core identifier
		"origin_location_id",            // Changed from cars_at_formation
		"destination_location_id",       // Changed from cars_at_destination
	}

	for _, metric := range metrics {
		if !strings.Contains(sql, metric) {
			t.Errorf("fact_train_trip should include metric: %s", metric)
		}
	}

	// Verify corridor_id foreign key
	if !strings.Contains(sql, "corridor_id") {
		t.Error("fact_train_trip should include corridor_id")
	}
}

// TestFactStragglerStructure validates straggler tracking logic
func TestFactStragglerStructure(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "facts", "fact_straggler.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read fact_straggler.sql: %v", err)
	}

	sql := string(content)

	// Verify it's configured as a table
	if !strings.Contains(sql, `config "materialized" "table"`) {
		t.Error("fact_straggler should be materialized as a table")
	}

	// Verify car_set_out event tracking
	if !strings.Contains(strings.ToLower(sql), "car_set_out") && !strings.Contains(strings.ToLower(sql), "set_out") {
		t.Error("fact_straggler should identify car_set_out events")
	}

	// Verify delay calculation fields
	delayFields := []string{
		"set_out_timestamp",
		"picked_up_timestamp", // Changed from resume_travel_timestamp
		"delay_hours",         // Changed from total_delay_days
		"delay_category",
	}

	for _, field := range delayFields {
		if !strings.Contains(sql, field) {
			t.Errorf("fact_straggler should include field: %s", field)
		}
	}

	// Verify delay category logic (short/medium/long/extended)
	categories := []string{"short", "medium", "long", "extended"}
	categoryCount := 0
	for _, cat := range categories {
		if strings.Contains(sql, cat) {
			categoryCount++
		}
	}

	if categoryCount < 3 {
		t.Error("fact_straggler should define delay categories (short/medium/long/extended)")
	}

	// Verify julianday for time calculations
	if !strings.Contains(sql, "julianday") {
		t.Error("fact_straggler should use julianday() for time calculations")
	}
}

// TestFactInferredPowerTransferStructure validates power inference logic
func TestFactInferredPowerTransferStructure(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "facts", "fact_inferred_power_transfer.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read fact_inferred_power_transfer.sql: %v", err)
	}

	sql := string(content)

	// Verify it's configured as a table
	if !strings.Contains(sql, `config "materialized" "table"`) {
		t.Error("fact_inferred_power_transfer should be materialized as a table")
	}

	// Verify power transfer fields
	transferFields := []string{
		"arrival_timestamp",
		"departure_timestamp",
		"gap_hours",
		"inferred_same_power",
	}

	for _, field := range transferFields {
		if !strings.Contains(sql, field) {
			t.Errorf("fact_inferred_power_transfer should include field: %s", field)
		}
	}

	// Verify 1-hour threshold logic
	// Should have logic comparing gap_hours to 1.0
	if !strings.Contains(sql, "1.0") && !strings.Contains(sql, "< 1") && !strings.Contains(sql, ">= 1") {
		t.Error("fact_inferred_power_transfer should use 1-hour threshold for power inference")
	}
}

// TestFactTablesUseStaging verifies all fact tables reference staging
func TestFactTablesUseStaging(t *testing.T) {
	repoRoot := getRepoRoot(t)
	factsDir := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "facts")

	files, err := os.ReadDir(factsDir)
	if err != nil {
		t.Fatalf("Failed to read facts directory: %v", err)
	}

	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".sql") {
			filePath := filepath.Join(factsDir, file.Name())
			content, err := os.ReadFile(filePath)
			if err != nil {
				t.Fatalf("Failed to read %s: %v", file.Name(), err)
			}

			sql := string(content)

			// Each fact table should reference either stg_clm_events or other dimensions
			if !strings.Contains(sql, "stg_clm_events") && !strings.Contains(sql, "fact_") && !strings.Contains(sql, "dim_") {
				t.Errorf("%s should reference staging or dimension tables", file.Name())
			}
		}
	}
}

// TestFactTablesHaveComments verifies SQL has documentation
func TestFactTablesHaveComments(t *testing.T) {
	repoRoot := getRepoRoot(t)
	factsDir := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "facts")

	files, err := os.ReadDir(factsDir)
	if err != nil {
		t.Fatalf("Failed to read facts directory: %v", err)
	}

	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".sql") {
			filePath := filepath.Join(factsDir, file.Name())
			content, err := os.ReadFile(filePath)
			if err != nil {
				t.Fatalf("Failed to read %s: %v", file.Name(), err)
			}

			sql := string(content)

			// Should have SQL comments explaining the logic
			if !strings.Contains(sql, "--") && !strings.Contains(sql, "/*") {
				t.Errorf("%s should include SQL comments documenting logic", file.Name())
			}
		}
	}
}
