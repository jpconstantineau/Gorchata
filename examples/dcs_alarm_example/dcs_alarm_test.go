package dcs_alarm_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/config"
	_ "modernc.org/sqlite"
)

// setupTestDB creates a temporary database for testing.
// Returns the database connection and a cleanup function.
func setupTestDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	// Create temp database
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Return DB and cleanup function
	cleanup := func() {
		db.Close()
	}

	return db, cleanup
}

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
	if cfg.Name != "dcs_alarm_example" {
		t.Errorf("Name = %q, want %q", cfg.Name, "dcs_alarm_example")
	}

	// Verify version
	if cfg.Version != "1.0.0" {
		t.Errorf("Version = %q, want %q", cfg.Version, "1.0.0")
	}

	// Verify profile
	if cfg.Profile != "dev" {
		t.Errorf("Profile = %q, want %q", cfg.Profile, "dev")
	}

	// Verify model paths
	if len(cfg.ModelPaths) != 1 {
		t.Errorf("ModelPaths length = %d, want 1", len(cfg.ModelPaths))
	}
	if len(cfg.ModelPaths) > 0 && cfg.ModelPaths[0] != "models" {
		t.Errorf("ModelPaths[0] = %q, want %q", cfg.ModelPaths[0], "models")
	}

	// Verify vars exist
	if cfg.Vars == nil {
		t.Fatal("Vars is nil, want non-nil")
	}

	// Verify start_date var
	if startDate, ok := cfg.Vars["start_date"]; !ok {
		t.Error("Vars['start_date'] not found")
	} else if startDate != "2026-02-06" {
		t.Errorf("Vars['start_date'] = %v, want %q", startDate, "2026-02-06")
	}

	// Verify end_date var
	if endDate, ok := cfg.Vars["end_date"]; !ok {
		t.Error("Vars['end_date'] not found")
	} else if endDate != "2026-02-07" {
		t.Errorf("Vars['end_date'] = %v, want %q", endDate, "2026-02-07")
	}

	// Verify alarm_rate_threshold_acceptable var
	if threshold, ok := cfg.Vars["alarm_rate_threshold_acceptable"]; !ok {
		t.Error("Vars['alarm_rate_threshold_acceptable'] not found")
	} else {
		// The value should be an int (2)
		if intVal, ok := threshold.(int); !ok {
			t.Errorf("Vars['alarm_rate_threshold_acceptable'] type = %T, want int", threshold)
		} else if intVal != 2 {
			t.Errorf("Vars['alarm_rate_threshold_acceptable'] = %d, want 2", intVal)
		}
	}

	// Verify alarm_rate_threshold_unacceptable var
	if threshold, ok := cfg.Vars["alarm_rate_threshold_unacceptable"]; !ok {
		t.Error("Vars['alarm_rate_threshold_unacceptable'] not found")
	} else {
		// The value should be an int (10)
		if intVal, ok := threshold.(int); !ok {
			t.Errorf("Vars['alarm_rate_threshold_unacceptable'] type = %T, want int", threshold)
		} else if intVal != 10 {
			t.Errorf("Vars['alarm_rate_threshold_unacceptable'] = %d, want 10", intVal)
		}
	}
}

// TestDatabaseConnection verifies profiles.yml exists and database path resolves
func TestDatabaseConnection(t *testing.T) {
	profilesPath := filepath.Join("profiles.yml")

	// Verify file exists
	if _, err := os.Stat(profilesPath); os.IsNotExist(err) {
		t.Fatalf("profiles.yml does not exist at %s", profilesPath)
	}

	// Load profiles config
	cfg, err := config.LoadProfiles(profilesPath)
	if err != nil {
		t.Fatalf("LoadProfiles() error = %v, want nil", err)
	}

	// Verify default profile exists
	if cfg.Default == nil {
		t.Fatal("Default profile is nil")
	}

	// Verify default target
	if cfg.Default.Target != "dev" {
		t.Errorf("Default.Target = %q, want %q", cfg.Default.Target, "dev")
	}

	// Verify dev output exists
	devOutput, err := cfg.GetOutput("dev")
	if err != nil {
		t.Fatalf("GetOutput('dev') error = %v, want nil", err)
	}

	// Verify output type
	if devOutput.Type != "sqlite" {
		t.Errorf("devOutput.Type = %q, want %q", devOutput.Type, "sqlite")
	}

	// Database path should not be empty (env var should expand to default)
	if devOutput.Database == "" {
		t.Error("devOutput.Database is empty")
	}

	// Verify database path contains expected default path
	expectedPath := "./examples/dcs_alarm_example/dcs_alarms.db"
	if devOutput.Database != expectedPath {
		t.Logf("Database path: %s (expected default: %s)", devOutput.Database, expectedPath)
	}
}

// TestDatabaseConnectionWithEnvVar tests environment variable expansion
func TestDatabaseConnectionWithEnvVar(t *testing.T) {
	// Set custom database path via environment variable
	customPath := filepath.Join(t.TempDir(), "custom_dcs_alarms.db")
	t.Setenv("DCS_ALARM_DB", customPath)

	profilesPath := filepath.Join("profiles.yml")

	// Load profiles config
	cfg, err := config.LoadProfiles(profilesPath)
	if err != nil {
		t.Fatalf("LoadProfiles() error = %v, want nil", err)
	}

	// Get dev output
	devOutput, err := cfg.GetOutput("dev")
	if err != nil {
		t.Fatalf("GetOutput('dev') error = %v, want nil", err)
	}

	// Verify custom path is used
	if devOutput.Database != customPath {
		t.Errorf("devOutput.Database = %q, want %q", devOutput.Database, customPath)
	}
}

// TestDirectoryStructure verifies all required directories exist
func TestDirectoryStructure(t *testing.T) {
	requiredDirs := []string{
		"models",
		"models/sources",
		"models/dimensions",
		"models/facts",
		"models/rollups",
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

// TestSourceModelsExist verifies that the raw alarm source model files exist
func TestSourceModelsExist(t *testing.T) {
	requiredFiles := []string{
		filepath.Join("models", "sources", "raw_alarm_events.sql"),
		filepath.Join("models", "sources", "raw_alarm_config.sql"),
	}

	for _, file := range requiredFiles {
		if _, err := os.Stat(file); os.IsNotExist(err) {
			t.Errorf("Required source model file %s does not exist", file)
		} else if err != nil {
			t.Errorf("Error checking file %s: %v", file, err)
		}
	}
}

// TestRawAlarmEventsParse verifies raw_alarm_events.sql contains valid SQL and Go template config
func TestRawAlarmEventsParse(t *testing.T) {
	modelPath := filepath.Join("models", "sources", "raw_alarm_events.sql")

	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read %s: %v", modelPath, err)
	}

	contentStr := string(content)

	// Verify Go template config header exists (not Jinja)
	if !strings.Contains(contentStr, `{{ config`) {
		t.Error("File does not contain Go template config directive {{ config ... }}")
	}

	// Verify it's NOT Jinja syntax
	if strings.Contains(contentStr, `config(materialized='table')`) {
		t.Error("File contains Jinja syntax, should use Go template syntax: {{ config \"materialized\" \"table\" }}")
	}

	// Verify SQL keywords
	requiredKeywords := []string{"SELECT", "FROM", "VALUES"}
	for _, keyword := range requiredKeywords {
		if !strings.Contains(strings.ToUpper(contentStr), keyword) {
			t.Errorf("File does not contain SQL keyword: %s", keyword)
		}
	}

	// Verify required columns are mentioned
	requiredColumns := []string{"event_id", "tag_id", "event_timestamp", "event_type", "priority_code", "area_code"}
	for _, col := range requiredColumns {
		if !strings.Contains(contentStr, col) {
			t.Errorf("File does not reference column: %s", col)
		}
	}
}

// TestAlarmConfigParse verifies raw_alarm_config.sql contains valid SQL and Go template config
func TestAlarmConfigParse(t *testing.T) {
	modelPath := filepath.Join("models", "sources", "raw_alarm_config.sql")

	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read %s: %v", modelPath, err)
	}

	contentStr := string(content)

	// Verify Go template config header exists
	if !strings.Contains(contentStr, `{{ config`) {
		t.Error("File does not contain Go template config directive {{ config ... }}")
	}

	// Verify SQL keywords
	requiredKeywords := []string{"SELECT", "FROM", "VALUES"}
	for _, keyword := range requiredKeywords {
		if !strings.Contains(strings.ToUpper(contentStr), keyword) {
			t.Errorf("File does not contain SQL keyword: %s", keyword)
		}
	}

	// Verify required columns are mentioned
	requiredColumns := []string{"tag_id", "tag_name", "alarm_type", "priority_code", "area_code"}
	for _, col := range requiredColumns {
		if !strings.Contains(contentStr, col) {
			t.Errorf("File does not reference column: %s", col)
		}
	}
}

// TestAlarmEventData verifies the raw_alarm_events model loads correctly with expected data
func TestAlarmEventData(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Read model file
	modelPath := filepath.Join("models", "sources", "raw_alarm_events.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read raw_alarm_events.sql: %v", err)
	}

	// Extract SQL (remove Go template config directive)
	contentStr := string(content)
	sqlContent := strings.ReplaceAll(contentStr, `{{ config "materialized" "table" }}`, "")
	sqlContent = strings.TrimSpace(sqlContent)

	// Wrap in CREATE TABLE for testing
	createTableSQL := "CREATE TABLE raw_alarm_events AS " + sqlContent

	// Execute SQL
	ctx := context.Background()
	_, err = db.ExecContext(ctx, createTableSQL)
	if err != nil {
		t.Fatalf("Failed to execute raw_alarm_events model: %v", err)
	}

	// Verify table has data
	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM raw_alarm_events").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count events: %v", err)
	}

	// Should have at least 30 events
	if count < 30 {
		t.Errorf("Event count = %d, want at least 30", count)
	}

	// Verify required columns exist and have correct types
	rows, err := db.QueryContext(ctx, `
		SELECT event_id, tag_id, event_timestamp, event_type, priority_code, 
		       alarm_value, setpoint_value, operator_id, area_code
		FROM raw_alarm_events 
		LIMIT 1
	`)
	if err != nil {
		t.Fatalf("Failed to query columns: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("No rows returned")
	}

	var eventID int
	var tagID, eventTimestamp, eventType, priorityCode, areaCode string
	var alarmValue, setpointValue float64
	var operatorID sql.NullString

	err = rows.Scan(&eventID, &tagID, &eventTimestamp, &eventType, &priorityCode,
		&alarmValue, &setpointValue, &operatorID, &areaCode)
	if err != nil {
		t.Fatalf("Failed to scan row: %v", err)
	}

	// Verify timestamp format (ISO 8601: YYYY-MM-DD HH:MM:SS)
	if len(eventTimestamp) != 19 {
		t.Errorf("event_timestamp format = %q, want format 'YYYY-MM-DD HH:MM:SS'", eventTimestamp)
	}

	// Verify event_type is one of expected values
	validEventTypes := []string{"ACTIVE", "ACKNOWLEDGED", "INACTIVE"}
	found := false
	for _, vt := range validEventTypes {
		if eventType == vt {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("event_type = %q, want one of %v", eventType, validEventTypes)
	}

	// Verify priority_code is one of expected values
	validPriorities := []string{"CRITICAL", "HIGH", "MEDIUM", "LOW"}
	found = false
	for _, vp := range validPriorities {
		if priorityCode == vp {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("priority_code = %q, want one of %v", priorityCode, validPriorities)
	}
}

// TestTwoProcessAreas verifies C-100 and D-200 areas are present in the data
func TestTwoProcessAreas(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Read and execute model file
	modelPath := filepath.Join("models", "sources", "raw_alarm_events.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read raw_alarm_events.sql: %v", err)
	}

	contentStr := string(content)
	sqlContent := strings.ReplaceAll(contentStr, `{{ config "materialized" "table" }}`, "")
	sqlContent = strings.TrimSpace(sqlContent)
	createTableSQL := "CREATE TABLE raw_alarm_events AS " + sqlContent

	ctx := context.Background()
	_, err = db.ExecContext(ctx, createTableSQL)
	if err != nil {
		t.Fatalf("Failed to execute raw_alarm_events model: %v", err)
	}

	// Verify both process areas exist
	areas := []string{"C-100", "D-200"}
	for _, area := range areas {
		var count int
		err = db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM raw_alarm_events WHERE area_code = ?",
			area).Scan(&count)
		if err != nil {
			t.Fatalf("Failed to count events for area %s: %v", area, err)
		}

		if count == 0 {
			t.Errorf("No events found for area_code = %s", area)
		} else {
			t.Logf("Found %d events for area %s", count, area)
		}
	}

	// Verify area distribution (C-100 should have ~50-60%, D-200 ~40-50%)
	var totalCount, c100Count, d200Count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM raw_alarm_events").Scan(&totalCount)
	if err != nil {
		t.Fatalf("Failed to count total events: %v", err)
	}

	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM raw_alarm_events WHERE area_code = 'C-100'").Scan(&c100Count)
	if err != nil {
		t.Fatalf("Failed to count C-100 events: %v", err)
	}

	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM raw_alarm_events WHERE area_code = 'D-200'").Scan(&d200Count)
	if err != nil {
		t.Fatalf("Failed to count D-200 events: %v", err)
	}

	c100Pct := float64(c100Count) / float64(totalCount) * 100
	d200Pct := float64(d200Count) / float64(totalCount) * 100

	t.Logf("Area distribution: C-100 = %.1f%%, D-200 = %.1f%%", c100Pct, d200Pct)

	// Very loose check - just verify both areas have reasonable representation
	if c100Pct < 30 || c100Pct > 70 {
		t.Errorf("C-100 percentage = %.1f%%, expected roughly 50-60%%", c100Pct)
	}
	if d200Pct < 30 || d200Pct > 70 {
		t.Errorf("D-200 percentage = %.1f%%, expected roughly 40-50%%", d200Pct)
	}
}

// TestDimensionModelsExist verifies all 7 dimension SQL files exist
func TestDimensionModelsExist(t *testing.T) {
	requiredFiles := []string{
		filepath.Join("models", "dimensions", "dim_alarm_tag.sql"),
		filepath.Join("models", "dimensions", "dim_equipment.sql"),
		filepath.Join("models", "dimensions", "dim_process_area.sql"),
		filepath.Join("models", "dimensions", "dim_operator.sql"),
		filepath.Join("models", "dimensions", "dim_priority.sql"),
		filepath.Join("models", "dimensions", "dim_dates.sql"),
		filepath.Join("models", "dimensions", "dim_time.sql"),
	}

	for _, file := range requiredFiles {
		if _, err := os.Stat(file); os.IsNotExist(err) {
			t.Errorf("Required dimension file %s does not exist", file)
		} else if err != nil {
			t.Errorf("Error checking file %s: %v", file, err)
		}
	}
}

// TestDimensionReferences verifies {{ ref "raw_alarm_config" }} usage in dim_alarm_tag
func TestDimensionReferences(t *testing.T) {
	modelPath := filepath.Join("models", "dimensions", "dim_alarm_tag.sql")

	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read %s: %v", modelPath, err)
	}

	contentStr := string(content)

	// Verify Go template config header exists
	if !strings.Contains(contentStr, `{{ config`) {
		t.Error("dim_alarm_tag.sql does not contain Go template config directive")
	}

	// Verify reference to raw_alarm_config
	if !strings.Contains(contentStr, `{{ ref "raw_alarm_config" }}`) {
		t.Error("dim_alarm_tag.sql does not reference raw_alarm_config using {{ ref }}")
	}
}

// TestAlarmTagDimension verifies SCD Type 2 structure with valid_from/valid_to/is_current
func TestAlarmTagDimension(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// First load raw_alarm_config
	configPath := filepath.Join("models", "sources", "raw_alarm_config.sql")
	configContent, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("Failed to read raw_alarm_config.sql: %v", err)
	}

	configSQL := strings.ReplaceAll(string(configContent), `{{ config "materialized" "table" }}`, "")
	createConfigSQL := "CREATE TABLE raw_alarm_config AS " + configSQL

	_, err = db.ExecContext(ctx, createConfigSQL)
	if err != nil {
		t.Fatalf("Failed to create raw_alarm_config: %v", err)
	}

	// Now load dim_alarm_tag
	modelPath := filepath.Join("models", "dimensions", "dim_alarm_tag.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read %s: %v", modelPath, err)
	}

	contentStr := string(content)
	// Replace template references
	sqlContent := strings.ReplaceAll(contentStr, `{{ config "materialized" "table" }}`, "")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_alarm_config" }}`, "raw_alarm_config")
	sqlContent = strings.TrimSpace(sqlContent)

	createTableSQL := "CREATE TABLE dim_alarm_tag AS " + sqlContent

	_, err = db.ExecContext(ctx, createTableSQL)
	if err != nil {
		t.Fatalf("Failed to execute dim_alarm_tag model: %v", err)
	}

	// Verify SCD Type 2 columns exist
	requiredColumns := []string{
		"tag_key", "tag_id", "tag_name", "alarm_type", "equipment_id", "area_code",
		"is_safety_critical", "is_active", "valid_from", "valid_to", "is_current",
	}

	for _, col := range requiredColumns {
		var count int
		err = db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM pragma_table_info('dim_alarm_tag') WHERE name = ?",
			col).Scan(&count)
		if err != nil {
			t.Fatalf("Failed to check column %s: %v", col, err)
		}
		if count == 0 {
			t.Errorf("Column %s does not exist in dim_alarm_tag", col)
		}
	}

	// Verify all records have is_current = 1 (no historical records in this example)
	var currentCount, totalCount int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dim_alarm_tag WHERE is_current = 1").Scan(&currentCount)
	if err != nil {
		t.Fatalf("Failed to count current records: %v", err)
	}

	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dim_alarm_tag").Scan(&totalCount)
	if err != nil {
		t.Fatalf("Failed to count total records: %v", err)
	}

	if currentCount != totalCount {
		t.Errorf("is_current = 1 count = %d, total count = %d, all records should be current", currentCount, totalCount)
	}

	// Verify valid_to = '9999-12-31' for current records
	var futureCount int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dim_alarm_tag WHERE valid_to = '9999-12-31'").Scan(&futureCount)
	if err != nil {
		t.Fatalf("Failed to count future valid_to: %v", err)
	}

	if futureCount != totalCount {
		t.Errorf("valid_to = '9999-12-31' count = %d, total count = %d, all records should have future end date", futureCount, totalCount)
	}

	// Verify we have 21 tags (from raw_alarm_config: 9 from C-100, 12 from D-200)
	if totalCount != 21 {
		t.Errorf("Total tag count = %d, want 21", totalCount)
	}
}

// TestTwoProcessAreasInDimensions verifies exactly C-100 and D-200 exist in dim_process_area
func TestTwoProcessAreasInDimensions(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Load dim_process_area
	modelPath := filepath.Join("models", "dimensions", "dim_process_area.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read %s: %v", modelPath, err)
	}

	contentStr := string(content)
	sqlContent := strings.ReplaceAll(contentStr, `{{ config "materialized" "table" }}`, "")
	sqlContent = strings.TrimSpace(sqlContent)

	createTableSQL := "CREATE TABLE dim_process_area AS " + sqlContent

	_, err = db.ExecContext(ctx, createTableSQL)
	if err != nil {
		t.Fatalf("Failed to execute dim_process_area model: %v", err)
	}

	// Verify exactly 2 rows
	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dim_process_area").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count process areas: %v", err)
	}

	if count != 2 {
		t.Errorf("Process area count = %d, want exactly 2", count)
	}

	// Verify C-100 exists
	var c100Count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dim_process_area WHERE area_code = 'C-100'").Scan(&c100Count)
	if err != nil {
		t.Fatalf("Failed to check C-100: %v", err)
	}
	if c100Count != 1 {
		t.Errorf("C-100 count = %d, want 1", c100Count)
	}

	// Verify D-200 exists
	var d200Count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dim_process_area WHERE area_code = 'D-200'").Scan(&d200Count)
	if err != nil {
		t.Fatalf("Failed to check D-200: %v", err)
	}
	if d200Count != 1 {
		t.Errorf("D-200 count = %d, want 1", d200Count)
	}

	// Verify required columns
	requiredColumns := []string{"area_key", "area_code", "area_name", "plant_id"}
	for _, col := range requiredColumns {
		var colCount int
		err = db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM pragma_table_info('dim_process_area') WHERE name = ?",
			col).Scan(&colCount)
		if err != nil {
			t.Fatalf("Failed to check column %s: %v", col, err)
		}
		if colCount == 0 {
			t.Errorf("Column %s does not exist in dim_process_area", col)
		}
	}
}

// TestTimeBuckets verifies dim_time has exactly 144 rows (10-minute buckets 0-143)
func TestTimeBuckets(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Load dim_time
	modelPath := filepath.Join("models", "dimensions", "dim_time.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read %s: %v", modelPath, err)
	}

	contentStr := string(content)
	sqlContent := strings.ReplaceAll(contentStr, `{{ config "materialized" "table" }}`, "")
	sqlContent = strings.TrimSpace(sqlContent)

	createTableSQL := "CREATE TABLE dim_time AS " + sqlContent

	_, err = db.ExecContext(ctx, createTableSQL)
	if err != nil {
		t.Fatalf("Failed to execute dim_time model: %v", err)
	}

	// Verify exactly 144 rows
	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dim_time").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count time buckets: %v", err)
	}

	if count != 144 {
		t.Errorf("Time bucket count = %d, want exactly 144", count)
	}

	// Verify time_key ranges from 0 to 143
	var minKey, maxKey int
	err = db.QueryRowContext(ctx, "SELECT MIN(time_key), MAX(time_key) FROM dim_time").Scan(&minKey, &maxKey)
	if err != nil {
		t.Fatalf("Failed to get min/max time_key: %v", err)
	}

	if minKey != 0 {
		t.Errorf("Min time_key = %d, want 0", minKey)
	}
	if maxKey != 143 {
		t.Errorf("Max time_key = %d, want 143", maxKey)
	}

	// Verify required columns
	requiredColumns := []string{"time_key", "time_bucket_10min", "hour", "minute_start", "time_display", "shift"}
	for _, col := range requiredColumns {
		var colCount int
		err = db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM pragma_table_info('dim_time') WHERE name = ?",
			col).Scan(&colCount)
		if err != nil {
			t.Fatalf("Failed to check column %s: %v", col, err)
		}
		if colCount == 0 {
			t.Errorf("Column %s does not exist in dim_time", col)
		}
	}

	// Verify calculation: time_key 0 should be 00:00, time_key 143 should be 23:50
	var timeDisplay string
	err = db.QueryRowContext(ctx, "SELECT time_display FROM dim_time WHERE time_key = 0").Scan(&timeDisplay)
	if err != nil {
		t.Fatalf("Failed to get time_display for time_key=0: %v", err)
	}
	if timeDisplay != "00:00" {
		t.Errorf("time_display for time_key=0 = %q, want '00:00'", timeDisplay)
	}

	err = db.QueryRowContext(ctx, "SELECT time_display FROM dim_time WHERE time_key = 143").Scan(&timeDisplay)
	if err != nil {
		t.Fatalf("Failed to get time_display for time_key=143: %v", err)
	}
	if timeDisplay != "23:50" {
		t.Errorf("time_display for time_key=143 = %q, want '23:50'", timeDisplay)
	}
}
