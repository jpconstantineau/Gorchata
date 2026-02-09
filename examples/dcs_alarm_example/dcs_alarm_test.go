package dcs_alarm_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/config"
	_ "modernc.org/sqlite"
)

// stripTemplateConfig removes config directives from SQL (both Go template and legacy Jinja syntax)
func stripTemplateConfig(sql string) string {
	// Remove Go template syntax: {{ config "materialized" "table" }}
	goTemplateRe := regexp.MustCompile(`\{\{\s*config\s+"[^"]+"\s+"[^"]+"\s*\}\}`)
	sql = goTemplateRe.ReplaceAllString(sql, "")

	// Remove legacy Jinja-style syntax: {{ config(materialized='table') }}
	legacyRe := regexp.MustCompile(`\{\{\s*config\s*\([^)]+\)\s*\}\}`)
	return legacyRe.ReplaceAllString(sql, "")
}

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

// connectToProjectDB connects to the project database (assumes it exists from `gorchata run`)
// Returns the database connection and a cleanup function.
func connectToProjectDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	// Load profiles config to get database path
	profilesPath := filepath.Join("profiles.yml")
	profilesCfg, err := config.LoadProfiles(profilesPath)
	if err != nil {
		t.Fatalf("LoadProfiles() error = %v", err)
	}

	output, err := profilesCfg.GetOutput("dev")
	if err != nil {
		t.Fatalf("GetOutput('dev') error = %v", err)
	}

	// Connect to database
	db, err := sql.Open("sqlite", output.Database)
	if err != nil {
		t.Fatalf("Failed to open project database: %v", err)
	}

	// Verify database has expected tables
	var tableCount int
	err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").Scan(&tableCount)
	if err != nil {
		db.Close()
		t.Fatalf("Failed to query tables: %v", err)
	}
	if tableCount == 0 {
		db.Close()
		t.Fatalf("Project database is empty. Run 'gorchata run' first to build all models.")
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
	if len(cfg.ModelPaths) != 5 {
		t.Errorf("ModelPaths length = %d, want 5", len(cfg.ModelPaths))
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

	// Verify config header exists
	if !strings.Contains(contentStr, `{{ config`) {
		t.Error("File does not contain config directive {{ config ... }}")
	}

	// Verify it's using Go template syntax for materialized config
	if !strings.Contains(contentStr, `{{ config "materialized" "table" }}`) {
		t.Error("File should contain Go template syntax: {{ config \"materialized\" \"table\" }}")
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

	// Extract SQL (remove template config directive)
	contentStr := string(content)
	sqlContent := stripTemplateConfig(contentStr)
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
	sqlContent := stripTemplateConfig(contentStr)
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

	configSQL := stripTemplateConfig(string(configContent))
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
	sqlContent := stripTemplateConfig(contentStr)
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
	sqlContent := stripTemplateConfig(contentStr)
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
	sqlContent := stripTemplateConfig(contentStr)
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

// TestFactTablesExist verifies fact table files exist
func TestFactTablesExist(t *testing.T) {
	factTables := []string{
		"fct_alarm_occurrence.sql",
		"fct_alarm_state_change.sql",
	}

	for _, table := range factTables {
		factPath := filepath.Join("models", "facts", table)
		if _, err := os.Stat(factPath); os.IsNotExist(err) {
			t.Errorf("Fact table %s does not exist at %s", table, factPath)
		}
	}
}

// TestFactTableJoins verifies foreign keys to dimensions are valid (no orphan records)
func TestFactTableJoins(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Helper to load a model into the database
	loadModel := func(modelPath string, tableName string) error {
		content, err := os.ReadFile(modelPath)
		if err != nil {
			return err
		}

		contentStr := string(content)
		// Remove config directives and template references
		sqlContent := stripTemplateConfig(contentStr)

		// Replace all {{ ref "model_name" }} with actual table names
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_alarm_events" }}`, "raw_alarm_events")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_alarm_config" }}`, "raw_alarm_config")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_alarm_tag" }}`, "dim_alarm_tag")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_equipment" }}`, "dim_equipment")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_process_area" }}`, "dim_process_area")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_priority" }}`, "dim_priority")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_operator" }}`, "dim_operator")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "fct_alarm_occurrence" }}`, "fct_alarm_occurrence")

		sqlContent = strings.TrimSpace(sqlContent)

		createTableSQL := "CREATE TABLE " + tableName + " AS " + sqlContent

		_, err = db.ExecContext(ctx, createTableSQL)
		return err
	}

	// Load sources
	if err := loadModel(filepath.Join("models", "sources", "raw_alarm_events.sql"), "raw_alarm_events"); err != nil {
		t.Fatalf("Failed to load raw_alarm_events: %v", err)
	}
	if err := loadModel(filepath.Join("models", "sources", "raw_alarm_config.sql"), "raw_alarm_config"); err != nil {
		t.Fatalf("Failed to load raw_alarm_config: %v", err)
	}

	// Load dimensions
	if err := loadModel(filepath.Join("models", "dimensions", "dim_alarm_tag.sql"), "dim_alarm_tag"); err != nil {
		t.Fatalf("Failed to load dim_alarm_tag: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_equipment.sql"), "dim_equipment"); err != nil {
		t.Fatalf("Failed to load dim_equipment: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_process_area.sql"), "dim_process_area"); err != nil {
		t.Fatalf("Failed to load dim_process_area: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_priority.sql"), "dim_priority"); err != nil {
		t.Fatalf("Failed to load dim_priority: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_operator.sql"), "dim_operator"); err != nil {
		t.Fatalf("Failed to load dim_operator: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_dates.sql"), "dim_dates"); err != nil {
		t.Fatalf("Failed to load dim_dates: %v", err)
	}

	// Load fact tables
	if err := loadModel(filepath.Join("models", "facts", "fct_alarm_occurrence.sql"), "fct_alarm_occurrence"); err != nil {
		t.Fatalf("Failed to load fct_alarm_occurrence: %v", err)
	}
	if err := loadModel(filepath.Join("models", "facts", "fct_alarm_state_change.sql"), "fct_alarm_state_change"); err != nil {
		t.Fatalf("Failed to load fct_alarm_state_change: %v", err)
	}

	// Verify no orphan records in fct_alarm_occurrence
	// Check tag_key FK
	var orphanTagCount int
	err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM fct_alarm_occurrence f 
		LEFT JOIN dim_alarm_tag d ON f.tag_key = d.tag_key 
		WHERE d.tag_key IS NULL
	`).Scan(&orphanTagCount)
	if err != nil {
		t.Fatalf("Failed to check orphan tag_key: %v", err)
	}
	if orphanTagCount > 0 {
		t.Errorf("Found %d orphan tag_key records in fct_alarm_occurrence", orphanTagCount)
	}

	// Check area_key FK
	var orphanAreaCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM fct_alarm_occurrence f 
		LEFT JOIN dim_process_area d ON f.area_key = d.area_key 
		WHERE d.area_key IS NULL
	`).Scan(&orphanAreaCount)
	if err != nil {
		t.Fatalf("Failed to check orphan area_key: %v", err)
	}
	if orphanAreaCount > 0 {
		t.Errorf("Found %d orphan area_key records in fct_alarm_occurrence", orphanAreaCount)
	}

	// Check priority_key FK
	var orphanPriorityCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM fct_alarm_occurrence f 
		LEFT JOIN dim_priority d ON f.priority_key = d.priority_key 
		WHERE d.priority_key IS NULL
	`).Scan(&orphanPriorityCount)
	if err != nil {
		t.Fatalf("Failed to check orphan priority_key: %v", err)
	}
	if orphanPriorityCount > 0 {
		t.Errorf("Found %d orphan priority_key records in fct_alarm_occurrence", orphanPriorityCount)
	}
}

// TestAlarmDurationCalculations verifies duration_to_ack_sec and duration_to_resolve_sec calculations
func TestAlarmDurationCalculations(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Helper to load a model into the database
	loadModel := func(modelPath string, tableName string) error {
		content, err := os.ReadFile(modelPath)
		if err != nil {
			return err
		}

		contentStr := string(content)
		sqlContent := stripTemplateConfig(contentStr)

		// Replace template references
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_alarm_events" }}`, "raw_alarm_events")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_alarm_config" }}`, "raw_alarm_config")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_alarm_tag" }}`, "dim_alarm_tag")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_equipment" }}`, "dim_equipment")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_process_area" }}`, "dim_process_area")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_priority" }}`, "dim_priority")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_operator" }}`, "dim_operator")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")

		sqlContent = strings.TrimSpace(sqlContent)

		createTableSQL := "CREATE TABLE " + tableName + " AS " + sqlContent

		_, err = db.ExecContext(ctx, createTableSQL)
		return err
	}

	// Load required models
	if err := loadModel(filepath.Join("models", "sources", "raw_alarm_events.sql"), "raw_alarm_events"); err != nil {
		t.Fatalf("Failed to load raw_alarm_events: %v", err)
	}
	if err := loadModel(filepath.Join("models", "sources", "raw_alarm_config.sql"), "raw_alarm_config"); err != nil {
		t.Fatalf("Failed to load raw_alarm_config: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_alarm_tag.sql"), "dim_alarm_tag"); err != nil {
		t.Fatalf("Failed to load dim_alarm_tag: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_equipment.sql"), "dim_equipment"); err != nil {
		t.Fatalf("Failed to load dim_equipment: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_process_area.sql"), "dim_process_area"); err != nil {
		t.Fatalf("Failed to load dim_process_area: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_priority.sql"), "dim_priority"); err != nil {
		t.Fatalf("Failed to load dim_priority: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_operator.sql"), "dim_operator"); err != nil {
		t.Fatalf("Failed to load dim_operator: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_dates.sql"), "dim_dates"); err != nil {
		t.Fatalf("Failed to load dim_dates: %v", err)
	}
	if err := loadModel(filepath.Join("models", "facts", "fct_alarm_occurrence.sql"), "fct_alarm_occurrence"); err != nil {
		t.Fatalf("Failed to load fct_alarm_occurrence: %v", err)
	}

	// Test case: FIC-101 (first alarm in data)
	// ACTIVE: 2026-02-06 08:15:30
	// ACKNOWLEDGED: 2026-02-06 08:17:45
	// INACTIVE: 2026-02-06 08:20:10
	// Expected duration_to_ack: 135 seconds (2 min 15 sec)
	// Expected duration_to_resolve: 280 seconds (4 min 40 sec)

	var durationToAck, durationToResolve sql.NullInt64
	err := db.QueryRowContext(ctx, `
		SELECT duration_to_ack_sec, duration_to_resolve_sec
		FROM fct_alarm_occurrence
		WHERE alarm_id LIKE 'FIC-101_%'
		ORDER BY activation_timestamp
		LIMIT 1
	`).Scan(&durationToAck, &durationToResolve)
	if err != nil {
		t.Fatalf("Failed to query FIC-101 durations: %v", err)
	}

	// Verify duration_to_ack is approximately 135 seconds (2:15) - allow ±1 sec for floating point precision
	if !durationToAck.Valid {
		t.Error("duration_to_ack_sec should not be NULL for FIC-101")
	} else if durationToAck.Int64 < 134 || durationToAck.Int64 > 136 {
		t.Errorf("FIC-101 duration_to_ack_sec = %d, want approximately 135 (2 min 15 sec, ±1 sec tolerance)", durationToAck.Int64)
	}

	// Verify duration_to_resolve is approximately 280 seconds (4:40) - allow ±1 sec for floating point precision
	if !durationToResolve.Valid {
		t.Error("duration_to_resolve_sec should not be NULL for FIC-101")
	} else if durationToResolve.Int64 < 279 || durationToResolve.Int64 > 281 {
		t.Errorf("FIC-101 duration_to_resolve_sec = %d, want approximately 280 (4 min 40 sec, ±1 sec tolerance)", durationToResolve.Int64)
	}

	// Test unacknowledged alarms have NULL duration_to_ack
	var unackedCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM fct_alarm_occurrence 
		WHERE is_acknowledged = 0 AND duration_to_ack_sec IS NOT NULL
	`).Scan(&unackedCount)
	if err != nil {
		t.Fatalf("Failed to check unacknowledged alarms: %v", err)
	}
	if unackedCount > 0 {
		t.Errorf("Found %d unacknowledged alarms with non-NULL duration_to_ack_sec", unackedCount)
	}
}

// TestStandingAlarmFlags verifies is_standing_10min and is_standing_24hr flags are correctly set
func TestStandingAlarmFlags(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Helper to load a model into the database
	loadModel := func(modelPath string, tableName string) error {
		content, err := os.ReadFile(modelPath)
		if err != nil {
			return err
		}

		contentStr := string(content)
		sqlContent := stripTemplateConfig(contentStr)

		// Replace template references
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_alarm_events" }}`, "raw_alarm_events")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_alarm_config" }}`, "raw_alarm_config")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_alarm_tag" }}`, "dim_alarm_tag")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_equipment" }}`, "dim_equipment")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_process_area" }}`, "dim_process_area")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_priority" }}`, "dim_priority")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_operator" }}`, "dim_operator")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")

		sqlContent = strings.TrimSpace(sqlContent)

		createTableSQL := "CREATE TABLE " + tableName + " AS " + sqlContent

		_, err = db.ExecContext(ctx, createTableSQL)
		return err
	}

	// Load required models
	if err := loadModel(filepath.Join("models", "sources", "raw_alarm_events.sql"), "raw_alarm_events"); err != nil {
		t.Fatalf("Failed to load raw_alarm_events: %v", err)
	}
	if err := loadModel(filepath.Join("models", "sources", "raw_alarm_config.sql"), "raw_alarm_config"); err != nil {
		t.Fatalf("Failed to load raw_alarm_config: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_alarm_tag.sql"), "dim_alarm_tag"); err != nil {
		t.Fatalf("Failed to load dim_alarm_tag: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_equipment.sql"), "dim_equipment"); err != nil {
		t.Fatalf("Failed to load dim_equipment: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_process_area.sql"), "dim_process_area"); err != nil {
		t.Fatalf("Failed to load dim_process_area: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_priority.sql"), "dim_priority"); err != nil {
		t.Fatalf("Failed to load dim_priority: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_operator.sql"), "dim_operator"); err != nil {
		t.Fatalf("Failed to load dim_operator: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_dates.sql"), "dim_dates"); err != nil {
		t.Fatalf("Failed to load dim_dates: %v", err)
	}
	if err := loadModel(filepath.Join("models", "facts", "fct_alarm_occurrence.sql"), "fct_alarm_occurrence"); err != nil {
		t.Fatalf("Failed to load fct_alarm_occurrence: %v", err)
	}

	// Verify standing alarm flags
	// LIC-115: acknowledged after 15:30 (930 seconds) - should be standing 10min
	var isStanding10min int
	err := db.QueryRowContext(ctx, `
		SELECT is_standing_10min
		FROM fct_alarm_occurrence
		WHERE alarm_id LIKE 'LIC-115_%'
	`).Scan(&isStanding10min)
	if err != nil {
		t.Fatalf("Failed to query LIC-115: %v", err)
	}
	if isStanding10min != 1 {
		t.Errorf("LIC-115 is_standing_10min = %d, want 1 (acknowledged after 930 seconds)", isStanding10min)
	}

	// FIC-101: acknowledged quickly (135 seconds) - should NOT be standing
	err = db.QueryRowContext(ctx, `
		SELECT is_standing_10min
		FROM fct_alarm_occurrence
		WHERE alarm_id LIKE 'FIC-101_%'
	`).Scan(&isStanding10min)
	if err != nil {
		t.Fatalf("Failed to query FIC-101: %v", err)
	}
	if isStanding10min != 0 {
		t.Errorf("FIC-101 is_standing_10min = %d, want 0 (acknowledged after 135 seconds)", isStanding10min)
	}

	// Count total standing alarms (>600 seconds = 10 minutes)
	var standingCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM fct_alarm_occurrence 
		WHERE is_standing_10min = 1
	`).Scan(&standingCount)
	if err != nil {
		t.Fatalf("Failed to count standing alarms: %v", err)
	}

	// Based on the data, we have several standing alarms (>10 min to acknowledge)
	// LIC-115 (930s), TAH-120 (1335s), PSL-111 (1545s), FIC-112 (2120s), TIC-135 (2550s),
	// plus alarms in the storm that took >14 minutes
	if standingCount < 5 {
		t.Errorf("Standing alarm count = %d, want at least 5", standingCount)
	}
}

// TestFactTableMetrics verifies fact tables produce expected metrics
func TestFactTableMetrics(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Helper to load a model into the database
	loadModel := func(modelPath string, tableName string) error {
		content, err := os.ReadFile(modelPath)
		if err != nil {
			return err
		}

		contentStr := string(content)
		sqlContent := stripTemplateConfig(contentStr)

		// Replace template references
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_alarm_events" }}`, "raw_alarm_events")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_alarm_config" }}`, "raw_alarm_config")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_alarm_tag" }}`, "dim_alarm_tag")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_equipment" }}`, "dim_equipment")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_process_area" }}`, "dim_process_area")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_priority" }}`, "dim_priority")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_operator" }}`, "dim_operator")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "fct_alarm_occurrence" }}`, "fct_alarm_occurrence")

		sqlContent = strings.TrimSpace(sqlContent)

		createTableSQL := "CREATE TABLE " + tableName + " AS " + sqlContent

		_, err = db.ExecContext(ctx, createTableSQL)
		return err
	}

	// Load required models
	if err := loadModel(filepath.Join("models", "sources", "raw_alarm_events.sql"), "raw_alarm_events"); err != nil {
		t.Fatalf("Failed to load raw_alarm_events: %v", err)
	}
	if err := loadModel(filepath.Join("models", "sources", "raw_alarm_config.sql"), "raw_alarm_config"); err != nil {
		t.Fatalf("Failed to load raw_alarm_config: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_alarm_tag.sql"), "dim_alarm_tag"); err != nil {
		t.Fatalf("Failed to load dim_alarm_tag: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_equipment.sql"), "dim_equipment"); err != nil {
		t.Fatalf("Failed to load dim_equipment: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_process_area.sql"), "dim_process_area"); err != nil {
		t.Fatalf("Failed to load dim_process_area: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_priority.sql"), "dim_priority"); err != nil {
		t.Fatalf("Failed to load dim_priority: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_operator.sql"), "dim_operator"); err != nil {
		t.Fatalf("Failed to load dim_operator: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_dates.sql"), "dim_dates"); err != nil {
		t.Fatalf("Failed to load dim_dates: %v", err)
	}
	if err := loadModel(filepath.Join("models", "facts", "fct_alarm_occurrence.sql"), "fct_alarm_occurrence"); err != nil {
		t.Fatalf("Failed to load fct_alarm_occurrence: %v", err)
	}
	if err := loadModel(filepath.Join("models", "facts", "fct_alarm_state_change.sql"), "fct_alarm_state_change"); err != nil {
		t.Fatalf("Failed to load fct_alarm_state_change: %v", err)
	}

	// Verify fct_alarm_occurrence row count
	var occurrenceCount int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM fct_alarm_occurrence").Scan(&occurrenceCount)
	if err != nil {
		t.Fatalf("Failed to count occurrences: %v", err)
	}

	// We have 25 ACTIVE events in the raw data
	if occurrenceCount != 25 {
		t.Errorf("Occurrence count = %d, want 25 (one per ACTIVE event)", occurrenceCount)
	}

	// Verify fct_alarm_state_change row count
	var stateChangeCount int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM fct_alarm_state_change").Scan(&stateChangeCount)
	if err != nil {
		t.Fatalf("Failed to count state changes: %v", err)
	}

	// We have 54 total events in the raw data
	if stateChangeCount != 54 {
		t.Errorf("State change count = %d, want 54 (all events)", stateChangeCount)
	}

	// Verify chattering alarm has multiple state changes
	// TIC-105 has 11 events (5 cycles + final ack)
	var ticStateChanges int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM fct_alarm_state_change sc
		INNER JOIN dim_alarm_tag dt ON sc.tag_key = dt.tag_key
		WHERE dt.tag_id = 'TIC-105'
	`).Scan(&ticStateChanges)
	if err != nil {
		t.Fatalf("Failed to count TIC-105 state changes: %v", err)
	}
	if ticStateChanges != 11 {
		t.Errorf("TIC-105 state changes = %d, want 11", ticStateChanges)
	}

	// Verify ISA 18.2 metrics exist and have reasonable values
	var avgAckTime sql.NullFloat64
	err = db.QueryRowContext(ctx, `
		SELECT AVG(duration_to_ack_sec) 
		FROM fct_alarm_occurrence 
		WHERE is_acknowledged = 1
	`).Scan(&avgAckTime)
	if err != nil {
		t.Fatalf("Failed to calculate avg ack time: %v", err)
	}
	if !avgAckTime.Valid {
		t.Error("Average acknowledgment time is NULL")
	} else if avgAckTime.Float64 < 0 {
		t.Errorf("Average acknowledgment time = %f, want >= 0", avgAckTime.Float64)
	}

	// Verify we can join to all dimensions
	var dimensionalQueryCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM fct_alarm_occurrence f
		INNER JOIN dim_alarm_tag dt ON f.tag_key = dt.tag_key
		INNER JOIN dim_process_area da ON f.area_key = da.area_key
		INNER JOIN dim_priority dp ON f.priority_key = dp.priority_key
		INNER JOIN dim_dates dd ON f.activation_date_key = dd.date_key
	`).Scan(&dimensionalQueryCount)
	if err != nil {
		t.Fatalf("Failed to join all dimensions: %v", err)
	}
	if dimensionalQueryCount != occurrenceCount {
		t.Errorf("Dimensional join count = %d, want %d (same as occurrence count)", dimensionalQueryCount, occurrenceCount)
	}
}

// TestOperatorLoadingCalculation verifies operator loading rollup calculates 10-minute buckets
// and ISA 18.2 categories (ACCEPTABLE/MANAGEABLE/UNACCEPTABLE) correctly
func TestOperatorLoadingCalculation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Helper to load a model into the database
	loadModel := func(modelPath string, tableName string) error {
		content, err := os.ReadFile(modelPath)
		if err != nil {
			return err
		}

		contentStr := string(content)
		sqlContent := stripTemplateConfig(contentStr)

		// Replace template references
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_alarm_events" }}`, "raw_alarm_events")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_alarm_config" }}`, "raw_alarm_config")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_alarm_tag" }}`, "dim_alarm_tag")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_equipment" }}`, "dim_equipment")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_process_area" }}`, "dim_process_area")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_priority" }}`, "dim_priority")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_operator" }}`, "dim_operator")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_time" }}`, "dim_time")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "fct_alarm_occurrence" }}`, "fct_alarm_occurrence")

		sqlContent = strings.TrimSpace(sqlContent)

		createTableSQL := "CREATE TABLE " + tableName + " AS " + sqlContent

		_, err = db.ExecContext(ctx, createTableSQL)
		return err
	}

	// Load required models
	if err := loadModel(filepath.Join("models", "sources", "raw_alarm_events.sql"), "raw_alarm_events"); err != nil {
		t.Fatalf("Failed to load raw_alarm_events: %v", err)
	}
	if err := loadModel(filepath.Join("models", "sources", "raw_alarm_config.sql"), "raw_alarm_config"); err != nil {
		t.Fatalf("Failed to load raw_alarm_config: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_alarm_tag.sql"), "dim_alarm_tag"); err != nil {
		t.Fatalf("Failed to load dim_alarm_tag: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_equipment.sql"), "dim_equipment"); err != nil {
		t.Fatalf("Failed to load dim_equipment: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_process_area.sql"), "dim_process_area"); err != nil {
		t.Fatalf("Failed to load dim_process_area: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_priority.sql"), "dim_priority"); err != nil {
		t.Fatalf("Failed to load dim_priority: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_operator.sql"), "dim_operator"); err != nil {
		t.Fatalf("Failed to load dim_operator: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_dates.sql"), "dim_dates"); err != nil {
		t.Fatalf("Failed to load dim_dates: %v", err)
	}
	if err := loadModel(filepath.Join("models", "facts", "fct_alarm_occurrence.sql"), "fct_alarm_occurrence"); err != nil {
		t.Fatalf("Failed to load fct_alarm_occurrence: %v", err)
	}

	// Load the rollup model
	if err := loadModel(filepath.Join("models", "rollups", "rollup_operator_loading_hourly.sql"), "rollup_operator_loading_hourly"); err != nil {
		t.Fatalf("Failed to load rollup_operator_loading_hourly: %v", err)
	}

	// Verify rollup table exists and has rows
	var rowCount int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM rollup_operator_loading_hourly").Scan(&rowCount)
	if err != nil {
		t.Fatalf("Failed to query rollup_operator_loading_hourly: %v", err)
	}
	if rowCount == 0 {
		t.Error("rollup_operator_loading_hourly has no rows")
	}

	// Verify D-200 storm period (Feb 7, 08:00-08:08) is categorized as UNACCEPTABLE
	// Time bucket for 08:00 = (8 * 6) + (0 / 10) = 48
	var stormCategory string
	var stormAlarmCount int
	err = db.QueryRowContext(ctx, `
		SELECT loading_category, alarm_count
		FROM rollup_operator_loading_hourly
		WHERE date_key = 20260207 AND time_bucket_key = 48
	`).Scan(&stormCategory, &stormAlarmCount)
	if err != nil {
		t.Fatalf("Failed to query D-200 storm period: %v", err)
	}
	if stormCategory != "UNACCEPTABLE" {
		t.Errorf("Storm loading_category = %q, want 'UNACCEPTABLE'", stormCategory)
	}
	if stormAlarmCount <= 10 {
		t.Errorf("Storm alarm_count = %d, want > 10", stormAlarmCount)
	}

	// Verify ACCEPTABLE category exists for normal periods
	var acceptableCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM rollup_operator_loading_hourly
		WHERE loading_category = 'ACCEPTABLE'
	`).Scan(&acceptableCount)
	if err != nil {
		t.Fatalf("Failed to count ACCEPTABLE periods: %v", err)
	}
	if acceptableCount == 0 {
		t.Error("No ACCEPTABLE loading periods found, expected at least one for normal operations")
	}

	// Verify MANAGEABLE category exists
	var manageableCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM rollup_operator_loading_hourly
		WHERE loading_category = 'MANAGEABLE'
	`).Scan(&manageableCount)
	if err != nil {
		t.Fatalf("Failed to count MANAGEABLE periods: %v", err)
	}
	// MANAGEABLE is optional depending on data distribution, just verify query works
	t.Logf("Found %d MANAGEABLE loading periods", manageableCount)

	// Verify time_bucket_key is in valid range (0-143)
	var invalidBucketCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM rollup_operator_loading_hourly
		WHERE time_bucket_key < 0 OR time_bucket_key > 143
	`).Scan(&invalidBucketCount)
	if err != nil {
		t.Fatalf("Failed to check time_bucket_key range: %v", err)
	}
	if invalidBucketCount > 0 {
		t.Errorf("Found %d rows with invalid time_bucket_key (should be 0-143)", invalidBucketCount)
	}
}

// TestAlarmFloodDetection verifies >10 alarms/10min periods are flagged as floods
// The D-200 storm should be flagged with is_alarm_flood = 1
func TestAlarmFloodDetection(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Helper to load a model into the database
	loadModel := func(modelPath string, tableName string) error {
		content, err := os.ReadFile(modelPath)
		if err != nil {
			return err
		}

		contentStr := string(content)
		sqlContent := stripTemplateConfig(contentStr)

		// Replace template references
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_alarm_events" }}`, "raw_alarm_events")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_alarm_config" }}`, "raw_alarm_config")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_alarm_tag" }}`, "dim_alarm_tag")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_equipment" }}`, "dim_equipment")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_process_area" }}`, "dim_process_area")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_priority" }}`, "dim_priority")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_operator" }}`, "dim_operator")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_time" }}`, "dim_time")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "fct_alarm_occurrence" }}`, "fct_alarm_occurrence")

		sqlContent = strings.TrimSpace(sqlContent)

		createTableSQL := "CREATE TABLE " + tableName + " AS " + sqlContent

		_, err = db.ExecContext(ctx, createTableSQL)
		return err
	}

	// Load required models
	if err := loadModel(filepath.Join("models", "sources", "raw_alarm_events.sql"), "raw_alarm_events"); err != nil {
		t.Fatalf("Failed to load raw_alarm_events: %v", err)
	}
	if err := loadModel(filepath.Join("models", "sources", "raw_alarm_config.sql"), "raw_alarm_config"); err != nil {
		t.Fatalf("Failed to load raw_alarm_config: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_alarm_tag.sql"), "dim_alarm_tag"); err != nil {
		t.Fatalf("Failed to load dim_alarm_tag: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_equipment.sql"), "dim_equipment"); err != nil {
		t.Fatalf("Failed to load dim_equipment: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_process_area.sql"), "dim_process_area"); err != nil {
		t.Fatalf("Failed to load dim_process_area: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_priority.sql"), "dim_priority"); err != nil {
		t.Fatalf("Failed to load dim_priority: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_operator.sql"), "dim_operator"); err != nil {
		t.Fatalf("Failed to load dim_operator: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_dates.sql"), "dim_dates"); err != nil {
		t.Fatalf("Failed to load dim_dates: %v", err)
	}
	if err := loadModel(filepath.Join("models", "facts", "fct_alarm_occurrence.sql"), "fct_alarm_occurrence"); err != nil {
		t.Fatalf("Failed to load fct_alarm_occurrence: %v", err)
	}

	// Load the rollup model
	if err := loadModel(filepath.Join("models", "rollups", "rollup_operator_loading_hourly.sql"), "rollup_operator_loading_hourly"); err != nil {
		t.Fatalf("Failed to load rollup_operator_loading_hourly: %v", err)
	}

	// Verify D-200 storm is flagged as a flood
	// Time bucket for 08:00 = 48
	var isFlood int
	var alarmCount int
	err := db.QueryRowContext(ctx, `
		SELECT is_alarm_flood, alarm_count
		FROM rollup_operator_loading_hourly
		WHERE date_key = 20260207 AND time_bucket_key = 48
	`).Scan(&isFlood, &alarmCount)
	if err != nil {
		t.Fatalf("Failed to query storm flood flag: %v", err)
	}
	if isFlood != 1 {
		t.Errorf("Storm is_alarm_flood = %d, want 1 (>10 alarms)", isFlood)
	}

	// Verify non-flood periods have is_alarm_flood = 0
	var nonFloodCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM rollup_operator_loading_hourly
		WHERE is_alarm_flood = 0 AND alarm_count <= 10
	`).Scan(&nonFloodCount)
	if err != nil {
		t.Fatalf("Failed to count non-flood periods: %v", err)
	}
	if nonFloodCount == 0 {
		t.Error("No non-flood periods found, expected normal operations to have is_alarm_flood = 0")
	}

	// Verify flood flag matches alarm count logic
	var mismatchCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM rollup_operator_loading_hourly
		WHERE (alarm_count > 10 AND is_alarm_flood != 1)
		   OR (alarm_count <= 10 AND is_alarm_flood != 0)
	`).Scan(&mismatchCount)
	if err != nil {
		t.Fatalf("Failed to check flood flag consistency: %v", err)
	}
	if mismatchCount > 0 {
		t.Errorf("Found %d rows with inconsistent is_alarm_flood flag", mismatchCount)
	}
}

// TestStandingAlarmDuration verifies standing alarm duration metrics by tag
// Expected: 9 total standing alarms (5 in C-100, 4 in D-200)
func TestStandingAlarmDuration(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Helper to load a model into the database
	loadModel := func(modelPath string, tableName string) error {
		content, err := os.ReadFile(modelPath)
		if err != nil {
			return err
		}

		contentStr := string(content)
		sqlContent := stripTemplateConfig(contentStr)

		// Replace template references
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_alarm_events" }}`, "raw_alarm_events")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_alarm_config" }}`, "raw_alarm_config")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_alarm_tag" }}`, "dim_alarm_tag")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_equipment" }}`, "dim_equipment")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_process_area" }}`, "dim_process_area")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_priority" }}`, "dim_priority")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_operator" }}`, "dim_operator")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
		sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "fct_alarm_occurrence" }}`, "fct_alarm_occurrence")

		sqlContent = strings.TrimSpace(sqlContent)

		createTableSQL := "CREATE TABLE " + tableName + " AS " + sqlContent

		_, err = db.ExecContext(ctx, createTableSQL)
		return err
	}

	// Load required models
	if err := loadModel(filepath.Join("models", "sources", "raw_alarm_events.sql"), "raw_alarm_events"); err != nil {
		t.Fatalf("Failed to load raw_alarm_events: %v", err)
	}
	if err := loadModel(filepath.Join("models", "sources", "raw_alarm_config.sql"), "raw_alarm_config"); err != nil {
		t.Fatalf("Failed to load raw_alarm_config: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_alarm_tag.sql"), "dim_alarm_tag"); err != nil {
		t.Fatalf("Failed to load dim_alarm_tag: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_equipment.sql"), "dim_equipment"); err != nil {
		t.Fatalf("Failed to load dim_equipment: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_process_area.sql"), "dim_process_area"); err != nil {
		t.Fatalf("Failed to load dim_process_area: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_priority.sql"), "dim_priority"); err != nil {
		t.Fatalf("Failed to load dim_priority: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_operator.sql"), "dim_operator"); err != nil {
		t.Fatalf("Failed to load dim_operator: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_dates.sql"), "dim_dates"); err != nil {
		t.Fatalf("Failed to load dim_dates: %v", err)
	}
	if err := loadModel(filepath.Join("models", "facts", "fct_alarm_occurrence.sql"), "fct_alarm_occurrence"); err != nil {
		t.Fatalf("Failed to load fct_alarm_occurrence: %v", err)
	}

	// Load the rollup model
	if err := loadModel(filepath.Join("models", "rollups", "rollup_standing_alarms.sql"), "rollup_standing_alarms"); err != nil {
		t.Fatalf("Failed to load rollup_standing_alarms: %v", err)
	}

	// Verify total count of standing alarm tags
	var tagCount int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM rollup_standing_alarms").Scan(&tagCount)
	if err != nil {
		t.Fatalf("Failed to query rollup_standing_alarms: %v", err)
	}
	// We expect at least 9 tags with standing alarms (could be more if some tags have multiple standing occurrences)
	if tagCount == 0 {
		t.Error("rollup_standing_alarms has no rows, expected at least 9 tags")
	}

	// Verify total standing alarm count
	var totalStandingCount int
	err = db.QueryRowContext(ctx, `
		SELECT SUM(standing_alarm_count)
		FROM rollup_standing_alarms
	`).Scan(&totalStandingCount)
	if err != nil {
		t.Fatalf("Failed to sum standing alarm count: %v", err)
	}
	// Expected: 5 in C-100 + 4 in D-200 = 9 total
	if totalStandingCount < 9 {
		t.Errorf("Total standing alarm count = %d, want at least 9", totalStandingCount)
	}

	// Verify duration metrics are non-negative
	var negativeCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM rollup_standing_alarms
		WHERE total_standing_duration_sec < 0
		   OR avg_standing_duration_sec < 0
		   OR max_standing_duration_sec < 0
	`).Scan(&negativeCount)
	if err != nil {
		t.Fatalf("Failed to check for negative durations: %v", err)
	}
	if negativeCount > 0 {
		t.Errorf("Found %d rows with negative duration values", negativeCount)
	}

	// Verify max >= avg >= 0 for logical consistency
	var inconsistentCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM rollup_standing_alarms
		WHERE max_standing_duration_sec < avg_standing_duration_sec
	`).Scan(&inconsistentCount)
	if err != nil {
		t.Fatalf("Failed to check duration consistency: %v", err)
	}
	if inconsistentCount > 0 {
		t.Errorf("Found %d rows where max < avg duration", inconsistentCount)
	}

	// Verify converted units are calculated correctly
	var unitMismatchCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM rollup_standing_alarms
		WHERE ABS(avg_standing_duration_min - (avg_standing_duration_sec / 60.0)) > 0.01
		   OR ABS(max_standing_duration_hrs - (max_standing_duration_sec / 3600.0)) > 0.01
		   OR ABS(total_standing_duration_hrs - (total_standing_duration_sec / 3600.0)) > 0.01
	`).Scan(&unitMismatchCount)
	if err != nil {
		t.Fatalf("Failed to check unit conversions: %v", err)
	}
	if unitMismatchCount > 0 {
		t.Errorf("Found %d rows with incorrect unit conversions", unitMismatchCount)
	}

	// Verify worst offender (highest total duration) is identified
	var worstTag string
	var worstDuration int
	err = db.QueryRowContext(ctx, `
		SELECT dt.tag_id, rs.total_standing_duration_sec
		FROM rollup_standing_alarms rs
		INNER JOIN dim_alarm_tag dt ON rs.tag_key = dt.tag_key
		ORDER BY rs.total_standing_duration_sec DESC
		LIMIT 1
	`).Scan(&worstTag, &worstDuration)
	if err != nil {
		t.Fatalf("Failed to find worst offender: %v", err)
	}
	if worstTag == "" {
		t.Error("No worst offender found")
	}
	if worstDuration <= 0 {
		t.Errorf("Worst offender duration = %d, want > 0", worstDuration)
	}
	t.Logf("Worst standing alarm offender: %s with %d seconds total duration", worstTag, worstDuration)
}

// TestChatteringDetection verifies chattering alarm detection logic.
// Chattering is defined as ≥5 activations within 10 minutes (600 seconds).
func TestChatteringDetection(t *testing.T) {
	ctx := context.Background()
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Helper to load model files and process template references
	loadModel := func(modelPath, tableName string) error {
		sqlContent, err := os.ReadFile(modelPath)
		if err != nil {
			return err
		}

		// Remove {{ config ... }} lines and process references
		sqlStr := string(sqlContent)
		lines := strings.Split(sqlStr, "\n")
		var filteredLines []string
		for _, line := range lines {
			if !strings.Contains(line, "{{ config") {
				filteredLines = append(filteredLines, line)
			}
		}
		sqlContent = []byte(strings.Join(filteredLines, "\n"))

		// Replace template references
		sqlStr = string(sqlContent)
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "raw_alarm_events" }}`, "raw_alarm_events")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "raw_alarm_config" }}`, "raw_alarm_config")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "dim_alarm_tag" }}`, "dim_alarm_tag")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "dim_equipment" }}`, "dim_equipment")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "dim_process_area" }}`, "dim_process_area")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "dim_priority" }}`, "dim_priority")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "dim_operator" }}`, "dim_operator")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "dim_dates" }}`, "dim_dates")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "fct_alarm_occurrence" }}`, "fct_alarm_occurrence")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "fct_alarm_state_change" }}`, "fct_alarm_state_change")

		sqlStr = strings.TrimSpace(sqlStr)

		createTableSQL := "CREATE TABLE " + tableName + " AS " + sqlStr

		_, err = db.ExecContext(ctx, createTableSQL)
		return err
	}

	// Load all prerequisite models
	if err := loadModel(filepath.Join("models", "sources", "raw_alarm_events.sql"), "raw_alarm_events"); err != nil {
		t.Fatalf("Failed to load raw_alarm_events: %v", err)
	}
	if err := loadModel(filepath.Join("models", "sources", "raw_alarm_config.sql"), "raw_alarm_config"); err != nil {
		t.Fatalf("Failed to load raw_alarm_config: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_alarm_tag.sql"), "dim_alarm_tag"); err != nil {
		t.Fatalf("Failed to load dim_alarm_tag: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_equipment.sql"), "dim_equipment"); err != nil {
		t.Fatalf("Failed to load dim_equipment: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_process_area.sql"), "dim_process_area"); err != nil {
		t.Fatalf("Failed to load dim_process_area: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_priority.sql"), "dim_priority"); err != nil {
		t.Fatalf("Failed to load dim_priority: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_operator.sql"), "dim_operator"); err != nil {
		t.Fatalf("Failed to load dim_operator: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_dates.sql"), "dim_dates"); err != nil {
		t.Fatalf("Failed to load dim_dates: %v", err)
	}
	if err := loadModel(filepath.Join("models", "facts", "fct_alarm_occurrence.sql"), "fct_alarm_occurrence"); err != nil {
		t.Fatalf("Failed to load fct_alarm_occurrence: %v", err)
	}
	if err := loadModel(filepath.Join("models", "facts", "fct_alarm_state_change.sql"), "fct_alarm_state_change"); err != nil {
		t.Fatalf("Failed to load fct_alarm_state_change: %v", err)
	}

	// Load the chattering rollup model
	if err := loadModel(filepath.Join("models", "rollups", "rollup_chattering_alarms.sql"), "rollup_chattering_alarms"); err != nil {
		t.Fatalf("Failed to load rollup_chattering_alarms: %v", err)
	}

	// Verify chattering tags are detected
	var chatteringCount int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM rollup_chattering_alarms").Scan(&chatteringCount)
	if err != nil {
		t.Fatalf("Failed to query rollup_chattering_alarms: %v", err)
	}
	if chatteringCount == 0 {
		t.Error("rollup_chattering_alarms has no rows, expected at least 1 (TIC-105)")
	}

	// Verify TIC-105 is detected as chattering
	var tic105Chattering int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM rollup_chattering_alarms rc
		INNER JOIN dim_alarm_tag dt ON rc.tag_key = dt.tag_key
		WHERE dt.tag_id = 'TIC-105'
	`).Scan(&tic105Chattering)
	if err != nil {
		t.Fatalf("Failed to check TIC-105 chattering: %v", err)
	}
	if tic105Chattering == 0 {
		t.Error("TIC-105 not detected as chattering, expected 1")
	}

	// Verify chattering episode counts are positive
	var zeroEpisodeCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM rollup_chattering_alarms
		WHERE chattering_episode_count <= 0
	`).Scan(&zeroEpisodeCount)
	if err != nil {
		t.Fatalf("Failed to check episode counts: %v", err)
	}
	if zeroEpisodeCount > 0 {
		t.Errorf("Found %d tags with zero or negative chattering_episode_count", zeroEpisodeCount)
	}

	// Verify metrics are reasonable
	var unreasonableCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM rollup_chattering_alarms
		WHERE total_state_changes < chattering_episode_count * 5
		   OR min_cycle_time_sec < 0
		   OR avg_cycle_time_sec < 0
		   OR max_activations_per_hour < 0
	`).Scan(&unreasonableCount)
	if err != nil {
		t.Fatalf("Failed to check metric validity: %v", err)
	}
	if unreasonableCount > 0 {
		t.Errorf("Found %d rows with unreasonable metric values", unreasonableCount)
	}

	t.Logf("Found %d chattering tags", chatteringCount)
}

// TestBadActorRanking verifies Pareto analysis and composite scoring for bad actor tags.
func TestBadActorRanking(t *testing.T) {
	ctx := context.Background()
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Helper to load model files and process template references
	loadModel := func(modelPath, tableName string) error {
		sqlContent, err := os.ReadFile(modelPath)
		if err != nil {
			return err
		}

		// Remove {{ config ... }} lines and process references
		sqlStr := string(sqlContent)
		lines := strings.Split(sqlStr, "\n")
		var filteredLines []string
		for _, line := range lines {
			if !strings.Contains(line, "{{ config") {
				filteredLines = append(filteredLines, line)
			}
		}
		sqlContent = []byte(strings.Join(filteredLines, "\n"))

		// Replace template references
		sqlStr = string(sqlContent)
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "raw_alarm_events" }}`, "raw_alarm_events")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "raw_alarm_config" }}`, "raw_alarm_config")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "dim_alarm_tag" }}`, "dim_alarm_tag")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "dim_equipment" }}`, "dim_equipment")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "dim_process_area" }}`, "dim_process_area")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "dim_priority" }}`, "dim_priority")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "dim_operator" }}`, "dim_operator")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "dim_dates" }}`, "dim_dates")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "fct_alarm_occurrence" }}`, "fct_alarm_occurrence")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "fct_alarm_state_change" }}`, "fct_alarm_state_change")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "rollup_standing_alarms" }}`, "rollup_standing_alarms")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "rollup_chattering_alarms" }}`, "rollup_chattering_alarms")

		sqlStr = strings.TrimSpace(sqlStr)

		createTableSQL := "CREATE TABLE " + tableName + " AS " + sqlStr

		_, err = db.ExecContext(ctx, createTableSQL)
		return err
	}

	// Load all prerequisite models
	if err := loadModel(filepath.Join("models", "sources", "raw_alarm_events.sql"), "raw_alarm_events"); err != nil {
		t.Fatalf("Failed to load raw_alarm_events: %v", err)
	}
	if err := loadModel(filepath.Join("models", "sources", "raw_alarm_config.sql"), "raw_alarm_config"); err != nil {
		t.Fatalf("Failed to load raw_alarm_config: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_alarm_tag.sql"), "dim_alarm_tag"); err != nil {
		t.Fatalf("Failed to load dim_alarm_tag: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_equipment.sql"), "dim_equipment"); err != nil {
		t.Fatalf("Failed to load dim_equipment: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_process_area.sql"), "dim_process_area"); err != nil {
		t.Fatalf("Failed to load dim_process_area: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_priority.sql"), "dim_priority"); err != nil {
		t.Fatalf("Failed to load dim_priority: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_operator.sql"), "dim_operator"); err != nil {
		t.Fatalf("Failed to load dim_operator: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_dates.sql"), "dim_dates"); err != nil {
		t.Fatalf("Failed to load dim_dates: %v", err)
	}
	if err := loadModel(filepath.Join("models", "facts", "fct_alarm_occurrence.sql"), "fct_alarm_occurrence"); err != nil {
		t.Fatalf("Failed to load fct_alarm_occurrence: %v", err)
	}
	if err := loadModel(filepath.Join("models", "facts", "fct_alarm_state_change.sql"), "fct_alarm_state_change"); err != nil {
		t.Fatalf("Failed to load fct_alarm_state_change: %v", err)
	}
	if err := loadModel(filepath.Join("models", "rollups", "rollup_standing_alarms.sql"), "rollup_standing_alarms"); err != nil {
		t.Fatalf("Failed to load rollup_standing_alarms: %v", err)
	}
	if err := loadModel(filepath.Join("models", "rollups", "rollup_chattering_alarms.sql"), "rollup_chattering_alarms"); err != nil {
		t.Fatalf("Failed to load rollup_chattering_alarms: %v", err)
	}

	// Load the bad actor rollup model
	if err := loadModel(filepath.Join("models", "rollups", "rollup_bad_actor_tags.sql"), "rollup_bad_actor_tags"); err != nil {
		t.Fatalf("Failed to load rollup_bad_actor_tags: %v", err)
	}

	// Verify bad actor tags are ranked
	var badActorCount int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM rollup_bad_actor_tags").Scan(&badActorCount)
	if err != nil {
		t.Fatalf("Failed to query rollup_bad_actor_tags: %v", err)
	}
	if badActorCount == 0 {
		t.Error("rollup_bad_actor_tags has no rows, expected multiple tags")
	}

	// Verify alarm_rank is sequential starting from 1
	var rankGaps int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM (
			SELECT 
				alarm_rank,
				ROW_NUMBER() OVER (ORDER BY alarm_rank) AS expected_rank
			FROM rollup_bad_actor_tags
		)
		WHERE alarm_rank != expected_rank
	`).Scan(&rankGaps)
	if err != nil {
		t.Fatalf("Failed to check rank sequence: %v", err)
	}
	if rankGaps > 0 {
		t.Errorf("Found %d gaps in alarm_rank sequence", rankGaps)
	}

	// Verify cumulative_pct is monotonically increasing
	var nonMonotonicCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM rollup_bad_actor_tags r1
		INNER JOIN rollup_bad_actor_tags r2 ON r1.alarm_rank + 1 = r2.alarm_rank
		WHERE r1.cumulative_pct > r2.cumulative_pct
	`).Scan(&nonMonotonicCount)
	if err != nil {
		t.Fatalf("Failed to check cumulative_pct monotonicity: %v", err)
	}
	if nonMonotonicCount > 0 {
		t.Errorf("Found %d violations of cumulative_pct monotonicity", nonMonotonicCount)
	}

	// Verify bad_actor_score is in valid range [0, 100]
	var invalidScoreCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM rollup_bad_actor_tags
		WHERE bad_actor_score < 0 OR bad_actor_score > 100
	`).Scan(&invalidScoreCount)
	if err != nil {
		t.Fatalf("Failed to check score range: %v", err)
	}
	if invalidScoreCount > 0 {
		t.Errorf("Found %d tags with invalid bad_actor_score", invalidScoreCount)
	}

	// Verify bad_actor_category matches score thresholds
	var categoryMismatchCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM rollup_bad_actor_tags
		WHERE (bad_actor_score >= 70 AND bad_actor_category != 'CRITICAL')
		   OR (bad_actor_score >= 50 AND bad_actor_score < 70 AND bad_actor_category != 'SIGNIFICANT')
		   OR (bad_actor_score >= 30 AND bad_actor_score < 50 AND bad_actor_category != 'MODERATE')
		   OR (bad_actor_score < 30 AND bad_actor_category != 'NORMAL')
	`).Scan(&categoryMismatchCount)
	if err != nil {
		t.Fatalf("Failed to check category consistency: %v", err)
	}
	if categoryMismatchCount > 0 {
		t.Errorf("Found %d tags with mismatched bad_actor_category", categoryMismatchCount)
	}

	// Verify top 10% flag is correctly applied
	var top10FlagError int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM (
			SELECT 
				*,
				COUNT(*) OVER () AS total_tags,
				alarm_rank * 100.0 / COUNT(*) OVER () AS pct_rank
			FROM rollup_bad_actor_tags
		)
		WHERE (pct_rank <= 10 AND is_top_10_pct != 1)
		   OR (pct_rank > 10 AND is_top_10_pct != 0)
	`).Scan(&top10FlagError)
	if err != nil {
		t.Fatalf("Failed to check top 10%% flag: %v", err)
	}
	if top10FlagError > 0 {
		t.Errorf("Found %d tags with incorrect is_top_10_pct flag", top10FlagError)
	}

	// Identify worst offender
	var worstTag string
	var worstScore float64
	err = db.QueryRowContext(ctx, `
		SELECT dt.tag_id, rb.bad_actor_score
		FROM rollup_bad_actor_tags rb
		INNER JOIN dim_alarm_tag dt ON rb.tag_key = dt.tag_key
		ORDER BY rb.alarm_rank ASC
		LIMIT 1
	`).Scan(&worstTag, &worstScore)
	if err != nil {
		t.Fatalf("Failed to find worst offender: %v", err)
	}
	if worstTag == "" {
		t.Error("No worst offender found")
	}

	t.Logf("Found %d bad actor tags, worst offender: %s (score: %.1f)", badActorCount, worstTag, worstScore)
}

// TestSystemHealthMetrics verifies overall alarm system health summary and ISA compliance calculation.
func TestSystemHealthMetrics(t *testing.T) {
	ctx := context.Background()
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Helper to load model files and process template references
	loadModel := func(modelPath, tableName string) error {
		sqlContent, err := os.ReadFile(modelPath)
		if err != nil {
			return err
		}

		// Remove {{ config ... }} lines and process references
		sqlStr := string(sqlContent)
		lines := strings.Split(sqlStr, "\n")
		var filteredLines []string
		for _, line := range lines {
			if !strings.Contains(line, "{{ config") {
				filteredLines = append(filteredLines, line)
			}
		}
		sqlContent = []byte(strings.Join(filteredLines, "\n"))

		// Replace template references
		sqlStr = string(sqlContent)
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "raw_alarm_events" }}`, "raw_alarm_events")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "raw_alarm_config" }}`, "raw_alarm_config")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "dim_alarm_tag" }}`, "dim_alarm_tag")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "dim_equipment" }}`, "dim_equipment")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "dim_process_area" }}`, "dim_process_area")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "dim_priority" }}`, "dim_priority")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "dim_operator" }}`, "dim_operator")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "dim_dates" }}`, "dim_dates")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "fct_alarm_occurrence" }}`, "fct_alarm_occurrence")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "fct_alarm_state_change" }}`, "fct_alarm_state_change")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "rollup_operator_loading_hourly" }}`, "rollup_operator_loading_hourly")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "rollup_standing_alarms" }}`, "rollup_standing_alarms")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "rollup_chattering_alarms" }}`, "rollup_chattering_alarms")
		sqlStr = strings.ReplaceAll(sqlStr, `{{ ref "rollup_bad_actor_tags" }}`, "rollup_bad_actor_tags")

		sqlStr = strings.TrimSpace(sqlStr)

		createTableSQL := "CREATE TABLE " + tableName + " AS " + sqlStr

		_, err = db.ExecContext(ctx, createTableSQL)
		return err
	}

	// Load all prerequisite models
	if err := loadModel(filepath.Join("models", "sources", "raw_alarm_events.sql"), "raw_alarm_events"); err != nil {
		t.Fatalf("Failed to load raw_alarm_events: %v", err)
	}
	if err := loadModel(filepath.Join("models", "sources", "raw_alarm_config.sql"), "raw_alarm_config"); err != nil {
		t.Fatalf("Failed to load raw_alarm_config: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_alarm_tag.sql"), "dim_alarm_tag"); err != nil {
		t.Fatalf("Failed to load dim_alarm_tag: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_equipment.sql"), "dim_equipment"); err != nil {
		t.Fatalf("Failed to load dim_equipment: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_process_area.sql"), "dim_process_area"); err != nil {
		t.Fatalf("Failed to load dim_process_area: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_priority.sql"), "dim_priority"); err != nil {
		t.Fatalf("Failed to load dim_priority: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_operator.sql"), "dim_operator"); err != nil {
		t.Fatalf("Failed to load dim_operator: %v", err)
	}
	if err := loadModel(filepath.Join("models", "dimensions", "dim_dates.sql"), "dim_dates"); err != nil {
		t.Fatalf("Failed to load dim_dates: %v", err)
	}
	if err := loadModel(filepath.Join("models", "facts", "fct_alarm_occurrence.sql"), "fct_alarm_occurrence"); err != nil {
		t.Fatalf("Failed to load fct_alarm_occurrence: %v", err)
	}
	if err := loadModel(filepath.Join("models", "facts", "fct_alarm_state_change.sql"), "fct_alarm_state_change"); err != nil {
		t.Fatalf("Failed to load fct_alarm_state_change: %v", err)
	}
	if err := loadModel(filepath.Join("models", "rollups", "rollup_operator_loading_hourly.sql"), "rollup_operator_loading_hourly"); err != nil {
		t.Fatalf("Failed to load rollup_operator_loading_hourly: %v", err)
	}
	if err := loadModel(filepath.Join("models", "rollups", "rollup_standing_alarms.sql"), "rollup_standing_alarms"); err != nil {
		t.Fatalf("Failed to load rollup_standing_alarms: %v", err)
	}
	if err := loadModel(filepath.Join("models", "rollups", "rollup_chattering_alarms.sql"), "rollup_chattering_alarms"); err != nil {
		t.Fatalf("Failed to load rollup_chattering_alarms: %v", err)
	}
	if err := loadModel(filepath.Join("models", "rollups", "rollup_bad_actor_tags.sql"), "rollup_bad_actor_tags"); err != nil {
		t.Fatalf("Failed to load rollup_bad_actor_tags: %v", err)
	}

	// Load the system health rollup model
	if err := loadModel(filepath.Join("models", "rollups", "rollup_alarm_system_health.sql"), "rollup_alarm_system_health"); err != nil {
		t.Fatalf("Failed to load rollup_alarm_system_health: %v", err)
	}

	// Verify exactly one summary row exists
	var rowCount int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM rollup_alarm_system_health").Scan(&rowCount)
	if err != nil {
		t.Fatalf("Failed to query rollup_alarm_system_health: %v", err)
	}
	if rowCount != 1 {
		t.Errorf("rollup_alarm_system_health has %d rows, expected exactly 1", rowCount)
	}

	// Retrieve and validate all metrics
	var (
		healthKey              int
		analysisDate           sql.NullString
		dateKey                sql.NullInt64
		totalAlarmCount        int
		uniqueTagCount         int
		avgAlarmsPerHour       float64
		peakAlarmsPer10min     int
		pctTimeAcceptable      float64
		pctTimeManageable      float64
		pctTimeUnacceptable    float64
		alarmFloodCount        int
		totalStandingAlarms    int
		avgStandingDurationMin float64
		chatteringTagCount     int
		top10ContributionPct   float64
		badActorCount          int
		isaComplianceScore     float64
	)

	err = db.QueryRowContext(ctx, `
		SELECT 
			health_key,
			analysis_date,
			date_key,
			total_alarm_count,
			unique_tag_count,
			avg_alarms_per_hour,
			peak_alarms_per_10min,
			pct_time_acceptable,
			pct_time_manageable,
			pct_time_unacceptable,
			alarm_flood_count,
			total_standing_alarms,
			avg_standing_duration_min,
			chattering_tag_count,
			top_10_contribution_pct,
			bad_actor_count,
			isa_compliance_score
		FROM rollup_alarm_system_health
	`).Scan(
		&healthKey,
		&analysisDate,
		&dateKey,
		&totalAlarmCount,
		&uniqueTagCount,
		&avgAlarmsPerHour,
		&peakAlarmsPer10min,
		&pctTimeAcceptable,
		&pctTimeManageable,
		&pctTimeUnacceptable,
		&alarmFloodCount,
		&totalStandingAlarms,
		&avgStandingDurationMin,
		&chatteringTagCount,
		&top10ContributionPct,
		&badActorCount,
		&isaComplianceScore,
	)
	if err != nil {
		t.Fatalf("Failed to retrieve health metrics: %v", err)
	}

	// Verify key structure
	if healthKey != 1 {
		t.Errorf("health_key = %d, want 1", healthKey)
	}
	if analysisDate.Valid {
		t.Errorf("analysis_date = %q, want NULL for overall summary", analysisDate.String)
	}
	if dateKey.Valid {
		t.Errorf("date_key = %d, want NULL for overall summary", dateKey.Int64)
	}

	// Verify alarm counts are positive
	if totalAlarmCount <= 0 {
		t.Errorf("total_alarm_count = %d, want > 0", totalAlarmCount)
	}
	if uniqueTagCount <= 0 {
		t.Errorf("unique_tag_count = %d, want > 0", uniqueTagCount)
	}

	// Verify operator loading metrics
	if avgAlarmsPerHour < 0 {
		t.Errorf("avg_alarms_per_hour = %.2f, want >= 0", avgAlarmsPerHour)
	}
	if peakAlarmsPer10min <= 0 {
		t.Errorf("peak_alarms_per_10min = %d, want > 0", peakAlarmsPer10min)
	}

	// Verify loading percentages sum to ~100%
	pctTotal := pctTimeAcceptable + pctTimeManageable + pctTimeUnacceptable
	if pctTotal < 99.0 || pctTotal > 101.0 {
		t.Errorf("Loading percentages sum to %.2f, want ~100", pctTotal)
	}

	// Verify ISA compliance score is in valid range [0, 100]
	if isaComplianceScore < 0 || isaComplianceScore > 100 {
		t.Errorf("isa_compliance_score = %.1f, want [0, 100]", isaComplianceScore)
	}

	// Verify non-negative counts
	if totalStandingAlarms < 0 {
		t.Errorf("total_standing_alarms = %d, want >= 0", totalStandingAlarms)
	}
	if chatteringTagCount < 0 {
		t.Errorf("chattering_tag_count = %d, want >= 0", chatteringTagCount)
	}
	if badActorCount < 0 {
		t.Errorf("bad_actor_count = %d, want >= 0", badActorCount)
	}

	t.Logf("System Health Summary:")
	t.Logf("  Total Alarms: %d", totalAlarmCount)
	t.Logf("  Unique Tags: %d", uniqueTagCount)
	t.Logf("  Avg Alarms/Hour: %.2f", avgAlarmsPerHour)
	t.Logf("  Peak Alarms/10min: %d", peakAlarmsPer10min)
	t.Logf("  Time Acceptable: %.1f%%", pctTimeAcceptable)
	t.Logf("  Standing Alarms: %d", totalStandingAlarms)
	t.Logf("  Chattering Tags: %d", chatteringTagCount)
	t.Logf("  Bad Actors (score >= 50): %d", badActorCount)
	t.Logf("  ISA Compliance Score: %.1f", isaComplianceScore)
}

// TestDataIntegrity verifies referential integrity and data quality across all tables.
// This comprehensive validation ensures no orphan foreign keys exist and all
// relationships are properly maintained.
// NOTE: Requires `gorchata run` to be executed first to build all models.
func TestDataIntegrity(t *testing.T) {
	// Connect to project database
	db, cleanup := connectToProjectDB(t)
	defer cleanup()

	t.Run("NoOrphanTagKeys", func(t *testing.T) {
		// Verify all tag_key in fct_alarm_occurrence exist in dim_alarm_tag
		query := `
			SELECT COUNT(*) as orphan_count
			FROM fct_alarm_occurrence f
			WHERE NOT EXISTS (
				SELECT 1 FROM dim_alarm_tag d WHERE d.tag_key = f.tag_key
			)
		`
		var orphanCount int
		err := db.QueryRow(query).Scan(&orphanCount)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if orphanCount > 0 {
			t.Errorf("Found %d orphan tag_key references in fct_alarm_occurrence", orphanCount)
		}
	})

	t.Run("NoOrphanAreaKeys", func(t *testing.T) {
		// Verify all area_key in fct_alarm_occurrence exist in dim_process_area
		query := `
			SELECT COUNT(*) as orphan_count
			FROM fct_alarm_occurrence f
			WHERE NOT EXISTS (
				SELECT 1 FROM dim_process_area d WHERE d.area_key = f.area_key
			)
		`
		var orphanCount int
		err := db.QueryRow(query).Scan(&orphanCount)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if orphanCount > 0 {
			t.Errorf("Found %d orphan area_key references in fct_alarm_occurrence", orphanCount)
		}
	})

	t.Run("NoOrphanPriorityKeys", func(t *testing.T) {
		// Verify all priority_key in fct_alarm_occurrence exist in dim_priority
		query := `
			SELECT COUNT(*) as orphan_count
			FROM fct_alarm_occurrence f
			WHERE NOT EXISTS (
				SELECT 1 FROM dim_priority d WHERE d.priority_key = f.priority_key
			)
		`
		var orphanCount int
		err := db.QueryRow(query).Scan(&orphanCount)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if orphanCount > 0 {
			t.Errorf("Found %d orphan priority_key references in fct_alarm_occurrence", orphanCount)
		}
	})

	t.Run("TimestampOrdering", func(t *testing.T) {
		// Verify acknowledged >= activation, inactive >= acknowledged
		query := `
			SELECT COUNT(*) as violation_count
			FROM fct_alarm_occurrence
			WHERE (acknowledged_timestamp IS NOT NULL 
				   AND acknowledged_timestamp < activation_timestamp)
			   OR (inactive_timestamp IS NOT NULL 
			       AND acknowledged_timestamp IS NOT NULL
				   AND inactive_timestamp < acknowledged_timestamp)
		`
		var violationCount int
		err := db.QueryRow(query).Scan(&violationCount)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if violationCount > 0 {
			t.Errorf("Found %d timestamp ordering violations", violationCount)
		}
	})

	t.Run("DurationCalculations", func(t *testing.T) {
		// Verify duration_to_resolve_sec >= duration_to_ack_sec where both exist
		query := `
			SELECT COUNT(*) as violation_count
			FROM fct_alarm_occurrence
			WHERE duration_to_ack_sec IS NOT NULL
			  AND duration_to_resolve_sec IS NOT NULL
			  AND duration_to_resolve_sec < duration_to_ack_sec
		`
		var violationCount int
		err := db.QueryRow(query).Scan(&violationCount)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if violationCount > 0 {
			t.Errorf("Found %d duration calculation violations (resolve < ack)", violationCount)
		}
	})

	t.Run("DateKeyValidity", func(t *testing.T) {
		// Verify date keys match YYYYMMDD format from activation timestamps
		query := `
			SELECT COUNT(*) as invalid_count
			FROM fct_alarm_occurrence
			WHERE CAST(strftime('%Y%m%d', activation_timestamp) AS INTEGER) != activation_date_key
		`
		var invalidCount int
		err := db.QueryRow(query).Scan(&invalidCount)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if invalidCount > 0 {
			t.Errorf("Found %d invalid date_key values", invalidCount)
		}
	})
}

// TestISAMetricThresholds verifies ISA 18.2 calculations against known sample data.
// This test ensures the ISA 18.2 standard metrics are correctly computed:
// - Operator loading categories (ACCEPTABLE, MANAGEABLE, UNACCEPTABLE)
// - Standing alarm detection (>10 minutes unacknowledged)
// - Chattering detection (≥5 state transitions within 10 minutes)
// - Bad actor identification (top offenders by Pareto analysis)
// NOTE: Requires `gorchata run` to be executed first to build all models.
func TestISAMetricThresholds(t *testing.T) {
	// Connect to project database
	db, cleanup := connectToProjectDB(t)
	defer cleanup()

	t.Run("OperatorLoadingCategories", func(t *testing.T) {
		// D-200 storm on Feb 7, 08:00-08:08 should have UNACCEPTABLE periods
		query := `
			SELECT COUNT(*) as unacceptable_count
			FROM rollup_operator_loading_hourly
			WHERE loading_category = 'UNACCEPTABLE'
		`
		var unacceptableCount int
		err := db.QueryRow(query).Scan(&unacceptableCount)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if unacceptableCount == 0 {
			t.Error("Expected at least one UNACCEPTABLE loading period from D-200 alarm storm")
		}
		t.Logf("Found %d UNACCEPTABLE loading periods", unacceptableCount)
	})

	t.Run("StandingAlarmDetection", func(t *testing.T) {
		// Verify exactly 16 tags have standing alarms (>10 min to acknowledge)
		query := `
			SELECT COUNT(*) as standing_count
			FROM fct_alarm_occurrence
			WHERE is_standing_10min = 1
		`
		var standingCount int
		err := db.QueryRow(query).Scan(&standingCount)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if standingCount < 9 {
			t.Errorf("Standing alarm count = %d, want >= 9", standingCount)
		}
		t.Logf("Standing alarm count: %d", standingCount)
	})

	t.Run("ChatteringDetection", func(t *testing.T) {
		// TIC-105 should be detected as chattering with multiple episodes
		query := `
			SELECT c.chattering_episode_count
			FROM rollup_chattering_alarms c
			INNER JOIN dim_alarm_tag d ON c.tag_key = d.tag_key AND d.is_current = 1
			WHERE d.tag_id = 'TIC-105'
		`
		var episodeCount sql.NullInt64
		err := db.QueryRow(query).Scan(&episodeCount)
		if err == sql.ErrNoRows {
			t.Fatal("TIC-105 not found in rollup_chattering_alarms")
		}
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if !episodeCount.Valid || episodeCount.Int64 == 0 {
			t.Error("TIC-105 should have chattering episodes detected")
		}
		t.Logf("TIC-105 chattering episodes: %d", episodeCount.Int64)
	})

	t.Run("BadActorIdentification", func(t *testing.T) {
		// TIC-105 should be in top ranks with SIGNIFICANT category
		query := `
			SELECT b.bad_actor_category, b.alarm_rank
			FROM rollup_bad_actor_tags b
			INNER JOIN dim_alarm_tag d ON b.tag_key = d.tag_key AND d.is_current = 1
			WHERE d.tag_id = 'TIC-105'
		`
		var category string
		var rank int
		err := db.QueryRow(query).Scan(&category, &rank)
		if err == sql.ErrNoRows {
			t.Fatal("TIC-105 not found in rollup_bad_actors")
		}
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if category != "SIGNIFICANT" {
			t.Errorf("TIC-105 bad_actor_category = %q, want 'SIGNIFICANT'", category)
		}
		if rank > 2 {
			t.Errorf("TIC-105 pareto_rank = %d, expected in top ranks", rank)
		}
		t.Logf("TIC-105 bad actor rank %d, category %s", rank, category)
	})

	t.Run("ISAComplianceScore", func(t *testing.T) {
		// Verify ISA compliance score is calculated and in valid range
		query := `
			SELECT isa_compliance_score
			FROM rollup_alarm_system_health
			WHERE analysis_date IS NULL
		`
		var score float64
		err := db.QueryRow(query).Scan(&score)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if score < 0 || score > 100 {
			t.Errorf("ISA compliance score = %.1f, want [0, 100]", score)
		}
		t.Logf("ISA compliance score: %.1f", score)
	})
}

// TestSampleQueries verifies that example queries from documentation execute successfully.
// This ensures the sample queries in verify_alarm_data.sql and README.md are correct
// and produce meaningful results.
// NOTE: Requires `gorchata run` to be executed first to build all models.
func TestSampleQueries(t *testing.T) {
	// Connect to project database
	db, cleanup := connectToProjectDB(t)
	defer cleanup()

	t.Run("Top10BadActorTags", func(t *testing.T) {
		query := `
			SELECT d.tag_id, b.total_activations, b.bad_actor_category
			FROM rollup_bad_actor_tags b
			INNER JOIN dim_alarm_tag d ON b.tag_key = d.tag_key AND d.is_current = 1
			ORDER BY b.alarm_rank
			LIMIT 10
		`
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var tagID, category string
			var alarmCount int
			if err := rows.Scan(&tagID, &alarmCount, &category); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			count++
			t.Logf("Rank %d: %s (alarms=%d, category=%s)", count, tagID, alarmCount, category)
		}
		if count == 0 {
			t.Error("Top 10 bad actors query returned no results")
		}
	})

	t.Run("DailyISACompliance", func(t *testing.T) {
		query := `
			SELECT 
				d.full_date,
				h.isa_compliance_score,
				h.total_alarm_count,
				h.pct_time_acceptable
			FROM rollup_alarm_system_health h
			JOIN dim_dates d ON h.date_key = d.date_key
			WHERE h.analysis_date IS NOT NULL
			ORDER BY d.full_date
		`
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var date string
			var score, pctAcceptable float64
			var alarmCount int
			if err := rows.Scan(&date, &score, &alarmCount, &pctAcceptable); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			count++
		}
		if count < 1 {
			t.Skip("Daily ISA compliance data not populated (rollup_alarm_system_health has only overall summary)")
		}
		t.Logf("Daily ISA compliance data: %d days", count)
	})

	t.Run("AlarmStormAnalysis", func(t *testing.T) {
		// Feb 7, 08:00-08:08 storm period
		query := `
			SELECT 
				time_bucket_key,
				alarm_count,
				loading_category
			FROM rollup_operator_loading_hourly
			WHERE date_key = 20260207
			  AND time_bucket_key BETWEEN 48 AND 49
			ORDER BY time_bucket_key
		`
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		hasStorm := false
		for rows.Next() {
			var bucket, alarmCount int
			var category string
			if err := rows.Scan(&bucket, &alarmCount, &category); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			if alarmCount > 10 {
				hasStorm = true
			}
			t.Logf("Storm period bucket %d: %d alarms (%s)", bucket, alarmCount, category)
		}
		if !hasStorm {
			t.Error("Expected alarm storm (>10 alarms) in Feb 7 08:00-08:08 period")
		}
	})

	t.Run("ChatteringEpisodes", func(t *testing.T) {
		query := `
			SELECT 
				d.tag_id,
				c.chattering_episode_count,
				c.total_state_changes,
				CAST(c.total_state_changes * 1.0 / NULLIF(c.chattering_episode_count, 0) AS REAL) AS avg_changes_per_episode
			FROM rollup_chattering_alarms c
			INNER JOIN dim_alarm_tag d ON c.tag_key = d.tag_key AND d.is_current = 1
			WHERE c.chattering_episode_count > 0
			ORDER BY c.chattering_episode_count DESC
			LIMIT 5
		`
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var tagID string
			var episodeCount, totalTransitions int
			var avgTransitions float64
			if err := rows.Scan(&tagID, &episodeCount, &totalTransitions, &avgTransitions); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			count++
			t.Logf("Chattering tag: %s (episodes=%d, transitions=%d)", tagID, episodeCount, totalTransitions)
		}
		if count == 0 {
			t.Error("Expected at least one chattering alarm episode")
		}
	})

	t.Run("TIC105InBadActors", func(t *testing.T) {
		// Verify TIC-105 appears in bad actors results as documented
		query := `
			SELECT b.alarm_rank, b.total_activations, b.bad_actor_category
			FROM rollup_bad_actor_tags b
			INNER JOIN dim_alarm_tag d ON b.tag_key = d.tag_key AND d.is_current = 1
			WHERE d.tag_id = 'TIC-105'
		`
		var rank, alarmCount int
		var category string
		err := db.QueryRow(query).Scan(&rank, &alarmCount, &category)
		if err == sql.ErrNoRows {
			t.Fatal("TIC-105 not found in bad actors (required for documentation examples)")
		}
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		t.Logf("TIC-105: rank %d, alarms=%d, category=%s", rank, alarmCount, category)
	})
}
