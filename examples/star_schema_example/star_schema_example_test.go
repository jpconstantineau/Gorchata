package star_schema_example_test

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

// setupTestDB creates a temporary database with the raw_sales model executed.
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

	// Read model file
	modelPath := filepath.Join("models", "sources", "raw_sales.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		db.Close()
		t.Fatalf("Failed to read raw_sales.sql: %v", err)
	}

	// Extract SQL (remove Jinja-like config directive for raw execution test)
	contentStr := string(content)
	sqlContent := strings.ReplaceAll(contentStr, "{{ config(materialized='table') }}", "")
	sqlContent = strings.TrimSpace(sqlContent)

	// Wrap in CREATE TABLE for testing
	createTableSQL := "CREATE TABLE raw_sales AS " + sqlContent

	// Execute SQL
	_, err = db.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		db.Close()
		t.Fatalf("Failed to execute raw_sales model: %v", err)
	}

	// Return DB and cleanup function
	cleanup := func() {
		db.Close()
	}

	return db, cleanup
}

// TestStarSchemaProjectConfig tests that the star_schema_example project config can be loaded
func TestStarSchemaProjectConfig(t *testing.T) {
	projectPath := filepath.Join("gorchata_project.yml")

	cfg, err := config.LoadProject(projectPath)
	if err != nil {
		t.Fatalf("LoadProject() error = %v, want nil", err)
	}

	// Verify project name
	if cfg.Name != "star_schema_example" {
		t.Errorf("Name = %q, want %q", cfg.Name, "star_schema_example")
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
	} else if startDate != "2024-01-01" {
		t.Errorf("Vars['start_date'] = %v, want %q", startDate, "2024-01-01")
	}

	// Verify end_date var
	if endDate, ok := cfg.Vars["end_date"]; !ok {
		t.Error("Vars['end_date'] not found")
	} else if endDate != "2024-12-31" {
		t.Errorf("Vars['end_date'] = %v, want %q", endDate, "2024-12-31")
	}
}

// TestStarSchemaProfilesConfig tests that the star_schema_example profiles config can be loaded
func TestStarSchemaProfilesConfig(t *testing.T) {
	profilesPath := filepath.Join("profiles.yml")

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

	// Database path should contain the example path
	// Note: env var expansion happens in LoadProfiles, so we'll get the actual value
	if devOutput.Database == "" {
		t.Error("devOutput.Database is empty")
	}
}

// TestStarSchemaProfilesConfigWithEnvVar tests env var expansion
func TestStarSchemaProfilesConfigWithEnvVar(t *testing.T) {
	// Set a custom env var
	customPath := "./custom/path/test.db"
	os.Setenv("STAR_SCHEMA_DB", customPath)
	defer os.Unsetenv("STAR_SCHEMA_DB")

	profilesPath := filepath.Join("profiles.yml")

	cfg, err := config.LoadProfiles(profilesPath)
	if err != nil {
		t.Fatalf("LoadProfiles() error = %v, want nil", err)
	}

	devOutput, err := cfg.GetOutput("dev")
	if err != nil {
		t.Fatalf("GetOutput('dev') error = %v, want nil", err)
	}

	// Should use the env var value
	if devOutput.Database != customPath {
		t.Errorf("devOutput.Database = %q, want %q", devOutput.Database, customPath)
	}
}

// TestStarSchemaProfilesConfigDefaultEnvVar tests default env var value
func TestStarSchemaProfilesConfigDefaultEnvVar(t *testing.T) {
	// Ensure env var is NOT set
	os.Unsetenv("STAR_SCHEMA_DB")

	profilesPath := filepath.Join("profiles.yml")

	cfg, err := config.LoadProfiles(profilesPath)
	if err != nil {
		t.Fatalf("LoadProfiles() error = %v, want nil", err)
	}

	devOutput, err := cfg.GetOutput("dev")
	if err != nil {
		t.Fatalf("GetOutput('dev') error = %v, want nil", err)
	}

	// Should use the default value
	expectedDefault := "./examples/star_schema_example/star_schema.db"
	if devOutput.Database != expectedDefault {
		t.Errorf("devOutput.Database = %q, want %q", devOutput.Database, expectedDefault)
	}
}

// TestStarSchemaDirectoryStructure verifies the required directory structure exists
func TestStarSchemaDirectoryStructure(t *testing.T) {
	requiredDirs := []string{
		"models",
		"models/sources",
		"models/dimensions",
		"models/facts",
		"models/rollups",
	}

	for _, dir := range requiredDirs {
		path := filepath.Join(dir)
		info, err := os.Stat(path)
		if err != nil {
			t.Errorf("Directory %q does not exist: %v", dir, err)
			continue
		}
		if !info.IsDir() {
			t.Errorf("Path %q exists but is not a directory", dir)
		}
	}
}

// TestStarSchemaREADMEExists verifies README.md exists
func TestStarSchemaREADMEExists(t *testing.T) {
	readmePath := filepath.Join("README.md")
	info, err := os.Stat(readmePath)
	if err != nil {
		t.Fatalf("README.md does not exist: %v", err)
	}
	if info.IsDir() {
		t.Fatal("README.md is a directory, expected a file")
	}
	if info.Size() == 0 {
		t.Error("README.md is empty")
	}
}

// TestRawSalesModelExists verifies raw_sales.sql model file exists
func TestRawSalesModelExists(t *testing.T) {
	modelPath := filepath.Join("models", "sources", "raw_sales.sql")
	info, err := os.Stat(modelPath)
	if err != nil {
		t.Fatalf("raw_sales.sql does not exist: %v", err)
	}
	if info.IsDir() {
		t.Fatal("raw_sales.sql is a directory, expected a file")
	}
	if info.Size() == 0 {
		t.Error("raw_sales.sql is empty")
	}
}

// TestRawSalesModelContent verifies raw_sales.sql has required content
func TestRawSalesModelContent(t *testing.T) {
	modelPath := filepath.Join("models", "sources", "raw_sales.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read raw_sales.sql: %v", err)
	}

	contentStr := string(content)

	// Check for config directive
	if !strings.Contains(contentStr, "{{ config(materialized='table') }}") {
		t.Error("raw_sales.sql missing config directive with materialized='table'")
	}

	// Check for expected column names in the model
	expectedColumns := []string{
		"sale_id", "sale_date", "sale_amount", "quantity",
		"customer_id", "customer_name", "customer_email", "customer_city", "customer_state",
		"product_id", "product_name", "product_category", "product_price",
	}

	for _, col := range expectedColumns {
		if !strings.Contains(contentStr, col) {
			t.Errorf("raw_sales.sql missing expected column: %s", col)
		}
	}

	// Check for VALUES clause (inline data)
	if !strings.Contains(contentStr, "VALUES") {
		t.Error("raw_sales.sql missing VALUES clause for inline data")
	}

	// Check for SELECT FROM pattern
	if !strings.Contains(contentStr, "SELECT") && !strings.Contains(contentStr, "FROM") {
		t.Error("raw_sales.sql missing SELECT FROM pattern")
	}
}

// TestRawSalesModelCompiles verifies raw_sales.sql compiles successfully
func TestRawSalesModelCompiles(t *testing.T) {
	modelPath := filepath.Join("models", "sources", "raw_sales.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read raw_sales.sql: %v", err)
	}

	contentStr := string(content)

	// Basic validation that SQL syntax keywords are present
	requiredKeywords := []string{"SELECT", "FROM", "VALUES"}
	for _, keyword := range requiredKeywords {
		if !strings.Contains(strings.ToUpper(contentStr), keyword) {
			t.Errorf("raw_sales.sql missing SQL keyword: %s", keyword)
		}
	}
}

// TestRawSalesModelExecution tests that raw_sales model can be executed
func TestRawSalesModelExecution(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Verify table was created
	var tableName string
	err := db.QueryRowContext(context.Background(),
		"SELECT name FROM sqlite_master WHERE type='table' AND name='raw_sales'").Scan(&tableName)
	if err != nil {
		t.Fatalf("Table raw_sales not found: %v", err)
	}
	if tableName != "raw_sales" {
		t.Errorf("Expected table 'raw_sales', got '%s'", tableName)
	}
}

// TestRawSalesColumns verifies raw_sales table has expected columns
func TestRawSalesColumns(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Query table info
	rows, err := db.QueryContext(context.Background(), "PRAGMA table_info(raw_sales)")
	if err != nil {
		t.Fatalf("Failed to get table info: %v", err)
	}
	defer rows.Close()

	// Collect column names
	var columns []string
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull, pk int
		var dfltValue sql.NullString
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		columns = append(columns, name)
	}

	// Expected columns
	expectedColumns := []string{
		"sale_id", "sale_date", "sale_amount", "quantity",
		"customer_id", "customer_name", "customer_email", "customer_city", "customer_state",
		"product_id", "product_name", "product_category", "product_price",
	}

	// Verify all expected columns exist
	for _, expected := range expectedColumns {
		found := false
		for _, col := range columns {
			if col == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected column %s not found in raw_sales table. Found columns: %v", expected, columns)
		}
	}
}

// TestRawSalesDataCount verifies raw_sales has 20-30 records
func TestRawSalesDataCount(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Count records
	var count int
	err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM raw_sales").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}

	if count < 20 || count > 30 {
		t.Errorf("Expected 20-30 records, got %d", count)
	}
}

// TestRawSalesSCDType2Data verifies data includes customer attribute changes over time
func TestRawSalesSCDType2Data(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Check for customer 1001 with different cities over time
	type customerRecord struct {
		saleDate     string
		city         string
		state        string
		email        string
		customerName string
	}

	rows, err := db.QueryContext(context.Background(),
		"SELECT sale_date, customer_city, customer_state, customer_email, customer_name FROM raw_sales WHERE customer_id = 1001 ORDER BY sale_date")
	if err != nil {
		t.Fatalf("Failed to query customer 1001: %v", err)
	}
	defer rows.Close()

	var records []customerRecord
	for rows.Next() {
		var r customerRecord
		if err := rows.Scan(&r.saleDate, &r.city, &r.state, &r.email, &r.customerName); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		records = append(records, r)
	}

	if len(records) < 3 {
		t.Errorf("Expected at least 3 records for customer 1001 (showing attribute changes over time), got %d", len(records))
	}

	// Check that there are different cities for the same customer (showing move)
	cities := make(map[string]bool)
	for _, r := range records {
		cities[r.city] = true
	}

	if len(cities) < 2 {
		t.Error("Expected customer 1001 to have at least 2 different cities (demonstrating SCD Type 2), but found only one city")
	}
}

// TestRawSalesDataDiversity verifies data diversity (multiple customers and products)
func TestRawSalesDataDiversity(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Count unique customers
	var customerCount int
	err := db.QueryRowContext(context.Background(), "SELECT COUNT(DISTINCT customer_id) FROM raw_sales").Scan(&customerCount)
	if err != nil {
		t.Fatalf("Failed to count unique customers: %v", err)
	}

	if customerCount < 5 || customerCount > 8 {
		t.Errorf("Expected 5-8 unique customers, got %d", customerCount)
	}

	// Count unique products
	var productCount int
	err = db.QueryRowContext(context.Background(), "SELECT COUNT(DISTINCT product_id) FROM raw_sales").Scan(&productCount)
	if err != nil {
		t.Fatalf("Failed to count unique products: %v", err)
	}

	if productCount < 8 || productCount > 12 {
		t.Errorf("Expected 8-12 unique products, got %d", productCount)
	}

	// Count unique categories
	var categoryCount int
	err = db.QueryRowContext(context.Background(), "SELECT COUNT(DISTINCT product_category) FROM raw_sales").Scan(&categoryCount)
	if err != nil {
		t.Fatalf("Failed to count unique categories: %v", err)
	}

	if categoryCount < 3 || categoryCount > 4 {
		t.Errorf("Expected 3-4 unique product categories, got %d", categoryCount)
	}
}
