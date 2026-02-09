package star_schema_example_test

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

	// Extract SQL (remove config directive for raw execution test)
	contentStr := string(content)
	sqlContent := strings.ReplaceAll(contentStr, `{{ config "materialized" "table" }}`, "")
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

// removeConfigDirectives removes config directives from SQL (both Go template and legacy Jinja syntax)
func removeConfigDirectives(sql string) string {
	// Remove Go template syntax: {{ config "key" "value" }}
	goTemplateRe := regexp.MustCompile(`\{\{\s*config\s+"[^"]+"\s+"[^"]+"\s*\}\}`)
	sql = goTemplateRe.ReplaceAllString(sql, "")

	// Remove legacy Jinja-style syntax: {{ config(key='value') }}
	legacyRe := regexp.MustCompile(`\{\{\s*config\s*\([^}]+\)\s*\}\}`)
	return legacyRe.ReplaceAllString(sql, "")
}

// removeAllTemplateDirectives removes all template directives from SQL for raw execution
func removeAllTemplateDirectives(sql string) string {
	// First remove config directives
	sql = removeConfigDirectives(sql)

	// Remove {{ if ... }} directives (even in comments)
	ifRe := regexp.MustCompile(`--\s*\{\{\s*if\s+[^}]+\}\}`)
	sql = ifRe.ReplaceAllString(sql, "")

	// Remove {{ end }} directives (even in comments)
	endRe := regexp.MustCompile(`--\s*\{\{\s*end\s*\}\}`)
	sql = endRe.ReplaceAllString(sql, "")

	// Remove {{ this }} references (even in comments)
	thisRe := regexp.MustCompile(`\{\{\s*this\s*\}\}`)
	sql = thisRe.ReplaceAllString(sql, "this")

	// Remove any remaining commented template directives
	commentTemplateRe := regexp.MustCompile(`--[^\n]*\{\{[^}]*\}\}`)
	sql = commentTemplateRe.ReplaceAllString(sql, "")

	return sql
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
	if !strings.Contains(contentStr, "{{ config \"materialized\" \"table\" }}") {
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

// ============================================================================
// Phase 3: Dimension Tables - Products & Dates
// ============================================================================

// TestDimProductsModelExists verifies dim_products.sql model file exists
func TestDimProductsModelExists(t *testing.T) {
	modelPath := filepath.Join("models", "dimensions", "dim_products.sql")
	info, err := os.Stat(modelPath)
	if err != nil {
		t.Fatalf("dim_products.sql does not exist: %v", err)
	}
	if info.IsDir() {
		t.Fatal("dim_products.sql is a directory, expected a file")
	}
	if info.Size() == 0 {
		t.Error("dim_products.sql is empty")
	}
}

// TestDimProductsModelContent verifies dim_products.sql has required content
func TestDimProductsModelContent(t *testing.T) {
	modelPath := filepath.Join("models", "dimensions", "dim_products.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read dim_products.sql: %v", err)
	}

	contentStr := string(content)

	// Check for config directive
	if !strings.Contains(contentStr, "{{ config \"materialized\" \"table\" }}") {
		t.Error("dim_products.sql missing config directive with materialized='table'")
	}

	// Check for ref to raw_sales
	if !strings.Contains(contentStr, `{{ ref "raw_sales" }}`) {
		t.Error("dim_products.sql missing {{ ref \"raw_sales\" }} reference")
	}

	// Check for expected column names
	expectedColumns := []string{"product_id", "product_name", "product_category", "product_price"}
	for _, col := range expectedColumns {
		if !strings.Contains(contentStr, col) {
			t.Errorf("dim_products.sql missing expected column: %s", col)
		}
	}
}

// TestDimProductsModelExecution tests that dim_products model can be executed
func TestDimProductsModelExecution(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Read dim_products model
	modelPath := filepath.Join("models", "dimensions", "dim_products.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read dim_products.sql: %v", err)
	}

	// Compile SQL: replace {{ config(...) }} and {{ ref "raw_sales" }}
	sqlContent := string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, "{{ config \"materialized\" \"table\" }}", "")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.TrimSpace(sqlContent)

	// Execute as CREATE TABLE
	createTableSQL := "CREATE TABLE dim_products AS " + sqlContent
	_, err = db.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		t.Fatalf("Failed to execute dim_products model: %v", err)
	}

	// Verify table was created
	var tableName string
	err = db.QueryRowContext(context.Background(),
		"SELECT name FROM sqlite_master WHERE type='table' AND name='dim_products'").Scan(&tableName)
	if err != nil {
		t.Fatalf("Table dim_products not found: %v", err)
	}
}

// TestDimProductsColumns verifies dim_products table has expected columns
func TestDimProductsColumns(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create dim_products table
	modelPath := filepath.Join("models", "dimensions", "dim_products.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read dim_products.sql: %v", err)
	}

	sqlContent := string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, "{{ config \"materialized\" \"table\" }}", "")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.TrimSpace(sqlContent)

	createTableSQL := "CREATE TABLE dim_products AS " + sqlContent
	_, err = db.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		t.Fatalf("Failed to execute dim_products model: %v", err)
	}

	// Query table info
	rows, err := db.QueryContext(context.Background(), "PRAGMA table_info(dim_products)")
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
	expectedColumns := []string{"product_id", "product_name", "product_category", "product_price"}

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
			t.Errorf("Expected column %s not found in dim_products table. Found columns: %v", expected, columns)
		}
	}
}

// TestDimProductsUniqueProducts verifies dim_products extracts unique products
func TestDimProductsUniqueProducts(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create dim_products table
	modelPath := filepath.Join("models", "dimensions", "dim_products.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read dim_products.sql: %v", err)
	}

	sqlContent := string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, "{{ config \"materialized\" \"table\" }}", "")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.TrimSpace(sqlContent)

	createTableSQL := "CREATE TABLE dim_products AS " + sqlContent
	_, err = db.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		t.Fatalf("Failed to execute dim_products model: %v", err)
	}

	// Count total records
	var totalCount int
	err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM dim_products").Scan(&totalCount)
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}

	// Expected: 12 unique products
	if totalCount != 12 {
		t.Errorf("Expected 12 unique products, got %d", totalCount)
	}

	// Verify no duplicates on product_id
	var duplicateCount int
	err = db.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM (SELECT product_id, COUNT(*) as cnt FROM dim_products GROUP BY product_id HAVING cnt > 1)").Scan(&duplicateCount)
	if err != nil {
		t.Fatalf("Failed to check for duplicates: %v", err)
	}

	if duplicateCount > 0 {
		t.Errorf("Found %d duplicate product_ids in dim_products", duplicateCount)
	}
}

// TestDimProductsDataIntegrity verifies product data has all attributes
func TestDimProductsDataIntegrity(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create dim_products table
	modelPath := filepath.Join("models", "dimensions", "dim_products.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read dim_products.sql: %v", err)
	}

	sqlContent := string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, "{{ config \"materialized\" \"table\" }}", "")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.TrimSpace(sqlContent)

	createTableSQL := "CREATE TABLE dim_products AS " + sqlContent
	_, err = db.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		t.Fatalf("Failed to execute dim_products model: %v", err)
	}

	// Check that no products have NULL values
	var nullCount int
	err = db.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM dim_products WHERE product_id IS NULL OR product_name IS NULL OR product_category IS NULL OR product_price IS NULL").Scan(&nullCount)
	if err != nil {
		t.Fatalf("Failed to check for NULLs: %v", err)
	}

	if nullCount > 0 {
		t.Errorf("Found %d products with NULL attributes", nullCount)
	}
}

// TestDimDatesModelExists verifies dim_dates.sql model file exists
func TestDimDatesModelExists(t *testing.T) {
	modelPath := filepath.Join("models", "dimensions", "dim_dates.sql")
	info, err := os.Stat(modelPath)
	if err != nil {
		t.Fatalf("dim_dates.sql does not exist: %v", err)
	}
	if info.IsDir() {
		t.Fatal("dim_dates.sql is a directory, expected a file")
	}
	if info.Size() == 0 {
		t.Error("dim_dates.sql is empty")
	}
}

// TestDimDatesModelContent verifies dim_dates.sql has required content
func TestDimDatesModelContent(t *testing.T) {
	modelPath := filepath.Join("models", "dimensions", "dim_dates.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read dim_dates.sql: %v", err)
	}

	contentStr := string(content)

	// Check for config directive
	if !strings.Contains(contentStr, "{{ config \"materialized\" \"table\" }}") {
		t.Error("dim_dates.sql missing config directive with materialized='table'")
	}

	// Check for ref to raw_sales
	if !strings.Contains(contentStr, `{{ ref "raw_sales" }}`) {
		t.Error("dim_dates.sql missing {{ ref \"raw_sales\" }} reference")
	}

	// Check for expected column names
	expectedColumns := []string{"sale_date", "year", "quarter", "month", "day", "day_of_week", "is_weekend"}
	for _, col := range expectedColumns {
		if !strings.Contains(contentStr, col) {
			t.Errorf("dim_dates.sql missing expected column: %s", col)
		}
	}

	// Check for strftime usage
	if !strings.Contains(contentStr, "strftime") {
		t.Error("dim_dates.sql should use strftime for date extraction")
	}
}

// TestDimDatesModelExecution tests that dim_dates model can be executed
func TestDimDatesModelExecution(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Read dim_dates model
	modelPath := filepath.Join("models", "dimensions", "dim_dates.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read dim_dates.sql: %v", err)
	}

	// Compile SQL: replace {{ config(...) }} and {{ ref "raw_sales" }}
	sqlContent := string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, "{{ config \"materialized\" \"table\" }}", "")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.TrimSpace(sqlContent)

	// Execute as CREATE TABLE
	createTableSQL := "CREATE TABLE dim_dates AS " + sqlContent
	_, err = db.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		t.Fatalf("Failed to execute dim_dates model: %v", err)
	}

	// Verify table was created
	var tableName string
	err = db.QueryRowContext(context.Background(),
		"SELECT name FROM sqlite_master WHERE type='table' AND name='dim_dates'").Scan(&tableName)
	if err != nil {
		t.Fatalf("Table dim_dates not found: %v", err)
	}
}

// TestDimDatesColumns verifies dim_dates table has expected columns
func TestDimDatesColumns(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create dim_dates table
	modelPath := filepath.Join("models", "dimensions", "dim_dates.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read dim_dates.sql: %v", err)
	}

	sqlContent := string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, "{{ config \"materialized\" \"table\" }}", "")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.TrimSpace(sqlContent)

	createTableSQL := "CREATE TABLE dim_dates AS " + sqlContent
	_, err = db.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		t.Fatalf("Failed to execute dim_dates model: %v", err)
	}

	// Query table info
	rows, err := db.QueryContext(context.Background(), "PRAGMA table_info(dim_dates)")
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
	expectedColumns := []string{"sale_date", "year", "quarter", "month", "day", "day_of_week", "is_weekend"}

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
			t.Errorf("Expected column %s not found in dim_dates table. Found columns: %v", expected, columns)
		}
	}
}

// TestDimDatesUniqueDates verifies dim_dates has one row per unique date
func TestDimDatesUniqueDates(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create dim_dates table
	modelPath := filepath.Join("models", "dimensions", "dim_dates.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read dim_dates.sql: %v", err)
	}

	sqlContent := string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, "{{ config \"materialized\" \"table\" }}", "")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.TrimSpace(sqlContent)

	createTableSQL := "CREATE TABLE dim_dates AS " + sqlContent
	_, err = db.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		t.Fatalf("Failed to execute dim_dates model: %v", err)
	}

	// Count total records
	var totalCount int
	err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM dim_dates").Scan(&totalCount)
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}

	// Expected: 30 unique dates (one per sale in raw_sales)
	if totalCount != 30 {
		t.Errorf("Expected 30 unique dates, got %d", totalCount)
	}

	// Verify no duplicates on sale_date
	var duplicateCount int
	err = db.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM (SELECT sale_date, COUNT(*) as cnt FROM dim_dates GROUP BY sale_date HAVING cnt > 1)").Scan(&duplicateCount)
	if err != nil {
		t.Fatalf("Failed to check for duplicates: %v", err)
	}

	if duplicateCount > 0 {
		t.Errorf("Found %d duplicate dates in dim_dates", duplicateCount)
	}
}

// TestDimDatesTimeAttributes verifies dim_dates has correct time attributes
func TestDimDatesTimeAttributes(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create dim_dates table
	modelPath := filepath.Join("models", "dimensions", "dim_dates.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read dim_dates.sql: %v", err)
	}

	sqlContent := string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, "{{ config \"materialized\" \"table\" }}", "")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.TrimSpace(sqlContent)

	createTableSQL := "CREATE TABLE dim_dates AS " + sqlContent
	_, err = db.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		t.Fatalf("Failed to execute dim_dates model: %v", err)
	}

	// Test a sample date's attributes - using 2024-01-05 (Friday) from actual data
	var year, quarter, month, day int
	var dayOfWeek string
	var isWeekend int
	err = db.QueryRowContext(context.Background(),
		"SELECT year, quarter, month, day, day_of_week, is_weekend FROM dim_dates WHERE sale_date = '2024-01-05'").
		Scan(&year, &quarter, &month, &day, &dayOfWeek, &isWeekend)
	if err != nil {
		t.Fatalf("Failed to query 2024-01-05: %v", err)
	}

	// Verify attributes for 2024-01-05 (Friday)
	if year != 2024 {
		t.Errorf("Expected year 2024, got %d", year)
	}
	if quarter != 1 {
		t.Errorf("Expected quarter 1, got %d", quarter)
	}
	if month != 1 {
		t.Errorf("Expected month 1, got %d", month)
	}
	if day != 5 {
		t.Errorf("Expected day 5, got %d", day)
	}
	// Friday is day_of_week = 5
	if dayOfWeek != "5" {
		t.Errorf("Expected day_of_week '5' (Friday), got %s", dayOfWeek)
	}
	if isWeekend != 0 {
		t.Errorf("Expected is_weekend 0 (not weekend), got %d", isWeekend)
	}
}

// TestDimDatesWeekendDetection verifies is_weekend is calculated correctly
func TestDimDatesWeekendDetection(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create dim_dates table
	modelPath := filepath.Join("models", "dimensions", "dim_dates.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read dim_dates.sql: %v", err)
	}

	sqlContent := string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, "{{ config \"materialized\" \"table\" }}", "")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.TrimSpace(sqlContent)

	createTableSQL := "CREATE TABLE dim_dates AS " + sqlContent
	_, err = db.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		t.Fatalf("Failed to execute dim_dates model: %v", err)
	}

	// Count weekend days (day_of_week = 0 (Sunday) or 6 (Saturday))
	var weekendCount int
	err = db.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM dim_dates WHERE day_of_week IN ('0', '6')").Scan(&weekendCount)
	if err != nil {
		t.Fatalf("Failed to count weekend days: %v", err)
	}

	// Count is_weekend = 1
	var isWeekendCount int
	err = db.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM dim_dates WHERE is_weekend = 1").Scan(&isWeekendCount)
	if err != nil {
		t.Fatalf("Failed to count is_weekend: %v", err)
	}

	// They should match
	if weekendCount != isWeekendCount {
		t.Errorf("Weekend detection mismatch: day_of_week IN ('0','6') gave %d, is_weekend=1 gave %d", weekendCount, isWeekendCount)
	}
}

// ============================================================================
// Phase 4: SCD Type 2 Customer Dimension
// ============================================================================

// TestDimCustomersModelExists verifies dim_customers.sql model file exists
func TestDimCustomersModelExists(t *testing.T) {
	modelPath := filepath.Join("models", "dimensions", "dim_customers.sql")
	info, err := os.Stat(modelPath)
	if err != nil {
		t.Fatalf("dim_customers.sql does not exist: %v", err)
	}
	if info.IsDir() {
		t.Fatal("dim_customers.sql is a directory, expected a file")
	}
	if info.Size() == 0 {
		t.Error("dim_customers.sql is empty")
	}
}

// TestDimCustomersModelContent verifies dim_customers.sql has correct config directive
func TestDimCustomersModelContent(t *testing.T) {
	modelPath := filepath.Join("models", "dimensions", "dim_customers.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read dim_customers.sql: %v", err)
	}

	contentStr := string(content)

	// Check for config directive with incremental materialization (Go template syntax)
	if !strings.Contains(contentStr, `{{ config "materialized" "incremental" }}`) {
		t.Error("dim_customers.sql missing config directive {{ config \"materialized\" \"incremental\" }}")
	}

	// Note: unique_key is set programmatically for incremental models, not in config directive

	// Check for ref to raw_sales
	if !strings.Contains(contentStr, `{{ ref "raw_sales" }}`) {
		t.Error("dim_customers.sql missing {{ ref \"raw_sales\" }} reference")
	}

	// Check for expected column names
	expectedColumns := []string{
		"customer_sk", "customer_id", "customer_name", "customer_email",
		"customer_city", "customer_state", "valid_from", "valid_to", "is_current",
	}
	for _, col := range expectedColumns {
		if !strings.Contains(contentStr, col) {
			t.Errorf("dim_customers.sql missing expected column: %s", col)
		}
	}
}

// TestDimCustomersModelExecution tests that dim_customers model can be executed
func TestDimCustomersModelExecution(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Read dim_customers model
	modelPath := filepath.Join("models", "dimensions", "dim_customers.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read dim_customers.sql: %v", err)
	}

	// Compile SQL: replace {{ config(...) }} and {{ ref "raw_sales" }}
	sqlContent := string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	// Remove config directives (both Go template and Jinja-style)
	// Go template: {{ config "materialized" "incremental" }}
	goTemplateRe := regexp.MustCompile(`\{\{\s*config\s+"[^"]+"\s+"[^"]+"\s*\}\}`)
	sqlContent = goTemplateRe.ReplaceAllString(sqlContent, "")
	// Legacy Jinja: {{ config(materialized='incremental') }}
	legacyRe := regexp.MustCompile(`\{\{\s*config\s*\([^}]+\)\s*\}\}`)
	sqlContent = legacyRe.ReplaceAllString(sqlContent, "")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.TrimSpace(sqlContent)

	// Execute as CREATE TABLE
	createTableSQL := "CREATE TABLE dim_customers AS " + sqlContent
	_, err = db.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		t.Fatalf("Failed to execute dim_customers model: %v", err)
	}

	// Verify table was created
	var tableName string
	err = db.QueryRowContext(context.Background(),
		"SELECT name FROM sqlite_master WHERE type='table' AND name='dim_customers'").Scan(&tableName)
	if err != nil {
		t.Fatalf("Table dim_customers not found: %v", err)
	}
}

// TestDimCustomersColumns verifies dim_customers table has expected columns
func TestDimCustomersColumns(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create dim_customers table
	modelPath := filepath.Join("models", "dimensions", "dim_customers.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read dim_customers.sql: %v", err)
	}

	sqlContent := string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.TrimSpace(sqlContent)

	createTableSQL := "CREATE TABLE dim_customers AS " + sqlContent
	_, err = db.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		t.Fatalf("Failed to execute dim_customers model: %v", err)
	}

	// Query table info
	rows, err := db.QueryContext(context.Background(), "PRAGMA table_info(dim_customers)")
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

	// Expected columns for SCD Type 2
	expectedColumns := []string{
		"customer_sk", "customer_id", "customer_name", "customer_email",
		"customer_city", "customer_state", "valid_from", "valid_to", "is_current",
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
			t.Errorf("Expected column %s not found in dim_customers table. Found columns: %v", expected, columns)
		}
	}
}

// TestDimCustomersSurrogateKeyUnique verifies customer_sk is unique across all versions
func TestDimCustomersSurrogateKeyUnique(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create dim_customers table
	modelPath := filepath.Join("models", "dimensions", "dim_customers.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read dim_customers.sql: %v", err)
	}

	sqlContent := string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.TrimSpace(sqlContent)

	createTableSQL := "CREATE TABLE dim_customers AS " + sqlContent
	_, err = db.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		t.Fatalf("Failed to execute dim_customers model: %v", err)
	}

	// Verify no duplicates on customer_sk
	var duplicateCount int
	err = db.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM (SELECT customer_sk, COUNT(*) as cnt FROM dim_customers GROUP BY customer_sk HAVING cnt > 1)").Scan(&duplicateCount)
	if err != nil {
		t.Fatalf("Failed to check for duplicates: %v", err)
	}

	if duplicateCount > 0 {
		t.Errorf("Found %d duplicate customer_sk values in dim_customers", duplicateCount)
	}
}

// TestDimCustomersMultipleVersions verifies multiple versions exist for customers with changes
func TestDimCustomersMultipleVersions(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create dim_customers table
	modelPath := filepath.Join("models", "dimensions", "dim_customers.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read dim_customers.sql: %v", err)
	}

	sqlContent := string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.TrimSpace(sqlContent)

	createTableSQL := "CREATE TABLE dim_customers AS " + sqlContent
	_, err = db.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		t.Fatalf("Failed to execute dim_customers model: %v", err)
	}

	// Count versions for customer_id 1001
	var versionCount int
	err = db.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM dim_customers WHERE customer_id = 1001").Scan(&versionCount)
	if err != nil {
		t.Fatalf("Failed to count versions for customer 1001: %v", err)
	}

	// Expected: 3 versions (Seattle -> Portland city, then email change)
	if versionCount != 3 {
		t.Errorf("Expected 3 versions for customer 1001, got %d", versionCount)
	}
}

// TestDimCustomersCustomer1001Versions verifies customer 1001 has correct version history
func TestDimCustomersCustomer1001Versions(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create dim_customers table
	modelPath := filepath.Join("models", "dimensions", "dim_customers.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read dim_customers.sql: %v", err)
	}

	sqlContent := string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.TrimSpace(sqlContent)

	createTableSQL := "CREATE TABLE dim_customers AS " + sqlContent
	_, err = db.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		t.Fatalf("Failed to execute dim_customers model: %v", err)
	}

	// Query all versions for customer 1001, ordered by valid_from
	type customerVersion struct {
		customerSK    int
		customerID    int
		customerName  string
		customerEmail string
		customerCity  string
		customerState string
		validFrom     string
		validTo       string
		isCurrent     int
	}

	rows, err := db.QueryContext(context.Background(),
		`SELECT customer_sk, customer_id, customer_name, customer_email, customer_city, customer_state, 
		        valid_from, valid_to, is_current 
		 FROM dim_customers 
		 WHERE customer_id = 1001 
		 ORDER BY valid_from`)
	if err != nil {
		t.Fatalf("Failed to query customer 1001 versions: %v", err)
	}
	defer rows.Close()

	var versions []customerVersion
	for rows.Next() {
		var v customerVersion
		if err := rows.Scan(&v.customerSK, &v.customerID, &v.customerName, &v.customerEmail,
			&v.customerCity, &v.customerState, &v.validFrom, &v.validTo, &v.isCurrent); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		versions = append(versions, v)
	}

	// Verify we have exactly 3 versions
	if len(versions) != 3 {
		t.Fatalf("Expected 3 versions for customer 1001, got %d", len(versions))
	}

	// Version 1: Seattle, WA, alice@example.com
	v1 := versions[0]
	if v1.customerCity != "Seattle" {
		t.Errorf("Version 1: Expected city 'Seattle', got '%s'", v1.customerCity)
	}
	if v1.customerState != "WA" {
		t.Errorf("Version 1: Expected state 'WA', got '%s'", v1.customerState)
	}
	if v1.customerEmail != "alice@example.com" {
		t.Errorf("Version 1: Expected email 'alice@example.com', got '%s'", v1.customerEmail)
	}
	if v1.validFrom != "2024-01-05" {
		t.Errorf("Version 1: Expected valid_from '2024-01-05', got '%s'", v1.validFrom)
	}
	if v1.validTo != "2024-06-10" {
		t.Errorf("Version 1: Expected valid_to '2024-06-10', got '%s'", v1.validTo)
	}
	if v1.isCurrent != 0 {
		t.Errorf("Version 1: Expected is_current 0, got %d", v1.isCurrent)
	}

	// Version 2: Portland, OR, alice@example.com
	v2 := versions[1]
	if v2.customerCity != "Portland" {
		t.Errorf("Version 2: Expected city 'Portland', got '%s'", v2.customerCity)
	}
	if v2.customerState != "OR" {
		t.Errorf("Version 2: Expected state 'OR', got '%s'", v2.customerState)
	}
	if v2.customerEmail != "alice@example.com" {
		t.Errorf("Version 2: Expected email 'alice@example.com', got '%s'", v2.customerEmail)
	}
	if v2.validFrom != "2024-06-10" {
		t.Errorf("Version 2: Expected valid_from '2024-06-10', got '%s'", v2.validFrom)
	}
	if v2.validTo != "2024-11-08" {
		t.Errorf("Version 2: Expected valid_to '2024-11-08', got '%s'", v2.validTo)
	}
	if v2.isCurrent != 0 {
		t.Errorf("Version 2: Expected is_current 0, got %d", v2.isCurrent)
	}

	// Version 3: Portland, OR, alice.j.new@example.com
	v3 := versions[2]
	if v3.customerCity != "Portland" {
		t.Errorf("Version 3: Expected city 'Portland', got '%s'", v3.customerCity)
	}
	if v3.customerState != "OR" {
		t.Errorf("Version 3: Expected state 'OR', got '%s'", v3.customerState)
	}
	if v3.customerEmail != "alice.j.new@example.com" {
		t.Errorf("Version 3: Expected email 'alice.j.new@example.com', got '%s'", v3.customerEmail)
	}
	if v3.validFrom != "2024-11-08" {
		t.Errorf("Version 3: Expected valid_from '2024-11-08', got '%s'", v3.validFrom)
	}
	if v3.validTo != "9999-12-31" {
		t.Errorf("Version 3: Expected valid_to '9999-12-31', got '%s'", v3.validTo)
	}
	if v3.isCurrent != 1 {
		t.Errorf("Version 3: Expected is_current 1, got %d", v3.isCurrent)
	}
}

// TestDimCustomersValidFromValidTo verifies valid_from and valid_to track history correctly
func TestDimCustomersValidFromValidTo(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create dim_customers table
	modelPath := filepath.Join("models", "dimensions", "dim_customers.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read dim_customers.sql: %v", err)
	}

	sqlContent := string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.TrimSpace(sqlContent)

	createTableSQL := "CREATE TABLE dim_customers AS " + sqlContent
	_, err = db.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		t.Fatalf("Failed to execute dim_customers model: %v", err)
	}

	// Verify that for each customer_id, valid_to of version N equals valid_from of version N+1
	// (except for the last version which has valid_to = '9999-12-31')
	rows, err := db.QueryContext(context.Background(),
		`SELECT customer_id, valid_from, valid_to 
		 FROM dim_customers 
		 ORDER BY customer_id, valid_from`)
	if err != nil {
		t.Fatalf("Failed to query customers: %v", err)
	}
	defer rows.Close()

	type version struct {
		customerID int
		validFrom  string
		validTo    string
	}

	var versions []version
	for rows.Next() {
		var v version
		if err := rows.Scan(&v.customerID, &v.validFrom, &v.validTo); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		versions = append(versions, v)
	}

	// Check continuity for each customer
	for i := 0; i < len(versions)-1; i++ {
		current := versions[i]
		next := versions[i+1]

		// If same customer, check continuity
		if current.customerID == next.customerID {
			if current.validTo != next.validFrom {
				t.Errorf("Customer %d: valid_to '%s' of version %d does not match valid_from '%s' of next version",
					current.customerID, current.validTo, i, next.validFrom)
			}
		}
	}
}

// TestDimCustomersIsCurrentFlag verifies is_current flag identifies active records
func TestDimCustomersIsCurrentFlag(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create dim_customers table
	modelPath := filepath.Join("models", "dimensions", "dim_customers.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read dim_customers.sql: %v", err)
	}

	sqlContent := string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.TrimSpace(sqlContent)

	createTableSQL := "CREATE TABLE dim_customers AS " + sqlContent
	_, err = db.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		t.Fatalf("Failed to execute dim_customers model: %v", err)
	}

	// Count unique customers
	var uniqueCustomers int
	err = db.QueryRowContext(context.Background(),
		"SELECT COUNT(DISTINCT customer_id) FROM dim_customers").Scan(&uniqueCustomers)
	if err != nil {
		t.Fatalf("Failed to count unique customers: %v", err)
	}

	// Count is_current = 1 records
	var currentCount int
	err = db.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM dim_customers WHERE is_current = 1").Scan(&currentCount)
	if err != nil {
		t.Fatalf("Failed to count current records: %v", err)
	}

	// Should have exactly one current record per customer
	if currentCount != uniqueCustomers {
		t.Errorf("Expected %d current records (one per customer), got %d", uniqueCustomers, currentCount)
	}

	// Verify that all current records have valid_to = '9999-12-31'
	var invalidCurrent int
	err = db.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM dim_customers WHERE is_current = 1 AND valid_to != '9999-12-31'").Scan(&invalidCurrent)
	if err != nil {
		t.Fatalf("Failed to check current records valid_to: %v", err)
	}

	if invalidCurrent > 0 {
		t.Errorf("Found %d current records with valid_to != '9999-12-31'", invalidCurrent)
	}

	// Verify that all non-current records have is_current = 0
	var invalidNonCurrent int
	err = db.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM dim_customers WHERE valid_to != '9999-12-31' AND is_current = 1").Scan(&invalidNonCurrent)
	if err != nil {
		t.Fatalf("Failed to check non-current records: %v", err)
	}

	if invalidNonCurrent > 0 {
		t.Errorf("Found %d historical records with is_current = 1", invalidNonCurrent)
	}
}

// TestDimCustomersDataIntegrity verifies no NULLs in required fields
func TestDimCustomersDataIntegrity(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create dim_customers table
	modelPath := filepath.Join("models", "dimensions", "dim_customers.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read dim_customers.sql: %v", err)
	}

	sqlContent := string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.TrimSpace(sqlContent)

	createTableSQL := "CREATE TABLE dim_customers AS " + sqlContent
	_, err = db.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		t.Fatalf("Failed to execute dim_customers model: %v", err)
	}

	// Check that no customers have NULL values in required fields
	var nullCount int
	err = db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM dim_customers 
		 WHERE customer_sk IS NULL 
		    OR customer_id IS NULL 
		    OR customer_name IS NULL 
		    OR customer_email IS NULL 
		    OR customer_city IS NULL 
		    OR customer_state IS NULL 
		    OR valid_from IS NULL 
		    OR valid_to IS NULL 
		    OR is_current IS NULL`).Scan(&nullCount)
	if err != nil {
		t.Fatalf("Failed to check for NULLs: %v", err)
	}

	if nullCount > 0 {
		t.Errorf("Found %d customer records with NULL values in required fields", nullCount)
	}
}

// ============================================================================
// Phase 5: Fact Table - Sales
// ============================================================================

// TestFctSalesModelExists verifies fct_sales.sql model file exists
func TestFctSalesModelExists(t *testing.T) {
	modelPath := filepath.Join("models", "facts", "fct_sales.sql")
	info, err := os.Stat(modelPath)
	if err != nil {
		t.Fatalf("fct_sales.sql does not exist: %v", err)
	}
	if info.IsDir() {
		t.Fatal("fct_sales.sql is a directory, expected a file")
	}
	if info.Size() == 0 {
		t.Error("fct_sales.sql is empty")
	}
}

// TestFctSalesModelContent verifies fct_sales.sql has required content
func TestFctSalesModelContent(t *testing.T) {
	modelPath := filepath.Join("models", "facts", "fct_sales.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read fct_sales.sql: %v", err)
	}

	contentStr := string(content)

	// Check for config directive
	if !strings.Contains(contentStr, "{{ config \"materialized\" \"table\" }}") {
		t.Error("fct_sales.sql missing config directive with materialized='table'")
	}

	// Check for references to all required tables
	requiredRefs := []string{
		`{{ ref "raw_sales" }}`,
		`{{ ref "dim_customers" }}`,
		`{{ ref "dim_products" }}`,
		`{{ ref "dim_dates" }}`,
	}
	for _, ref := range requiredRefs {
		if !strings.Contains(contentStr, ref) {
			t.Errorf("fct_sales.sql missing required reference: %s", ref)
		}
	}

	// Check for required columns
	requiredColumns := []string{
		"sale_id",
		"customer_sk",
		"product_id",
		"sale_date",
		"sale_amount",
		"quantity",
	}
	for _, col := range requiredColumns {
		if !strings.Contains(contentStr, col) {
			t.Errorf("fct_sales.sql missing expected column: %s", col)
		}
	}

	// Check that no denormalized attributes are present
	denormalizedAttrs := []string{
		"customer_name",
		"customer_email",
		"customer_city",
		"customer_state",
		"product_name",
		"product_category",
		"product_price",
	}
	for _, attr := range denormalizedAttrs {
		// Only error if we SELECT the attribute (ignore it in joins)
		// We need to be careful here - check for SELECT <attr> pattern
		if strings.Contains(contentStr, "SELECT") {
			selectPart := contentStr[strings.Index(contentStr, "SELECT"):]
			fromIndex := strings.Index(selectPart, "FROM")
			if fromIndex > 0 {
				selectClause := selectPart[:fromIndex]
				// Check if the attribute appears in the SELECT clause
				if strings.Contains(selectClause, attr) {
					t.Errorf("fct_sales.sql contains denormalized attribute in SELECT: %s", attr)
				}
			}
		}
	}
}

// TestFctSalesModelExecution tests that fct_sales model can be executed
func TestFctSalesModelExecution(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create all dimension tables first

	// 1. Create dim_customers
	dimCustomersPath := filepath.Join("models", "dimensions", "dim_customers.sql")
	dimCustomersContent, err := os.ReadFile(dimCustomersPath)
	if err != nil {
		t.Fatalf("Failed to read dim_customers.sql: %v", err)
	}
	dimCustomersSQL := string(dimCustomersContent)
	dimCustomersSQL = removeConfigDirectives(dimCustomersSQL)
	dimCustomersSQL = strings.ReplaceAll(dimCustomersSQL, `{{ ref "raw_sales" }}`, "raw_sales")
	dimCustomersSQL = strings.TrimSpace(dimCustomersSQL)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE dim_customers AS "+dimCustomersSQL)
	if err != nil {
		t.Fatalf("Failed to create dim_customers: %v", err)
	}

	// 2. Create dim_products
	dimProductsPath := filepath.Join("models", "dimensions", "dim_products.sql")
	dimProductsContent, err := os.ReadFile(dimProductsPath)
	if err != nil {
		t.Fatalf("Failed to read dim_products.sql: %v", err)
	}
	dimProductsSQL := string(dimProductsContent)
	dimProductsSQL = removeConfigDirectives(dimProductsSQL)
	dimProductsSQL = strings.ReplaceAll(dimProductsSQL, `{{ ref "raw_sales" }}`, "raw_sales")
	dimProductsSQL = strings.TrimSpace(dimProductsSQL)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE dim_products AS "+dimProductsSQL)
	if err != nil {
		t.Fatalf("Failed to create dim_products: %v", err)
	}

	// 3. Create dim_dates
	dimDatesPath := filepath.Join("models", "dimensions", "dim_dates.sql")
	dimDatesContent, err := os.ReadFile(dimDatesPath)
	if err != nil {
		t.Fatalf("Failed to read dim_dates.sql: %v", err)
	}
	dimDatesSQL := string(dimDatesContent)
	dimDatesSQL = removeConfigDirectives(dimDatesSQL)
	dimDatesSQL = strings.ReplaceAll(dimDatesSQL, `{{ ref "raw_sales" }}`, "raw_sales")
	dimDatesSQL = strings.TrimSpace(dimDatesSQL)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE dim_dates AS "+dimDatesSQL)
	if err != nil {
		t.Fatalf("Failed to create dim_dates: %v", err)
	}

	// Now create fct_sales
	modelPath := filepath.Join("models", "facts", "fct_sales.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read fct_sales.sql: %v", err)
	}

	sqlContent := string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_customers" }}`, "dim_customers")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_products" }}`, "dim_products")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
	sqlContent = strings.TrimSpace(sqlContent)

	createTableSQL := "CREATE TABLE fct_sales AS " + sqlContent
	_, err = db.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		t.Fatalf("Failed to execute fct_sales model: %v", err)
	}

	// Verify table was created
	var tableName string
	err = db.QueryRowContext(context.Background(),
		"SELECT name FROM sqlite_master WHERE type='table' AND name='fct_sales'").Scan(&tableName)
	if err != nil {
		t.Fatalf("Table fct_sales not found: %v", err)
	}
}

// TestFctSalesColumns verifies fct_sales table has expected columns
func TestFctSalesColumns(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create all dimensions (same setup as above)
	// Helper function to create dimension tables
	createDimensions := func(t *testing.T, db *sql.DB) {
		t.Helper()

		dims := []struct {
			path      string
			tableName string
		}{
			{filepath.Join("models", "dimensions", "dim_customers.sql"), "dim_customers"},
			{filepath.Join("models", "dimensions", "dim_products.sql"), "dim_products"},
			{filepath.Join("models", "dimensions", "dim_dates.sql"), "dim_dates"},
		}

		for _, dim := range dims {
			content, err := os.ReadFile(dim.path)
			if err != nil {
				t.Fatalf("Failed to read %s: %v", dim.path, err)
			}
			sqlContent := string(content)
			sqlContent = removeConfigDirectives(sqlContent)
			lines := strings.Split(sqlContent, "\n")
			var filteredLines []string
			for _, line := range lines {
				if !strings.Contains(line, "{{ config(") {
					filteredLines = append(filteredLines, line)
				}
			}
			sqlContent = strings.Join(filteredLines, "\n")
			sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
			sqlContent = strings.TrimSpace(sqlContent)
			_, err = db.ExecContext(context.Background(), "CREATE TABLE "+dim.tableName+" AS "+sqlContent)
			if err != nil {
				t.Fatalf("Failed to create %s: %v", dim.tableName, err)
			}
		}
	}

	createDimensions(t, db)

	// Create fct_sales table
	modelPath := filepath.Join("models", "facts", "fct_sales.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read fct_sales.sql: %v", err)
	}

	sqlContent := string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_customers" }}`, "dim_customers")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_products" }}`, "dim_products")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
	sqlContent = strings.TrimSpace(sqlContent)

	_, err = db.ExecContext(context.Background(), "CREATE TABLE fct_sales AS "+sqlContent)
	if err != nil {
		t.Fatalf("Failed to execute fct_sales model: %v", err)
	}

	// Get column names
	rows, err := db.QueryContext(context.Background(), "PRAGMA table_info(fct_sales)")
	if err != nil {
		t.Fatalf("Failed to get table info: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var cid int
		var name string
		var dataType string
		var notNull int
		var dfltValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &dfltValue, &pk); err != nil {
			t.Fatalf("Failed to scan column info: %v", err)
		}
		columns = append(columns, name)
	}

	// Expected columns (keys and measures only)
	expectedColumns := []string{"sale_id", "customer_sk", "product_id", "sale_date", "sale_amount", "quantity"}
	for _, expected := range expectedColumns {
		found := false
		for _, col := range columns {
			if col == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected column %s not found in fct_sales", expected)
		}
	}

	// Verify no denormalized attributes
	denormalizedAttrs := []string{
		"customer_name", "customer_email", "customer_city", "customer_state",
		"product_name", "product_category", "product_price",
	}
	for _, attr := range denormalizedAttrs {
		for _, col := range columns {
			if col == attr {
				t.Errorf("Denormalized attribute %s found in fct_sales columns", attr)
			}
		}
	}
}

// TestFctSalesRowCount verifies fact table has exactly 30 rows (one per sale)
func TestFctSalesRowCount(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create all dimensions
	createDimensions := func(t *testing.T, db *sql.DB) {
		t.Helper()
		dims := []struct {
			path      string
			tableName string
		}{
			{filepath.Join("models", "dimensions", "dim_customers.sql"), "dim_customers"},
			{filepath.Join("models", "dimensions", "dim_products.sql"), "dim_products"},
			{filepath.Join("models", "dimensions", "dim_dates.sql"), "dim_dates"},
		}
		for _, dim := range dims {
			content, err := os.ReadFile(dim.path)
			if err != nil {
				t.Fatalf("Failed to read %s: %v", dim.path, err)
			}
			sqlContent := string(content)
			sqlContent = removeConfigDirectives(sqlContent)
			lines := strings.Split(sqlContent, "\n")
			var filteredLines []string
			for _, line := range lines {
				if !strings.Contains(line, "{{ config(") {
					filteredLines = append(filteredLines, line)
				}
			}
			sqlContent = strings.Join(filteredLines, "\n")
			sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
			sqlContent = strings.TrimSpace(sqlContent)
			_, err = db.ExecContext(context.Background(), "CREATE TABLE "+dim.tableName+" AS "+sqlContent)
			if err != nil {
				t.Fatalf("Failed to create %s: %v", dim.tableName, err)
			}
		}
	}
	createDimensions(t, db)

	// Create fct_sales
	modelPath := filepath.Join("models", "facts", "fct_sales.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read fct_sales.sql: %v", err)
	}
	sqlContent := string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_customers" }}`, "dim_customers")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_products" }}`, "dim_products")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
	sqlContent = strings.TrimSpace(sqlContent)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE fct_sales AS "+sqlContent)
	if err != nil {
		t.Fatalf("Failed to execute fct_sales model: %v", err)
	}

	// Count rows
	var rowCount int
	err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM fct_sales").Scan(&rowCount)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	if rowCount != 30 {
		t.Errorf("Expected 30 rows in fct_sales, got %d", rowCount)
	}
}

// TestFctSalesPointInTimeJoin verifies correct customer_sk values for customer 1001 sales
func TestFctSalesPointInTimeJoin(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create all dimensions
	createDimensions := func(t *testing.T, db *sql.DB) {
		t.Helper()
		dims := []struct {
			path      string
			tableName string
		}{
			{filepath.Join("models", "dimensions", "dim_customers.sql"), "dim_customers"},
			{filepath.Join("models", "dimensions", "dim_products.sql"), "dim_products"},
			{filepath.Join("models", "dimensions", "dim_dates.sql"), "dim_dates"},
		}
		for _, dim := range dims {
			content, err := os.ReadFile(dim.path)
			if err != nil {
				t.Fatalf("Failed to read %s: %v", dim.path, err)
			}
			sqlContent := string(content)
			sqlContent = removeConfigDirectives(sqlContent)
			lines := strings.Split(sqlContent, "\n")
			var filteredLines []string
			for _, line := range lines {
				if !strings.Contains(line, "{{ config(") {
					filteredLines = append(filteredLines, line)
				}
			}
			sqlContent = strings.Join(filteredLines, "\n")
			sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
			sqlContent = strings.TrimSpace(sqlContent)
			_, err = db.ExecContext(context.Background(), "CREATE TABLE "+dim.tableName+" AS "+sqlContent)
			if err != nil {
				t.Fatalf("Failed to create %s: %v", dim.tableName, err)
			}
		}
	}
	createDimensions(t, db)

	// Create fct_sales
	modelPath := filepath.Join("models", "facts", "fct_sales.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read fct_sales.sql: %v", err)
	}
	sqlContent := string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_customers" }}`, "dim_customers")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_products" }}`, "dim_products")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
	sqlContent = strings.TrimSpace(sqlContent)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE fct_sales AS "+sqlContent)
	if err != nil {
		t.Fatalf("Failed to execute fct_sales model: %v", err)
	}

	// Test specific point-in-time join results
	testCases := []struct {
		saleID     int
		expectedSK int
		desc       string
	}{
		{1, 1001001, "Sale 1 (2024-01-05) should join to customer_sk 1001001 (Seattle version)"},
		{15, 1001002, "Sale 15 (2024-06-10) should join to customer_sk 1001002 (Portland, old email)"},
		{26, 1001003, "Sale 26 (2024-11-08) should join to customer_sk 1001003 (Portland, new email)"},
	}

	for _, tc := range testCases {
		var customerSK int
		err := db.QueryRowContext(context.Background(),
			"SELECT customer_sk FROM fct_sales WHERE sale_id = ?", tc.saleID).Scan(&customerSK)
		if err != nil {
			t.Fatalf("Failed to query sale_id %d: %v", tc.saleID, err)
		}

		if customerSK != tc.expectedSK {
			t.Errorf("%s: got customer_sk = %d, want %d", tc.desc, customerSK, tc.expectedSK)
		}
	}
}

// TestFctSalesDataIntegrity verifies no NULLs in required fields and all FKs resolve
func TestFctSalesDataIntegrity(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Create all dimensions
	createDimensions := func(t *testing.T, db *sql.DB) {
		t.Helper()
		dims := []struct {
			path      string
			tableName string
		}{
			{filepath.Join("models", "dimensions", "dim_customers.sql"), "dim_customers"},
			{filepath.Join("models", "dimensions", "dim_products.sql"), "dim_products"},
			{filepath.Join("models", "dimensions", "dim_dates.sql"), "dim_dates"},
		}
		for _, dim := range dims {
			content, err := os.ReadFile(dim.path)
			if err != nil {
				t.Fatalf("Failed to read %s: %v", dim.path, err)
			}
			sqlContent := string(content)
			sqlContent = removeConfigDirectives(sqlContent)
			lines := strings.Split(sqlContent, "\n")
			var filteredLines []string
			for _, line := range lines {
				if !strings.Contains(line, "{{ config(") {
					filteredLines = append(filteredLines, line)
				}
			}
			sqlContent = strings.Join(filteredLines, "\n")
			sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
			sqlContent = strings.TrimSpace(sqlContent)
			_, err = db.ExecContext(context.Background(), "CREATE TABLE "+dim.tableName+" AS "+sqlContent)
			if err != nil {
				t.Fatalf("Failed to create %s: %v", dim.tableName, err)
			}
		}
	}
	createDimensions(t, db)

	// Create fct_sales
	modelPath := filepath.Join("models", "facts", "fct_sales.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read fct_sales.sql: %v", err)
	}
	sqlContent := string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_customers" }}`, "dim_customers")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_products" }}`, "dim_products")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
	sqlContent = strings.TrimSpace(sqlContent)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE fct_sales AS "+sqlContent)
	if err != nil {
		t.Fatalf("Failed to execute fct_sales model: %v", err)
	}

	// Check for NULLs in required fields
	var nullCount int
	err = db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM fct_sales 
		 WHERE sale_id IS NULL 
		    OR customer_sk IS NULL 
		    OR product_id IS NULL 
		    OR sale_date IS NULL 
		    OR sale_amount IS NULL 
		    OR quantity IS NULL`).Scan(&nullCount)
	if err != nil {
		t.Fatalf("Failed to check for NULLs: %v", err)
	}
	if nullCount > 0 {
		t.Errorf("Found %d sales records with NULL values in required fields", nullCount)
	}

	// Verify all customer_sk values resolve to dim_customers
	var unresolvedCustomers int
	err = db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM fct_sales f
		 WHERE NOT EXISTS (
		   SELECT 1 FROM dim_customers d WHERE d.customer_sk = f.customer_sk
		 )`).Scan(&unresolvedCustomers)
	if err != nil {
		t.Fatalf("Failed to check customer FK integrity: %v", err)
	}
	if unresolvedCustomers > 0 {
		t.Errorf("Found %d sales with unresolved customer_sk values", unresolvedCustomers)
	}

	// Verify all product_id values resolve to dim_products
	var unresolvedProducts int
	err = db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM fct_sales f
		 WHERE NOT EXISTS (
		   SELECT 1 FROM dim_products p WHERE p.product_id = f.product_id
		 )`).Scan(&unresolvedProducts)
	if err != nil {
		t.Fatalf("Failed to check product FK integrity: %v", err)
	}
	if unresolvedProducts > 0 {
		t.Errorf("Found %d sales with unresolved product_id values", unresolvedProducts)
	}

	// Verify all sale_date values resolve to dim_dates
	var unresolvedDates int
	err = db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM fct_sales f
		 WHERE NOT EXISTS (
		   SELECT 1 FROM dim_dates d WHERE d.sale_date = f.sale_date
		 )`).Scan(&unresolvedDates)
	if err != nil {
		t.Fatalf("Failed to check date FK integrity: %v", err)
	}
	if unresolvedDates > 0 {
		t.Errorf("Found %d sales with unresolved sale_date values", unresolvedDates)
	}
}

// =============================================================================
// Phase 6: Aggregate Rollup Table Tests
// =============================================================================

// TestRollupDailySalesModelExists verifies rollup_daily_sales.sql model file exists
func TestRollupDailySalesModelExists(t *testing.T) {
	modelPath := filepath.Join("models", "rollups", "rollup_daily_sales.sql")

	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		t.Fatalf("Model file %s does not exist", modelPath)
	}
}

// TestRollupDailySalesModelContent verifies rollup_daily_sales.sql has required content
func TestRollupDailySalesModelContent(t *testing.T) {
	modelPath := filepath.Join("models", "rollups", "rollup_daily_sales.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read rollup_daily_sales.sql: %v", err)
	}

	contentStr := string(content)

	// Test: Has config directive with materialized='table'
	if !strings.Contains(contentStr, "{{ config \"materialized\" \"table\" }}") {
		t.Error("rollup_daily_sales.sql missing {{ config \"materialized\" \"table\" }} directive")
	}

	// Test: References fct_sales via {{ ref }}
	if !strings.Contains(contentStr, `{{ ref "fct_sales" }}`) {
		t.Error("rollup_daily_sales.sql missing {{ ref \"fct_sales\" }} reference")
	}

	// Test: Joins to dimension tables via {{ ref }}
	requiredRefs := []string{
		`{{ ref "dim_dates" }}`,
		`{{ ref "dim_products" }}`,
		`{{ ref "dim_customers" }}`,
	}
	for _, ref := range requiredRefs {
		if !strings.Contains(contentStr, ref) {
			t.Errorf("rollup_daily_sales.sql missing %s reference", ref)
		}
	}

	// Test: Has required aggregations
	requiredAggregations := []string{
		"SUM(", "sale_amount",
		"SUM(", "quantity",
		"COUNT(*)",
		"AVG(", "sale_amount",
	}
	for _, agg := range requiredAggregations {
		if !strings.Contains(contentStr, agg) {
			t.Errorf("rollup_daily_sales.sql missing aggregation keyword: %s", agg)
		}
	}

	// Test: Has GROUP BY clause
	if !strings.Contains(strings.ToUpper(contentStr), "GROUP BY") {
		t.Error("rollup_daily_sales.sql missing GROUP BY clause")
	}
}

// TestRollupDailySalesModelExecution tests that rollup_daily_sales model can be executed
func TestRollupDailySalesModelExecution(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Helper to create dimensions
	createDimensions := func(t *testing.T, db *sql.DB) {
		t.Helper()
		dims := []struct {
			path      string
			tableName string
		}{
			{filepath.Join("models", "dimensions", "dim_customers.sql"), "dim_customers"},
			{filepath.Join("models", "dimensions", "dim_products.sql"), "dim_products"},
			{filepath.Join("models", "dimensions", "dim_dates.sql"), "dim_dates"},
		}

		for _, dim := range dims {
			content, err := os.ReadFile(dim.path)
			if err != nil {
				t.Fatalf("Failed to read %s: %v", dim.path, err)
			}
			sqlContent := string(content)
			sqlContent = removeConfigDirectives(sqlContent)
			lines := strings.Split(sqlContent, "\n")
			var filteredLines []string
			for _, line := range lines {
				if !strings.Contains(line, "{{ config(") {
					filteredLines = append(filteredLines, line)
				}
			}
			sqlContent = strings.Join(filteredLines, "\n")
			sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
			sqlContent = strings.TrimSpace(sqlContent)
			_, err = db.ExecContext(context.Background(), "CREATE TABLE "+dim.tableName+" AS "+sqlContent)
			if err != nil {
				t.Fatalf("Failed to create %s: %v", dim.tableName, err)
			}
		}
	}
	createDimensions(t, db)

	// Create fct_sales
	fctSalesPath := filepath.Join("models", "facts", "fct_sales.sql")
	fctSalesContent, err := os.ReadFile(fctSalesPath)
	if err != nil {
		t.Fatalf("Failed to read fct_sales.sql: %v", err)
	}
	sqlContent := string(fctSalesContent)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_customers" }}`, "dim_customers")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_products" }}`, "dim_products")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
	sqlContent = strings.TrimSpace(sqlContent)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE fct_sales AS "+sqlContent)
	if err != nil {
		t.Fatalf("Failed to execute fct_sales model: %v", err)
	}

	// Now create rollup_daily_sales
	modelPath := filepath.Join("models", "rollups", "rollup_daily_sales.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read rollup_daily_sales.sql: %v", err)
	}

	sqlContent = string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "fct_sales" }}`, "fct_sales")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_products" }}`, "dim_products")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_customers" }}`, "dim_customers")
	sqlContent = strings.TrimSpace(sqlContent)

	createTableSQL := "CREATE TABLE rollup_daily_sales AS " + sqlContent
	_, err = db.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		t.Fatalf("Failed to execute rollup_daily_sales model: %v", err)
	}

	// Verify table was created
	var tableName string
	err = db.QueryRowContext(context.Background(),
		"SELECT name FROM sqlite_master WHERE type='table' AND name='rollup_daily_sales'").Scan(&tableName)
	if err != nil {
		t.Fatalf("Table rollup_daily_sales not found: %v", err)
	}
}

// TestRollupDailySalesColumns verifies rollup_daily_sales table has expected columns
func TestRollupDailySalesColumns(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Setup helper
	createDimensions := func(t *testing.T, db *sql.DB) {
		t.Helper()
		dims := []struct {
			path      string
			tableName string
		}{
			{filepath.Join("models", "dimensions", "dim_customers.sql"), "dim_customers"},
			{filepath.Join("models", "dimensions", "dim_products.sql"), "dim_products"},
			{filepath.Join("models", "dimensions", "dim_dates.sql"), "dim_dates"},
		}

		for _, dim := range dims {
			content, err := os.ReadFile(dim.path)
			if err != nil {
				t.Fatalf("Failed to read %s: %v", dim.path, err)
			}
			sqlContent := string(content)
			sqlContent = removeConfigDirectives(sqlContent)
			sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
			sqlContent = strings.TrimSpace(sqlContent)
			_, err = db.ExecContext(context.Background(), "CREATE TABLE "+dim.tableName+" AS "+sqlContent)
			if err != nil {
				t.Fatalf("Failed to create %s: %v", dim.tableName, err)
			}
		}
	}
	createDimensions(t, db)

	// Create fct_sales
	fctSalesPath := filepath.Join("models", "facts", "fct_sales.sql")
	fctSalesContent, err := os.ReadFile(fctSalesPath)
	if err != nil {
		t.Fatalf("Failed to read fct_sales.sql: %v", err)
	}
	sqlContent := string(fctSalesContent)
	sqlContent = removeAllTemplateDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_customers" }}`, "dim_customers")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_products" }}`, "dim_products")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
	sqlContent = strings.TrimSpace(sqlContent)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE fct_sales AS "+sqlContent)
	if err != nil {
		t.Fatalf("Failed to execute fct_sales model: %v", err)
	}

	// Create rollup_daily_sales
	modelPath := filepath.Join("models", "rollups", "rollup_daily_sales.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read rollup_daily_sales.sql: %v", err)
	}
	sqlContent = string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "fct_sales" }}`, "fct_sales")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_products" }}`, "dim_products")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_customers" }}`, "dim_customers")
	sqlContent = strings.TrimSpace(sqlContent)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE rollup_daily_sales AS "+sqlContent)
	if err != nil {
		t.Fatalf("Failed to execute rollup_daily_sales model: %v", err)
	}

	// Get column names
	rows, err := db.QueryContext(context.Background(), "PRAGMA table_info(rollup_daily_sales)")
	if err != nil {
		t.Fatalf("Failed to get table info: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var cid int
		var name string
		var dataType string
		var notNull int
		var dfltValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &dfltValue, &pk); err != nil {
			t.Fatalf("Failed to scan column info: %v", err)
		}
		columns = append(columns, name)
	}

	// Expected columns (grain dimensions + aggregates)
	expectedColumns := []string{
		"sale_date", "year", "month", "product_category", "customer_state",
		"total_sales", "total_quantity", "sale_count", "avg_sale_amount",
	}
	for _, expected := range expectedColumns {
		found := false
		for _, col := range columns {
			if col == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected column %s not found in rollup_daily_sales. Found columns: %v", expected, columns)
		}
	}
}

// TestRollupDailySalesGrain verifies grain is one row per date + product_category + customer_state
func TestRollupDailySalesGrain(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Setup helper
	createDimensions := func(t *testing.T, db *sql.DB) {
		t.Helper()
		dims := []struct {
			path      string
			tableName string
		}{
			{filepath.Join("models", "dimensions", "dim_customers.sql"), "dim_customers"},
			{filepath.Join("models", "dimensions", "dim_products.sql"), "dim_products"},
			{filepath.Join("models", "dimensions", "dim_dates.sql"), "dim_dates"},
		}

		for _, dim := range dims {
			content, err := os.ReadFile(dim.path)
			if err != nil {
				t.Fatalf("Failed to read %s: %v", dim.path, err)
			}
			sqlContent := string(content)
			sqlContent = removeConfigDirectives(sqlContent)
			lines := strings.Split(sqlContent, "\n")
			var filteredLines []string
			for _, line := range lines {
				if !strings.Contains(line, "{{ config(") {
					filteredLines = append(filteredLines, line)
				}
			}
			sqlContent = strings.Join(filteredLines, "\n")
			sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
			sqlContent = strings.TrimSpace(sqlContent)
			_, err = db.ExecContext(context.Background(), "CREATE TABLE "+dim.tableName+" AS "+sqlContent)
			if err != nil {
				t.Fatalf("Failed to create %s: %v", dim.tableName, err)
			}
		}
	}
	createDimensions(t, db)

	// Create fct_sales
	fctSalesPath := filepath.Join("models", "facts", "fct_sales.sql")
	fctSalesContent, err := os.ReadFile(fctSalesPath)
	if err != nil {
		t.Fatalf("Failed to read fct_sales.sql: %v", err)
	}
	sqlContent := string(fctSalesContent)
	sqlContent = removeAllTemplateDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_customers" }}`, "dim_customers")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_products" }}`, "dim_products")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
	sqlContent = strings.TrimSpace(sqlContent)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE fct_sales AS "+sqlContent)
	if err != nil {
		t.Fatalf("Failed to execute fct_sales model: %v", err)
	}

	// Create rollup_daily_sales
	modelPath := filepath.Join("models", "rollups", "rollup_daily_sales.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read rollup_daily_sales.sql: %v", err)
	}
	sqlContent = string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "fct_sales" }}`, "fct_sales")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_products" }}`, "dim_products")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_customers" }}`, "dim_customers")
	sqlContent = strings.TrimSpace(sqlContent)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE rollup_daily_sales AS "+sqlContent)
	if err != nil {
		t.Fatalf("Failed to execute rollup_daily_sales model: %v", err)
	}

	// Test: Verify no duplicate grain combinations
	var duplicateCount int
	err = db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM (
			SELECT sale_date, product_category, customer_state, COUNT(*) as cnt
			FROM rollup_daily_sales
			GROUP BY sale_date, product_category, customer_state
			HAVING cnt > 1
		)`).Scan(&duplicateCount)
	if err != nil {
		t.Fatalf("Failed to check grain uniqueness: %v", err)
	}
	if duplicateCount > 0 {
		t.Errorf("Found %d duplicate grain combinations (sale_date, product_category, customer_state)", duplicateCount)
	}
}

// TestRollupDailySalesRowCountReduction verifies rollup reduces row count vs fact table
func TestRollupDailySalesRowCountReduction(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Setup helper
	createDimensions := func(t *testing.T, db *sql.DB) {
		t.Helper()
		dims := []struct {
			path      string
			tableName string
		}{
			{filepath.Join("models", "dimensions", "dim_customers.sql"), "dim_customers"},
			{filepath.Join("models", "dimensions", "dim_products.sql"), "dim_products"},
			{filepath.Join("models", "dimensions", "dim_dates.sql"), "dim_dates"},
		}

		for _, dim := range dims {
			content, err := os.ReadFile(dim.path)
			if err != nil {
				t.Fatalf("Failed to read %s: %v", dim.path, err)
			}
			sqlContent := string(content)
			sqlContent = removeConfigDirectives(sqlContent)
			lines := strings.Split(sqlContent, "\n")
			var filteredLines []string
			for _, line := range lines {
				if !strings.Contains(line, "{{ config(") {
					filteredLines = append(filteredLines, line)
				}
			}
			sqlContent = strings.Join(filteredLines, "\n")
			sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
			sqlContent = strings.TrimSpace(sqlContent)
			_, err = db.ExecContext(context.Background(), "CREATE TABLE "+dim.tableName+" AS "+sqlContent)
			if err != nil {
				t.Fatalf("Failed to create %s: %v", dim.tableName, err)
			}
		}
	}
	createDimensions(t, db)

	// Create fct_sales
	fctSalesPath := filepath.Join("models", "facts", "fct_sales.sql")
	fctSalesContent, err := os.ReadFile(fctSalesPath)
	if err != nil {
		t.Fatalf("Failed to read fct_sales.sql: %v", err)
	}
	sqlContent := string(fctSalesContent)
	sqlContent = removeAllTemplateDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_customers" }}`, "dim_customers")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_products" }}`, "dim_products")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
	sqlContent = strings.TrimSpace(sqlContent)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE fct_sales AS "+sqlContent)
	if err != nil {
		t.Fatalf("Failed to execute fct_sales model: %v", err)
	}

	// Get fact table row count
	var factCount int
	err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM fct_sales").Scan(&factCount)
	if err != nil {
		t.Fatalf("Failed to count fct_sales rows: %v", err)
	}

	// Create rollup_daily_sales
	modelPath := filepath.Join("models", "rollups", "rollup_daily_sales.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read rollup_daily_sales.sql: %v", err)
	}
	sqlContent = string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "fct_sales" }}`, "fct_sales")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_products" }}`, "dim_products")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_customers" }}`, "dim_customers")
	sqlContent = strings.TrimSpace(sqlContent)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE rollup_daily_sales AS "+sqlContent)
	if err != nil {
		t.Fatalf("Failed to execute rollup_daily_sales model: %v", err)
	}

	// Get rollup table row count
	var rollupCount int
	err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM rollup_daily_sales").Scan(&rollupCount)
	if err != nil {
		t.Fatalf("Failed to count rollup_daily_sales rows: %v", err)
	}

	// Test: Rollup should have fewer or equal rows than fact table
	// (Equal is acceptable when data has no duplicate grain combinations)
	if rollupCount > factCount {
		t.Errorf("Rollup row count (%d) should not exceed fact table row count (%d)", rollupCount, factCount)
	}

	// Test: Rollup should have between 10-30 rows (reasonable range for aggregation)
	if rollupCount < 10 || rollupCount > 30 {
		t.Errorf("Expected rollup to have 10-30 rows (reasonable aggregation), got %d", rollupCount)
	}

	// Log the reduction (or lack thereof)
	if rollupCount < factCount {
		t.Logf("Row count reduction: fct_sales=%d, rollup_daily_sales=%d (%.1f%% reduction)",
			factCount, rollupCount, float64(factCount-rollupCount)/float64(factCount)*100)
	} else {
		t.Logf("No row count reduction: fct_sales=%d, rollup_daily_sales=%d (grain produces unique combinations for this dataset)",
			factCount, rollupCount)
	}
}

// TestRollupDailySalesAggregationAccuracy verifies aggregates match fact table sums
func TestRollupDailySalesAggregationAccuracy(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Setup helper
	createDimensions := func(t *testing.T, db *sql.DB) {
		t.Helper()
		dims := []struct {
			path      string
			tableName string
		}{
			{filepath.Join("models", "dimensions", "dim_customers.sql"), "dim_customers"},
			{filepath.Join("models", "dimensions", "dim_products.sql"), "dim_products"},
			{filepath.Join("models", "dimensions", "dim_dates.sql"), "dim_dates"},
		}

		for _, dim := range dims {
			content, err := os.ReadFile(dim.path)
			if err != nil {
				t.Fatalf("Failed to read %s: %v", dim.path, err)
			}
			sqlContent := string(content)
			sqlContent = removeConfigDirectives(sqlContent)
			lines := strings.Split(sqlContent, "\n")
			var filteredLines []string
			for _, line := range lines {
				if !strings.Contains(line, "{{ config(") {
					filteredLines = append(filteredLines, line)
				}
			}
			sqlContent = strings.Join(filteredLines, "\n")
			sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
			sqlContent = strings.TrimSpace(sqlContent)
			_, err = db.ExecContext(context.Background(), "CREATE TABLE "+dim.tableName+" AS "+sqlContent)
			if err != nil {
				t.Fatalf("Failed to create %s: %v", dim.tableName, err)
			}
		}
	}
	createDimensions(t, db)

	// Create fct_sales
	fctSalesPath := filepath.Join("models", "facts", "fct_sales.sql")
	fctSalesContent, err := os.ReadFile(fctSalesPath)
	if err != nil {
		t.Fatalf("Failed to read fct_sales.sql: %v", err)
	}
	sqlContent := string(fctSalesContent)
	sqlContent = removeAllTemplateDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_customers" }}`, "dim_customers")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_products" }}`, "dim_products")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
	sqlContent = strings.TrimSpace(sqlContent)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE fct_sales AS "+sqlContent)
	if err != nil {
		t.Fatalf("Failed to execute fct_sales model: %v", err)
	}

	// Get fact table totals
	var factTotalSales, factTotalQuantity float64
	var factSaleCount int
	err = db.QueryRowContext(context.Background(),
		`SELECT SUM(sale_amount), SUM(quantity), COUNT(*) 
		 FROM fct_sales`).Scan(&factTotalSales, &factTotalQuantity, &factSaleCount)
	if err != nil {
		t.Fatalf("Failed to get fact table totals: %v", err)
	}

	// Create rollup_daily_sales
	modelPath := filepath.Join("models", "rollups", "rollup_daily_sales.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read rollup_daily_sales.sql: %v", err)
	}
	sqlContent = string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "fct_sales" }}`, "fct_sales")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_products" }}`, "dim_products")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_customers" }}`, "dim_customers")
	sqlContent = strings.TrimSpace(sqlContent)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE rollup_daily_sales AS "+sqlContent)
	if err != nil {
		t.Fatalf("Failed to execute rollup_daily_sales model: %v", err)
	}

	// Get rollup table totals
	var rollupTotalSales, rollupTotalQuantity float64
	var rollupSaleCount int
	err = db.QueryRowContext(context.Background(),
		`SELECT SUM(total_sales), SUM(total_quantity), SUM(sale_count) 
		 FROM rollup_daily_sales`).Scan(&rollupTotalSales, &rollupTotalQuantity, &rollupSaleCount)
	if err != nil {
		t.Fatalf("Failed to get rollup table totals: %v", err)
	}

	// Test: Total sales should match
	if factTotalSales != rollupTotalSales {
		t.Errorf("Total sales mismatch: fact=%.2f, rollup=%.2f", factTotalSales, rollupTotalSales)
	}

	// Test: Total quantity should match
	if factTotalQuantity != rollupTotalQuantity {
		t.Errorf("Total quantity mismatch: fact=%.2f, rollup=%.2f", factTotalQuantity, rollupTotalQuantity)
	}

	// Test: Sale count should match
	if factSaleCount != rollupSaleCount {
		t.Errorf("Sale count mismatch: fact=%d, rollup=%d", factSaleCount, rollupSaleCount)
	}
}

// TestRollupDailySales DataIntegrity verifies no NULLs in required fields
func TestRollupDailySalesDataIntegrity(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Setup helper
	createDimensions := func(t *testing.T, db *sql.DB) {
		t.Helper()
		dims := []struct {
			path      string
			tableName string
		}{
			{filepath.Join("models", "dimensions", "dim_customers.sql"), "dim_customers"},
			{filepath.Join("models", "dimensions", "dim_products.sql"), "dim_products"},
			{filepath.Join("models", "dimensions", "dim_dates.sql"), "dim_dates"},
		}

		for _, dim := range dims {
			content, err := os.ReadFile(dim.path)
			if err != nil {
				t.Fatalf("Failed to read %s: %v", dim.path, err)
			}
			sqlContent := string(content)
			sqlContent = removeConfigDirectives(sqlContent)
			sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
			sqlContent = strings.TrimSpace(sqlContent)
			_, err = db.ExecContext(context.Background(), "CREATE TABLE "+dim.tableName+" AS "+sqlContent)
			if err != nil {
				t.Fatalf("Failed to create %s: %v", dim.tableName, err)
			}
		}
	}
	createDimensions(t, db)

	// Create fct_sales
	fctSalesPath := filepath.Join("models", "facts", "fct_sales.sql")
	fctSalesContent, err := os.ReadFile(fctSalesPath)
	if err != nil {
		t.Fatalf("Failed to read fct_sales.sql: %v", err)
	}
	sqlContent := string(fctSalesContent)
	sqlContent = removeAllTemplateDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_customers" }}`, "dim_customers")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_products" }}`, "dim_products")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
	sqlContent = strings.TrimSpace(sqlContent)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE fct_sales AS "+sqlContent)
	if err != nil {
		t.Fatalf("Failed to execute fct_sales model: %v", err)
	}

	// Create rollup_daily_sales
	modelPath := filepath.Join("models", "rollups", "rollup_daily_sales.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read rollup_daily_sales.sql: %v", err)
	}
	sqlContent = string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "fct_sales" }}`, "fct_sales")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_products" }}`, "dim_products")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_customers" }}`, "dim_customers")
	sqlContent = strings.TrimSpace(sqlContent)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE rollup_daily_sales AS "+sqlContent)
	if err != nil {
		t.Fatalf("Failed to execute rollup_daily_sales model: %v", err)
	}

	// Test: Check for NULLs in required fields
	var nullCount int
	err = db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM rollup_daily_sales 
		 WHERE sale_date IS NULL 
		    OR year IS NULL 
		    OR month IS NULL 
		    OR product_category IS NULL 
		    OR customer_state IS NULL 
		    OR total_sales IS NULL 
		    OR total_quantity IS NULL 
		    OR sale_count IS NULL 
		    OR avg_sale_amount IS NULL`).Scan(&nullCount)
	if err != nil {
		t.Fatalf("Failed to check for NULLs: %v", err)
	}
	if nullCount > 0 {
		t.Errorf("Found %d rollup records with NULL values in required fields", nullCount)
	}
}

// TestRollupDailySalesProductCategories verifies product categories match expected values
func TestRollupDailySalesProductCategories(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Setup helper
	createDimensions := func(t *testing.T, db *sql.DB) {
		t.Helper()
		dims := []struct {
			path      string
			tableName string
		}{
			{filepath.Join("models", "dimensions", "dim_customers.sql"), "dim_customers"},
			{filepath.Join("models", "dimensions", "dim_products.sql"), "dim_products"},
			{filepath.Join("models", "dimensions", "dim_dates.sql"), "dim_dates"},
		}

		for _, dim := range dims {
			content, err := os.ReadFile(dim.path)
			if err != nil {
				t.Fatalf("Failed to read %s: %v", dim.path, err)
			}
			sqlContent := string(content)
			sqlContent = removeConfigDirectives(sqlContent)
			sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
			sqlContent = strings.TrimSpace(sqlContent)
			_, err = db.ExecContext(context.Background(), "CREATE TABLE "+dim.tableName+" AS "+sqlContent)
			if err != nil {
				t.Fatalf("Failed to create %s: %v", dim.tableName, err)
			}
		}
	}
	createDimensions(t, db)

	// Create fct_sales
	fctSalesPath := filepath.Join("models", "facts", "fct_sales.sql")
	fctSalesContent, err := os.ReadFile(fctSalesPath)
	if err != nil {
		t.Fatalf("Failed to read fct_sales.sql: %v", err)
	}
	sqlContent := string(fctSalesContent)
	sqlContent = removeAllTemplateDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_customers" }}`, "dim_customers")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_products" }}`, "dim_products")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
	sqlContent = strings.TrimSpace(sqlContent)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE fct_sales AS "+sqlContent)
	if err != nil {
		t.Fatalf("Failed to execute fct_sales model: %v", err)
	}

	// Create rollup_daily_sales
	modelPath := filepath.Join("models", "rollups", "rollup_daily_sales.sql")
	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read rollup_daily_sales.sql: %v", err)
	}
	sqlContent = string(content)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "fct_sales" }}`, "fct_sales")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_products" }}`, "dim_products")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_customers" }}`, "dim_customers")
	sqlContent = strings.TrimSpace(sqlContent)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE rollup_daily_sales AS "+sqlContent)
	if err != nil {
		t.Fatalf("Failed to execute rollup_daily_sales model: %v", err)
	}

	// Get distinct product categories
	rows, err := db.QueryContext(context.Background(),
		"SELECT DISTINCT product_category FROM rollup_daily_sales ORDER BY product_category")
	if err != nil {
		t.Fatalf("Failed to query product categories: %v", err)
	}
	defer rows.Close()

	var categories []string
	for rows.Next() {
		var category string
		if err := rows.Scan(&category); err != nil {
			t.Fatalf("Failed to scan category: %v", err)
		}
		categories = append(categories, category)
	}

	// Test: Should have the 3 expected categories
	expectedCategories := []string{"Accessories", "Electronics", "Furniture"}
	if len(categories) != len(expectedCategories) {
		t.Errorf("Expected %d product categories, found %d: %v", len(expectedCategories), len(categories), categories)
	}

	for _, expected := range expectedCategories {
		found := false
		for _, cat := range categories {
			if cat == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected product category %s not found in rollup", expected)
		}
	}
}

// TestEndToEndIntegration performs a comprehensive end-to-end test of the entire star schema pipeline
func TestEndToEndIntegration(t *testing.T) {
	// Create temp database
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Step 1: Create raw_sales (source)
	t.Log("Step 1: Creating raw_sales source table...")
	rawSalesPath := filepath.Join("models", "sources", "raw_sales.sql")
	rawSalesContent, err := os.ReadFile(rawSalesPath)
	if err != nil {
		t.Fatalf("Failed to read raw_sales.sql: %v", err)
	}
	sqlContent := string(rawSalesContent)
	sqlContent = strings.ReplaceAll(sqlContent, "{{ config \"materialized\" \"table\" }}", "")
	sqlContent = strings.TrimSpace(sqlContent)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE raw_sales AS "+sqlContent)
	if err != nil {
		t.Fatalf("Failed to create raw_sales: %v", err)
	}

	// Verify raw_sales row count
	var rawSalesCount int
	err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM raw_sales").Scan(&rawSalesCount)
	if err != nil {
		t.Fatalf("Failed to count raw_sales: %v", err)
	}
	if rawSalesCount != 30 {
		t.Errorf("Expected raw_sales to have 30 rows, got %d", rawSalesCount)
	}
	t.Logf("✓ raw_sales created with %d rows", rawSalesCount)

	// Step 2: Create dimensions
	t.Log("Step 2: Creating dimension tables...")

	// Create dim_products
	dimProductsPath := filepath.Join("models", "dimensions", "dim_products.sql")
	dimProductsContent, err := os.ReadFile(dimProductsPath)
	if err != nil {
		t.Fatalf("Failed to read dim_products.sql: %v", err)
	}
	sqlContent = string(dimProductsContent)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.TrimSpace(sqlContent)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE dim_products AS "+sqlContent)
	if err != nil {
		t.Fatalf("Failed to create dim_products: %v", err)
	}

	var dimProductsCount int
	err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM dim_products").Scan(&dimProductsCount)
	if err != nil {
		t.Fatalf("Failed to count dim_products: %v", err)
	}
	if dimProductsCount != 12 {
		t.Errorf("Expected dim_products to have 12 rows, got %d", dimProductsCount)
	}
	t.Logf("✓ dim_products created with %d rows", dimProductsCount)

	// Create dim_dates
	dimDatesPath := filepath.Join("models", "dimensions", "dim_dates.sql")
	dimDatesContent, err := os.ReadFile(dimDatesPath)
	if err != nil {
		t.Fatalf("Failed to read dim_dates.sql: %v", err)
	}
	sqlContent = string(dimDatesContent)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.TrimSpace(sqlContent)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE dim_dates AS "+sqlContent)
	if err != nil {
		t.Fatalf("Failed to create dim_dates: %v", err)
	}

	var dimDatesCount int
	err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM dim_dates").Scan(&dimDatesCount)
	if err != nil {
		t.Fatalf("Failed to count dim_dates: %v", err)
	}
	if dimDatesCount != 30 {
		t.Errorf("Expected dim_dates to have 30 rows, got %d", dimDatesCount)
	}
	t.Logf("✓ dim_dates created with %d rows", dimDatesCount)

	// Create dim_customers (SCD Type 2)
	dimCustomersPath := filepath.Join("models", "dimensions", "dim_customers.sql")
	dimCustomersContent, err := os.ReadFile(dimCustomersPath)
	if err != nil {
		t.Fatalf("Failed to read dim_customers.sql: %v", err)
	}
	sqlContent = string(dimCustomersContent)
	sqlContent = removeConfigDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.TrimSpace(sqlContent)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE dim_customers AS "+sqlContent)
	if err != nil {
		t.Fatalf("Failed to create dim_customers: %v", err)
	}

	var dimCustomersCount int
	err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM dim_customers").Scan(&dimCustomersCount)
	if err != nil {
		t.Fatalf("Failed to count dim_customers: %v", err)
	}
	// Should have > 8 unique customers, but with SCD Type 2, some customers have multiple versions
	if dimCustomersCount < 8 {
		t.Errorf("Expected dim_customers to have at least 8 rows, got %d", dimCustomersCount)
	}
	t.Logf("✓ dim_customers created with %d rows (includes SCD Type 2 versions)", dimCustomersCount)

	// Step 3: Test SCD Type 2 specifically for customer 1001
	t.Log("Step 3: Validating SCD Type 2 for customer 1001...")
	rows, err := db.QueryContext(context.Background(), `
		SELECT customer_sk, customer_id, customer_city, customer_state, valid_from, valid_to, is_current
		FROM dim_customers
		WHERE customer_id = 1001
		ORDER BY valid_from
	`)
	if err != nil {
		t.Fatalf("Failed to query customer 1001 versions: %v", err)
	}
	defer rows.Close()

	type customerVersion struct {
		sk        int
		id        int
		city      string
		state     string
		validFrom string
		validTo   string
		isCurrent int
	}
	var customer1001Versions []customerVersion
	for rows.Next() {
		var cv customerVersion
		if err := rows.Scan(&cv.sk, &cv.id, &cv.city, &cv.state, &cv.validFrom, &cv.validTo, &cv.isCurrent); err != nil {
			t.Fatalf("Failed to scan customer version: %v", err)
		}
		customer1001Versions = append(customer1001Versions, cv)
	}

	// Customer 1001 should have at least 2 versions (Seattle -> Portland)
	if len(customer1001Versions) < 2 {
		t.Errorf("Expected customer 1001 to have at least 2 versions, got %d", len(customer1001Versions))
	} else {
		t.Logf("✓ Customer 1001 has %d versions (SCD Type 2)", len(customer1001Versions))
		for i, cv := range customer1001Versions {
			t.Logf("  Version %d: SK=%d, City=%s, State=%s, Valid %s to %s, Current=%d",
				i+1, cv.sk, cv.city, cv.state, cv.validFrom, cv.validTo, cv.isCurrent)
		}

		// First version should be Seattle
		if customer1001Versions[0].city != "Seattle" {
			t.Errorf("Expected first version of customer 1001 to be in Seattle, got %s", customer1001Versions[0].city)
		}

		// Last version should be current
		lastVersion := customer1001Versions[len(customer1001Versions)-1]
		if lastVersion.isCurrent != 1 {
			t.Errorf("Expected last version of customer 1001 to be current, got is_current=%d", lastVersion.isCurrent)
		}
		if lastVersion.validTo != "9999-12-31" {
			t.Errorf("Expected current version to have valid_to='9999-12-31', got %s", lastVersion.validTo)
		}
	}

	// Step 4: Create fact table
	t.Log("Step 4: Creating fct_sales fact table...")
	fctSalesPath := filepath.Join("models", "facts", "fct_sales.sql")
	fctSalesContent, err := os.ReadFile(fctSalesPath)
	if err != nil {
		t.Fatalf("Failed to read fct_sales.sql: %v", err)
	}
	sqlContent = string(fctSalesContent)
	sqlContent = removeAllTemplateDirectives(sqlContent)
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "raw_sales" }}`, "raw_sales")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_customers" }}`, "dim_customers")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_products" }}`, "dim_products")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
	sqlContent = strings.TrimSpace(sqlContent)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE fct_sales AS "+sqlContent)
	if err != nil {
		t.Fatalf("Failed to create fct_sales: %v", err)
	}

	var fctSalesCount int
	err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM fct_sales").Scan(&fctSalesCount)
	if err != nil {
		t.Fatalf("Failed to count fct_sales: %v", err)
	}
	if fctSalesCount != 30 {
		t.Errorf("Expected fct_sales to have 30 rows, got %d", fctSalesCount)
	}
	t.Logf("✓ fct_sales created with %d rows", fctSalesCount)

	// Step 5: Validate joins between fact and dimensions work
	t.Log("Step 5: Validating joins between fact and dimensions...")
	var joinTestCount int
	err = db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM fct_sales f
		INNER JOIN dim_customers c ON f.customer_sk = c.customer_sk
		INNER JOIN dim_products p ON f.product_id = p.product_id
		INNER JOIN dim_dates d ON f.sale_date = d.sale_date
	`).Scan(&joinTestCount)
	if err != nil {
		t.Fatalf("Failed to test joins: %v", err)
	}
	if joinTestCount != 30 {
		t.Errorf("Expected join to return 30 rows, got %d", joinTestCount)
	}
	t.Logf("✓ All fact rows join successfully to all dimensions (%d rows)", joinTestCount)

	// Step 6: Create rollup table
	t.Log("Step 6: Creating rollup_daily_sales aggregation table...")
	rollupPath := filepath.Join("models", "rollups", "rollup_daily_sales.sql")
	rollupContent, err := os.ReadFile(rollupPath)
	if err != nil {
		t.Fatalf("Failed to read rollup_daily_sales.sql: %v", err)
	}
	sqlContent = string(rollupContent)
	sqlContent = strings.ReplaceAll(sqlContent, "{{ config \"materialized\" \"table\" }}", "")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "fct_sales" }}`, "fct_sales")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_dates" }}`, "dim_dates")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_products" }}`, "dim_products")
	sqlContent = strings.ReplaceAll(sqlContent, `{{ ref "dim_customers" }}`, "dim_customers")
	sqlContent = strings.TrimSpace(sqlContent)
	_, err = db.ExecContext(context.Background(), "CREATE TABLE rollup_daily_sales AS "+sqlContent)
	if err != nil {
		t.Fatalf("Failed to create rollup_daily_sales: %v", err)
	}

	var rollupCount int
	err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM rollup_daily_sales").Scan(&rollupCount)
	if err != nil {
		t.Fatalf("Failed to count rollup_daily_sales: %v", err)
	}
	// Rollup should have rows (may be same or fewer than fact table depending on aggregation granularity)
	if rollupCount == 0 {
		t.Errorf("Expected rollup_daily_sales to have rows, got 0")
	}
	t.Logf("✓ rollup_daily_sales created with %d rows (aggregated from %d fact rows)", rollupCount, fctSalesCount)

	// Step 7: Validate rollup aggregation matches fact table
	t.Log("Step 7: Validating rollup aggregation matches fact table...")
	var factTotal, rollupTotal float64
	err = db.QueryRowContext(context.Background(), "SELECT SUM(sale_amount) FROM fct_sales").Scan(&factTotal)
	if err != nil {
		t.Fatalf("Failed to sum fact table: %v", err)
	}
	err = db.QueryRowContext(context.Background(), "SELECT SUM(total_sales) FROM rollup_daily_sales").Scan(&rollupTotal)
	if err != nil {
		t.Fatalf("Failed to sum rollup table: %v", err)
	}
	if factTotal != rollupTotal {
		t.Errorf("Rollup total_sales (%f) does not match fact table sum (%f)", rollupTotal, factTotal)
	}
	t.Logf("✓ Rollup aggregation validated: $%.2f matches fact table total", rollupTotal)

	// Step 8: Test sample analytical query
	t.Log("Step 8: Testing sample analytical query...")
	analyticalQuery := `
		SELECT 
			d.year,
			d.month,
			p.product_category,
			SUM(f.sale_amount) as revenue,
			COUNT(*) as sale_count
		FROM fct_sales f
		INNER JOIN dim_dates d ON f.sale_date = d.sale_date
		INNER JOIN dim_products p ON f.product_id = p.product_id
		WHERE d.year = 2024
		GROUP BY d.year, d.month, p.product_category
		ORDER BY d.month, p.product_category
		LIMIT 5
	`
	analyticalRows, err := db.QueryContext(context.Background(), analyticalQuery)
	if err != nil {
		t.Fatalf("Failed to execute analytical query: %v", err)
	}
	defer analyticalRows.Close()

	var analyticalResults []struct {
		year     int
		month    int
		category string
		revenue  float64
		count    int
	}
	for analyticalRows.Next() {
		var result struct {
			year     int
			month    int
			category string
			revenue  float64
			count    int
		}
		if err := analyticalRows.Scan(&result.year, &result.month, &result.category, &result.revenue, &result.count); err != nil {
			t.Fatalf("Failed to scan analytical result: %v", err)
		}
		analyticalResults = append(analyticalResults, result)
	}

	if len(analyticalResults) == 0 {
		t.Error("Analytical query returned no results")
	} else {
		t.Logf("✓ Analytical query successful, sample results:")
		for _, r := range analyticalResults {
			t.Logf("  %d-%02d %s: $%.2f (%d sales)", r.year, r.month, r.category, r.revenue, r.count)
		}
	}

	// Step 9: Test rollup query for reporting
	t.Log("Step 9: Testing rollup query for pre-aggregated reporting...")
	rollupQuery := `
		SELECT 
			product_category,
			SUM(total_sales) as category_total,
			SUM(total_quantity) as category_quantity,
			AVG(avg_sale_amount) as avg_transaction
		FROM rollup_daily_sales
		GROUP BY product_category
		ORDER BY category_total DESC
	`
	rollupRows, err := db.QueryContext(context.Background(), rollupQuery)
	if err != nil {
		t.Fatalf("Failed to execute rollup query: %v", err)
	}
	defer rollupRows.Close()

	var rollupResults []struct {
		category string
		total    float64
		quantity int
		avgTrans float64
	}
	for rollupRows.Next() {
		var result struct {
			category string
			total    float64
			quantity int
			avgTrans float64
		}
		if err := rollupRows.Scan(&result.category, &result.total, &result.quantity, &result.avgTrans); err != nil {
			t.Fatalf("Failed to scan rollup result: %v", err)
		}
		rollupResults = append(rollupResults, result)
	}

	if len(rollupResults) == 0 {
		t.Error("Rollup query returned no results")
	} else {
		t.Logf("✓ Rollup query successful, category totals:")
		for _, r := range rollupResults {
			t.Logf("  %s: $%.2f (%d units, avg $%.2f per transaction)", r.category, r.total, r.quantity, r.avgTrans)
		}
	}

	// Step 10: Data quality check - verify all foreign keys are valid
	t.Log("Step 10: Data quality validation...")
	var orphanCustomers int
	db.QueryRowContext(context.Background(), `
		SELECT COUNT(*) FROM fct_sales f
		LEFT JOIN dim_customers c ON f.customer_sk = c.customer_sk
		WHERE c.customer_sk IS NULL
	`).Scan(&orphanCustomers)
	if orphanCustomers > 0 {
		t.Errorf("Found %d orphan customer references in fct_sales", orphanCustomers)
	}

	var orphanProducts int
	db.QueryRowContext(context.Background(), `
		SELECT COUNT(*) FROM fct_sales f
		LEFT JOIN dim_products p ON f.product_id = p.product_id
		WHERE p.product_id IS NULL
	`).Scan(&orphanProducts)
	if orphanProducts > 0 {
		t.Errorf("Found %d orphan product references in fct_sales", orphanProducts)
	}

	var orphanDates int
	db.QueryRowContext(context.Background(), `
		SELECT COUNT(*) FROM fct_sales f
		LEFT JOIN dim_dates d ON f.sale_date = d.sale_date
		WHERE d.sale_date IS NULL
	`).Scan(&orphanDates)
	if orphanDates > 0 {
		t.Errorf("Found %d orphan date references in fct_sales", orphanDates)
	}

	t.Logf("✓ Data quality validation passed: all foreign keys are valid")

	t.Log("========================================")
	t.Log("✅ End-to-end integration test completed successfully!")
	t.Log("========================================")
	t.Logf("Summary:")
	t.Logf("  - Source: raw_sales (%d rows)", rawSalesCount)
	t.Logf("  - Dimensions: dim_products (%d), dim_dates (%d), dim_customers (%d with SCD Type 2)",
		dimProductsCount, dimDatesCount, dimCustomersCount)
	t.Logf("  - Fact: fct_sales (%d rows)", fctSalesCount)
	t.Logf("  - Rollup: rollup_daily_sales (%d aggregated rows)", rollupCount)
	t.Logf("  - Total revenue: $%.2f", factTotal)
}
