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
	if !strings.Contains(contentStr, "{{ config(materialized='table') }}") {
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
	sqlContent = strings.ReplaceAll(sqlContent, "{{ config(materialized='table') }}", "")
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
	sqlContent = strings.ReplaceAll(sqlContent, "{{ config(materialized='table') }}", "")
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
	sqlContent = strings.ReplaceAll(sqlContent, "{{ config(materialized='table') }}", "")
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
	sqlContent = strings.ReplaceAll(sqlContent, "{{ config(materialized='table') }}", "")
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
	if !strings.Contains(contentStr, "{{ config(materialized='table') }}") {
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
	sqlContent = strings.ReplaceAll(sqlContent, "{{ config(materialized='table') }}", "")
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
	sqlContent = strings.ReplaceAll(sqlContent, "{{ config(materialized='table') }}", "")
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
	sqlContent = strings.ReplaceAll(sqlContent, "{{ config(materialized='table') }}", "")
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
	sqlContent = strings.ReplaceAll(sqlContent, "{{ config(materialized='table') }}", "")
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
	sqlContent = strings.ReplaceAll(sqlContent, "{{ config(materialized='table') }}", "")
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

	// Check for config directive with incremental materialization
	if !strings.Contains(contentStr, "materialized='incremental'") {
		t.Error("dim_customers.sql missing config directive with materialized='incremental'")
	}

	// Check for unique_key in config
	if !strings.Contains(contentStr, "unique_key") {
		t.Error("dim_customers.sql missing unique_key in config directive")
	}

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
	// Remove config directive (we'll treat as table for testing)
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
