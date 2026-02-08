package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/domain/test"
	"github.com/jpconstantineau/gorchata/internal/domain/test/executor"
	"github.com/jpconstantineau/gorchata/internal/domain/test/generic"
	"github.com/jpconstantineau/gorchata/internal/domain/test/storage"
	"github.com/jpconstantineau/gorchata/internal/template"
)

// TestIntegration_ExecuteTestsEndToEnd tests the full workflow:
// 1. Load project config
// 2. Discover tests from schema files
// 3. Execute tests
// 4. Verify results
func TestIntegration_ExecuteTestsEndToEnd(t *testing.T) {
	// Setup test project
	projectDir := SetupTestProject(t)

	// Create database with valid data
	adapter, dbPath := CreateTestDatabase(t)
	defer adapter.Close()

	// Load configuration
	cfg := LoadTestConfig(t, projectDir)

	// Change to project directory for test discovery (LoadTestConfig changes back on return)
	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}
	defer os.Chdir(oldDir)

	if err := os.Chdir(projectDir); err != nil {
		t.Fatalf("failed to change to project directory: %v", err)
	}

	// Update database path in config to use test database
	cfg.Output.Database = dbPath

	// Reconnect adapter with correct path
	ctx := context.Background()
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("failed to reconnect: %v", err)
	}

	// Note: sample data already created by CreateTestDatabase()

	// Build models first (required for schema tests to reference model tables)
	if err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE IF NOT EXISTS users AS
		SELECT id, name, email, status, created_at FROM raw_users
	`); err != nil {
		t.Fatalf("failed to create users model: %v", err)
	}

	if err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE IF NOT EXISTS orders AS
		SELECT id, user_id, total_amount, status, order_date FROM raw_orders
	`); err != nil {
		t.Fatalf("failed to create orders model: %v", err)
	}

	// Create test registry
	registry := generic.NewDefaultRegistry()

	// Debug: check config paths
	t.Logf("Project dir: %s", projectDir)
	t.Logf("Config model paths: %v", cfg.Project.ModelPaths)
	t.Logf("Config test paths: %v", cfg.Project.TestPaths)
	t.Logf("Database: %s", cfg.Output.Database)

	// Debug: check if files exist
	schemaPath := filepath.Join(projectDir, "models", "schema.yml")
	if _, err := os.Stat(schemaPath); err != nil {
		t.Logf("WARNING: schema.yml not found at %s: %v", schemaPath, err)
	} else {
		t.Logf("Found schema.yml at %s", schemaPath)
	}

	testsDir := filepath.Join(projectDir, "tests")
	if entries, err := os.ReadDir(testsDir); err != nil {
		t.Logf("WARNING: cannot read tests dir %s: %v", testsDir, err)
	} else {
		t.Logf("Tests dir contents (%d files): %v", len(entries), entries)
	}

	// Debug: check current working directory before discovery
	if cwd, err := os.Getwd(); err == nil {
		t.Logf("Current working directory at discovery: %s", cwd)
		t.Logf("Project directory: %s", projectDir)
		if cwd != projectDir {
			t.Logf("WARNING: Not in project directory for discovery!")
		}
	}

	// Discover all tests
	allTests, err := executor.DiscoverAllTests(cfg, registry)
	if err != nil {
		t.Fatalf("failed to discover tests: %v", err)
	}

	if len(allTests) == 0 {
		t.Fatalf("no tests discovered (project: %s, model-paths: %v, test-paths: %v)",
			projectDir, cfg.Project.ModelPaths, cfg.Project.TestPaths)
	}

	t.Logf("Discovered %d tests", len(allTests))

	// Create test engine
	templateEngine := template.New()
	failureStore := storage.NewSQLiteFailureStore(adapter)
	if err := failureStore.Initialize(ctx); err != nil {
		t.Fatalf("failed to initialize failure store: %v", err)
	}

	engine, err := executor.NewTestEngine(adapter, templateEngine, failureStore)
	if err != nil {
		t.Fatalf("failed to create test engine: %v", err)
	}

	// Execute tests
	summary, err := engine.ExecuteTests(ctx, allTests)
	if err != nil {
		t.Fatalf("failed to execute tests: %v", err)
	}

	// Verify summary
	if summary.TotalTests == 0 {
		t.Fatal("no tests executed")
	}

	t.Logf("Test Summary: Total=%d, Passed=%d, Failed=%d, Warnings=%d",
		summary.TotalTests, summary.PassedTests, summary.FailedTests, summary.WarningTests)

	// With valid data, all tests should pass
	AssertTestsPassed(t, summary)
}

// TestIntegration_NotNullTest verifies not_null test execution
func TestIntegration_NotNullTest(t *testing.T) {
	adapter, _ := CreateTestDatabase(t)
	defer adapter.Close()

	ctx := context.Background()

	// Create a table with NULL values
	err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE test_table (
			id INTEGER,
			name TEXT
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert data with NULLs
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO test_table (id, name) VALUES
		(1, 'Alice'),
		(2, NULL),
		(3, 'Charlie'),
		(4, NULL)
	`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Create not_null test
	notNullTest := &generic.NotNullTest{}
	testSQL, err := notNullTest.GenerateSQL("test_table", "name", nil)
	if err != nil {
		t.Fatalf("failed to generate SQL: %v", err)
	}

	testObj, err := test.NewTest(
		"not_null_test_table_name",
		"not_null",
		"test_table",
		"name",
		test.GenericTest,
		testSQL,
	)
	if err != nil {
		t.Fatalf("failed to create test: %v", err)
	}

	// Execute test
	templateEngine := template.New()
	failureStore := storage.NewSQLiteFailureStore(adapter)
	if err := failureStore.Initialize(ctx); err != nil {
		t.Fatalf("failed to initialize failure store: %v", err)
	}

	engine, err := executor.NewTestEngine(adapter, templateEngine, failureStore)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	result, err := engine.ExecuteTest(ctx, testObj)
	if err != nil {
		t.Fatalf("failed to execute test: %v", err)
	}

	// Verify failure count matches (2 NULL values)
	if result.FailureCount != 2 {
		t.Errorf("expected 2 failures, got %d", result.FailureCount)
	}

	if result.Status != test.StatusFailed {
		t.Errorf("expected status failed, got %s", result.Status)
	}
}

// TestIntegration_UniqueTest verifies unique test execution
func TestIntegration_UniqueTest(t *testing.T) {
	adapter, _ := CreateTestDatabase(t)
	defer adapter.Close()

	ctx := context.Background()

	// Create table with duplicate values
	err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE test_table (
			id INTEGER,
			email TEXT
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert duplicate emails
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO test_table (id, email) VALUES
		(1, 'alice@example.com'),
		(2, 'bob@example.com'),
		(3, 'alice@example.com'),
		(4, 'charlie@example.com'),
		(5, 'bob@example.com')
	`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Create unique test
	uniqueTest := &generic.UniqueTest{}
	testSQL, err := uniqueTest.GenerateSQL("test_table", "email", nil)
	if err != nil {
		t.Fatalf("failed to generate SQL: %v", err)
	}

	testObj, err := test.NewTest(
		"unique_test_table_email",
		"unique",
		"test_table",
		"email",
		test.GenericTest,
		testSQL,
	)
	if err != nil {
		t.Fatalf("failed to create test: %v", err)
	}

	// Execute test
	templateEngine := template.New()
	failureStore := storage.NewSQLiteFailureStore(adapter)
	if err := failureStore.Initialize(ctx); err != nil {
		t.Fatalf("failed to initialize failure store: %v", err)
	}

	engine, err := executor.NewTestEngine(adapter, templateEngine, failureStore)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	result, err := engine.ExecuteTest(ctx, testObj)
	if err != nil {
		t.Fatalf("failed to execute test: %v", err)
	}

	// Should detect duplicates (2 emails appear more than once)
	if result.FailureCount == 0 {
		t.Error("expected failures, got 0")
	}

	if result.Status != test.StatusFailed {
		t.Errorf("expected status failed, got %s", result.Status)
	}
}

// TestIntegration_AcceptedValuesTest verifies accepted_values test
func TestIntegration_AcceptedValuesTest(t *testing.T) {
	adapter, _ := CreateTestDatabase(t)
	defer adapter.Close()

	ctx := context.Background()

	// Create table with invalid status values
	err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE test_table (
			id INTEGER,
			status TEXT
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert data with invalid statuses
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO test_table (id, status) VALUES
		(1, 'active'),
		(2, 'inactive'),
		(3, 'suspended'),
		(4, 'active'),
		(5, 'deleted')
	`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Create accepted_values test
	acceptedValuesTest := &generic.AcceptedValuesTest{}
	config := map[string]interface{}{
		"values": []interface{}{"active", "inactive"},
	}

	testSQL, err := acceptedValuesTest.GenerateSQL("test_table", "status", config)
	if err != nil {
		t.Fatalf("failed to generate SQL: %v", err)
	}

	testObj, err := test.NewTest(
		"accepted_values_test_table_status",
		"accepted_values",
		"test_table",
		"status",
		test.GenericTest,
		testSQL,
	)
	if err != nil {
		t.Fatalf("failed to create test: %v", err)
	}

	// Execute test
	templateEngine := template.New()
	failureStore := storage.NewSQLiteFailureStore(adapter)
	if err := failureStore.Initialize(ctx); err != nil {
		t.Fatalf("failed to initialize failure store: %v", err)
	}

	engine, err := executor.NewTestEngine(adapter, templateEngine, failureStore)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	result, err := engine.ExecuteTest(ctx, testObj)
	if err != nil {
		t.Fatalf("failed to execute test: %v", err)
	}

	// Should detect 2 invalid values (suspended, deleted)
	if result.FailureCount != 2 {
		t.Errorf("expected 2 failures, got %d", result.FailureCount)
	}

	if result.Status != test.StatusFailed {
		t.Errorf("expected status failed, got %s", result.Status)
	}
}

// TestIntegration_RelationshipsTest verifies foreign key relationships
func TestIntegration_RelationshipsTest(t *testing.T) {
	adapter, _ := CreateTestDatabase(t)
	defer adapter.Close()

	ctx := context.Background()

	// Create parent and child tables
	err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT
		)
	`)
	if err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			user_id INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("failed to create orders table: %v", err)
	}

	// Insert data
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')
	`)
	if err != nil {
		t.Fatalf("failed to insert users: %v", err)
	}

	// Insert orders with invalid foreign key
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO orders (id, user_id) VALUES
		(1, 1),
		(2, 2),
		(3, 999)
	`)
	if err != nil {
		t.Fatalf("failed to insert orders: %v", err)
	}

	// Create relationships test
	relationshipsTest := &generic.RelationshipsTest{}
	config := map[string]interface{}{
		"to":    "users",
		"field": "id",
	}

	testSQL, err := relationshipsTest.GenerateSQL("orders", "user_id", config)
	if err != nil {
		t.Fatalf("failed to generate SQL: %v", err)
	}

	testObj, err := test.NewTest(
		"relationships_orders_user_id",
		"relationships",
		"orders",
		"user_id",
		test.GenericTest,
		testSQL,
	)
	if err != nil {
		t.Fatalf("failed to create test: %v", err)
	}

	// Execute test
	templateEngine := template.New()
	failureStore := storage.NewSQLiteFailureStore(adapter)
	if err := failureStore.Initialize(ctx); err != nil {
		t.Fatalf("failed to initialize failure store: %v", err)
	}

	engine, err := executor.NewTestEngine(adapter, templateEngine, failureStore)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	result, err := engine.ExecuteTest(ctx, testObj)
	if err != nil {
		t.Fatalf("failed to execute test: %v", err)
	}

	// Should detect 1 invalid foreign key (999)
	if result.FailureCount != 1 {
		t.Errorf("expected 1 failure, got %d", result.FailureCount)
	}

	if result.Status != test.StatusFailed {
		t.Errorf("expected status failed, got %s", result.Status)
	}
}

// TestIntegration_SingularTest verifies custom SQL tests
func TestIntegration_SingularTest(t *testing.T) {
	// Setup test project (includes singular test)
	projectDir := SetupTestProject(t)

	// Create database
	adapter, dbPath := CreateTestDatabase(t)
	defer adapter.Close()

	ctx := context.Background()

	// Load configuration
	cfg := LoadTestConfig(t, projectDir)
	cfg.Output.Database = dbPath

	// Reconnect
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("failed to reconnect: %v", err)
	}

	// Note: sample data already created by CreateTestDatabase()

	// Build models (required for test discovery)
	if err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE IF NOT EXISTS users AS
		SELECT id, name, email, status, created_at FROM raw_users
	`); err != nil {
		t.Fatalf("failed to create users model: %v", err)
	}

	if err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE IF NOT EXISTS orders AS
		SELECT id, user_id, total_amount, status, order_date FROM raw_orders
	`); err != nil {
		t.Fatalf("failed to create orders model: %v", err)
	}

	// Insert order with negative amount (will fail singular test)
	err := adapter.ExecuteDDL(ctx, `
		INSERT INTO raw_orders (id, user_id, total_amount, status)
		VALUES (100, 1, -50.00, 'completed')
	`)
	if err != nil {
		t.Fatalf("failed to insert invalid order: %v", err)
	}

	// Create registry
	registry := generic.NewDefaultRegistry()

	// Discover tests (should include singular test)
	allTests, err := executor.DiscoverAllTests(cfg, registry)
	if err != nil {
		t.Fatalf("failed to discover tests: %v", err)
	}

	// Find singular test
	var singularTest *test.Test
	for _, tst := range allTests {
		if tst.Type == test.SingularTest {
			singularTest = tst
			break
		}
	}

	if singularTest == nil {
		t.Fatal("singular test not found")
	}

	// Execute singular test
	templateEngine := template.New()
	failureStore := storage.NewSQLiteFailureStore(adapter)
	if err := failureStore.Initialize(ctx); err != nil {
		t.Fatalf("failed to initialize failure store: %v", err)
	}

	engine, err := executor.NewTestEngine(adapter, templateEngine, failureStore)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	result, err := engine.ExecuteTest(ctx, singularTest)
	if err != nil {
		t.Fatalf("failed to execute test: %v", err)
	}

	// Should detect 1 invalid order (negative amount)
	if result.FailureCount != 1 {
		t.Errorf("expected 1 failure, got %d", result.FailureCount)
	}

	if result.Status != test.StatusFailed {
		t.Errorf("expected status failed, got %s", result.Status)
	}
}

// TestIntegration_ThresholdEvaluation verifies error_if/warn_if
func TestIntegration_ThresholdEvaluation(t *testing.T) {
	adapter, _ := CreateTestDatabase(t)
	defer adapter.Close()

	ctx := context.Background()

	// Create table
	err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE test_table (
			id INTEGER,
			name TEXT
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert data with 3 NULLs
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO test_table (id, name) VALUES
		(1, 'Alice'),
		(2, NULL),
		(3, NULL),
		(4, 'Bob'),
		(5, NULL)
	`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Create not_null test with warn_if threshold
	notNullTest := &generic.NotNullTest{}
	testSQL, err := notNullTest.GenerateSQL("test_table", "name", nil)
	if err != nil {
		t.Fatalf("failed to generate SQL: %v", err)
	}

	testObj, err := test.NewTest(
		"not_null_test_table_name",
		"not_null",
		"test_table",
		"name",
		test.GenericTest,
		testSQL,
	)
	if err != nil {
		t.Fatalf("failed to create test: %v", err)
	}

	// Set warn_if threshold: warn if failures > 2
	testObj.Config.WarnIf = &test.ConditionalThreshold{
		Operator: test.OperatorGreaterThan,
		Value:    2,
	}

	// Execute test
	templateEngine := template.New()
	failureStore := storage.NewSQLiteFailureStore(adapter)
	if err := failureStore.Initialize(ctx); err != nil {
		t.Fatalf("failed to initialize failure store: %v", err)
	}

	engine, err := executor.NewTestEngine(adapter, templateEngine, failureStore)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	result, err := engine.ExecuteTest(ctx, testObj)
	if err != nil {
		t.Fatalf("failed to execute test: %v", err)
	}

	// Should have 3 failures but status should be warning (not error)
	if result.FailureCount != 3 {
		t.Errorf("expected 3 failures, got %d", result.FailureCount)
	}

	if result.Status != test.StatusWarning {
		t.Errorf("expected status warning, got %s", result.Status)
	}
}

// TestIntegration_TestSelection verifies --select and --exclude
func TestIntegration_TestSelection(t *testing.T) {
	// Setup test project
	projectDir := SetupTestProject(t)

	// Create database
	adapter, dbPath := CreateTestDatabase(t)
	defer adapter.Close()

	ctx := context.Background()

	// Load configuration
	cfg := LoadTestConfig(t, projectDir)
	cfg.Output.Database = dbPath

	// Reconnect
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("failed to reconnect: %v", err)
	}

	// Note: sample data already created by CreateTestDatabase()

	// Build models (required for test discovery)
	if err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE IF NOT EXISTS users AS
		SELECT id, name, email, status, created_at FROM raw_users
	`); err != nil {
		t.Fatalf("failed to create users model: %v", err)
	}

	if err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE IF NOT EXISTS orders AS
		SELECT id, user_id, total_amount, status, order_date FROM raw_orders
	`); err != nil {
		t.Fatalf("failed to create orders model: %v", err)
	}

	// Create registry
	registry := generic.NewDefaultRegistry()

	// Discover all tests
	allTests, err := executor.DiscoverAllTests(cfg, registry)
	if err != nil {
		t.Fatalf("failed to discover tests: %v", err)
	}

	totalTests := len(allTests)
	t.Logf("Total tests: %d", totalTests)

	// Test 1: Select only not_null tests
	selector1 := executor.NewTestSelector([]string{"not_null_*"}, nil, nil, nil)
	selected1 := selector1.Filter(allTests)

	t.Logf("Selected not_null tests: %d", len(selected1))

	for _, tst := range selected1 {
		if tst.Name != "not_null" {
			t.Errorf("expected only not_null tests, found: %s", tst.Name)
		}
	}

	// Test 2: Exclude unique tests
	selector2 := executor.NewTestSelector(nil, []string{"unique_*"}, nil, nil)
	selected2 := selector2.Filter(allTests)

	t.Logf("Tests after excluding unique: %d", len(selected2))

	for _, tst := range selected2 {
		if tst.Name == "unique" {
			t.Error("found unique test that should have been excluded")
		}
	}

	// Test 3: Select tests for specific model
	selector3 := executor.NewTestSelector(nil, nil, nil, []string{"users"})
	selected3 := selector3.Filter(allTests)

	t.Logf("Tests for users model: %d", len(selected3))

	for _, tst := range selected3 {
		if tst.ModelName != "users" {
			t.Errorf("expected only users tests, found: %s", tst.ModelName)
		}
	}
}
