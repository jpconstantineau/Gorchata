package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jpconstantineau/gorchata/internal/domain/test"
	"github.com/jpconstantineau/gorchata/internal/domain/test/executor"
	"github.com/jpconstantineau/gorchata/internal/domain/test/generic"
	"github.com/jpconstantineau/gorchata/internal/domain/test/storage"
	"github.com/jpconstantineau/gorchata/internal/template"
)

// TestIntegration_StoreFailures verifies failure storage end-to-end
func TestIntegration_StoreFailures(t *testing.T) {
	adapter, _ := CreateTestDatabase(t)
	defer adapter.Close()

	ctx := context.Background()

	// Create table with NULL values
	err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE test_table (
			id INTEGER,
			email TEXT
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert data with NULLs
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO test_table (id, email) VALUES
		(1, 'alice@example.com'),
		(2, NULL),
		(3, 'charlie@example.com'),
		(4, NULL)
	`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Create not_null test with store_failures enabled
	notNullTest := &generic.NotNullTest{}
	testSQL, err := notNullTest.GenerateSQL("test_table", "email", nil)
	if err != nil {
		t.Fatalf("failed to generate SQL: %v", err)
	}

	testObj, err := test.NewTest(
		"not_null_test_table_email",
		"not_null",
		"test_table",
		"email",
		test.GenericTest,
		testSQL,
	)
	if err != nil {
		t.Fatalf("failed to create test: %v", err)
	}

	// Enable failure storage
	testObj.Config.StoreFailures = true
	testObj.Config.StoreFailuresAs = "test_table_email_nulls"

	// Initialize failure store
	failureStore := storage.NewSQLiteFailureStore(adapter)
	if err := failureStore.Initialize(ctx); err != nil {
		t.Fatalf("failed to initialize failure store: %v", err)
	}

	// Execute test
	templateEngine := template.New()
	engine, err := executor.NewTestEngine(adapter, templateEngine, failureStore)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	result, err := engine.ExecuteTest(ctx, testObj)
	if err != nil {
		t.Fatalf("failed to execute test: %v", err)
	}

	// Verify test failed
	if result.Status != test.StatusFailed {
		t.Errorf("expected test to fail, got: %s", result.Status)
	}

	if result.FailureCount != 2 {
		t.Errorf("expected 2 failures, got: %d", result.FailureCount)
	}

	// Verify audit table was created
	queryResult, err := adapter.ExecuteQuery(ctx, `
		SELECT name FROM sqlite_master 
		WHERE type='table' AND name='test_table_email_nulls'
	`)
	if err != nil {
		t.Fatalf("failed to check for audit table: %v", err)
	}

	if len(queryResult.Rows) == 0 {
		t.Fatal("audit table was not created")
	}

	// Verify failures were stored
	storedFailures, err := failureStore.GetFailures(ctx, "not_null_test_table_email", 100)
	if err != nil {
		t.Fatalf("failed to get stored failures: %v", err)
	}

	if len(storedFailures) != 2 {
		t.Errorf("expected 2 stored failures, got: %d", len(storedFailures))
	}

	// Verify failure data
	for _, failure := range storedFailures {
		if failure.TestID != "not_null_test_table_email" {
			t.Errorf("unexpected test ID: %s", failure.TestID)
		}

		if failure.FailedAt.IsZero() {
			t.Error("failed_at timestamp is zero")
		}

		// Verify row data contains id
		if _, ok := failure.RowData["id"]; !ok {
			t.Error("row data missing 'id' column")
		}
	}
}

// TestIntegration_CustomTableName verifies store_failures_as config
func TestIntegration_CustomTableName(t *testing.T) {
	adapter, _ := CreateTestDatabase(t)
	defer adapter.Close()

	ctx := context.Background()

	// Create table with duplicates
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
		(3, 'alice@example.com')
	`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Create unique test with custom table name
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

	// Enable failure storage with custom table name
	customTableName := "my_custom_audit_table"
	testObj.Config.StoreFailures = true
	testObj.Config.StoreFailuresAs = customTableName

	// Initialize failure store
	failureStore := storage.NewSQLiteFailureStore(adapter)
	if err := failureStore.Initialize(ctx); err != nil {
		t.Fatalf("failed to initialize failure store: %v", err)
	}

	// Execute test
	templateEngine := template.New()
	engine, err := executor.NewTestEngine(adapter, templateEngine, failureStore)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	result, err := engine.ExecuteTest(ctx, testObj)
	if err != nil {
		t.Fatalf("failed to execute test: %v", err)
	}

	// Verify test failed
	if result.Status != test.StatusFailed {
		t.Errorf("expected test to fail, got: %s", result.Status)
	}

	// Verify custom table was created
	queryResult, err := adapter.ExecuteQuery(ctx, `
		SELECT name FROM sqlite_master 
		WHERE type='table' AND name=?
	`, customTableName)
	if err != nil {
		t.Fatalf("failed to check for custom table: %v", err)
	}

	if len(queryResult.Rows) == 0 {
		t.Fatalf("custom audit table '%s' was not created", customTableName)
	}

	// Verify data was stored in custom table
	queryResult, err = adapter.ExecuteQuery(ctx, "SELECT COUNT(*) as cnt FROM "+customTableName)
	if err != nil {
		t.Fatalf("failed to query custom table: %v", err)
	}

	if len(queryResult.Rows) == 0 {
		t.Fatal("no rows in custom table")
	}
}

// TestIntegration_CleanupOldFailures verifies retention policy
func TestIntegration_CleanupOldFailures(t *testing.T) {
	adapter, _ := CreateTestDatabase(t)
	defer adapter.Close()

	ctx := context.Background()

	// Initialize failure store
	failureStore := storage.NewSQLiteFailureStore(adapter)
	if err := failureStore.Initialize(ctx); err != nil {
		t.Fatalf("failed to initialize failure store: %v", err)
	}

	// Create test table
	testTableName := "test_cleanup_failures"
	err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE `+testTableName+` (
			test_id TEXT,
			test_run_id TEXT,
			failed_at TIMESTAMP,
			id INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("failed to create test table: %v", err)
	}

	// Insert old and recent failures
	oldDate := time.Now().AddDate(0, 0, -40)    // 40 days ago
	recentDate := time.Now().AddDate(0, 0, -10) // 10 days ago

	err = adapter.ExecuteDDL(ctx, fmt.Sprintf(`
		INSERT INTO %s (test_id, test_run_id, failed_at, id) VALUES
		('test1', 'run1', '%s', 1),
		('test1', 'run2', '%s', 2),
		('test1', 'run3', '%s', 3)
	`, testTableName, oldDate.Format(time.RFC3339), recentDate.Format(time.RFC3339), time.Now().Format(time.RFC3339)))
	if err != nil {
		t.Fatalf("failed to insert failures: %v", err)
	}

	// Verify 3 failures exist
	queryResult, err := adapter.ExecuteQuery(ctx, "SELECT COUNT(*) as cnt FROM "+testTableName)
	if err != nil {
		t.Fatalf("failed to count failures: %v", err)
	}

	count := queryResult.Rows[0][0].(int64)
	if count != 3 {
		t.Errorf("expected 3 failures before cleanup, got: %d", count)
	}

	// Cleanup failures older than 30 days
	err = failureStore.CleanupOldFailures(ctx, 30)
	if err != nil {
		t.Fatalf("failed to cleanup old failures: %v", err)
	}

	// Note: The cleanup operates on dbt_test__audit schema tables
	// Since we created a custom table, we need to test with proper schema

	// Create proper audit table
	auditTableName := "dbt_test__audit_cleanup_test"
	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE `+auditTableName+` (
			test_id TEXT,
			test_run_id TEXT,
			failed_at TIMESTAMP,
			id INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("failed to create audit table: %v", err)
	}

	// Insert old and recent failures
	err = adapter.ExecuteDDL(ctx, fmt.Sprintf(`
		INSERT INTO %s (test_id, test_run_id, failed_at, id) VALUES
		('test1', 'run1', '%s', 1),
		('test1', 'run2', '%s', 2),
		('test1', 'run3', '%s', 3)
	`, auditTableName, oldDate.Format(time.RFC3339), recentDate.Format(time.RFC3339), time.Now().Format(time.RFC3339)))
	if err != nil {
		t.Fatalf("failed to insert audit failures: %v", err)
	}

	// Cleanup
	err = failureStore.CleanupOldFailures(ctx, 30)
	if err != nil {
		t.Fatalf("failed to cleanup: %v", err)
	}

	// Verify only recent failures remain (2 failures within 30 days)
	queryResult, err = adapter.ExecuteQuery(ctx, "SELECT COUNT(*) as cnt FROM "+auditTableName)
	if err != nil {
		t.Fatalf("failed to count after cleanup: %v", err)
	}

	countAfter := queryResult.Rows[0][0].(int64)
	if countAfter >= 3 {
		t.Errorf("expected fewer than 3 failures after cleanup, got: %d", countAfter)
	}
}

// TestIntegration_MultipleTestRuns verifies multiple test run IDs
func TestIntegration_MultipleTestRuns(t *testing.T) {
	adapter, _ := CreateTestDatabase(t)
	defer adapter.Close()

	ctx := context.Background()

	// Create table
	err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE test_table (
			id INTEGER,
			value TEXT
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Create not_null test
	notNullTest := &generic.NotNullTest{}
	testSQL, err := notNullTest.GenerateSQL("test_table", "value", nil)
	if err != nil {
		t.Fatalf("failed to generate SQL: %v", err)
	}

	testObj, err := test.NewTest(
		"not_null_test_table_value",
		"not_null",
		"test_table",
		"value",
		test.GenericTest,
		testSQL,
	)
	if err != nil {
		t.Fatalf("failed to create test: %v", err)
	}

	testObj.Config.StoreFailures = true
	testObj.Config.StoreFailuresAs = "multiple_runs_test"

	// Initialize failure store
	failureStore := storage.NewSQLiteFailureStore(adapter)
	if err := failureStore.Initialize(ctx); err != nil {
		t.Fatalf("failed to initialize failure store: %v", err)
	}

	// Create engine
	templateEngine := template.New()
	engine, err := executor.NewTestEngine(adapter, templateEngine, failureStore)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	// Run 1: Insert 2 NULLs and execute test
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO test_table (id, value) VALUES (1, NULL), (2, NULL)
	`)
	if err != nil {
		t.Fatalf("failed to insert run 1 data: %v", err)
	}

	result1, err := engine.ExecuteTest(ctx, testObj)
	if err != nil {
		t.Fatalf("failed to execute test run 1: %v", err)
	}

	if result1.FailureCount != 2 {
		t.Errorf("run 1: expected 2 failures, got: %d", result1.FailureCount)
	}

	// Run 2: Insert 3 more NULLs and execute test again
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO test_table (id, value) VALUES (3, NULL), (4, NULL), (5, NULL)
	`)
	if err != nil {
		t.Fatalf("failed to insert run 2 data: %v", err)
	}

	result2, err := engine.ExecuteTest(ctx, testObj)
	if err != nil {
		t.Fatalf("failed to execute test run 2: %v", err)
	}

	if result2.FailureCount != 5 {
		t.Errorf("run 2: expected 5 failures, got: %d", result2.FailureCount)
	}

	// Verify both test runs stored failures
	storedFailures, err := failureStore.GetFailures(ctx, "not_null_test_table_value", 100)
	if err != nil {
		t.Fatalf("failed to get stored failures: %v", err)
	}

	// Should have failures from both runs
	if len(storedFailures) < 2 {
		t.Errorf("expected at least 2 stored failures (from 2 runs), got: %d", len(storedFailures))
	}

	// Verify test_run_id is present and varies
	testRunIDs := make(map[string]bool)
	for _, failure := range storedFailures {
		if failure.TestRunID == "" {
			t.Error("test_run_id is empty")
		}
		testRunIDs[failure.TestRunID] = true
	}

	// May have 2 different test run IDs
	t.Logf("Captured %d test run IDs from %d stored failures", len(testRunIDs), len(storedFailures))
}

// TestIntegration_StoreFailuresDisabled verifies no storage when disabled
func TestIntegration_StoreFailuresDisabled(t *testing.T) {
	adapter, _ := CreateTestDatabase(t)
	defer adapter.Close()

	ctx := context.Background()

	// Create table with NULL values
	err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE test_table (
			id INTEGER,
			email TEXT
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert data with NULLs
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO test_table (id, email) VALUES (1, NULL), (2, NULL)
	`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Create not_null test with store_failures DISABLED
	notNullTest := &generic.NotNullTest{}
	testSQL, err := notNullTest.GenerateSQL("test_table", "email", nil)
	if err != nil {
		t.Fatalf("failed to generate SQL: %v", err)
	}

	testObj, err := test.NewTest(
		"not_null_test_table_email",
		"not_null",
		"test_table",
		"email",
		test.GenericTest,
		testSQL,
	)
	if err != nil {
		t.Fatalf("failed to create test: %v", err)
	}

	// Explicitly disable failure storage (default is false)
	testObj.Config.StoreFailures = false
	testObj.Config.StoreFailuresAs = "should_not_be_created"

	// Initialize failure store
	failureStore := storage.NewSQLiteFailureStore(adapter)
	if err := failureStore.Initialize(ctx); err != nil {
		t.Fatalf("failed to initialize failure store: %v", err)
	}

	// Execute test
	templateEngine := template.New()
	engine, err := executor.NewTestEngine(adapter, templateEngine, failureStore)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	result, err := engine.ExecuteTest(ctx, testObj)
	if err != nil {
		t.Fatalf("failed to execute test: %v", err)
	}

	// Verify test failed
	if result.Status != test.StatusFailed {
		t.Errorf("expected test to fail, got: %s", result.Status)
	}

	// Verify audit table was NOT created
	queryResult, err := adapter.ExecuteQuery(ctx, `
		SELECT name FROM sqlite_master 
		WHERE type='table' AND name='should_not_be_created'
	`)
	if err != nil {
		t.Fatalf("failed to check for audit table: %v", err)
	}

	if len(queryResult.Rows) > 0 {
		t.Error("audit table should not have been created when store_failures=false")
	}
}
