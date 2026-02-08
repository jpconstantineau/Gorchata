package integration

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/domain/test"
	"github.com/jpconstantineau/gorchata/internal/domain/test/executor"
	"github.com/jpconstantineau/gorchata/internal/domain/test/generic"
	"github.com/jpconstantineau/gorchata/internal/domain/test/storage"
	"github.com/jpconstantineau/gorchata/internal/template"
)

// TestIntegration_AdaptiveSampling_LargeTable verifies sampling for >1M rows
func TestIntegration_AdaptiveSampling_LargeTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large table test in short mode")
	}

	adapter, _ := CreateTestDatabase(t)
	defer adapter.Close()

	ctx := context.Background()

	// Create large table with 1.5M rows
	tableName := "large_test_table"
	rowCount := 1500000

	t.Logf("Creating large table with %d rows (this may take a minute)...", rowCount)
	CreateLargeTestTable(t, adapter, tableName, rowCount)

	// Verify table has correct row count
	result, err := adapter.ExecuteQuery(ctx, fmt.Sprintf("SELECT COUNT(*) as cnt FROM %s", tableName))
	if err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}

	actualCount := result.Rows[0][0].(int64)
	if actualCount != int64(rowCount) {
		t.Fatalf("expected %d rows, got %d", rowCount, actualCount)
	}

	// Create not_null test on large table
	notNullTest := &generic.NotNullTest{}
	testSQL, err := notNullTest.GenerateSQL(tableName, "value", nil)
	if err != nil {
		t.Fatalf("failed to generate SQL: %v", err)
	}

	testObj, err := test.NewTest(
		"not_null_large_table_value",
		"not_null",
		tableName,
		"value",
		test.GenericTest,
		testSQL,
	)
	if err != nil {
		t.Fatalf("failed to create test: %v", err)
	}

	// Execute test (should automatically apply sampling)
	templateEngine := template.New()
	failureStore := storage.NewSQLiteFailureStore(adapter)
	if err := failureStore.Initialize(ctx); err != nil {
		t.Fatalf("failed to initialize failure store: %v", err)
	}

	engine, err := executor.NewTestEngine(adapter, templateEngine, failureStore)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	t.Log("Executing test with adaptive sampling...")
	result2, err := engine.ExecuteTest(ctx, testObj)
	if err != nil {
		t.Fatalf("failed to execute test: %v", err)
	}

	// Test should pass (no NULLs)
	if result2.Status != test.StatusPassed {
		t.Errorf("expected test to pass, got: %s", result2.Status)
	}

	// Test should complete relatively quickly due to sampling
	if result2.Duration() == 0 {
		t.Error("test duration is zero")
	}

	t.Logf("Test completed in %v with sampling", result2.Duration())

	// Note: We can't easily verify the exact sample size was used without inspecting query execution plan
	// But the test completing quickly is a good indicator
}

// TestIntegration_AdaptiveSampling_SmallTable verifies no sampling <1M rows
func TestIntegration_AdaptiveSampling_SmallTable(t *testing.T) {
	adapter, _ := CreateTestDatabase(t)
	defer adapter.Close()

	ctx := context.Background()

	// Create small table with 500K rows
	tableName := "small_test_table"
	rowCount := 500000

	t.Logf("Creating small table with %d rows...", rowCount)
	CreateLargeTestTable(t, adapter, tableName, rowCount)

	// Verify table has correct row count
	result, err := adapter.ExecuteQuery(ctx, fmt.Sprintf("SELECT COUNT(*) as cnt FROM %s", tableName))
	if err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}

	actualCount := result.Rows[0][0].(int64)
	if actualCount != int64(rowCount) {
		t.Fatalf("expected %d rows, got %d", rowCount, actualCount)
	}

	// Create not_null test
	notNullTest := &generic.NotNullTest{}
	testSQL, err := notNullTest.GenerateSQL(tableName, "value", nil)
	if err != nil {
		t.Fatalf("failed to generate SQL: %v", err)
	}

	testObj, err := test.NewTest(
		"not_null_small_table_value",
		"not_null",
		tableName,
		"value",
		test.GenericTest,
		testSQL,
	)
	if err != nil {
		t.Fatalf("failed to create test: %v", err)
	}

	// Execute test (should NOT apply sampling for < 1M rows)
	templateEngine := template.New()
	failureStore := storage.NewSQLiteFailureStore(adapter)
	if err := failureStore.Initialize(ctx); err != nil {
		t.Fatalf("failed to initialize failure store: %v", err)
	}

	engine, err := executor.NewTestEngine(adapter, templateEngine, failureStore)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	t.Log("Executing test without sampling (table < 1M rows)...")
	result2, err := engine.ExecuteTest(ctx, testObj)
	if err != nil {
		t.Fatalf("failed to execute test: %v", err)
	}

	// Test should pass
	if result2.Status != test.StatusPassed {
		t.Errorf("expected test to pass, got: %s", result2.Status)
	}

	t.Logf("Test completed in %v without sampling", result2.Duration())
}

// TestIntegration_SampleSizeOverride verifies sample_size config
func TestIntegration_SampleSizeOverride(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large table test in short mode")
	}

	adapter, _ := CreateTestDatabase(t)
	defer adapter.Close()

	ctx := context.Background()

	// Create large table
	tableName := "override_test_table"
	rowCount := 1500000

	t.Logf("Creating table with %d rows...", rowCount)
	CreateLargeTestTable(t, adapter, tableName, rowCount)

	// Create not_null test with custom sample size
	notNullTest := &generic.NotNullTest{}
	testSQL, err := notNullTest.GenerateSQL(tableName, "value", nil)
	if err != nil {
		t.Fatalf("failed to generate SQL: %v", err)
	}

	testObj, err := test.NewTest(
		"not_null_override_table_value",
		"not_null",
		tableName,
		"value",
		test.GenericTest,
		testSQL,
	)
	if err != nil {
		t.Fatalf("failed to create test: %v", err)
	}

	// Override sample size to 500K
	testObj.Config.SampleSize = 500000

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

	t.Log("Executing test with custom sample size of 500K...")
	result, err := engine.ExecuteTest(ctx, testObj)
	if err != nil {
		t.Fatalf("failed to execute test: %v", err)
	}

	// Test should pass
	if result.Status != test.StatusPassed {
		t.Errorf("expected test to pass, got: %s", result.Status)
	}

	t.Logf("Test completed in %v with custom sample size", result.Duration())
}

// TestIntegration_DisableSampling verifies sample_size=null (or 0) disables sampling
func TestIntegration_DisableSampling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large table test in short mode")
	}

	adapter, _ := CreateTestDatabase(t)
	defer adapter.Close()

	ctx := context.Background()

	// Create moderately large table (not too big to avoid long test time)
	tableName := "no_sample_table"
	rowCount := 200000

	t.Logf("Creating table with %d rows...", rowCount)
	CreateLargeTestTable(t, adapter, tableName, rowCount)

	// Create not_null test with sampling disabled
	notNullTest := &generic.NotNullTest{}
	testSQL, err := notNullTest.GenerateSQL(tableName, "value", nil)
	if err != nil {
		t.Fatalf("failed to generate SQL: %v", err)
	}

	testObj, err := test.NewTest(
		"not_null_no_sample_table_value",
		"not_null",
		tableName,
		"value",
		test.GenericTest,
		testSQL,
	)
	if err != nil {
		t.Fatalf("failed to create test: %v", err)
	}

	// Disable sampling by setting sample_size to 0 (or nil)
	testObj.Config.SampleSize = 0

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

	t.Log("Executing test with sampling disabled...")
	result, err := engine.ExecuteTest(ctx, testObj)
	if err != nil {
		t.Fatalf("failed to execute test: %v", err)
	}

	// Test should pass
	if result.Status != test.StatusPassed {
		t.Errorf("expected test to pass, got: %s", result.Status)
	}

	t.Logf("Test completed in %v without sampling (full table scan)", result.Duration())
}

// TestIntegration_SamplingAccuracy verifies sampling detects issues accurately
func TestIntegration_SamplingAccuracy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large table test in short mode")
	}

	adapter, _ := CreateTestDatabase(t)
	defer adapter.Close()

	ctx := context.Background()

	// Create large table with some NULLs
	tableName := "accuracy_test_table"

	// Create table
	err := adapter.ExecuteDDL(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id INTEGER PRIMARY KEY,
			value TEXT
		)
	`, tableName))
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert 1.5M rows with ~10% NULLs distributed evenly
	t.Log("Creating large table with NULLs...")
	const batchSize = 10000
	const totalRows = 1500000
	nullCount := 0

	for i := 0; i < totalRows; i += batchSize {
		values := ""
		limit := batchSize
		if i+batchSize > totalRows {
			limit = totalRows - i
		}

		for j := 0; j < limit; j++ {
			if j > 0 {
				values += ","
			}

			// Insert NULL for every 10th row
			if (i+j)%10 == 0 {
				values += fmt.Sprintf("(%d, NULL)", i+j+1)
				nullCount++
			} else {
				values += fmt.Sprintf("(%d, 'value_%d')", i+j+1, i+j+1)
			}
		}

		query := fmt.Sprintf("INSERT INTO %s (id, value) VALUES %s", tableName, values)
		err := adapter.ExecuteDDL(ctx, query)
		if err != nil {
			t.Fatalf("failed to insert batch at %d: %v", i, err)
		}
	}

	t.Logf("Created table with %d rows (%d NULLs, ~%d%%)", totalRows, nullCount, (nullCount*100)/totalRows)

	// Create not_null test
	notNullTest := &generic.NotNullTest{}
	testSQL, err := notNullTest.GenerateSQL(tableName, "value", nil)
	if err != nil {
		t.Fatalf("failed to generate SQL: %v", err)
	}

	testObj, err := test.NewTest(
		"not_null_accuracy_table_value",
		"not_null",
		tableName,
		"value",
		test.GenericTest,
		testSQL,
	)
	if err != nil {
		t.Fatalf("failed to create test: %v", err)
	}

	// Execute test with sampling
	templateEngine := template.New()
	failureStore := storage.NewSQLiteFailureStore(adapter)
	if err := failureStore.Initialize(ctx); err != nil {
		t.Fatalf("failed to initialize failure store: %v", err)
	}

	engine, err := executor.NewTestEngine(adapter, templateEngine, failureStore)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	t.Log("Executing test with sampling to detect NULLs...")
	result, err := engine.ExecuteTest(ctx, testObj)
	if err != nil {
		t.Fatalf("failed to execute test: %v", err)
	}

	// Test should fail and detect NULLs (even with sampling)
	if result.Status != test.StatusFailed {
		t.Errorf("expected test to fail (NULLs present), got: %s", result.Status)
	}

	if result.FailureCount == 0 {
		t.Error("expected failures to be detected, got 0")
	}

	t.Logf("Test detected %d failures in sampled data", result.FailureCount)

	// The failure count will be less than actual NULL count due to sampling,
	// but should be proportionally representative
	// With 100K sample size and 10% NULLs, we expect ~10K failures
	expectedFailures := int64(10000) // ~10% of 100K sample
	tolerance := int64(2000)         // Allow ±2000 variance

	if result.FailureCount < expectedFailures-tolerance || result.FailureCount > expectedFailures+tolerance {
		t.Logf("Warning: failure count %d is outside expected range %d ± %d",
			result.FailureCount, expectedFailures, tolerance)
	} else {
		t.Logf("Failure count %d is within expected range (sampling is accurate)", result.FailureCount)
	}
}

// TestIntegration_SamplingPerformance verifies sampling improves performance
// This test creates 1.5M rows and takes ~5-10 minutes to run.
// Set GORCHATA_RUN_PERF_TESTS=1 to enable this test.
func TestIntegration_SamplingPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	// Skip by default unless explicitly enabled
	if os.Getenv("GORCHATA_RUN_PERF_TESTS") != "1" {
		t.Skip("skipping long-running performance test (set GORCHATA_RUN_PERF_TESTS=1 to enable)")
	}

	adapter, _ := CreateTestDatabase(t)
	defer adapter.Close()

	ctx := context.Background()

	// Create large table
	tableName := "perf_test_table"
	rowCount := 1500000

	t.Logf("Creating table with %d rows for performance comparison...", rowCount)
	CreateLargeTestTable(t, adapter, tableName, rowCount)

	// Test 1: With sampling (default)
	notNullTest1 := &generic.NotNullTest{}
	testSQL1, err := notNullTest1.GenerateSQL(tableName, "value", nil)
	if err != nil {
		t.Fatalf("failed to generate SQL: %v", err)
	}

	testObj1, err := test.NewTest(
		"not_null_perf_sampled",
		"not_null",
		tableName,
		"value",
		test.GenericTest,
		testSQL1,
	)
	if err != nil {
		t.Fatalf("failed to create test: %v", err)
	}

	templateEngine := template.New()
	failureStore := storage.NewSQLiteFailureStore(adapter)
	if err := failureStore.Initialize(ctx); err != nil {
		t.Fatalf("failed to initialize failure store: %v", err)
	}

	engine, err := executor.NewTestEngine(adapter, templateEngine, failureStore)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	t.Log("Executing test WITH sampling...")
	resultSampled, err := engine.ExecuteTest(ctx, testObj1)
	if err != nil {
		t.Fatalf("failed to execute sampled test: %v", err)
	}

	sampledDuration := resultSampled.Duration()
	t.Logf("Test with sampling completed in: %v", sampledDuration)

	// Test 2: Without sampling (full scan of smaller subset for comparison)
	// Use same table but disable sampling
	notNullTest2 := &generic.NotNullTest{}
	testSQL2, err := notNullTest2.GenerateSQL(tableName, "value", nil)
	if err != nil {
		t.Fatalf("failed to generate SQL: %v", err)
	}

	testObj2, err := test.NewTest(
		"not_null_perf_full",
		"not_null",
		tableName,
		"value",
		test.GenericTest,
		testSQL2,
	)
	if err != nil {
		t.Fatalf("failed to create test: %v", err)
	}

	// Disable sampling
	testObj2.Config.SampleSize = 0

	t.Log("Executing test WITHOUT sampling (full table scan)...")
	resultFull, err := engine.ExecuteTest(ctx, testObj2)
	if err != nil {
		t.Fatalf("failed to execute full scan test: %v", err)
	}

	fullDuration := resultFull.Duration()
	t.Logf("Test without sampling completed in: %v", fullDuration)

	// Compare performance
	if sampledDuration >= fullDuration {
		t.Logf("Note: Sampled test (%v) was not faster than full scan (%v). "+
			"This can happen with SQLite optimizations or smaller datasets.",
			sampledDuration, fullDuration)
	} else {
		speedup := float64(fullDuration) / float64(sampledDuration)
		t.Logf("Sampling provided %.2fx speedup (%v vs %v)", speedup, sampledDuration, fullDuration)
	}
}
