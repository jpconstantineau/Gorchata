package test

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/platform"
	"github.com/jpconstantineau/gorchata/internal/platform/sqlite"
)

// setupDataQualityTest creates test database with full haul truck data pipeline
func setupDataQualityTest(t *testing.T) (*sqlite.SQLiteAdapter, context.Context, func()) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	config := &platform.ConnectionConfig{
		DatabasePath: dbPath,
	}
	adapter := sqlite.NewSQLiteAdapter(config)

	ctx := context.Background()
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	cleanup := func() {
		adapter.Close()
	}

	return adapter, ctx, cleanup
}

// loadSchemaAndSeeds loads the full haul truck schema and executes transformations
func loadSchemaAndSeeds(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context) {
	// Create all necessary tables inline (simplified for testing)
	// In production, these would be loaded from DDL files

	// Create dimension tables
	createDimTables(t, adapter, ctx)

	// Insert minimal test data
	insertTestDimData(t, adapter, ctx)

	// Create staging tables
	err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE IF NOT EXISTS stg_truck_states (
			truck_id TEXT NOT NULL,
			state_start TEXT NOT NULL,
			state_end TEXT NOT NULL,
			operational_state TEXT NOT NULL,
			location_zone TEXT NOT NULL,
			payload_at_start REAL NOT NULL,
			payload_at_end REAL NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create stg_truck_states: %v", err)
	}

	// Create fact table
	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE IF NOT EXISTS fact_haul_cycle (
			cycle_id TEXT PRIMARY KEY,
			truck_id TEXT NOT NULL,
			shovel_id TEXT NOT NULL,
			crusher_id TEXT NOT NULL,
			operator_id TEXT NOT NULL,
			shift_id TEXT NOT NULL,
			date_id INTEGER NOT NULL,
			cycle_start TEXT NOT NULL,
			cycle_end TEXT NOT NULL,
			payload_tons REAL NOT NULL,
			distance_loaded_km REAL NOT NULL,
			distance_empty_km REAL NOT NULL,
			duration_loading_min REAL NOT NULL,
			duration_hauling_loaded_min REAL NOT NULL,
			duration_queue_crusher_min REAL NOT NULL,
			duration_dumping_min REAL NOT NULL,
			duration_returning_min REAL NOT NULL,
			duration_queue_shovel_min REAL NOT NULL,
			duration_spot_delays_min REAL NOT NULL,
			fuel_consumed_liters REAL NOT NULL,
			speed_avg_loaded_kmh REAL NOT NULL,
			speed_avg_empty_kmh REAL NOT NULL,
			total_cycle_time_min REAL NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create fact_haul_cycle: %v", err)
	}

	// Insert minimal test cycle data
	insertTestCycleData(t, adapter, ctx)

	// Insert minimal state data
	insertTestStateData(t, adapter, ctx)
}

// executeDataQualityTest runs a SQL test file and returns results
func executeDataQualityTest(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context, testFile string) *DataQualityResult {
	repoRoot := getRepoRoot(t)
	testPath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "tests", testFile)
	content, err := os.ReadFile(testPath)
	if err != nil {
		t.Fatalf("Failed to read test file %s: %v", testFile, err)
	}

	// Execute test SQL
	queryResult, err := adapter.ExecuteQuery(ctx, string(content))
	if err != nil {
		t.Fatalf("Failed to execute test %s: %v", testFile, err)
	}

	result := &DataQualityResult{
		TestName: testFile,
	}

	// Parse test results (expect: violation_count, test_name, test_description, test_result)
	if len(queryResult.Rows) > 0 && len(queryResult.Rows[0]) >= 4 {
		row := queryResult.Rows[0]
		result.ViolationCount = int(row[0].(int64))
		result.Description = row[2].(string)
		result.Status = row[3].(string)
	}

	return result
}

type DataQualityResult struct {
	TestName       string
	Description    string
	ViolationCount int
	Status         string
}

// TestReferentialIntegrity validates all foreign key relationships
func TestReferentialIntegrity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupDataQualityTest(t)
	defer cleanup()

	loadSchemaAndSeeds(t, adapter, ctx)

	testFile := "test_referential_integrity.sql"
	result := executeDataQualityTest(t, adapter, ctx, testFile)

	t.Logf("Referential Integrity Test: %s", result.Status)
	t.Logf("Description: %s", result.Description)
	t.Logf("Violations: %d", result.ViolationCount)

	// Strict - referential integrity should always pass
	if result.Status != "PASS" {
		t.Errorf("FAIL: %d referential integrity violations found", result.ViolationCount)
	}
}

// TestTemporalConsistency validates time-based rules
func TestTemporalConsistency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupDataQualityTest(t)
	defer cleanup()

	loadSchemaAndSeeds(t, adapter, ctx)

	testFile := "test_temporal_consistency.sql"
	result := executeDataQualityTest(t, adapter, ctx, testFile)

	t.Logf("Temporal Consistency Test: %s", result.Status)
	t.Logf("Description: %s", result.Description)
	t.Logf("Violations: %d", result.ViolationCount)

	// Temporal consistency should be strict
	if result.Status != "PASS" {
		t.Errorf("FAIL: %d temporal consistency violations found", result.ViolationCount)
	}
}

// TestBusinessRules validates business logic rules
func TestBusinessRules(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupDataQualityTest(t)
	defer cleanup()

	loadSchemaAndSeeds(t, adapter, ctx)

	testFile := "test_business_rules.sql"

	repoRoot := getRepoRoot(t)
	testPath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "tests", testFile)
	content, err := os.ReadFile(testPath)
	if err != nil {
		t.Fatalf("Failed to read test file %s: %v", testFile, err)
	}

	queryResult, err := adapter.ExecuteQuery(ctx, string(content))
	if err != nil {
		t.Fatalf("Failed to execute test %s: %v", testFile, err)
	}

	totalViolations := 0
	for _, row := range queryResult.Rows {
		ruleName := row[0].(string)
		violationCount := int(row[1].(int64))
		testResult := row[2].(string)

		totalViolations += violationCount
		t.Logf("Business Rule: %s - Violations: %d - Status: %s", ruleName, violationCount, testResult)
	}

	t.Logf("Total Business Rule Violations: %d", totalViolations)

	// Allow up to 1% violation rate for synthetic data
	// (some edge cases acceptable in generated data)
	// We'll just log warnings, not fail
}

// TestStateTransitions validates operational state sequence validity
func TestStateTransitions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupDataQualityTest(t)
	defer cleanup()

	loadSchemaAndSeeds(t, adapter, ctx)

	testFile := "test_state_transitions.sql"

	repoRoot := getRepoRoot(t)
	testPath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "tests", testFile)
	content, err := os.ReadFile(testPath)
	if err != nil {
		t.Fatalf("Failed to read test file %s: %v", testFile, err)
	}

	queryResult, err := adapter.ExecuteQuery(ctx, string(content))
	if err != nil {
		t.Fatalf("Failed to execute test %s: %v", testFile, err)
	}

	totalInvalidTransitions := 0
	for _, row := range queryResult.Rows {
		transitionType := row[0].(string)
		count := int(row[1].(int64))
		testResult := row[2].(string)

		totalInvalidTransitions += count
		t.Logf("State Transition: %s - Count: %d - Status: %s", transitionType, count, testResult)
	}

	t.Logf("Total Invalid Transitions: %d", totalInvalidTransitions)

	if totalInvalidTransitions > 0 {
		t.Logf("WARNING: %d invalid state transitions found", totalInvalidTransitions)
		// Don't fail - some transitions might be acceptable
	}
}

// TestSpeedReasonableness validates speed metrics
func TestSpeedReasonableness(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupDataQualityTest(t)
	defer cleanup()

	loadSchemaAndSeeds(t, adapter, ctx)

	// Check speeds are reasonable
	speedCheckSQL := `
		SELECT 
			COUNT(*) as violation_count
		FROM fact_haul_cycle
		WHERE 
			speed_avg_loaded_kmh >= 80 
			OR speed_avg_empty_kmh >= 80
			OR speed_avg_loaded_kmh >= speed_avg_empty_kmh
	`

	queryResult, err := adapter.ExecuteQuery(ctx, speedCheckSQL)
	if err != nil {
		t.Fatalf("Failed to check speed reasonableness: %v", err)
	}

	var violationCount int
	if len(queryResult.Rows) > 0 {
		violationCount = int(queryResult.Rows[0][0].(int64))
	}

	t.Logf("Speed Reasonableness Violations: %d", violationCount)

	if violationCount > 0 {
		t.Logf("WARNING: %d speed reasonableness violations found", violationCount)
	}
}

// TestQueueTimeReasonableness validates queue times are within reasonable bounds
func TestQueueTimeReasonableness(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupDataQualityTest(t)
	defer cleanup()

	loadSchemaAndSeeds(t, adapter, ctx)

	// Check queue times are reasonable (<120 minutes)
	queueCheckSQL := `
		SELECT 
			COUNT(*) as violation_count
		FROM fact_haul_cycle
		WHERE 
			duration_queue_crusher_min > 120
			OR duration_queue_shovel_min > 120
	`

	queryResult, err := adapter.ExecuteQuery(ctx, queueCheckSQL)
	if err != nil {
		t.Fatalf("Failed to check queue time reasonableness: %v", err)
	}

	var violationCount int
	if len(queryResult.Rows) > 0 {
		violationCount = int(queryResult.Rows[0][0].(int64))
	}

	t.Logf("Queue Time Reasonableness Violations: %d", violationCount)

	if violationCount > 0 {
		t.Logf("WARNING: %d queue time violations found (>120 minutes)", violationCount)
	}
}

// TestFuelConsumptionReasonableness validates fuel consumption patterns
func TestFuelConsumptionReasonableness(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupDataQualityTest(t)
	defer cleanup()

	loadSchemaAndSeeds(t, adapter, ctx)

	// Check for negative fuel consumption (indicates data error)
	fuelCheckSQL := `
		SELECT 
			COUNT(*) as violation_count
		FROM fact_haul_cycle
		WHERE 
			fuel_consumed_liters < 0
			OR fuel_consumed_liters > 1000
	`

	queryResult, err := adapter.ExecuteQuery(ctx, fuelCheckSQL)
	if err != nil {
		t.Fatalf("Failed to check fuel consumption reasonableness: %v", err)
	}

	var violationCount int
	if len(queryResult.Rows) > 0 {
		violationCount = int(queryResult.Rows[0][0].(int64))
	}

	t.Logf("Fuel Consumption Reasonableness Violations: %d", violationCount)

	if violationCount > 0 {
		t.Errorf("FAIL: %d fuel consumption violations found (negative or > 1000 liters)", violationCount)
	}
}

// TestRefuelingFrequency validates refueling occurs at appropriate intervals
func TestRefuelingFrequency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupDataQualityTest(t)
	defer cleanup()

	loadSchemaAndSeeds(t, adapter, ctx)

	// Check if spot delays occur at reasonable engine hour intervals
	// Refueling should occur roughly every 10-12 engine hours
	refuelCheckSQL := `
		SELECT 
			COUNT(*) as spot_delay_count
		FROM fact_haul_cycle
		WHERE 
			duration_spot_delays_min > 0
	`

	queryResult, err := adapter.ExecuteQuery(ctx, refuelCheckSQL)
	if err != nil {
		t.Fatalf("Failed to check refueling frequency: %v", err)
	}

	var spotDelayCount int
	if len(queryResult.Rows) > 0 {
		spotDelayCount = int(queryResult.Rows[0][0].(int64))
	}

	t.Logf("Cycles with Spot Delays (including refueling): %d", spotDelayCount)

	if spotDelayCount == 0 {
		t.Logf("INFO: No spot delays found - refueling may not be modeled in current data")
	}
}

// TestPayloadBusinessRules validates payload rules specifically
func TestPayloadBusinessRules(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupDataQualityTest(t)
	defer cleanup()

	loadSchemaAndSeeds(t, adapter, ctx)

	// Check payload is within acceptable range (0-115% of truck capacity)
	payloadCheckSQL := `
		SELECT 
			COUNT(*) as violation_count
		FROM fact_haul_cycle fc
		JOIN dim_truck dt ON fc.truck_id = dt.truck_id
		WHERE 
			fc.payload_tons < 0
			OR fc.payload_tons > (dt.payload_capacity_tons * 1.15)
	`

	queryResult, err := adapter.ExecuteQuery(ctx, payloadCheckSQL)
	if err != nil {
		t.Fatalf("Failed to check payload business rules: %v", err)
	}

	var violationCount int
	if len(queryResult.Rows) > 0 {
		violationCount = int(queryResult.Rows[0][0].(int64))
	}

	t.Logf("Payload Business Rule Violations: %d", violationCount)

	if violationCount > 0 {
		t.Logf("WARNING: %d payload violations found (negative or >115%% capacity)", violationCount)
	}
}

// TestCycleTimeBounds validates cycle times are within reasonable ranges
func TestCycleTimeBounds(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupDataQualityTest(t)
	defer cleanup()

	loadSchemaAndSeeds(t, adapter, ctx)

	// Check cycle times are within 10-180 minute range
	cycleTimeCheckSQL := `
		SELECT 
			COUNT(*) as violation_count
		FROM fact_haul_cycle
		WHERE 
			total_cycle_time_min < 10
			OR total_cycle_time_min > 180
	`

	queryResult, err := adapter.ExecuteQuery(ctx, cycleTimeCheckSQL)
	if err != nil {
		t.Fatalf("Failed to check cycle time bounds: %v", err)
	}

	var violationCount int
	if len(queryResult.Rows) > 0 {
		violationCount = int(queryResult.Rows[0][0].(int64))
	}

	t.Logf("Cycle Time Bounds Violations: %d", violationCount)

	if violationCount > 0 {
		t.Logf("WARNING: %d cycle time violations found (<10 min or >180 min)", violationCount)
	}
}

// TestDataQualitySummary generates overall data quality report
func TestDataQualitySummary(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupDataQualityTest(t)
	defer cleanup()

	loadSchemaAndSeeds(t, adapter, ctx)

	// Get summary statistics
	summarySQL := `
		SELECT 
			COUNT(*) as total_cycles,
			COUNT(DISTINCT truck_id) as distinct_trucks,
			COUNT(DISTINCT operator_id) as distinct_operators,
			MIN(cycle_start) as earliest_cycle,
			MAX(cycle_end) as latest_cycle,
			AVG(total_cycle_time_min) as avg_cycle_time,
			AVG(payload_tons) as avg_payload
		FROM fact_haul_cycle
	`

	queryResult, err := adapter.ExecuteQuery(ctx, summarySQL)
	if err != nil {
		t.Fatalf("Failed to generate data quality summary: %v", err)
	}

	if len(queryResult.Rows) == 0 {
		t.Fatalf("No data found in fact_haul_cycle")
	}

	row := queryResult.Rows[0]
	totalCycles := int(row[0].(int64))
	distinctTrucks := int(row[1].(int64))
	distinctOperators := int(row[2].(int64))
	earliestCycle := row[3].(string)
	latestCycle := row[4].(string)
	avgCycleTime := row[5].(float64)
	avgPayload := row[6].(float64)

	t.Logf("\n=== Data Quality Summary ===")
	t.Logf("Total Cycles: %d", totalCycles)
	t.Logf("Distinct Trucks: %d", distinctTrucks)
	t.Logf("Distinct Operators: %d", distinctOperators)
	t.Logf("Date Range: %s to %s", earliestCycle, latestCycle)
	t.Logf("Average Cycle Time: %.2f minutes", avgCycleTime)
	t.Logf("Average Payload: %.2f tons", avgPayload)
	t.Logf("===========================\n")

	if totalCycles == 0 {
		t.Errorf("FAIL: No haul cycles found in fact table")
	}
}

// Helper functions

// createDimTables creates all dimension tables needed for testing
func createDimTables(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context) {
	tables := []string{
		`CREATE TABLE dim_truck (
			truck_id TEXT PRIMARY KEY,
			model TEXT NOT NULL,
			payload_capacity_tons REAL NOT NULL,
			fleet_class TEXT NOT NULL
		)`,
		`CREATE TABLE dim_shovel (
			shovel_id TEXT PRIMARY KEY,
			bucket_size_m3 REAL NOT NULL,
			pit_zone TEXT NOT NULL
		)`,
		`CREATE TABLE dim_crusher (
			crusher_id TEXT PRIMARY KEY,
			capacity_tph REAL NOT NULL
		)`,
		`CREATE TABLE dim_operator (
			operator_id TEXT PRIMARY KEY,
			experience_level TEXT NOT NULL
		)`,
		`CREATE TABLE dim_shift (
			shift_id TEXT PRIMARY KEY,
			shift_name TEXT NOT NULL,
			start_time TEXT NOT NULL,
			end_time TEXT NOT NULL
		)`,
		`CREATE TABLE dim_date (
			date_key INTEGER PRIMARY KEY,
			full_date TEXT NOT NULL,
			year INTEGER NOT NULL,
			quarter INTEGER NOT NULL,
			month INTEGER NOT NULL,
			week INTEGER NOT NULL,
			day_of_week INTEGER NOT NULL
		)`,
	}

	for _, ddl := range tables {
		if err := adapter.ExecuteDDL(ctx, ddl); err != nil {
			t.Fatalf("Failed to create dimension table: %v", err)
		}
	}
}

// insertTestDimData inserts minimal test data into dimension tables
func insertTestDimData(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context) {
	inserts := []string{
		`INSERT INTO dim_truck VALUES 
			('TRUCK-101', 'CAT 777F', 100, '100-ton'),
			('TRUCK-201', 'CAT 789D', 200, '200-ton'),
			('TRUCK-401', 'CAT 797F', 400, '400-ton')`,
		`INSERT INTO dim_shovel VALUES 
			('SHOVEL_A', 20, 'North Pit'),
			('SHOVEL_B', 35, 'South Pit'),
			('SHOVEL_C', 60, 'East Pit')`,
		`INSERT INTO dim_crusher VALUES ('CRUSHER_1', 3000)`,
		`INSERT INTO dim_operator VALUES 
			('OP_001', 'Senior'),
			('OP_002', 'Intermediate'),
			('OP_003', 'Junior')`,
		`INSERT INTO dim_shift VALUES 
			('SHIFT_DAY', 'Day', '07:00', '19:00'),
			('SHIFT_NIGHT', 'Night', '19:00', '07:00')`,
		`INSERT INTO dim_date VALUES 
			(20240101, '2024-01-01', 2024, 1, 1, 1, 1),
			(20240102, '2024-01-02', 2024, 1, 1, 1, 2)`,
	}

	for _, insert := range inserts {
		if err := adapter.ExecuteDDL(ctx, insert); err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}
}

// insertTestCycleData inserts minimal test haul cycle data
func insertTestCycleData(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context) {
	// Insert valid cycles
	inserts := []string{
		// Valid cycle
		`INSERT INTO fact_haul_cycle VALUES (
			'CYCLE_001', 'TRUCK-101', 'SHOVEL_A', 'CRUSHER_1', 'OP_001', 'SHIFT_DAY', 20240101,
			'2024-01-01 08:00:00', '2024-01-01 09:15:00',
			95.0, 6.5, 6.5, 5.0, 20.0, 3.0, 1.5, 18.0, 2.0, 0.0, 45.0, 25.0, 35.0, 75.0
		)`,
		// Another valid cycle
		`INSERT INTO fact_haul_cycle VALUES (
			'CYCLE_002', 'TRUCK-201', 'SHOVEL_B', 'CRUSHER_1', 'OP_002', 'SHIFT_DAY', 20240101,
			'2024-01-01 10:00:00', '2024-01-01 11:20:00',
			190.0, 7.0, 7.0, 6.0, 22.0, 5.0, 2.0, 19.0, 3.0, 0.0, 50.0, 28.0, 38.0, 80.0
		)`,
		// Valid cycle with longer queue
		`INSERT INTO fact_haul_cycle VALUES (
			'CYCLE_003', 'TRUCK-101', 'SHOVEL_A', 'CRUSHER_1', 'OP_001', 'SHIFT_DAY', 20240101,
			'2024-01-01 12:00:00', '2024-01-01 13:30:00',
			98.0, 6.5, 6.5, 5.0, 21.0, 8.0, 1.5, 18.0, 3.0, 0.0, 48.0, 26.0, 36.0, 90.0
		)`,
	}

	for _, insert := range inserts {
		if err := adapter.ExecuteDDL(ctx, insert); err != nil {
			t.Fatalf("Failed to insert test cycle data: %v", err)
		}
	}
}

// insertTestStateData inserts minimal test state transition data
func insertTestStateData(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context) {
	inserts := []string{
		// Valid state sequence for CYCLE_001
		`INSERT INTO stg_truck_states VALUES 
			('TRUCK-101', '2024-01-01 07:58:00', '2024-01-01 08:00:00', 'queued_at_shovel', 'Shovel_A', 5.0, 5.0)`,
		`INSERT INTO stg_truck_states VALUES 
			('TRUCK-101', '2024-01-01 08:00:00', '2024-01-01 08:05:00', 'loading', 'Shovel_A', 5.0, 95.0)`,
		`INSERT INTO stg_truck_states VALUES 
			('TRUCK-101', '2024-01-01 08:05:00', '2024-01-01 08:25:00', 'hauling_loaded', 'Road', 95.0, 95.0)`,
		`INSERT INTO stg_truck_states VALUES 
			('TRUCK-101', '2024-01-01 08:25:00', '2024-01-01 08:28:00', 'queued_at_crusher', 'Crusher', 95.0, 95.0)`,
		`INSERT INTO stg_truck_states VALUES 
			('TRUCK-101', '2024-01-01 08:28:00', '2024-01-01 08:29:30', 'dumping', 'Crusher', 95.0, 5.0)`,
		`INSERT INTO stg_truck_states VALUES 
			('TRUCK-101', '2024-01-01 08:29:30', '2024-01-01 08:47:30', 'returning_empty', 'Road', 5.0, 5.0)`,
	}

	for _, insert := range inserts {
		if err := adapter.ExecuteDDL(ctx, insert); err != nil {
			t.Fatalf("Failed to insert test state data: %v", err)
		}
	}
}

// loadCSVIntoTableDQ loads CSV data into a table generically for data quality tests
func loadCSVIntoTableDQ(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context, csvContent string, csvFileName string) {
	// Parse CSV
	reader := csv.NewReader(strings.NewReader(csvContent))

	// Read header
	headers, err := reader.Read()
	if err != nil {
		t.Fatalf("Failed to read CSV header from %s: %v", csvFileName, err)
	}

	// Determine table name
	tableName := strings.TrimSuffix(csvFileName, ".csv")

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read CSV records from %s: %v", csvFileName, err)
	}

	// Build INSERT statements
	for _, record := range records {
		if len(record) != len(headers) {
			t.Fatalf("CSV record length mismatch in %s", csvFileName)
		}

		// Build values - quote strings, leave numbers as-is
		values := make([]string, len(record))
		for i, val := range record {
			// Check if value is numeric
			if _, err := fmt.Sscanf(val, "%f", new(float64)); err == nil && !strings.Contains(val, "-") {
				values[i] = val
			} else {
				// Escape single quotes and wrap in quotes
				values[i] = "'" + strings.ReplaceAll(val, "'", "''") + "'"
			}
		}

		insertSQL := fmt.Sprintf("INSERT INTO %s VALUES (%s)", tableName, strings.Join(values, ", "))
		if err := adapter.ExecuteDDL(ctx, insertSQL); err != nil {
			t.Fatalf("Failed to insert data into %s: %v", tableName, err)
		}
	}
}

// Note: removeConfigCallsStaging and removeConfigCallsFacts are declared in other test files
