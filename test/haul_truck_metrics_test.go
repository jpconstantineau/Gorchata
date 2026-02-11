package test

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/platform"
	"github.com/jpconstantineau/gorchata/internal/platform/sqlite"
	"github.com/jpconstantineau/gorchata/internal/template"
)

// setupMetricsTest creates a test database with dimension tables, fact_haul_cycle, and test data
func setupMetricsTest(t *testing.T) (*sqlite.SQLiteAdapter, context.Context, func()) {
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

	// Create dimension tables
	err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE dim_truck (
			truck_id TEXT PRIMARY KEY,
			model TEXT NOT NULL,
			payload_capacity_tons REAL NOT NULL,
			fleet_class TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create dim_truck: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE dim_shovel (
			shovel_id TEXT PRIMARY KEY,
			bucket_size_m3 REAL NOT NULL,
			pit_zone TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create dim_shovel: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE dim_crusher (
			crusher_id TEXT PRIMARY KEY,
			capacity_tph REAL NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create dim_crusher: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE dim_operator (
			operator_id TEXT PRIMARY KEY,
			experience_level TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create dim_operator: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE dim_shift (
			shift_id TEXT PRIMARY KEY,
			shift_name TEXT NOT NULL,
			start_time TEXT NOT NULL,
			end_time TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create dim_shift: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE dim_date (
			date_key INTEGER PRIMARY KEY,
			full_date TEXT NOT NULL,
			year INTEGER NOT NULL,
			quarter INTEGER NOT NULL,
			month INTEGER NOT NULL,
			week INTEGER NOT NULL,
			day_of_week INTEGER NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create dim_date: %v", err)
	}

	// Create fact table
	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE fact_haul_cycle (
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
			speed_avg_empty_kmh REAL NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create fact_haul_cycle: %v", err)
	}

	// Create staging table for shovel utilization
	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE stg_truck_states (
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

	// Insert test dimension data
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO dim_truck VALUES 
		('TRUCK_001', 'CAT_793F', 200, '200-ton'),
		('TRUCK_002', 'CAT_797F', 400, '400-ton'),
		('TRUCK_003', 'CAT_777G', 100, '100-ton')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test trucks: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO dim_shovel VALUES 
		('SHOVEL_A', 20, 'North Pit'),
		('SHOVEL_B', 35, 'South Pit'),
		('SHOVEL_C', 60, 'East Pit')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test shovels: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO dim_crusher VALUES 
		('CRUSHER_1', 3000)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test crusher: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO dim_operator VALUES 
		('OP_001', 'Senior'),
		('OP_002', 'Intermediate'),
		('OP_003', 'Junior')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test operators: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO dim_shift VALUES 
		('SHIFT_DAY', 'Day', '07:00', '19:00'),
		('SHIFT_NIGHT', 'Night', '19:00', '07:00')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test shifts: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO dim_date VALUES 
		(20240101, '2024-01-01', 2024, 1, 1, 1, 1),
		(20240102, '2024-01-02', 2024, 1, 1, 1, 2)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test dates: %v", err)
	}

	return adapter, ctx, cleanup
}

// Helper functions for metrics tests

// removeConfigCallsMetrics removes {{ config ... }} calls from SQL template content
func removeConfigCallsMetrics(content string) string {
	// Remove Go template syntax: {{ config "key" "value" }}
	goTemplateRe := regexp.MustCompile(`{{\s*config\s+"[^"]+"\s+"[^"]+"\s*}}`)
	content = goTemplateRe.ReplaceAllString(content, "")

	// Remove legacy Jinja-style syntax: {{ config(key='value') }}
	legacyRe := regexp.MustCompile(`{{\s*config\s*\([^}]+\)\s*}}`)
	return legacyRe.ReplaceAllString(content, "")
}

// getFloat safely converts interface{} to float64
func getFloatMetrics(val interface{}) float64 {
	switch v := val.(type) {
	case int64:
		return float64(v)
	case float64:
		return v
	case string:
		f, _ := strconv.ParseFloat(v, 64)
		return f
	default:
		return 0
	}
}

// executeMetricsSQL loads and executes a metrics SQL model
func executeMetricsSQL(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context, modelName, tableName string) {
	repoRoot := getRepoRootMetrics(t)
	modelPath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "models", "metrics", modelName+".sql")

	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read %s.sql: %v", modelName, err)
	}

	// Remove config calls
	contentStr := removeConfigCallsMetrics(string(content))

	// Parse and render template
	templateEngine := template.New()
	tmpl, err := templateEngine.Parse(modelName, contentStr)
	if err != nil {
		t.Fatalf("Failed to parse template: %v", err)
	}

	ctx2 := template.NewContext(template.WithCurrentModel(modelName))
	rendered, err := template.Render(tmpl, ctx2, nil)
	if err != nil {
		t.Fatalf("Failed to render template: %v", err)
	}

	// Create metrics table
	err = adapter.ExecuteDDL(ctx, "DROP TABLE IF EXISTS "+tableName)
	if err != nil {
		t.Fatalf("Failed to drop existing %s: %v", tableName, err)
	}

	err = adapter.ExecuteDDL(ctx, "CREATE TABLE "+tableName+" AS "+rendered)
	if err != nil {
		t.Fatalf("Failed to execute %s model: %v", modelName, err)
	}
}

// getRepoRootMetrics returns path to repo root for locating SQL files
func getRepoRootMetrics(t *testing.T) string {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current directory: %v", err)
	}

	// From test/ directory, go up one level to repo root
	return filepath.Dir(cwd)
}

// TestTruckDailyProductivityCalculation validates per-truck daily productivity metrics
func TestTruckDailyProductivityCalculation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupMetricsTest(t)
	defer cleanup()

	// Insert test cycle data: TRUCK_001 on 2024-01-01 with 3 cycles
	err := adapter.ExecuteDDL(ctx, `
		INSERT INTO fact_haul_cycle VALUES
		-- Day 1: TRUCK_001 completes 3 cycles
		('TRUCK_001_2024-01-01T08:00:00', 'TRUCK_001', 'SHOVEL_A', 'CRUSHER_1', 'OP_001', 'SHIFT_DAY', 20240101,
		 '2024-01-01T08:00:00', '2024-01-01T08:45:00',
		 190, 6.5, 6.5, 6, 15, 3, 1.5, 12, 2, 4, 85, 28, 42),
		('TRUCK_001_2024-01-01T09:00:00', 'TRUCK_001', 'SHOVEL_A', 'CRUSHER_1', 'OP_001', 'SHIFT_DAY', 20240101,
		 '2024-01-01T09:00:00', '2024-01-01T09:50:00',
		 195, 6.5, 6.5, 7, 16, 5, 1.5, 13, 3, 2, 90, 27, 41),
		('TRUCK_001_2024-01-01T10:00:00', 'TRUCK_001', 'SHOVEL_A', 'CRUSHER_1', 'OP_001', 'SHIFT_DAY', 20240101,
		 '2024-01-01T10:00:00', '2024-01-01T11:00:00',
		 200, 6.5, 6.5, 8, 18, 8, 1.5, 15, 4, 5, 95, 25, 38),
		-- Day 2: TRUCK_001 completes 2 cycles
		('TRUCK_001_2024-01-02T08:00:00', 'TRUCK_001', 'SHOVEL_A', 'CRUSHER_1', 'OP_001', 'SHIFT_DAY', 20240102,
		 '2024-01-02T08:00:00', '2024-01-02T09:00:00',
		 185, 6.5, 6.5, 6, 15, 3, 1.5, 12, 2, 4, 85, 28, 42),
		('TRUCK_001_2024-01-02T09:15:00', 'TRUCK_001', 'SHOVEL_A', 'CRUSHER_1', 'OP_001', 'SHIFT_DAY', 20240102,
		 '2024-01-02T09:15:00', '2024-01-02T10:15:00',
		 180, 6.5, 6.5, 7, 16, 5, 1.5, 13, 3, 2, 90, 27, 41)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test cycles: %v", err)
	}

	// Execute the metrics SQL model
	executeMetricsSQL(t, adapter, ctx, "truck_daily_productivity", "truck_daily_productivity")

	// Query and validate results
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			truck_id,
			date_id,
			total_tons_moved,
			cycles_completed,
			ROUND(avg_cycle_time_min, 1) AS avg_cycle_time_min,
			ROUND(tons_per_hour, 1) AS tons_per_hour,
			ROUND(avg_payload_utilization_pct, 1) AS avg_payload_utilization_pct,
			ROUND(total_spot_delay_min, 1) AS total_spot_delay_min
		FROM truck_daily_productivity
		WHERE truck_id = 'TRUCK_001' AND date_id = 20240101
	`)
	if err != nil {
		t.Fatalf("Failed to query metrics: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected one row for TRUCK_001 on 2024-01-01")
	}

	row := result.Rows[0]
	truckID := row[0].(string)
	dateID := row[1].(int64)
	totalTons := getFloatMetrics(row[2])
	cyclesCompleted := getFloatMetrics(row[3])
	avgCycleTime := getFloatMetrics(row[4])
	_ = getFloatMetrics(row[5]) // tonsPerHour - not validated in this test
	avgPayloadUtil := getFloatMetrics(row[6])
	totalSpotDelay := getFloatMetrics(row[7])

	// Validate calculations
	if truckID != "TRUCK_001" {
		t.Errorf("Expected truck_id 'TRUCK_001', got '%s'", truckID)
	}
	if dateID != 20240101 {
		t.Errorf("Expected date_id 20240101, got %d", dateID)
	}
	// Total tons: 190 + 195 + 200 = 585
	if math.Abs(totalTons-585) > 0.1 {
		t.Errorf("Expected total_tons_moved 585, got %.1f", totalTons)
	}
	// Cycles: 3
	if cyclesCompleted != 3 {
		t.Errorf("Expected cycles_completed 3, got %.0f", cyclesCompleted)
	}
	// Avg cycle time: (45 + 50 + 60) / 3 = 51.7 minutes
	if math.Abs(avgCycleTime-51.7) > 0.5 {
		t.Errorf("Expected avg_cycle_time_min ~51.7, got %.1f", avgCycleTime)
	}
	// Payload utilization avg: (190/200 + 195/200 + 200/200) / 3 * 100 = 97.5%
	if math.Abs(avgPayloadUtil-97.5) > 0.5 {
		t.Errorf("Expected avg_payload_utilization_pct ~97.5, got %.1f", avgPayloadUtil)
	}
	// Total spot delays: 4 + 2 + 5 = 11 minutes
	if math.Abs(totalSpotDelay-11) > 0.1 {
		t.Errorf("Expected total_spot_delay_min 11, got %.1f", totalSpotDelay)
	}
}

// TestShovelUtilizationMetrics validates shovel loading time and utilization calculations
func TestShovelUtilizationMetrics(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupMetricsTest(t)
	defer cleanup()

	// Insert state data for shovel utilization
	// Shovel A: 2 loading sessions (6 min + 8 min = 14 min) during a 12-hour shift (720 min)
	err := adapter.ExecuteDDL(ctx, `
		INSERT INTO stg_truck_states VALUES
		-- SHOVEL_A loading states
		('TRUCK_001', '2024-01-01T08:00:00', '2024-01-01T08:06:00', 'loading', 'Shovel_A', 0, 190),
		('TRUCK_002', '2024-01-01T08:30:00', '2024-01-01T08:38:00', 'loading', 'Shovel_A', 0, 380),
		-- SHOVEL_B loading states
		('TRUCK_003', '2024-01-01T09:00:00', '2024-01-01T09:10:00', 'loading', 'Shovel_B', 0, 95)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test states: %v", err)
	}

	// Execute the metrics SQL model
	executeMetricsSQL(t, adapter, ctx, "shovel_utilization", "shovel_utilization")

	// Query and validate results
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			shovel_id,
			date_id,
			shift_id,
			ROUND(total_loading_time_hours, 2) AS total_loading_hours,
			truck_loads_completed,
			ROUND(avg_loading_duration_min, 1) AS avg_loading_min,
			ROUND(tons_loaded, 0) AS tons_loaded,
			ROUND(utilization_pct, 2) AS utilization_pct
		FROM shovel_utilization
		WHERE shovel_id = 'SHOVEL_A' AND date_id = 20240101
		ORDER BY shovel_id
	`)
	if err != nil {
		t.Fatalf("Failed to query metrics: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected one row for SHOVEL_A")
	}

	row := result.Rows[0]
	shovelID := row[0].(string)
	totalLoadingHours := getFloatMetrics(row[3])
	loadsCompleted := getFloatMetrics(row[4])
	avgLoadingMin := getFloatMetrics(row[5])
	tonsLoaded := getFloatMetrics(row[6])
	utilizationPct := getFloatMetrics(row[7])

	// Validate calculations
	if shovelID != "SHOVEL_A" {
		t.Errorf("Expected shovel_id 'SHOVEL_A', got '%s'", shovelID)
	}
	// Total loading time: 6 + 8 = 14 minutes = 0.23 hours
	if math.Abs(totalLoadingHours-0.23) > 0.02 {
		t.Errorf("Expected total_loading_time_hours ~0.23, got %.2f", totalLoadingHours)
	}
	// Loads completed: 2
	if loadsCompleted != 2 {
		t.Errorf("Expected truck_loads_completed 2, got %.0f", loadsCompleted)
	}
	// Avg loading duration: 14 / 2 = 7 minutes
	if math.Abs(avgLoadingMin-7.0) > 0.2 {
		t.Errorf("Expected avg_loading_duration_min ~7.0, got %.1f", avgLoadingMin)
	}
	// Tons loaded: 190 + 380 = 570
	if math.Abs(tonsLoaded-570) > 1 {
		t.Errorf("Expected tons_loaded ~570, got %.0f", tonsLoaded)
	}
	// Utilization: 14 min / 720 min * 100 = 1.94%
	if math.Abs(utilizationPct-1.94) > 0.1 {
		t.Errorf("Expected utilization_pct ~1.94, got %.2f", utilizationPct)
	}
}

// TestCrusherThroughputCalculation validates crusher throughput and queue metrics
func TestCrusherThroughputCalculation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupMetricsTest(t)
	defer cleanup()

	// Insert test cycle data with crusher metrics
	err := adapter.ExecuteDDL(ctx, `
		INSERT INTO fact_haul_cycle VALUES
		-- 3 trucks dump at crusher: 190, 195, 200 tons with various queue and dump times
		('C1', 'TRUCK_001', 'SHOVEL_A', 'CRUSHER_1', 'OP_001', 'SHIFT_DAY', 20240101,
		 '2024-01-01T08:00:00', '2024-01-01T08:45:00',
		 190, 6.5, 6.5, 6, 15, 3, 1.5, 12, 2, 0, 85, 28, 42),
		('C2', 'TRUCK_002', 'SHOVEL_B', 'CRUSHER_1', 'OP_002', 'SHIFT_DAY', 20240101,
		 '2024-01-01T08:15:00', '2024-01-01T09:00:00',
		 195, 6.5, 6.5, 7, 16, 5, 2.0, 13, 3, 0, 90, 27, 41),
		('C3', 'TRUCK_003', 'SHOVEL_C', 'CRUSHER_1', 'OP_003', 'SHIFT_DAY', 20240101,
		 '2024-01-01T08:30:00', '2024-01-01T09:30:00',
		 200, 6.5, 6.5, 8, 18, 8, 1.5, 15, 4, 0, 95, 25, 38)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test cycles: %v", err)
	}

	// Execute the metrics SQL model
	executeMetricsSQL(t, adapter, ctx, "crusher_throughput", "crusher_throughput")

	// Query and validate results
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			crusher_id,
			date_id,
			shift_id,
			ROUND(tons_received, 0) AS tons_received,
			truck_arrivals,
			ROUND(tons_per_hour, 1) AS tons_per_hour,
			ROUND(avg_dump_duration_min, 2) AS avg_dump_min,
			ROUND(avg_queue_time_min, 1) AS avg_queue_min,
			ROUND(max_queue_time_min, 1) AS max_queue_min
		FROM crusher_throughput
		WHERE crusher_id = 'CRUSHER_1' AND date_id = 20240101
	`)
	if err != nil {
		t.Fatalf("Failed to query metrics: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected one row for CRUSHER_1")
	}

	row := result.Rows[0]
	crusherID := row[0].(string)
	tonsReceived := getFloatMetrics(row[3])
	truckArrivals := getFloatMetrics(row[4])
	_ = getFloatMetrics(row[5]) // tonsPerHour - not validated in this test
	avgDumpMin := getFloatMetrics(row[6])
	avgQueueMin := getFloatMetrics(row[7])
	maxQueueMin := getFloatMetrics(row[8])

	// Validate calculations
	if crusherID != "CRUSHER_1" {
		t.Errorf("Expected crusher_id 'CRUSHER_1', got '%s'", crusherID)
	}
	// Total tons: 190 + 195 + 200 = 585
	if math.Abs(tonsReceived-585) > 1 {
		t.Errorf("Expected tons_received ~585, got %.0f", tonsReceived)
	}
	// Truck arrivals: 3
	if truckArrivals != 3 {
		t.Errorf("Expected truck_arrivals 3, got %.0f", truckArrivals)
	}
	// Avg dump duration: (1.5 + 2.0 + 1.5) / 3 = 1.67 minutes
	if math.Abs(avgDumpMin-1.67) > 0.1 {
		t.Errorf("Expected avg_dump_duration_min ~1.67, got %.2f", avgDumpMin)
	}
	// Avg queue time: (3 + 5 + 8) / 3 = 5.33 minutes
	if math.Abs(avgQueueMin-5.33) > 0.2 {
		t.Errorf("Expected avg_queue_time_min ~5.33, got %.1f", avgQueueMin)
	}
	// Max queue time: 8 minutes
	if math.Abs(maxQueueMin-8.0) > 0.1 {
		t.Errorf("Expected max_queue_time_min ~8.0, got %.1f", maxQueueMin)
	}
}

// TestQueueAnalysisMetrics validates queue analysis by location
func TestQueueAnalysisMetrics(t *testing.T) {
	adapter, ctx, cleanup := setupMetricsTest(t)
	defer cleanup()

	// Insert test cycle data with various queue times
	err := adapter.ExecuteDDL(ctx, `
		INSERT INTO fact_haul_cycle VALUES
		-- Crusher queues: 3, 5, 8, 10 minutes
		('Q1', 'TRUCK_001', 'SHOVEL_A', 'CRUSHER_1', 'OP_001', 'SHIFT_DAY', 20240101,
		 '2024-01-01T08:00:00', '2024-01-01T08:45:00',
		 190, 6.5, 6.5, 6, 15, 3, 1.5, 12, 2, 0, 85, 28, 42),
		('Q2', 'TRUCK_002', 'SHOVEL_B', 'CRUSHER_1', 'OP_002', 'SHIFT_DAY', 20240101,
		 '2024-01-01T08:15:00', '2024-01-01T09:00:00',
		 195, 6.5, 6.5, 7, 16, 5, 2.0, 13, 0, 0, 90, 27, 41),
		('Q3', 'TRUCK_003', 'SHOVEL_C', 'CRUSHER_1', 'OP_003', 'SHIFT_DAY', 20240101,
		 '2024-01-01T08:30:00', '2024-01-01T09:30:00',
		 200, 6.5, 6.5, 8, 18, 8, 1.5, 15, 0, 0, 95, 25, 38),
		('Q4', 'TRUCK_001', 'SHOVEL_A', 'CRUSHER_1', 'OP_001', 'SHIFT_DAY', 20240101,
		 '2024-01-01T10:00:00', '2024-01-01T11:00:00',
		 200, 6.5, 6.5, 6, 15, 10, 1.5, 12, 3, 0, 85, 28, 42),
		-- Shovel queues: 2, 3, 4 minutes
		('Q5', 'TRUCK_001', 'SHOVEL_A', 'CRUSHER_1', 'OP_001', 'SHIFT_DAY', 20240101,
		 '2024-01-01T12:00:00', '2024-01-01T13:00:00',
		 200, 6.5, 6.5, 6, 15, 2, 1.5, 12, 3, 0, 85, 28, 42)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test cycles: %v", err)
	}

	// Execute the metrics SQL model
	executeMetricsSQL(t, adapter, ctx, "queue_analysis", "queue_analysis")

	// Query and validate crusher queue results
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			location_id,
			location_type,
			date_id,
			ROUND(avg_queue_time_min, 2) AS avg_queue_min,
			ROUND(max_queue_time_min, 1) AS max_queue_min,
			queue_events_count,
			trucks_affected
		FROM queue_analysis
		WHERE location_type = 'CRUSHER' AND date_id = 20240101
	`)
	if err != nil {
		t.Fatalf("Failed to query metrics: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected one row for crusher queues")
	}

	row := result.Rows[0]
	_ = row[0].(string) // locationID - not validated in detail
	locationType := row[1].(string)
	avgQueueMin := getFloatMetrics(row[3])
	maxQueueMin := getFloatMetrics(row[4])
	queueEvents := getFloatMetrics(row[5])
	trucksAffected := getFloatMetrics(row[6])

	// Validate calculations for crusher
	if locationType != "CRUSHER" {
		t.Errorf("Expected location_type 'CRUSHER', got '%s'", locationType)
	}
	// Avg queue time: (3 + 5 + 8 + 10 + 2) / 5 = 5.6 minutes (only non-zero queues should count)
	// Actually all 5 cycles have queues (including 2), so: (3+5+8+10+2)/5 = 5.6
	if math.Abs(avgQueueMin-5.6) > 0.2 {
		t.Errorf("Expected avg_queue_time_min ~5.6, got %.2f", avgQueueMin)
	}
	// Max queue time: 10 minutes
	if math.Abs(maxQueueMin-10.0) > 0.1 {
		t.Errorf("Expected max_queue_time_min ~10.0, got %.1f", maxQueueMin)
	}
	// Queue events: 5 (all cycles had queue time)
	if queueEvents != 5 {
		t.Errorf("Expected queue_events_count 5, got %.0f", queueEvents)
	}
	// Trucks affected: 3 unique trucks
	if trucksAffected != 3 {
		t.Errorf("Expected trucks_affected 3, got %.0f", trucksAffected)
	}
}

// TestFleetSummaryRollup validates fleet-wide aggregated metrics
func TestFleetSummaryRollup(t *testing.T) {
	adapter, ctx, cleanup := setupMetricsTest(t)
	defer cleanup()

	// Insert test cycle data for multiple trucks
	err := adapter.ExecuteDDL(ctx, `
		INSERT INTO fact_haul_cycle VALUES
		-- Day shift: 3 cycles from 2 trucks
		('F1', 'TRUCK_001', 'SHOVEL_A', 'CRUSHER_1', 'OP_001', 'SHIFT_DAY', 20240101,
		 '2024-01-01T08:00:00', '2024-01-01T08:45:00',
		 190, 6.5, 6.5, 6, 15, 3, 1.5, 12, 2, 4, 85, 28, 42),
		('F2', 'TRUCK_001', 'SHOVEL_A', 'CRUSHER_1', 'OP_001', 'SHIFT_DAY', 20240101,
		 '2024-01-01T09:00:00', '2024-01-01T09:50:00',
		 195, 6.5, 6.5, 7, 16, 5, 1.5, 13, 3, 2, 90, 27, 41),
		('F3', 'TRUCK_002', 'SHOVEL_B', 'CRUSHER_1', 'OP_002', 'SHIFT_DAY', 20240101,
		 '2024-01-01T08:30:00', '2024-01-01T09:30:00',
		 380, 6.5, 6.5, 8, 18, 8, 2.0, 15, 4, 5, 95, 25, 38),
		-- Night shift: 2 cycles from 1 truck
		('F4', 'TRUCK_003', 'SHOVEL_C', 'CRUSHER_1', 'OP_003', 'SHIFT_NIGHT', 20240101,
		 '2024-01-01T20:00:00', '2024-01-01T21:00:00',
		 95, 6.5, 6.5, 7, 17, 6, 1.5, 14, 5, 3, 88, 26, 40),
		('F5', 'TRUCK_003', 'SHOVEL_C', 'CRUSHER_1', 'OP_003', 'SHIFT_NIGHT', 20240101,
		 '2024-01-01T21:15:00', '2024-01-01T22:15:00',
		 98, 6.5, 6.5, 6, 16, 4, 1.5, 13, 3, 2, 86, 27, 41)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test cycles: %v", err)
	}

	// Execute the metrics SQL model
	executeMetricsSQL(t, adapter, ctx, "fleet_summary", "fleet_summary")

	// Query and validate day shift results
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			date_id,
			shift_id,
			ROUND(total_tons_moved, 0) AS total_tons,
			total_cycles_completed,
			ROUND(fleet_avg_cycle_time_min, 1) AS avg_cycle_min,
			ROUND(total_fuel_consumed_liters, 0) AS total_fuel,
			ROUND(total_spot_delay_hours, 2) AS spot_delay_hours
		FROM fleet_summary
		WHERE date_id = 20240101 AND shift_id = 'SHIFT_DAY'
	`)
	if err != nil {
		t.Fatalf("Failed to query metrics: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected one row for day shift")
	}

	row := result.Rows[0]
	_ = row[0].(int64) // dateID - not validated
	shiftID := row[1].(string)
	totalTons := getFloatMetrics(row[2])
	totalCycles := getFloatMetrics(row[3])
	avgCycleMin := getFloatMetrics(row[4])
	totalFuel := getFloatMetrics(row[5])
	spotDelayHours := getFloatMetrics(row[6])

	// Validate calculations for day shift
	if shiftID != "SHIFT_DAY" {
		t.Errorf("Expected shift_id 'SHIFT_DAY', got '%s'", shiftID)
	}
	// Total tons: 190 + 195 + 380 = 765
	if math.Abs(totalTons-765) > 1 {
		t.Errorf("Expected total_tons_moved ~765, got %.0f", totalTons)
	}
	// Total cycles: 3
	if totalCycles != 3 {
		t.Errorf("Expected total_cycles_completed 3, got %.0f", totalCycles)
	}
	// Avg cycle time: (45 + 50 + 60) / 3 = 51.7 minutes
	if math.Abs(avgCycleMin-51.7) > 0.5 {
		t.Errorf("Expected fleet_avg_cycle_time_min ~51.7, got %.1f", avgCycleMin)
	}
	// Total fuel: 85 + 90 + 95 = 270
	if math.Abs(totalFuel-270) > 1 {
		t.Errorf("Expected total_fuel_consumed_liters ~270, got %.0f", totalFuel)
	}
	// Total spot delays: (4 + 2 + 5) / 60 = 0.18 hours
	if math.Abs(spotDelayHours-0.18) > 0.02 {
		t.Errorf("Expected total_spot_delay_hours ~0.18, got %.2f", spotDelayHours)
	}
}

// TestPayloadUtilizationDistribution validates payload distribution by band
func TestPayloadUtilizationDistribution(t *testing.T) {
	adapter, ctx, cleanup := setupMetricsTest(t)
	defer cleanup()

	// Insert test cycle data with various payload utilizations
	// TRUCK_001 capacity: 200 tons
	// Underload (<85%): <170 tons
	// Suboptimal (85-95%): 170-190 tons
	// Optimal (95-105%): 190-210 tons
	// Overload (>105%): >210 tons
	err := adapter.ExecuteDDL(ctx, `
		INSERT INTO fact_haul_cycle VALUES
		('P1', 'TRUCK_001', 'SHOVEL_A', 'CRUSHER_1', 'OP_001', 'SHIFT_DAY', 20240101,
		 '2024-01-01T08:00:00', '2024-01-01T08:45:00',
		 160, 6.5, 6.5, 6, 15, 3, 1.5, 12, 2, 0, 85, 28, 42),  -- 80% underload
		('P2', 'TRUCK_001', 'SHOVEL_A', 'CRUSHER_1', 'OP_001', 'SHIFT_DAY', 20240101,
		 '2024-01-01T09:00:00', '2024-01-01T09:50:00',
		 185, 6.5, 6.5, 7, 16, 5, 1.5, 13, 3, 0, 90, 27, 41),  -- 92.5% suboptimal
		('P3', 'TRUCK_001', 'SHOVEL_A', 'CRUSHER_1', 'OP_001', 'SHIFT_DAY', 20240101,
		 '2024-01-01T10:00:00', '2024-01-01T11:00:00',
		 195, 6.5, 6.5, 8, 18, 8, 1.5, 15, 4, 0, 95, 25, 38),  -- 97.5% optimal
		('P4', 'TRUCK_001', 'SHOVEL_A', 'CRUSHER_1', 'OP_001', 'SHIFT_DAY', 20240101,
		 '2024-01-01T11:15:00', '2024-01-01T12:15:00',
		 200, 6.5, 6.5, 6, 15, 3, 1.5, 12, 2, 0, 85, 28, 42),  -- 100% optimal
		('P5', 'TRUCK_001', 'SHOVEL_A', 'CRUSHER_1', 'OP_001', 'SHIFT_DAY', 20240101,
		 '2024-01-01T13:00:00', '2024-01-01T14:00:00',
		 215, 6.5, 6.5, 7, 16, 5, 1.5, 13, 3, 0, 90, 27, 41)   -- 107.5% overload
	`)
	if err != nil {
		t.Fatalf("Failed to insert test cycles: %v", err)
	}

	// Execute the metrics SQL model - use fleet_summary which should include distribution
	executeMetricsSQL(t, adapter, ctx, "fleet_summary", "fleet_summary")

	// Query and validate payload distribution
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			date_id,
			shift_id,
			cycles_underload,
			cycles_suboptimal,
			cycles_optimal,
			cycles_overload
		FROM fleet_summary
		WHERE date_id = 20240101 AND shift_id = 'SHIFT_DAY'
	`)
	if err != nil {
		t.Fatalf("Failed to query metrics: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected one row for payload distribution")
	}

	row := result.Rows[0]
	cyclesUnderload := getFloatMetrics(row[2])
	cyclesSuboptimal := getFloatMetrics(row[3])
	cyclesOptimal := getFloatMetrics(row[4])
	cyclesOverload := getFloatMetrics(row[5])

	// Validate distribution counts
	if cyclesUnderload != 1 {
		t.Errorf("Expected cycles_underload 1, got %.0f", cyclesUnderload)
	}
	if cyclesSuboptimal != 1 {
		t.Errorf("Expected cycles_suboptimal 1, got %.0f", cyclesSuboptimal)
	}
	if cyclesOptimal != 2 {
		t.Errorf("Expected cycles_optimal 2, got %.0f", cyclesOptimal)
	}
	if cyclesOverload != 1 {
		t.Errorf("Expected cycles_overload 1, got %.0f", cyclesOverload)
	}
}

// TestShiftComparisonMetrics ensures day vs night shift metrics are calculated separately
func TestShiftComparisonMetrics(t *testing.T) {
	adapter, ctx, cleanup := setupMetricsTest(t)
	defer cleanup()

	// Insert test cycle data for both shifts
	err := adapter.ExecuteDDL(ctx, `
		INSERT INTO fact_haul_cycle VALUES
		-- Day shift: 2 cycles
		('S1', 'TRUCK_001', 'SHOVEL_A', 'CRUSHER_1', 'OP_001', 'SHIFT_DAY', 20240101,
		 '2024-01-01T08:00:00', '2024-01-01T08:45:00',
		 190, 6.5, 6.5, 6, 15, 3, 1.5, 12, 2, 0, 85, 28, 42),
		('S2', 'TRUCK_001', 'SHOVEL_A', 'CRUSHER_1', 'OP_001', 'SHIFT_DAY', 20240101,
		 '2024-01-01T09:00:00', '2024-01-01T10:00:00',
		 195, 6.5, 6.5, 7, 16, 5, 1.5, 13, 3, 0, 90, 27, 41),
		-- Night shift: 3 cycles
		('S3', 'TRUCK_002', 'SHOVEL_B', 'CRUSHER_1', 'OP_002', 'SHIFT_NIGHT', 20240101,
		 '2024-01-01T20:00:00', '2024-01-01T20:50:00',
		 380, 6.5, 6.5, 8, 18, 8, 2.0, 15, 4, 0, 95, 25, 38),
		('S4', 'TRUCK_002', 'SHOVEL_B', 'CRUSHER_1', 'OP_002', 'SHIFT_NIGHT', 20240101,
		 '2024-01-01T21:00:00', '2024-01-01T22:00:00',
		 385, 6.5, 6.5, 9, 19, 9, 2.0, 16, 5, 0, 96, 24, 37),
		('S5', 'TRUCK_002', 'SHOVEL_B', 'CRUSHER_1', 'OP_002', 'SHIFT_NIGHT', 20240101,
		 '2024-01-01T22:30:00', '2024-01-01T23:30:00',
		 390, 6.5, 6.5, 8, 17, 7, 2.0, 14, 6, 0, 94, 26, 39)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test cycles: %v", err)
	}

	// Execute the metrics SQL model
	executeMetricsSQL(t, adapter, ctx, "fleet_summary", "fleet_summary")

	// Query and validate both shifts
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			shift_id,
			ROUND(total_tons_moved, 0) AS total_tons,
			total_cycles_completed
		FROM fleet_summary
		WHERE date_id = 20240101
		ORDER BY shift_id
	`)
	if err != nil {
		t.Fatalf("Failed to query metrics: %v", err)
	}

	if len(result.Rows) < 2 {
		t.Fatal("Expected two rows for day and night shifts")
	}

	// Validate day shift (first row after ORDER BY shift_id)
	row := result.Rows[0]
	shiftID := row[0].(string)
	totalTons := getFloatMetrics(row[1])
	totalCycles := getFloatMetrics(row[2])

	if shiftID != "SHIFT_DAY" {
		t.Errorf("Expected shift_id 'SHIFT_DAY', got '%s'", shiftID)
	}
	// Day shift tons: 190 + 195 = 385
	if math.Abs(totalTons-385) > 1 {
		t.Errorf("Expected day shift total_tons_moved ~385, got %.0f", totalTons)
	}
	// Day shift cycles: 2
	if totalCycles != 2 {
		t.Errorf("Expected day shift cycles 2, got %.0f", totalCycles)
	}

	// Validate night shift (second row)
	row = result.Rows[1]
	shiftID = row[0].(string)
	totalTons = getFloatMetrics(row[1])
	totalCycles = getFloatMetrics(row[2])

	if shiftID != "SHIFT_NIGHT" {
		t.Errorf("Expected shift_id 'SHIFT_NIGHT', got '%s'", shiftID)
	}
	// Night shift tons: 380 + 385 + 390 = 1155
	if math.Abs(totalTons-1155) > 1 {
		t.Errorf("Expected night shift total_tons_moved ~1155, got %.0f", totalTons)
	}
	// Night shift cycles: 3
	if totalCycles != 3 {
		t.Errorf("Expected night shift cycles 3, got %.0f", totalCycles)
	}
}

// TestOperatorPerformanceMetrics validates operator-level productivity and efficiency
func TestOperatorPerformanceMetrics(t *testing.T) {
	adapter, ctx, cleanup := setupMetricsTest(t)
	defer cleanup()

	// Insert test cycle data for different operators
	// OP_001 (Senior): faster cycles, better payload
	// OP_002 (Intermediate): medium performance
	// OP_003 (Junior): slower cycles, more spot delays
	err := adapter.ExecuteDDL(ctx, `
		INSERT INTO fact_haul_cycle VALUES
		-- OP_001: 2 cycles, avg 45 min, 97.5% utilization, 1 min spot delay avg
		('O1', 'TRUCK_001', 'SHOVEL_A', 'CRUSHER_1', 'OP_001', 'SHIFT_DAY', 20240101,
		 '2024-01-01T08:00:00', '2024-01-01T08:45:00',
		 195, 6.5, 6.5, 6, 15, 3, 1.5, 12, 2, 1, 85, 28, 42),
		('O2', 'TRUCK_001', 'SHOVEL_A', 'CRUSHER_1', 'OP_001', 'SHIFT_DAY', 20240101,
		 '2024-01-01T09:00:00', '2024-01-01T09:45:00',
		 195, 6.5, 6.5, 6, 15, 3, 1.5, 12, 2, 1, 85, 28, 42),
		-- OP_002: 2 cycles, avg 55 min, 95% utilization, 3 min spot delay avg
		('O3', 'TRUCK_001', 'SHOVEL_A', 'CRUSHER_1', 'OP_002', 'SHIFT_DAY', 20240101,
		 '2024-01-01T11:00:00', '2024-01-01T11:55:00',
		 190, 6.5, 6.5, 7, 17, 5, 1.5, 14, 3, 3, 90, 26, 40),
		('O4', 'TRUCK_001', 'SHOVEL_A', 'CRUSHER_1', 'OP_002', 'SHIFT_DAY', 20240101,
		 '2024-01-01T12:00:00', '2024-01-01T12:55:00',
		 190, 6.5, 6.5, 7, 17, 5, 1.5, 14, 3, 3, 90, 26, 40),
		-- OP_003: 2 cycles, avg 65 min, 92.5% utilization, 6 min spot delay avg
		('O5', 'TRUCK_001', 'SHOVEL_A', 'CRUSHER_1', 'OP_003', 'SHIFT_DAY', 20240101,
		 '2024-01-01T14:00:00', '2024-01-01T15:05:00',
		 185, 6.5, 6.5, 8, 19, 7, 1.5, 16, 4, 6, 92, 24, 38),
		('O6', 'TRUCK_001', 'SHOVEL_A', 'CRUSHER_1', 'OP_003', 'SHIFT_DAY', 20240101,
		 '2024-01-01T15:15:00', '2024-01-01T16:20:00',
		 185, 6.5, 6.5, 8, 19, 7, 1.5, 16, 4, 6, 92, 24, 38)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test cycles: %v", err)
	}

	// Execute the metrics SQL model - use truck_daily_productivity which includes operator
	executeMetricsSQL(t, adapter, ctx, "truck_daily_productivity", "truck_daily_productivity")

	// Query operator performance (aggregate from truck_daily_productivity)
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			operator_id,
			SUM(cycles_completed) AS total_cycles,
			ROUND(AVG(avg_cycle_time_min), 1) AS avg_cycle_time,
			ROUND(AVG(avg_payload_utilization_pct), 1) AS avg_payload_util,
			ROUND(AVG(total_spot_delay_min), 1) AS avg_spot_delay_per_day
		FROM truck_daily_productivity
		WHERE date_id = 20240101
		GROUP BY operator_id
		ORDER BY operator_id
	`)
	if err != nil {
		t.Fatalf("Failed to query metrics: %v", err)
	}

	if len(result.Rows) < 3 {
		t.Fatal("Expected three rows for three operators")
	}

	// Validate OP_001 (Senior - best performance)
	row := result.Rows[0]
	operatorID := row[0].(string)
	_ = getFloatMetrics(row[1]) // totalCycles - not validated in detail
	avgCycleTime := getFloatMetrics(row[2])
	avgPayloadUtil := getFloatMetrics(row[3])
	_ = getFloatMetrics(row[4]) // avgSpotDelay - not validated in detail

	if operatorID != "OP_001" {
		t.Errorf("Expected operator_id 'OP_001', got '%s'", operatorID)
	}
	// OP_001: 45 min avg cycle time
	if math.Abs(avgCycleTime-45.0) > 0.5 {
		t.Errorf("Expected OP_001 avg_cycle_time ~45.0, got %.1f", avgCycleTime)
	}
	// OP_001: 97.5% payload utilization
	if math.Abs(avgPayloadUtil-97.5) > 0.5 {
		t.Errorf("Expected OP_001 avg_payload_utilization_pct ~97.5, got %.1f", avgPayloadUtil)
	}

	// Validate OP_002 (Intermediate - medium performance)
	row = result.Rows[1]
	operatorID = row[0].(string)
	_ = getFloatMetrics(row[1]) // totalCycles - not validated in detail
	avgCycleTime = getFloatMetrics(row[2])
	avgPayloadUtil = getFloatMetrics(row[3])

	if operatorID != "OP_002" {
		t.Errorf("Expected operator_id 'OP_002', got '%s'", operatorID)
	}
	// OP_002: 55 min avg cycle time
	if math.Abs(avgCycleTime-55.0) > 0.5 {
		t.Errorf("Expected OP_002 avg_cycle_time ~55.0, got %.1f", avgCycleTime)
	}
	// OP_002: 95% payload utilization
	if math.Abs(avgPayloadUtil-95.0) > 0.5 {
		t.Errorf("Expected OP_002 avg_payload_utilization_pct ~95.0, got %.1f", avgPayloadUtil)
	}

	// Validate OP_003 (Junior - slower performance)
	row = result.Rows[2]
	operatorID = row[0].(string)
	_ = getFloatMetrics(row[1]) // totalCycles - not validated in detail
	avgCycleTime = getFloatMetrics(row[2])
	avgPayloadUtil = getFloatMetrics(row[3])

	if operatorID != "OP_003" {
		t.Errorf("Expected operator_id 'OP_003', got '%s'", operatorID)
	}
	// OP_003: 65 min avg cycle time
	if math.Abs(avgCycleTime-65.0) > 0.5 {
		t.Errorf("Expected OP_003 avg_cycle_time ~65.0, got %.1f", avgCycleTime)
	}
	// OP_003: 92.5% payload utilization
	if math.Abs(avgPayloadUtil-92.5) > 0.5 {
		t.Errorf("Expected OP_003 avg_payload_utilization_pct ~92.5, got %.1f", avgPayloadUtil)
	}
}
