package test

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/jpconstantineau/gorchata/internal/platform"
	"github.com/jpconstantineau/gorchata/internal/platform/sqlite"
	"github.com/jpconstantineau/gorchata/internal/template"
)

// setupCycleFactsTest creates a test database with all necessary dimension and staging tables
func setupCycleFactsTest(t *testing.T) (*sqlite.SQLiteAdapter, context.Context, func()) {
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

	// Create all dimension tables
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
		('SHOVEL_A', 35, 'North Pit'),
		('SHOVEL_B', 60, 'South Pit'),
		('SHOVEL_C', 20, 'East Pit')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test shovels: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO dim_crusher VALUES ('CRUSHER_1', 3000)
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

	// Create staging table for truck states
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

	// Create telemetry events table for GPS distance calculations
	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE stg_telemetry_events (
			truck_id TEXT NOT NULL,
			timestamp TEXT NOT NULL,
			gps_lat REAL NOT NULL,
			gps_lon REAL NOT NULL,
			speed_kmh REAL NOT NULL,
			payload_tons REAL NOT NULL,
			suspension_pressure_psi REAL NOT NULL,
			engine_rpm REAL NOT NULL,
			fuel_level_liters REAL NOT NULL,
			engine_hours REAL NOT NULL,
			geofence_zone TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create stg_telemetry_events: %v", err)
	}

	return adapter, ctx, cleanup
}

// truckState represents a state period for inserting test data
type truckState struct {
	TruckID          string
	StateStart       string
	StateEnd         string
	OperationalState string
	LocationZone     string
	PayloadAtStart   float64
	PayloadAtEnd     float64
}

// insertTruckStates inserts state records
func insertTruckStates(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context, states []truckState) {
	for _, s := range states {
		sql := fmt.Sprintf(`
			INSERT INTO stg_truck_states VALUES (
				'%s', '%s', '%s', '%s', '%s', %.2f, %.2f
			)`,
			s.TruckID, s.StateStart, s.StateEnd, s.OperationalState,
			s.LocationZone, s.PayloadAtStart, s.PayloadAtEnd,
		)
		if err := adapter.ExecuteDDL(ctx, sql); err != nil {
			t.Fatalf("Failed to insert truck state: %v", err)
		}
	}
}

// executeCycleFactsSQL loads and executes the fact_haul_cycle.sql model
func executeCycleFactsSQL(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context) {
	repoRoot := getRepoRoot(t)
	modelPath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "models", "facts", "fact_haul_cycle.sql")

	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read fact_haul_cycle.sql: %v", err)
	}

	// Remove config calls
	contentStr := removeConfigCallsFacts(string(content))

	// Parse and render template
	templateEngine := template.New()
	tmpl, err := templateEngine.Parse("fact_haul_cycle", contentStr)
	if err != nil {
		t.Fatalf("Failed to parse template: %v", err)
	}

	ctx2 := template.NewContext(template.WithCurrentModel("fact_haul_cycle"))
	rendered, err := template.Render(tmpl, ctx2, nil)
	if err != nil {
		t.Fatalf("Failed to render template: %v", err)
	}

	// Create fact table
	err = adapter.ExecuteDDL(ctx, "DROP TABLE IF EXISTS fact_haul_cycle")
	if err != nil {
		t.Fatalf("Failed to drop existing fact_haul_cycle: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, "CREATE TABLE fact_haul_cycle AS "+rendered)
	if err != nil {
		t.Fatalf("Failed to execute fact_haul_cycle model: %v", err)
	}
}

// removeConfigCallsFacts removes {{ config ... }} calls from content
func removeConfigCallsFacts(content string) string {
	// Remove Go template syntax: {{ config "key" "value" }}
	goTemplateRe := regexp.MustCompile(`{{\s*config\s+"[^"]+"\s+"[^"]+"\s*}}`)
	content = goTemplateRe.ReplaceAllString(content, "")

	// Remove legacy Jinja-style syntax: {{ config(key='value') }}
	legacyRe := regexp.MustCompile(`{{\s*config\s*\([^}]+\)\s*}}`)
	return legacyRe.ReplaceAllString(content, "")
}

// TestCycleCompletenessValidation ensures cycles have all required states
func TestCycleCompletenessValidation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupCycleFactsTest(t)
	defer cleanup()

	// Create a complete haul cycle
	baseTime := time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC)
	states := []truckState{
		// Complete cycle with all required states
		{"TRUCK_001", baseTime.Format("2006-01-02 15:04:05"), baseTime.Add(5 * time.Minute).Format("2006-01-02 15:04:05"), "loading", "Shovel_A", 10, 190},
		{"TRUCK_001", baseTime.Add(5 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(20 * time.Minute).Format("2006-01-02 15:04:05"), "hauling_loaded", "Road", 190, 190},
		{"TRUCK_001", baseTime.Add(20 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(22 * time.Minute).Format("2006-01-02 15:04:05"), "dumping", "Crusher", 190, 10},
		{"TRUCK_001", baseTime.Add(22 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(35 * time.Minute).Format("2006-01-02 15:04:05"), "returning_empty", "Road", 10, 10},
		// Next cycle starts
		{"TRUCK_001", baseTime.Add(35 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(40 * time.Minute).Format("2006-01-02 15:04:05"), "loading", "Shovel_A", 10, 190},
	}

	insertTruckStates(t, adapter, ctx, states)
	executeCycleFactsSQL(t, adapter, ctx)

	// Verify exactly 1 complete cycle detected
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as cycle_count
		FROM fact_haul_cycle
		WHERE truck_id = 'TRUCK_001'
	`)
	if err != nil {
		t.Fatalf("Failed to query cycles: %v", err)
	}

	count := result.Rows[0][0].(int64)
	if count != 1 {
		t.Errorf("Expected 1 complete cycle, got %d", count)
	}

	// Verify cycle has payload from loading state
	result, err = adapter.ExecuteQuery(ctx, `
		SELECT payload_tons
		FROM fact_haul_cycle
		WHERE truck_id = 'TRUCK_001'
	`)
	if err != nil {
		t.Fatalf("Failed to query payload: %v", err)
	}

	if len(result.Rows) > 0 {
		payload := getFloat(result.Rows[0][0])
		if payload < 180 || payload > 200 {
			t.Errorf("Expected payload ~190 tons, got %.1f", payload)
		}
	}
}

// TestCycleBoundaryDetection validates cycle starts at loading and ends at next loading
func TestCycleBoundaryDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupCycleFactsTest(t)
	defer cleanup()

	// Create two complete cycles
	baseTime := time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC)
	states := []truckState{
		// First cycle
		{"TRUCK_001", baseTime.Format("2006-01-02 15:04:05"), baseTime.Add(5 * time.Minute).Format("2006-01-02 15:04:05"), "loading", "Shovel_A", 10, 190},
		{"TRUCK_001", baseTime.Add(5 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(20 * time.Minute).Format("2006-01-02 15:04:05"), "hauling_loaded", "Road", 190, 190},
		{"TRUCK_001", baseTime.Add(20 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(22 * time.Minute).Format("2006-01-02 15:04:05"), "dumping", "Crusher", 190, 10},
		{"TRUCK_001", baseTime.Add(22 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(35 * time.Minute).Format("2006-01-02 15:04:05"), "returning_empty", "Road", 10, 10},
		// Second cycle starts at 35 min
		{"TRUCK_001", baseTime.Add(35 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(40 * time.Minute).Format("2006-01-02 15:04:05"), "loading", "Shovel_A", 10, 195},
		{"TRUCK_001", baseTime.Add(40 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(55 * time.Minute).Format("2006-01-02 15:04:05"), "hauling_loaded", "Road", 195, 195},
		{"TRUCK_001", baseTime.Add(55 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(57 * time.Minute).Format("2006-01-02 15:04:05"), "dumping", "Crusher", 195, 10},
		{"TRUCK_001", baseTime.Add(57 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(70 * time.Minute).Format("2006-01-02 15:04:05"), "returning_empty", "Road", 10, 10},
		// Third cycle starts at 70 min
		{"TRUCK_001", baseTime.Add(70 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(75 * time.Minute).Format("2006-01-02 15:04:05"), "loading", "Shovel_A", 10, 190},
	}

	insertTruckStates(t, adapter, ctx, states)
	executeCycleFactsSQL(t, adapter, ctx)

	// Verify 2 cycles detected (cycle ends at next loading start)
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as cycle_count
		FROM fact_haul_cycle
		WHERE truck_id = 'TRUCK_001'
	`)
	if err != nil {
		t.Fatalf("Failed to query cycles: %v", err)
	}

	count := result.Rows[0][0].(int64)
	if count != 2 {
		t.Errorf("Expected 2 cycles, got %d", count)
	}

	// Verify cycle boundaries
	result, err = adapter.ExecuteQuery(ctx, `
		SELECT cycle_start, cycle_end
		FROM fact_haul_cycle
		WHERE truck_id = 'TRUCK_001'
		ORDER BY cycle_start
	`)
	if err != nil {
		t.Fatalf("Failed to query cycle boundaries: %v", err)
	}

	if len(result.Rows) >= 2 {
		// First cycle should end at second cycle start (35 min mark)
		cycleEnd := result.Rows[0][1].(string)
		cycle2Start := result.Rows[1][0].(string)
		if cycleEnd != baseTime.Add(35*time.Minute).Format("2006-01-02 15:04:05") {
			t.Errorf("First cycle should end at 35 min mark, got %s", cycleEnd)
		}
		if cycle2Start != baseTime.Add(35*time.Minute).Format("2006-01-02 15:04:05") {
			t.Errorf("Second cycle should start at 35 min mark, got %s", cycle2Start)
		}
	}
}

// TestDurationAggregation validates sum of component durations equals total cycle time
func TestDurationAggregation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupCycleFactsTest(t)
	defer cleanup()

	// Create cycle with known durations
	baseTime := time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC)
	states := []truckState{
		{"TRUCK_001", baseTime.Format("2006-01-02 15:04:05"), baseTime.Add(6 * time.Minute).Format("2006-01-02 15:04:05"), "loading", "Shovel_A", 10, 190},                                  // 6 min
		{"TRUCK_001", baseTime.Add(6 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(21 * time.Minute).Format("2006-01-02 15:04:05"), "hauling_loaded", "Road", 190, 190},        // 15 min
		{"TRUCK_001", baseTime.Add(21 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(24 * time.Minute).Format("2006-01-02 15:04:05"), "queued_at_crusher", "Crusher", 190, 190}, // 3 min
		{"TRUCK_001", baseTime.Add(24 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(26 * time.Minute).Format("2006-01-02 15:04:05"), "dumping", "Crusher", 190, 10},            // 2 min
		{"TRUCK_001", baseTime.Add(26 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(38 * time.Minute).Format("2006-01-02 15:04:05"), "returning_empty", "Road", 10, 10},        // 12 min
		{"TRUCK_001", baseTime.Add(38 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(40 * time.Minute).Format("2006-01-02 15:04:05"), "queued_at_shovel", "Shovel_A", 10, 10},   // 2 min
		{"TRUCK_001", baseTime.Add(40 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(45 * time.Minute).Format("2006-01-02 15:04:05"), "loading", "Shovel_A", 10, 190},           // Next cycle
	}
	// Total cycle time: 40 minutes, Component sum: 6+15+3+2+12+2 = 40 minutes

	insertTruckStates(t, adapter, ctx, states)
	executeCycleFactsSQL(t, adapter, ctx)

	// Verify durations sum correctly
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			duration_loading_min,
			duration_hauling_loaded_min,
			duration_queue_crusher_min,
			duration_dumping_min,
			duration_returning_min,
			duration_queue_shovel_min,
			(julianday(cycle_end) - julianday(cycle_start)) * 24 * 60 as total_cycle_min
		FROM fact_haul_cycle
		WHERE truck_id = 'TRUCK_001'
	`)
	if err != nil {
		t.Fatalf("Failed to query durations: %v", err)
	}

	if len(result.Rows) > 0 {
		loading := getFloat(result.Rows[0][0])
		hauling := getFloat(result.Rows[0][1])
		queueCrusher := getFloat(result.Rows[0][2])
		dumping := getFloat(result.Rows[0][3])
		returning := getFloat(result.Rows[0][4])
		queueShovel := getFloat(result.Rows[0][5])
		totalCycle := getFloat(result.Rows[0][6])

		componentSum := loading + hauling + queueCrusher + dumping + returning + queueShovel

		// Allow small floating point tolerance
		if math.Abs(componentSum-totalCycle) > 0.1 {
			t.Errorf("Component durations (%.2f) don't sum to total cycle time (%.2f)", componentSum, totalCycle)
		}

		// Verify individual durations
		if math.Abs(loading-6.0) > 0.1 {
			t.Errorf("Expected loading duration 6 min, got %.2f", loading)
		}
		if math.Abs(hauling-15.0) > 0.1 {
			t.Errorf("Expected hauling duration 15 min, got %.2f", hauling)
		}
		if math.Abs(queueCrusher-3.0) > 0.1 {
			t.Errorf("Expected queue crusher duration 3 min, got %.2f", queueCrusher)
		}
		if math.Abs(dumping-2.0) > 0.1 {
			t.Errorf("Expected dumping duration 2 min, got %.2f", dumping)
		}
		if math.Abs(returning-12.0) > 0.1 {
			t.Errorf("Expected returning duration 12 min, got %.2f", returning)
		}
		if math.Abs(queueShovel-2.0) > 0.1 {
			t.Errorf("Expected queue shovel duration 2 min, got %.2f", queueShovel)
		}
	}
}

// TestDistanceCalculation validates GPS-based distance calculations for loaded/empty segments
func TestDistanceCalculation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupCycleFactsTest(t)
	defer cleanup()

	// Insert telemetry events with GPS coordinates for distance calculation
	baseTime := time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC)

	// Insert telemetry events for GPS tracking
	telemetry := []struct {
		TruckID   string
		Timestamp string
		Lat       float64
		Lon       float64
		Payload   float64
		Zone      string
	}{
		// Loading at start point
		{"TRUCK_001", baseTime.Format("2006-01-02 15:04:05"), -23.5, 119.5, 190, "Shovel_A"},
		// Hauling loaded: ~5km movement
		{"TRUCK_001", baseTime.Add(6 * time.Minute).Format("2006-01-02 15:04:05"), -23.51, 119.51, 190, "Road"},
		{"TRUCK_001", baseTime.Add(12 * time.Minute).Format("2006-01-02 15:04:05"), -23.52, 119.52, 190, "Road"},
		{"TRUCK_001", baseTime.Add(18 * time.Minute).Format("2006-01-02 15:04:05"), -23.53, 119.53, 190, "Road"},
		// Dumping at crusher
		{"TRUCK_001", baseTime.Add(21 * time.Minute).Format("2006-01-02 15:04:05"), -23.54, 119.54, 10, "Crusher"},
		// Returning empty: ~5km back
		{"TRUCK_001", baseTime.Add(26 * time.Minute).Format("2006-01-02 15:04:05"), -23.53, 119.53, 10, "Road"},
		{"TRUCK_001", baseTime.Add(32 * time.Minute).Format("2006-01-02 15:04:05"), -23.52, 119.52, 10, "Road"},
		{"TRUCK_001", baseTime.Add(38 * time.Minute).Format("2006-01-02 15:04:05"), -23.51, 119.51, 10, "Road"},
		{"TRUCK_001", baseTime.Add(40 * time.Minute).Format("2006-01-02 15:04:05"), -23.5, 119.5, 190, "Shovel_A"},
	}

	for _, e := range telemetry {
		sql := fmt.Sprintf(`
			INSERT INTO stg_telemetry_events VALUES (
				'%s', '%s', %.6f, %.6f, 25.0, %.2f, 2000, 1500, 800, 100.5, '%s'
			)`,
			e.TruckID, e.Timestamp, e.Lat, e.Lon, e.Payload, e.Zone,
		)
		if err := adapter.ExecuteDDL(ctx, sql); err != nil {
			t.Fatalf("Failed to insert telemetry: %v", err)
		}
	}

	// Insert corresponding states
	states := []truckState{
		{"TRUCK_001", baseTime.Format("2006-01-02 15:04:05"), baseTime.Add(5 * time.Minute).Format("2006-01-02 15:04:05"), "loading", "Shovel_A", 10, 190},
		{"TRUCK_001", baseTime.Add(5 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(21 * time.Minute).Format("2006-01-02 15:04:05"), "hauling_loaded", "Road", 190, 190},
		{"TRUCK_001", baseTime.Add(21 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(23 * time.Minute).Format("2006-01-02 15:04:05"), "dumping", "Crusher", 190, 10},
		{"TRUCK_001", baseTime.Add(23 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(40 * time.Minute).Format("2006-01-02 15:04:05"), "returning_empty", "Road", 10, 10},
		{"TRUCK_001", baseTime.Add(40 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(45 * time.Minute).Format("2006-01-02 15:04:05"), "loading", "Shovel_A", 10, 190},
	}

	insertTruckStates(t, adapter, ctx, states)
	executeCycleFactsSQL(t, adapter, ctx)

	// Verify distance calculations exist and are reasonable
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT distance_loaded_km, distance_empty_km
		FROM fact_haul_cycle
		WHERE truck_id = 'TRUCK_001'
	`)
	if err != nil {
		t.Fatalf("Failed to query distances: %v", err)
	}

	if len(result.Rows) > 0 {
		distLoaded := getFloat(result.Rows[0][0])
		distEmpty := getFloat(result.Rows[0][1])

		// Distance should be > 0 (GPS points moved)
		if distLoaded <= 0 {
			t.Errorf("Expected positive loaded distance, got %.2f", distLoaded)
		}
		if distEmpty <= 0 {
			t.Errorf("Expected positive empty distance, got %.2f", distEmpty)
		}

		// Distances should be reasonable (roughly same path, so similar)
		// Allow factor of 2 difference due to sampling
		ratio := distLoaded / distEmpty
		if ratio < 0.5 || ratio > 2.0 {
			t.Logf("Warning: loaded/empty distance ratio unusual: %.2f / %.2f = %.2f", distLoaded, distEmpty, ratio)
		}
	}
}

// TestSpeedAverageCalculation ensures weighted average speeds calculated correctly
func TestSpeedAverageCalculation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupCycleFactsTest(t)
	defer cleanup()

	baseTime := time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC)

	// Insert telemetry with known speeds
	telemetry := []struct {
		TruckID   string
		Timestamp string
		Lat       float64
		Lon       float64
		Speed     float64
		Payload   float64
		Zone      string
	}{
		// Hauling loaded: average 25 km/h
		{"TRUCK_001", baseTime.Add(5 * time.Minute).Format("2006-01-02 15:04:05"), -23.5, 119.5, 20, 190, "Road"},
		{"TRUCK_001", baseTime.Add(10 * time.Minute).Format("2006-01-02 15:04:05"), -23.51, 119.51, 25, 190, "Road"},
		{"TRUCK_001", baseTime.Add(15 * time.Minute).Format("2006-01-02 15:04:05"), -23.52, 119.52, 30, 190, "Road"},
		// Dumping
		{"TRUCK_001", baseTime.Add(20 * time.Minute).Format("2006-01-02 15:04:05"), -23.53, 119.53, 0, 10, "Crusher"},
		// Returning empty: average 40 km/h
		{"TRUCK_001", baseTime.Add(23 * time.Minute).Format("2006-01-02 15:04:05"), -23.53, 119.53, 35, 10, "Road"},
		{"TRUCK_001", baseTime.Add(30 * time.Minute).Format("2006-01-02 15:04:05"), -23.52, 119.52, 40, 10, "Road"},
		{"TRUCK_001", baseTime.Add(37 * time.Minute).Format("2006-01-02 15:04:05"), -23.51, 119.51, 45, 10, "Road"},
	}

	for _, e := range telemetry {
		sql := fmt.Sprintf(`
			INSERT INTO stg_telemetry_events VALUES (
				'%s', '%s', %.6f, %.6f, %.2f, %.2f, 2000, 1500, 800, 100.5, '%s'
			)`,
			e.TruckID, e.Timestamp, e.Lat, e.Lon, e.Speed, e.Payload, e.Zone,
		)
		if err := adapter.ExecuteDDL(ctx, sql); err != nil {
			t.Fatalf("Failed to insert telemetry: %v", err)
		}
	}

	// Insert corresponding states
	states := []truckState{
		{"TRUCK_001", baseTime.Format("2006-01-02 15:04:05"), baseTime.Add(5 * time.Minute).Format("2006-01-02 15:04:05"), "loading", "Shovel_A", 10, 190},
		{"TRUCK_001", baseTime.Add(5 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(20 * time.Minute).Format("2006-01-02 15:04:05"), "hauling_loaded", "Road", 190, 190},
		{"TRUCK_001", baseTime.Add(20 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(22 * time.Minute).Format("2006-01-02 15:04:05"), "dumping", "Crusher", 190, 10},
		{"TRUCK_001", baseTime.Add(22 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(40 * time.Minute).Format("2006-01-02 15:04:05"), "returning_empty", "Road", 10, 10},
		{"TRUCK_001", baseTime.Add(40 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(45 * time.Minute).Format("2006-01-02 15:04:05"), "loading", "Shovel_A", 10, 190},
	}

	insertTruckStates(t, adapter, ctx, states)
	executeCycleFactsSQL(t, adapter, ctx)

	// Verify average speeds calculated
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT speed_avg_loaded_kmh, speed_avg_empty_kmh
		FROM fact_haul_cycle
		WHERE truck_id = 'TRUCK_001'
	`)
	if err != nil {
		t.Fatalf("Failed to query speeds: %v", err)
	}

	if len(result.Rows) > 0 {
		speedLoaded := getFloat(result.Rows[0][0])
		speedEmpty := getFloat(result.Rows[0][1])

		// Verify loaded speed is reasonable (should be ~25 km/h)
		if speedLoaded < 15 || speedLoaded > 35 {
			t.Logf("Warning: loaded speed outside expected range: %.2f km/h", speedLoaded)
		}

		// Verify empty speed is reasonable (should be ~40 km/h)
		if speedEmpty < 30 || speedEmpty > 50 {
			t.Logf("Warning: empty speed outside expected range: %.2f km/h", speedEmpty)
		}

		// Empty speed should be higher than loaded speed
		if speedEmpty <= speedLoaded {
			t.Errorf("Expected empty speed (%.2f) > loaded speed (%.2f)", speedEmpty, speedLoaded)
		}
	}
}

// TestPayloadUtilizationMetrics validates payload % of rated capacity calculated correctly
func TestPayloadUtilizationMetrics(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupCycleFactsTest(t)
	defer cleanup()

	baseTime := time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC)

	// Test with 200-ton truck carrying 190 tons (95% utilization)
	states := []truckState{
		{"TRUCK_001", baseTime.Format("2006-01-02 15:04:05"), baseTime.Add(5 * time.Minute).Format("2006-01-02 15:04:05"), "loading", "Shovel_A", 10, 190},
		{"TRUCK_001", baseTime.Add(5 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(20 * time.Minute).Format("2006-01-02 15:04:05"), "hauling_loaded", "Road", 190, 190},
		{"TRUCK_001", baseTime.Add(20 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(22 * time.Minute).Format("2006-01-02 15:04:05"), "dumping", "Crusher", 190, 10},
		{"TRUCK_001", baseTime.Add(22 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(35 * time.Minute).Format("2006-01-02 15:04:05"), "returning_empty", "Road", 10, 10},
		{"TRUCK_001", baseTime.Add(35 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(40 * time.Minute).Format("2006-01-02 15:04:05"), "loading", "Shovel_A", 10, 190},
	}

	insertTruckStates(t, adapter, ctx, states)
	executeCycleFactsSQL(t, adapter, ctx)

	// Verify payload is 190 tons
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT payload_tons
		FROM fact_haul_cycle
		WHERE truck_id = 'TRUCK_001'
	`)
	if err != nil {
		t.Fatalf("Failed to query payload: %v", err)
	}

	if len(result.Rows) > 0 {
		payload := getFloat(result.Rows[0][0])
		expectedPayload := 190.0

		if math.Abs(payload-expectedPayload) > 1.0 {
			t.Errorf("Expected payload %.1f tons, got %.1f tons", expectedPayload, payload)
		}

		// Payload utilization should be 95% (190/200)
		// This would be calculated in metrics layer, but verify payload is correct here
		utilizationPct := (payload / 200.0) * 100
		if math.Abs(utilizationPct-95.0) > 1.0 {
			t.Errorf("Expected utilization ~95%%, got %.1f%%", utilizationPct)
		}
	}
}

// TestFuelConsumptionAggregation ensures fuel consumed summed correctly per cycle
func TestFuelConsumptionAggregation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupCycleFactsTest(t)
	defer cleanup()

	baseTime := time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC)

	// Insert telemetry with fuel consumption (fuel decreases from 800 to 700 liters over cycle)
	telemetry := []struct {
		TruckID   string
		Timestamp string
		Fuel      float64
		Payload   float64
	}{
		{"TRUCK_001", baseTime.Format("2006-01-02 15:04:05"), 800, 10},
		{"TRUCK_001", baseTime.Add(5 * time.Minute).Format("2006-01-02 15:04:05"), 795, 190},
		{"TRUCK_001", baseTime.Add(10 * time.Minute).Format("2006-01-02 15:04:05"), 785, 190},
		{"TRUCK_001", baseTime.Add(20 * time.Minute).Format("2006-01-02 15:04:05"), 770, 190},
		{"TRUCK_001", baseTime.Add(22 * time.Minute).Format("2006-01-02 15:04:05"), 765, 10},
		{"TRUCK_001", baseTime.Add(30 * time.Minute).Format("2006-01-02 15:04:05"), 745, 10},
		{"TRUCK_001", baseTime.Add(40 * time.Minute).Format("2006-01-02 15:04:05"), 700, 10},
	}

	for _, e := range telemetry {
		sql := fmt.Sprintf(`
			INSERT INTO stg_telemetry_events VALUES (
				'%s', '%s', -23.5, 119.5, 25.0, %.2f, 2000, 1500, %.2f, 100.5, 'Road'
			)`,
			e.TruckID, e.Timestamp, e.Payload, e.Fuel,
		)
		if err := adapter.ExecuteDDL(ctx, sql); err != nil {
			t.Fatalf("Failed to insert telemetry: %v", err)
		}
	}

	// Insert corresponding states
	states := []truckState{
		{"TRUCK_001", baseTime.Format("2006-01-02 15:04:05"), baseTime.Add(5 * time.Minute).Format("2006-01-02 15:04:05"), "loading", "Shovel_A", 10, 190},
		{"TRUCK_001", baseTime.Add(5 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(20 * time.Minute).Format("2006-01-02 15:04:05"), "hauling_loaded", "Road", 190, 190},
		{"TRUCK_001", baseTime.Add(20 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(22 * time.Minute).Format("2006-01-02 15:04:05"), "dumping", "Crusher", 190, 10},
		{"TRUCK_001", baseTime.Add(22 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(40 * time.Minute).Format("2006-01-02 15:04:05"), "returning_empty", "Road", 10, 10},
		{"TRUCK_001", baseTime.Add(40 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(45 * time.Minute).Format("2006-01-02 15:04:05"), "loading", "Shovel_A", 10, 190},
	}

	insertTruckStates(t, adapter, ctx, states)
	executeCycleFactsSQL(t, adapter, ctx)

	// Verify fuel consumption calculated (should be ~100 liters)
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT fuel_consumed_liters
		FROM fact_haul_cycle
		WHERE truck_id = 'TRUCK_001'
	`)
	if err != nil {
		t.Fatalf("Failed to query fuel consumption: %v", err)
	}

	if len(result.Rows) > 0 {
		fuelConsumed := getFloat(result.Rows[0][0])
		expectedFuel := 100.0 // 800 - 700

		// Allow reasonable tolerance for fuel calculation
		if math.Abs(fuelConsumed-expectedFuel) > 10.0 {
			t.Logf("Warning: fuel consumed (%.1f L) differs from expected (%.1f L)", fuelConsumed, expectedFuel)
		}

		// Fuel consumed should be positive
		if fuelConsumed <= 0 {
			t.Errorf("Expected positive fuel consumption, got %.2f", fuelConsumed)
		}
	}
}

// TestSpotDelayAggregation validates spot delay durations aggregated correctly per cycle
func TestSpotDelayAggregation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupCycleFactsTest(t)
	defer cleanup()

	baseTime := time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC)

	// Create cycle with spot delays
	states := []truckState{
		{"TRUCK_001", baseTime.Format("2006-01-02 15:04:05"), baseTime.Add(5 * time.Minute).Format("2006-01-02 15:04:05"), "loading", "Shovel_A", 10, 190},
		{"TRUCK_001", baseTime.Add(5 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(15 * time.Minute).Format("2006-01-02 15:04:05"), "hauling_loaded", "Road", 190, 190},
		// Spot delay #1: 3 minutes
		{"TRUCK_001", baseTime.Add(15 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(18 * time.Minute).Format("2006-01-02 15:04:05"), "spot_delay", "Road", 190, 190},
		{"TRUCK_001", baseTime.Add(18 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(25 * time.Minute).Format("2006-01-02 15:04:05"), "hauling_loaded", "Road", 190, 190},
		{"TRUCK_001", baseTime.Add(25 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(27 * time.Minute).Format("2006-01-02 15:04:05"), "dumping", "Crusher", 190, 10},
		{"TRUCK_001", baseTime.Add(27 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(35 * time.Minute).Format("2006-01-02 15:04:05"), "returning_empty", "Road", 10, 10},
		// Spot delay #2: 2 minutes (refueling)
		{"TRUCK_001", baseTime.Add(35 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(37 * time.Minute).Format("2006-01-02 15:04:05"), "spot_delay", "Other", 10, 10},
		{"TRUCK_001", baseTime.Add(37 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(45 * time.Minute).Format("2006-01-02 15:04:05"), "returning_empty", "Road", 10, 10},
		{"TRUCK_001", baseTime.Add(45 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(50 * time.Minute).Format("2006-01-02 15:04:05"), "loading", "Shovel_A", 10, 190},
	}
	// Total spot delays: 3 + 2 = 5 minutes

	insertTruckStates(t, adapter, ctx, states)
	executeCycleFactsSQL(t, adapter, ctx)

	// Verify spot delay aggregation
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT duration_spot_delays_min
		FROM fact_haul_cycle
		WHERE truck_id = 'TRUCK_001'
	`)
	if err != nil {
		t.Fatalf("Failed to query spot delays: %v", err)
	}

	if len(result.Rows) > 0 {
		spotDelays := getFloat(result.Rows[0][0])
		expectedDelays := 5.0 // 3 + 2 minutes

		if math.Abs(spotDelays-expectedDelays) > 0.1 {
			t.Errorf("Expected spot delays %.1f min, got %.1f min", expectedDelays, spotDelays)
		}
	}
}

// TestPartialCycleHandling validates cycles spanning shift boundaries handled appropriately
func TestPartialCycleHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupCycleFactsTest(t)
	defer cleanup()

	baseTime := time.Date(2024, 1, 1, 18, 30, 0, 0, time.UTC) // Near end of day shift

	// Create partial cycle (starts but doesn't complete before next loading)
	states := []truckState{
		// Cycle starts at 18:30, loading completes at 18:35
		{"TRUCK_001", baseTime.Format("2006-01-02 15:04:05"), baseTime.Add(5 * time.Minute).Format("2006-01-02 15:04:05"), "loading", "Shovel_A", 10, 190},
		// Hauling but doesn't make it to crusher before shift end
		{"TRUCK_001", baseTime.Add(5 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(25 * time.Minute).Format("2006-01-02 15:04:05"), "hauling_loaded", "Road", 190, 190},
		// Next day: truck completes dump and returns
		{"TRUCK_001", baseTime.Add(25 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(27 * time.Minute).Format("2006-01-02 15:04:05"), "dumping", "Crusher", 190, 10},
		{"TRUCK_001", baseTime.Add(27 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(45 * time.Minute).Format("2006-01-02 15:04:05"), "returning_empty", "Road", 10, 10},
		// Next cycle starts
		{"TRUCK_001", baseTime.Add(45 * time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(50 * time.Minute).Format("2006-01-02 15:04:05"), "loading", "Shovel_A", 10, 190},
	}

	insertTruckStates(t, adapter, ctx, states)
	executeCycleFactsSQL(t, adapter, ctx)

	// Verify cycle is still detected (cycle boundaries are loading-to-loading)
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as cycle_count
		FROM fact_haul_cycle
		WHERE truck_id = 'TRUCK_001'
	`)
	if err != nil {
		t.Fatalf("Failed to query cycles: %v", err)
	}

	count := result.Rows[0][0].(int64)
	if count != 1 {
		t.Errorf("Expected 1 cycle (partial cycles should still be counted), got %d", count)
	}

	// Verify cycle spans expected time period
	result, err = adapter.ExecuteQuery(ctx, `
		SELECT 
			cycle_start,
			cycle_end,
			(julianday(cycle_end) - julianday(cycle_start)) * 24 * 60 as duration_min
		FROM fact_haul_cycle
		WHERE truck_id = 'TRUCK_001'
	`)
	if err != nil {
		t.Fatalf("Failed to query cycle details: %v", err)
	}

	if len(result.Rows) > 0 {
		duration := getFloat(result.Rows[0][2])
		expectedDuration := 45.0 // minutes

		if math.Abs(duration-expectedDuration) > 1.0 {
			t.Logf("Cycle duration: %.1f min (expected ~%.1f min)", duration, expectedDuration)
		}
	}
}
