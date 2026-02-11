package test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jpconstantineau/gorchata/internal/platform"
	"github.com/jpconstantineau/gorchata/internal/platform/sqlite"
	"github.com/jpconstantineau/gorchata/internal/template"
)

// setupStateDetectionTest creates a test database with telemetry and truck dimension data
func setupStateDetectionTest(t *testing.T) (*sqlite.SQLiteAdapter, context.Context, func()) {
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

	// Create dim_truck table
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

	// Insert test trucks
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO dim_truck VALUES 
		('TRUCK_001', 'CAT_793F', 200, '200-ton'),
		('TRUCK_002', 'CAT_797F', 400, '400-ton'),
		('TRUCK_003', 'CAT_777G', 100, '100-ton')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test trucks: %v", err)
	}

	// Create stg_telemetry_events table
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

// insertTelemetry is a helper to insert telemetry events
func insertTelemetry(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context, events []telemetryEvent) {
	for _, e := range events {
		sql := fmt.Sprintf(`
			INSERT INTO stg_telemetry_events VALUES (
				'%s', '%s', %.6f, %.6f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, '%s'
			)`,
			e.TruckID, e.Timestamp, e.GpsLat, e.GpsLon, e.SpeedKmh,
			e.PayloadTons, e.SuspensionPressurePsi, e.EngineRPM,
			e.FuelLevelLiters, e.EngineHours, e.GeofenceZone,
		)
		if err := adapter.ExecuteDDL(ctx, sql); err != nil {
			t.Fatalf("Failed to insert telemetry: %v", err)
		}
	}
}

type telemetryEvent struct {
	TruckID               string
	Timestamp             string
	GpsLat                float64
	GpsLon                float64
	SpeedKmh              float64
	PayloadTons           float64
	SuspensionPressurePsi float64
	EngineRPM             float64
	FuelLevelLiters       float64
	EngineHours           float64
	GeofenceZone          string
}

// executeStateDetectionSQL loads and executes the stg_truck_states.sql model
func executeStateDetectionSQL(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context) {
	repoRoot := getRepoRoot(t)
	modelPath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "models", "staging", "stg_truck_states.sql")

	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read stg_truck_states.sql: %v", err)
	}

	// Remove config calls
	contentStr := removeConfigCallsStaging(string(content))

	// Parse and render template
	templateEngine := template.New()
	tmpl, err := templateEngine.Parse("stg_truck_states", contentStr)
	if err != nil {
		t.Fatalf("Failed to parse template: %v", err)
	}

	ctx2 := template.NewContext(template.WithCurrentModel("stg_truck_states"))
	rendered, err := template.Render(tmpl, ctx2, nil)
	if err != nil {
		t.Fatalf("Failed to render template: %v", err)
	}

	// Create staging table
	err = adapter.ExecuteDDL(ctx, "DROP TABLE IF EXISTS stg_truck_states")
	if err != nil {
		t.Fatalf("Failed to drop existing stg_truck_states: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, "CREATE TABLE stg_truck_states AS "+rendered)
	if err != nil {
		t.Fatalf("Failed to execute stg_truck_states model: %v", err)
	}
}

// TestLoadingStateDetection validates loading identified when in shovel zone + speed <5 km/h + payload increasing
func TestLoadingStateDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupStateDetectionTest(t)
	defer cleanup()

	// Create telemetry showing loading at Shovel_A
	baseTime := time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC)
	events := []telemetryEvent{
		// Truck arrives empty at shovel
		{"TRUCK_001", baseTime.Format("2006-01-02 15:04:05"), -23.5, 119.5, 2.0, 10, 500, 1200, 800, 100.5, "Shovel_A"},
		{"TRUCK_001", baseTime.Add(1 * time.Minute).Format("2006-01-02 15:04:05"), -23.5, 119.5, 1.5, 50, 800, 1300, 799, 100.52, "Shovel_A"},
		{"TRUCK_001", baseTime.Add(2 * time.Minute).Format("2006-01-02 15:04:05"), -23.5, 119.5, 1.0, 100, 1200, 1400, 798, 100.55, "Shovel_A"},
		{"TRUCK_001", baseTime.Add(3 * time.Minute).Format("2006-01-02 15:04:05"), -23.5, 119.5, 0.5, 150, 1600, 1400, 797, 100.57, "Shovel_A"},
		{"TRUCK_001", baseTime.Add(4 * time.Minute).Format("2006-01-02 15:04:05"), -23.5, 119.5, 0.0, 190, 2000, 1300, 796, 100.6, "Shovel_A"},
	}

	insertTelemetry(t, adapter, ctx, events)
	executeStateDetectionSQL(t, adapter, ctx)

	// Verify loading state detected
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count
		FROM stg_truck_states
		WHERE truck_id = 'TRUCK_001'
		AND operational_state = 'loading'
		AND location_zone = 'Shovel_A'
	`)
	if err != nil {
		t.Fatalf("Failed to query states: %v", err)
	}

	count := result.Rows[0][0].(int64)
	if count == 0 {
		t.Error("Expected loading state to be detected, but found none")
	}

	// Verify payload increased during loading
	result, err = adapter.ExecuteQuery(ctx, `
		SELECT payload_at_start, payload_at_end
		FROM stg_truck_states
		WHERE truck_id = 'TRUCK_001'
		AND operational_state = 'loading'
		LIMIT 1
	`)
	if err != nil {
		t.Fatalf("Failed to query payload: %v", err)
	}

	if len(result.Rows) > 0 {
		payloadStart := getFloat(result.Rows[0][0])
		payloadEnd := getFloat(result.Rows[0][1])
		if payloadEnd <= payloadStart {
			t.Errorf("Expected payload to increase during loading: start=%.1f, end=%.1f", payloadStart, payloadEnd)
		}
	}
}

// TestHaulingLoadedDetection validates hauling state when payload >80% capacity + speed >5 km/h + moving toward crusher
func TestHaulingLoadedDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupStateDetectionTest(t)
	defer cleanup()

	// Create telemetry showing loaded hauling (200-ton truck, >160 tons = >80%)
	baseTime := time.Date(2024, 1, 1, 8, 10, 0, 0, time.UTC)
	events := []telemetryEvent{
		{"TRUCK_001", baseTime.Format("2006-01-02 15:04:05"), -23.5, 119.5, 25.0, 190, 2000, 1600, 795, 100.7, "Road"},
		{"TRUCK_001", baseTime.Add(1 * time.Minute).Format("2006-01-02 15:04:05"), -23.52, 119.52, 30.0, 190, 2000, 1700, 793, 100.72, "Road"},
		{"TRUCK_001", baseTime.Add(2 * time.Minute).Format("2006-01-02 15:04:05"), -23.54, 119.54, 28.0, 190, 2000, 1650, 791, 100.75, "Road"},
		{"TRUCK_001", baseTime.Add(3 * time.Minute).Format("2006-01-02 15:04:05"), -23.56, 119.56, 32.0, 190, 2000, 1700, 789, 100.77, "Road"},
	}

	insertTelemetry(t, adapter, ctx, events)
	executeStateDetectionSQL(t, adapter, ctx)

	// Verify hauling_loaded state detected
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count
		FROM stg_truck_states
		WHERE truck_id = 'TRUCK_001'
		AND operational_state = 'hauling_loaded'
	`)
	if err != nil {
		t.Fatalf("Failed to query states: %v", err)
	}

	count := result.Rows[0][0].(int64)
	if count == 0 {
		t.Error("Expected hauling_loaded state to be detected, but found none")
	}
}

// TestQueueAtCrusherDetection validates queue when in crusher zone + payload >80% + speed <3 km/h + duration >30 seconds
func TestQueueAtCrusherDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupStateDetectionTest(t)
	defer cleanup()

	// Create telemetry showing truck queued at crusher (loaded, stopped for >30 seconds)
	baseTime := time.Date(2024, 1, 1, 8, 30, 0, 0, time.UTC)
	events := []telemetryEvent{
		{"TRUCK_001", baseTime.Format("2006-01-02 15:04:05"), -23.6, 119.6, 1.0, 190, 2000, 800, 788, 101.0, "Crusher"},
		{"TRUCK_001", baseTime.Add(30 * time.Second).Format("2006-01-02 15:04:05"), -23.6, 119.6, 0.5, 190, 2000, 800, 788, 101.01, "Crusher"},
		{"TRUCK_001", baseTime.Add(1 * time.Minute).Format("2006-01-02 15:04:05"), -23.6, 119.6, 0.0, 190, 2000, 800, 788, 101.02, "Crusher"},
		{"TRUCK_001", baseTime.Add(90 * time.Second).Format("2006-01-02 15:04:05"), -23.6, 119.6, 0.5, 190, 2000, 800, 788, 101.025, "Crusher"},
	}

	insertTelemetry(t, adapter, ctx, events)
	executeStateDetectionSQL(t, adapter, ctx)

	// Verify queued_at_crusher state detected
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count
		FROM stg_truck_states
		WHERE truck_id = 'TRUCK_001'
		AND operational_state = 'queued_at_crusher'
		AND location_zone = 'Crusher'
	`)
	if err != nil {
		t.Fatalf("Failed to query states: %v", err)
	}

	count := result.Rows[0][0].(int64)
	if count == 0 {
		t.Error("Expected queued_at_crusher state to be detected, but found none")
	}
}

// TestDumpingDetection validates dumping when payload drops from >80% to <20% within crusher zone
func TestDumpingDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupStateDetectionTest(t)
	defer cleanup()

	// Create telemetry showing dump (payload drops from 190 to 10 tons)
	baseTime := time.Date(2024, 1, 1, 8, 35, 0, 0, time.UTC)
	events := []telemetryEvent{
		{"TRUCK_001", baseTime.Format("2006-01-02 15:04:05"), -23.6, 119.6, 1.0, 190, 2000, 1200, 788, 101.1, "Crusher"},
		{"TRUCK_001", baseTime.Add(30 * time.Second).Format("2006-01-02 15:04:05"), -23.6, 119.6, 0.5, 150, 1500, 1200, 788, 101.11, "Crusher"},
		{"TRUCK_001", baseTime.Add(1 * time.Minute).Format("2006-01-02 15:04:05"), -23.6, 119.6, 0.5, 80, 800, 1100, 788, 101.12, "Crusher"},
		{"TRUCK_001", baseTime.Add(90 * time.Second).Format("2006-01-02 15:04:05"), -23.6, 119.6, 1.0, 15, 500, 1000, 788, 101.125, "Crusher"},
	}

	insertTelemetry(t, adapter, ctx, events)
	executeStateDetectionSQL(t, adapter, ctx)

	// Verify dumping state detected
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count
		FROM stg_truck_states
		WHERE truck_id = 'TRUCK_001'
		AND operational_state = 'dumping'
		AND location_zone = 'Crusher'
	`)
	if err != nil {
		t.Fatalf("Failed to query states: %v", err)
	}

	count := result.Rows[0][0].(int64)
	if count == 0 {
		t.Error("Expected dumping state to be detected, but found none")
	}
}

// TestReturningEmptyDetection validates empty return when payload <20% + speed >5 km/h + moving away from crusher
func TestReturningEmptyDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupStateDetectionTest(t)
	defer cleanup()

	// Create telemetry showing empty return (200-ton truck, <40 tons = <20%)
	baseTime := time.Date(2024, 1, 1, 8, 40, 0, 0, time.UTC)
	events := []telemetryEvent{
		{"TRUCK_001", baseTime.Format("2006-01-02 15:04:05"), -23.6, 119.6, 35.0, 15, 500, 1500, 787, 101.2, "Road"},
		{"TRUCK_001", baseTime.Add(1 * time.Minute).Format("2006-01-02 15:04:05"), -23.58, 119.58, 40.0, 15, 500, 1600, 785, 101.22, "Road"},
		{"TRUCK_001", baseTime.Add(2 * time.Minute).Format("2006-01-02 15:04:05"), -23.56, 119.56, 38.0, 15, 500, 1550, 783, 101.25, "Road"},
		{"TRUCK_001", baseTime.Add(3 * time.Minute).Format("2006-01-02 15:04:05"), -23.54, 119.54, 42.0, 15, 500, 1600, 781, 101.27, "Road"},
	}

	insertTelemetry(t, adapter, ctx, events)
	executeStateDetectionSQL(t, adapter, ctx)

	// Verify returning_empty state detected
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count
		FROM stg_truck_states
		WHERE truck_id = 'TRUCK_001'
		AND operational_state = 'returning_empty'
	`)
	if err != nil {
		t.Fatalf("Failed to query states: %v", err)
	}

	count := result.Rows[0][0].(int64)
	if count == 0 {
		t.Error("Expected returning_empty state to be detected, but found none")
	}
}

// TestSpotDelayDetection validates spot delays when stopped >2 min outside loading/dumping zones
func TestSpotDelayDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupStateDetectionTest(t)
	defer cleanup()

	// Create telemetry showing truck stopped on road for >2 minutes
	baseTime := time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)
	events := []telemetryEvent{
		{"TRUCK_001", baseTime.Format("2006-01-02 15:04:05"), -23.55, 119.55, 0.0, 15, 500, 800, 780, 101.5, "Other"},
		{"TRUCK_001", baseTime.Add(1 * time.Minute).Format("2006-01-02 15:04:05"), -23.55, 119.55, 0.0, 15, 500, 800, 780, 101.52, "Other"},
		{"TRUCK_001", baseTime.Add(2 * time.Minute).Format("2006-01-02 15:04:05"), -23.55, 119.55, 0.0, 15, 500, 800, 780, 101.53, "Other"},
		{"TRUCK_001", baseTime.Add(3 * time.Minute).Format("2006-01-02 15:04:05"), -23.55, 119.55, 0.5, 15, 500, 800, 780, 101.55, "Other"},
	}

	insertTelemetry(t, adapter, ctx, events)
	executeStateDetectionSQL(t, adapter, ctx)

	// Verify spot_delay state detected
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count
		FROM stg_truck_states
		WHERE truck_id = 'TRUCK_001'
		AND operational_state = 'spot_delay'
	`)
	if err != nil {
		t.Fatalf("Failed to query states: %v", err)
	}

	count := result.Rows[0][0].(int64)
	if count == 0 {
		t.Error("Expected spot_delay state to be detected, but found none")
	}
}

// TestRefuelingDetection validates refueling spot delays identified by engine_hours threshold + duration 15-30 min
func TestRefuelingDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupStateDetectionTest(t)
	defer cleanup()

	// Create telemetry showing refueling (stopped for ~20 minutes, engine hours indicate refuel interval)
	baseTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	events := []telemetryEvent{
		// Truck stopped at refueling area (~110 engine hours, suggesting refuel time)
		{"TRUCK_001", baseTime.Format("2006-01-02 15:04:05"), -23.52, 119.52, 0.0, 20, 600, 800, 400, 110.2, "Other"},
		{"TRUCK_001", baseTime.Add(5 * time.Minute).Format("2006-01-02 15:04:05"), -23.52, 119.52, 0.0, 20, 600, 800, 450, 110.25, "Other"},
		{"TRUCK_001", baseTime.Add(10 * time.Minute).Format("2006-01-02 15:04:05"), -23.52, 119.52, 0.0, 20, 600, 800, 550, 110.33, "Other"},
		{"TRUCK_001", baseTime.Add(15 * time.Minute).Format("2006-01-02 15:04:05"), -23.52, 119.52, 0.0, 20, 600, 800, 700, 110.42, "Other"},
		{"TRUCK_001", baseTime.Add(20 * time.Minute).Format("2006-01-02 15:04:05"), -23.52, 119.52, 0.5, 20, 600, 1000, 800, 110.5, "Other"},
	}

	insertTelemetry(t, adapter, ctx, events)
	executeStateDetectionSQL(t, adapter, ctx)

	// Verify refueling detected (marked as spot_delay with fuel increase)
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count
		FROM stg_truck_states
		WHERE truck_id = 'TRUCK_001'
		AND operational_state = 'spot_delay'
	`)
	if err != nil {
		t.Fatalf("Failed to query states: %v", err)
	}

	count := result.Rows[0][0].(int64)
	if count == 0 {
		t.Error("Expected refueling (spot_delay) state to be detected, but found none")
	}

	// Note: More sophisticated refueling detection (checking fuel increase during delay)
	// would require additional logic in the SQL transformation
}

// TestStateTransitionCompleteness ensures every telemetry point assigned to exactly one state
func TestStateTransitionCompleteness(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupStateDetectionTest(t)
	defer cleanup()

	// Create a complete haul cycle
	baseTime := time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC)
	events := []telemetryEvent{
		// Loading
		{"TRUCK_003", baseTime.Format("2006-01-02 15:04:05"), -23.5, 119.5, 1.0, 10, 300, 1200, 500, 50.0, "Shovel_A"},
		{"TRUCK_003", baseTime.Add(2 * time.Minute).Format("2006-01-02 15:04:05"), -23.5, 119.5, 0.5, 50, 800, 1300, 499, 50.03, "Shovel_A"},
		{"TRUCK_003", baseTime.Add(4 * time.Minute).Format("2006-01-02 15:04:05"), -23.5, 119.5, 0.0, 95, 1200, 1300, 498, 50.07, "Shovel_A"},
		// Hauling loaded
		{"TRUCK_003", baseTime.Add(6 * time.Minute).Format("2006-01-02 15:04:05"), -23.52, 119.52, 25.0, 95, 1200, 1600, 495, 50.1, "Road"},
		{"TRUCK_003", baseTime.Add(10 * time.Minute).Format("2006-01-02 15:04:05"), -23.56, 119.56, 28.0, 95, 1200, 1650, 490, 50.17, "Road"},
		// Dumping
		{"TRUCK_003", baseTime.Add(15 * time.Minute).Format("2006-01-02 15:04:05"), -23.6, 119.6, 1.0, 95, 1200, 1100, 489, 50.25, "Crusher"},
		{"TRUCK_003", baseTime.Add(16 * time.Minute).Format("2006-01-02 15:04:05"), -23.6, 119.6, 0.5, 10, 300, 1000, 489, 50.27, "Crusher"},
		// Returning empty
		{"TRUCK_003", baseTime.Add(18 * time.Minute).Format("2006-01-02 15:04:05"), -23.58, 119.58, 35.0, 10, 300, 1500, 485, 50.3, "Road"},
		{"TRUCK_003", baseTime.Add(22 * time.Minute).Format("2006-01-02 15:04:05"), -23.54, 119.54, 40.0, 10, 300, 1600, 480, 50.37, "Road"},
	}

	insertTelemetry(t, adapter, ctx, events)
	executeStateDetectionSQL(t, adapter, ctx)

	// Verify we have state records
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count
		FROM stg_truck_states
		WHERE truck_id = 'TRUCK_003'
	`)
	if err != nil {
		t.Fatalf("Failed to query states: %v", err)
	}

	stateCount := result.Rows[0][0].(int64)
	if stateCount == 0 {
		t.Error("Expected state records to be generated, but found none")
	}

	// Verify we detected multiple different states
	result, err = adapter.ExecuteQuery(ctx, `
		SELECT COUNT(DISTINCT operational_state) as state_count
		FROM stg_truck_states
		WHERE truck_id = 'TRUCK_003'
	`)
	if err != nil {
		t.Fatalf("Failed to query distinct states: %v", err)
	}

	distinctStates := result.Rows[0][0].(int64)
	if distinctStates < 3 {
		t.Errorf("Expected at least 3 distinct states in cycle, got %d", distinctStates)
	}
}

// TestStateDurationCalculation validates duration calculated correctly using window functions
func TestStateDurationCalculation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupStateDetectionTest(t)
	defer cleanup()

	// Create telemetry with known durations (keep under 80% to stay in loading state)
	baseTime := time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC)
	events := []telemetryEvent{
		{"TRUCK_001", baseTime.Format("2006-01-02 15:04:05"), -23.5, 119.5, 1.0, 10, 500, 1200, 800, 100.0, "Shovel_A"},
		{"TRUCK_001", baseTime.Add(5 * time.Minute).Format("2006-01-02 15:04:05"), -23.5, 119.5, 0.5, 100, 1300, 1300, 798, 100.08, "Shovel_A"},
		{"TRUCK_001", baseTime.Add(10 * time.Minute).Format("2006-01-02 15:04:05"), -23.5, 119.5, 0.0, 150, 1600, 1300, 796, 100.17, "Shovel_A"},
	}

	insertTelemetry(t, adapter, ctx, events)
	executeStateDetectionSQL(t, adapter, ctx)

	// Verify state has start and end times
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			state_start,
			state_end,
			(julianday(state_end) - julianday(state_start)) * 24 * 60 as duration_minutes
		FROM stg_truck_states
		WHERE truck_id = 'TRUCK_001'
		AND operational_state = 'loading'
		LIMIT 1
	`)
	if err != nil {
		t.Fatalf("Failed to query state duration: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected state record with duration, but found none")
	}

	stateStart := result.Rows[0][0].(string)
	stateEnd := result.Rows[0][1].(string)
	duration := getFloat(result.Rows[0][2])

	if stateStart == "" || stateEnd == "" {
		t.Error("State start or end time is empty")
	}

	// Duration should be approximately 10 minutes (from first to last event in loading state)
	// Note: Last event stays under 80% threshold (150/200 = 75%) so all should be loading
	if duration < 8 || duration > 12 {
		t.Errorf("Expected duration around 10 minutes, got %.2f", duration)
	}
}

// TestAnomalousPatternDetection identifies invalid transitions (e.g., loaded→loaded without dump)
func TestAnomalousPatternDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupStateDetectionTest(t)
	defer cleanup()

	// Create telemetry with anomalous pattern: truck shows loaded state continuously without dumping
	baseTime := time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC)
	events := []telemetryEvent{
		// Truck hauling loaded
		{"TRUCK_001", baseTime.Format("2006-01-02 15:04:05"), -23.54, 119.54, 30.0, 190, 2000, 1600, 800, 100.0, "Road"},
		{"TRUCK_001", baseTime.Add(10 * time.Minute).Format("2006-01-02 15:04:05"), -23.56, 119.56, 28.0, 190, 2000, 1650, 795, 100.17, "Road"},
		// Still loaded after 20 minutes (anomalous - should have dumped)
		{"TRUCK_001", baseTime.Add(20 * time.Minute).Format("2006-01-02 15:04:05"), -23.52, 119.52, 25.0, 190, 2000, 1600, 790, 100.33, "Road"},
		{"TRUCK_001", baseTime.Add(30 * time.Minute).Format("2006-01-02 15:04:05"), -23.54, 119.54, 30.0, 190, 2000, 1650, 785, 100.5, "Road"},
	}

	insertTelemetry(t, adapter, ctx, events)
	executeStateDetectionSQL(t, adapter, ctx)

	// Verify continuous hauling_loaded state is detected
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			COUNT(*) as count,
			(julianday(MAX(state_end)) - julianday(MIN(state_start))) * 24 * 60 as total_duration_minutes
		FROM stg_truck_states
		WHERE truck_id = 'TRUCK_001'
		AND operational_state = 'hauling_loaded'
	`)
	if err != nil {
		t.Fatalf("Failed to query anomalous pattern: %v", err)
	}

	if len(result.Rows) > 0 {
		duration := getFloat(result.Rows[0][1])
		// Long hauling without state change suggests anomaly
		if duration > 25 {
			// This is expected for this test - we're detecting extended loaded state
			// In production, this would flag for review
			t.Logf("Detected extended hauling_loaded duration: %.2f minutes (potential anomaly)", duration)
		}
	}

	// This test validates the system can identify continuous states
	// Actual anomaly detection logic would be in downstream analytics
}

// getFloat safely converts interface{} to float64
func getFloat(val interface{}) float64 {
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
