package test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jpconstantineau/gorchata/internal/platform"
	"github.com/jpconstantineau/gorchata/internal/platform/sqlite"
	"github.com/jpconstantineau/gorchata/internal/template"
)

// setupBufferConstraintTest creates a test database with production areas and equipment
func setupBufferConstraintTest(t *testing.T) (*sqlite.SQLiteAdapter, context.Context, func()) {
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

	// Create dim_production_area table
	err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE dim_production_area (
			area_id TEXT PRIMARY KEY,
			area_name TEXT NOT NULL,
			sequence_order INTEGER NOT NULL,
			upstream_area_id TEXT,
			downstream_area_id TEXT,
			buffer_capacity_hours REAL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create dim_production_area: %v", err)
	}

	// Insert production areas with buffer capacities
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO dim_production_area VALUES 
		('AREA_STRANDING', 'Stranding', 1, NULL, 'AREA_DRYING', 4.0),
		('AREA_DRYING', 'Drying', 2, 'AREA_STRANDING', 'AREA_FORMING', 2.0),
		('AREA_FORMING', 'Forming', 3, 'AREA_DRYING', 'AREA_PRESSING', 1.0),
		('AREA_PRESSING', 'Pressing', 4, 'AREA_FORMING', 'AREA_FINISHING', 0.5),
		('AREA_FINISHING', 'Finishing', 5, 'AREA_PRESSING', NULL, 0.0)
	`)
	if err != nil {
		t.Fatalf("Failed to insert production areas: %v", err)
	}

	// Create dim_equipment table
	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE dim_equipment (
			equipment_id TEXT PRIMARY KEY,
			equipment_name TEXT NOT NULL,
			equipment_type TEXT NOT NULL,
			production_area TEXT NOT NULL,
			rated_capacity_units_hr REAL NOT NULL,
			criticality_level TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create dim_equipment: %v", err)
	}

	// Insert equipment across production areas
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO dim_equipment VALUES 
		('STRAND-01', 'Strander Line 1', 'Strander', 'AREA_STRANDING', 6.0, 'Important'),
		('DRYER-01', 'Primary Rotary Dryer', 'Dryer', 'AREA_DRYING', 10.0, 'Critical'),
		('FORMER-01', 'Mat Former Station', 'Former', 'AREA_FORMING', 8.0, 'Critical'),
		('PRESS-01', 'Continuous Hot Press', 'Press', 'AREA_PRESSING', 18.0, 'Critical'),
		('SAW-01', 'Panel Saw Station', 'Saw', 'AREA_FINISHING', 20.0, 'Important')
	`)
	if err != nil {
		t.Fatalf("Failed to insert equipment: %v", err)
	}

	// Create stg_equipment_state_history table
	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE stg_equipment_state_history (
			equipment_id TEXT NOT NULL,
			state_start_timestamp TEXT NOT NULL,
			state_end_timestamp TEXT NOT NULL,
			state_duration_min REAL NOT NULL,
			machine_state TEXT NOT NULL,
			reason_code_id TEXT,
			shift_id TEXT NOT NULL,
			date_id TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create stg_equipment_state_history: %v", err)
	}

	// Create dim_date table
	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE dim_date (
			date_id TEXT PRIMARY KEY,
			date_actual DATE NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create dim_date: %v", err)
	}

	// Insert test dates
	baseDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 7; i++ {
		date := baseDate.Add(time.Duration(i) * 24 * time.Hour)
		dateID := date.Format("20060102")
		sql := fmt.Sprintf(`INSERT INTO dim_date VALUES ('%s', '%s')`,
			dateID, date.Format("2006-01-02"))
		if err := adapter.ExecuteDDL(ctx, sql); err != nil {
			t.Fatalf("Failed to insert date: %v", err)
		}
	}

	// Create empty forecast_demand table (tests can populate if needed)
	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE forecast_demand (
			date_id TEXT PRIMARY KEY,
			demand_tons_per_day REAL NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create forecast_demand: %v", err)
	}

	return adapter, ctx, cleanup
}

// bufferEvent represents a buffer level snapshot
type bufferEvent struct {
	BufferID         string
	Timestamp        string
	CurrentLevel     float64
	CapacityPct      float64
	InflowRate       float64
	OutflowRate      float64
	UpstreamStatus   string
	DownstreamStatus string
}

// insertStateEvents is a helper to insert equipment state events
func insertStateEvents(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context, equipmentID, startTime, endTime string, durationMin float64, state, shiftID, dateID string) {
	sql := fmt.Sprintf(`
		INSERT INTO stg_equipment_state_history VALUES (
			'%s', '%s', '%s', %.2f, '%s', NULL, '%s', '%s'
		)`,
		equipmentID, startTime, endTime, durationMin, state, shiftID, dateID,
	)
	if err := adapter.ExecuteDDL(ctx, sql); err != nil {
		t.Fatalf("Failed to insert state event: %v", err)
	}
}

// executeBufferUtilizationSQL loads and executes the buffer_utilization_analysis.sql model
func executeBufferUtilizationSQL(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context) {
	repoRoot := getRepoRoot(t)
	modelPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "models", "metrics", "buffer_utilization_analysis.sql")

	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read buffer_utilization_analysis.sql: %v", err)
	}

	contentStr := removeConfigCallsStaging(string(content))

	templateEngine := template.New()
	tmpl, err := templateEngine.Parse("buffer_utilization_analysis", contentStr)
	if err != nil {
		t.Fatalf("Failed to parse template: %v", err)
	}

	ctx2 := template.NewContext(template.WithCurrentModel("buffer_utilization_analysis"))
	rendered, err := template.Render(tmpl, ctx2, nil)
	if err != nil {
		t.Fatalf("Failed to render template: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, "DROP TABLE IF EXISTS buffer_utilization_analysis")
	if err != nil {
		t.Fatalf("Failed to drop existing buffer_utilization_analysis: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, "CREATE TABLE buffer_utilization_analysis AS "+rendered)
	if err != nil {
		t.Fatalf("Failed to execute buffer_utilization_analysis model: %v", err)
	}
}

// executeStarvationBlockingSQL loads and executes the starvation_blocking_analysis.sql model
func executeStarvationBlockingSQL(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context) {
	repoRoot := getRepoRoot(t)
	modelPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "models", "metrics", "starvation_blocking_analysis.sql")

	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read starvation_blocking_analysis.sql: %v", err)
	}

	contentStr := removeConfigCallsStaging(string(content))

	templateEngine := template.New()
	tmpl, err := templateEngine.Parse("starvation_blocking_analysis", contentStr)
	if err != nil {
		t.Fatalf("Failed to parse template: %v", err)
	}

	ctx2 := template.NewContext(template.WithCurrentModel("starvation_blocking_analysis"))
	rendered, err := template.Render(tmpl, ctx2, nil)
	if err != nil {
		t.Fatalf("Failed to render template: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, "DROP TABLE IF EXISTS starvation_blocking_analysis")
	if err != nil {
		t.Fatalf("Failed to drop existing starvation_blocking_analysis: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, "CREATE TABLE starvation_blocking_analysis AS "+rendered)
	if err != nil {
		t.Fatalf("Failed to execute starvation_blocking_analysis model: %v", err)
	}
}

// executeConstraintAnalysisSQL loads and executes the constraint_analysis.sql model
func executeConstraintAnalysisSQL(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context) {
	repoRoot := getRepoRoot(t)
	modelPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "models", "metrics", "constraint_analysis.sql")

	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read constraint_analysis.sql: %v", err)
	}

	contentStr := removeConfigCallsStaging(string(content))

	templateEngine := template.New()
	tmpl, err := templateEngine.Parse("constraint_analysis", contentStr)
	if err != nil {
		t.Fatalf("Failed to parse template: %v", err)
	}

	ctx2 := template.NewContext(template.WithCurrentModel("constraint_analysis"))
	rendered, err := template.Render(tmpl, ctx2, nil)
	if err != nil {
		t.Fatalf("Failed to render template: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, "DROP TABLE IF EXISTS constraint_analysis")
	if err != nil {
		t.Fatalf("Failed to drop existing constraint_analysis: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, "CREATE TABLE constraint_analysis AS "+rendered)
	if err != nil {
		t.Fatalf("Failed to execute constraint_analysis model: %v", err)
	}
}

// TestOSBBufferLevelTracking validates buffer inventory levels calculated correctly over time
func TestOSBBufferLevelTracking(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupBufferConstraintTest(t)
	defer cleanup()

	// Scenario: Steady state operation with buffer between stranding and drying
	// Strander produces 6 tons/hr, Dryer consumes 10 tons/hr
	// Buffer starts at 50% (12 tons in 24-ton capacity buffer)
	// After 1 hour: 12 + 6 - 10 = 8 tons (33%)
	// After 2 hours: 8 + 6 - 10 = 4 tons (17%)

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Both machines running for 24 hours
	insertStateEvents(t, adapter, ctx, "STRAND-01",
		baseTime.Format("2006-01-02 15:04:05"),
		baseTime.Add(24*time.Hour).Format("2006-01-02 15:04:05"),
		1440, "Running", "DAY", "20240101")

	insertStateEvents(t, adapter, ctx, "DRYER-01",
		baseTime.Format("2006-01-02 15:04:05"),
		baseTime.Add(24*time.Hour).Format("2006-01-02 15:04:05"),
		1440, "Running", "DAY", "20240101")

	executeBufferUtilizationSQL(t, adapter, ctx)

	// Verify buffer level calculation
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			buffer_name,
			avg_buffer_level_pct,
			min_buffer_level_pct,
			max_buffer_level_pct,
			total_hours_analyzed
		FROM buffer_utilization_analysis
		WHERE buffer_name LIKE '%Stranding to Drying%'
	`)
	if err != nil {
		t.Fatalf("Failed to query buffer utilization: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected buffer utilization record, got none")
	}

	avgLevel := getFloat(result.Rows[0][1])
	minLevel := getFloat(result.Rows[0][2])
	maxLevel := getFloat(result.Rows[0][3])

	// Buffer should deplete over time since dryer (10 t/hr) > strander (6 t/hr)
	if minLevel >= avgLevel {
		t.Errorf("Expected minimum buffer level < average, got min=%.1f%%, avg=%.1f%%", minLevel, avgLevel)
	}

	t.Logf("Buffer (Stranding→Drying): avg=%.1f%%, min=%.1f%%, max=%.1f%%", avgLevel, minLevel, maxLevel)
}

// TestOSBBufferCapacityUtilization validates % utilization calculated (current_level / capacity)
func TestOSBBufferCapacityUtilization(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupBufferConstraintTest(t)
	defer cleanup()

	// Scenario: Dryer down for 6 hours, strander continues producing
	// Buffer fills up: upstream producing, downstream not consuming
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Strander running for 12 hours
	insertStateEvents(t, adapter, ctx, "STRAND-01",
		baseTime.Format("2006-01-02 15:04:05"),
		baseTime.Add(12*time.Hour).Format("2006-01-02 15:04:05"),
		720, "Running", "DAY", "20240101")

	// Dryer down for first 6 hours
	insertStateEvents(t, adapter, ctx, "DRYER-01",
		baseTime.Format("2006-01-02 15:04:05"),
		baseTime.Add(6*time.Hour).Format("2006-01-02 15:04:05"),
		360, "Unplanned Downtime", "DAY", "20240101")

	// Dryer running for next 6 hours
	insertStateEvents(t, adapter, ctx, "DRYER-01",
		baseTime.Add(6*time.Hour).Format("2006-01-02 15:04:05"),
		baseTime.Add(12*time.Hour).Format("2006-01-02 15:04:05"),
		360, "Running", "DAY", "20240101")

	executeBufferUtilizationSQL(t, adapter, ctx)

	// Verify buffer reached high utilization during dryer downtime
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			buffer_name,
			max_buffer_level_pct,
			hours_above_90_pct
		FROM buffer_utilization_analysis
		WHERE buffer_name LIKE '%Stranding to Drying%'
	`)
	if err != nil {
		t.Fatalf("Failed to query buffer utilization: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected buffer utilization record, got none")
	}

	maxLevel := getFloat(result.Rows[0][1])
	hoursAbove90 := getFloat(result.Rows[0][2])

	// Buffer should reach high utilization when dryer is down
	if maxLevel < 80.0 {
		t.Errorf("Expected high buffer utilization during dryer downtime, got max=%.1f%%", maxLevel)
	}

	if hoursAbove90 < 2.0 {
		t.Errorf("Expected buffer >90%% for at least 2 hours, got %.1f hours", hoursAbove90)
	}

	t.Logf("Buffer utilization: max=%.1f%%, hours above 90%%=%.1f", maxLevel, hoursAbove90)
}

// TestOSBStarvationEventDetection validates equipment starved events correlated with upstream buffer depletion
func TestOSBStarvationEventDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupBufferConstraintTest(t)
	defer cleanup()

	// Scenario: Strander down causes dryer to starve (buffer depletes)
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Strander down for 8 hours
	insertStateEvents(t, adapter, ctx, "STRAND-01",
		baseTime.Format("2006-01-02 15:04:05"),
		baseTime.Add(8*time.Hour).Format("2006-01-02 15:04:05"),
		480, "Unplanned Downtime", "DAY", "20240101")

	// Dryer starved after buffer depletes (starts ~2 hours after strander stops)
	insertStateEvents(t, adapter, ctx, "DRYER-01",
		baseTime.Format("2006-01-02 15:04:05"),
		baseTime.Add(2*time.Hour).Format("2006-01-02 15:04:05"),
		120, "Running", "DAY", "20240101")

	insertStateEvents(t, adapter, ctx, "DRYER-01",
		baseTime.Add(2*time.Hour).Format("2006-01-02 15:04:05"),
		baseTime.Add(8*time.Hour).Format("2006-01-02 15:04:05"),
		360, "Starved", "DAY", "20240101")

	executeStarvationBlockingSQL(t, adapter, ctx)

	// Verify starvation event detected and correlated with upstream failure
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			equipment_id,
			starved_event_count,
			total_starved_time_min,
			upstream_equipment_causing_starvation
		FROM starvation_blocking_analysis
		WHERE equipment_id = 'DRYER-01'
	`)
	if err != nil {
		t.Fatalf("Failed to query starvation analysis: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected starvation analysis record for DRYER-01, got none")
	}

	starvedCount := getFloat(result.Rows[0][1])
	starvedTime := getFloat(result.Rows[0][2])
	upstreamCause := result.Rows[0][3].(string)

	// Verify dryer starvation event detected
	if starvedCount < 1.0 {
		t.Errorf("Expected at least 1 starvation event, got %.0f", starvedCount)
	}

	// Verify starved time ~360 minutes
	if starvedTime < 300.0 || starvedTime > 400.0 {
		t.Errorf("Expected ~360 min starvation, got %.0f min", starvedTime)
	}

	// Verify upstream cause identified as strander
	if upstreamCause != "STRAND-01" {
		t.Errorf("Expected upstream cause STRAND-01, got %s", upstreamCause)
	}

	t.Logf("Starvation detected: %s starved %.0f min due to %s failure", result.Rows[0][0], starvedTime, upstreamCause)
}

// TestOSBBlockingEventDetection validates equipment blocked events correlated with downstream buffer full
func TestOSBBlockingEventDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupBufferConstraintTest(t)
	defer cleanup()

	// Scenario: Press down causes former to block (buffer fills up)
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Former running for 6 hours, then blocked
	insertStateEvents(t, adapter, ctx, "FORMER-01",
		baseTime.Format("2006-01-02 15:04:05"),
		baseTime.Add(1*time.Hour).Format("2006-01-02 15:04:05"),
		60, "Running", "DAY", "20240101")

	insertStateEvents(t, adapter, ctx, "FORMER-01",
		baseTime.Add(1*time.Hour).Format("2006-01-02 15:04:05"),
		baseTime.Add(6*time.Hour).Format("2006-01-02 15:04:05"),
		300, "Blocked", "DAY", "20240101")

	// Press down for 6 hours
	insertStateEvents(t, adapter, ctx, "PRESS-01",
		baseTime.Format("2006-01-02 15:04:05"),
		baseTime.Add(6*time.Hour).Format("2006-01-02 15:04:05"),
		360, "Unplanned Downtime", "DAY", "20240101")

	executeStarvationBlockingSQL(t, adapter, ctx)

	// Verify blocking event detected and correlated with downstream failure
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			equipment_id,
			blocked_event_count,
			total_blocked_time_min,
			downstream_equipment_causing_blocking
		FROM starvation_blocking_analysis
		WHERE equipment_id = 'FORMER-01'
	`)
	if err != nil {
		t.Fatalf("Failed to query blocking analysis: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected blocking analysis record for FORMER-01, got none")
	}

	blockedCount := getFloat(result.Rows[0][1])
	blockedTime := getFloat(result.Rows[0][2])
	downstreamCause := result.Rows[0][3].(string)

	// Verify former blocking event detected
	if blockedCount < 1.0 {
		t.Errorf("Expected at least 1 blocking event, got %.0f", blockedCount)
	}

	// Verify blocked time ~300 minutes
	if blockedTime < 250.0 || blockedTime > 350.0 {
		t.Errorf("Expected ~300 min blocking, got %.0f min", blockedTime)
	}

	// Verify downstream cause identified as press
	if downstreamCause != "PRESS-01" {
		t.Errorf("Expected downstream cause PRESS-01, got %s", downstreamCause)
	}

	t.Logf("Blocking detected: %s blocked %.0f min due to %s failure", result.Rows[0][0], blockedTime, downstreamCause)
}

// TestOSBDowntimePropagationAnalysis validates dryer outage causes upstream blocking and downstream starvation
func TestOSBDowntimePropagationAnalysis(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupBufferConstraintTest(t)
	defer cleanup()

	// Scenario: Dryer failure propagates both ways:
	// - Upstream: strander continues until buffer full, then blocked
	// - Downstream: former runs until buffer empty, then starved

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Dryer down for 8 hours (critical failure)
	insertStateEvents(t, adapter, ctx, "DRYER-01",
		baseTime.Format("2006-01-02 15:04:05"),
		baseTime.Add(8*time.Hour).Format("2006-01-02 15:04:05"),
		480, "Unplanned Downtime", "DAY", "20240101")

	// Strander runs 4 hours, then blocked (buffer full)
	insertStateEvents(t, adapter, ctx, "STRAND-01",
		baseTime.Format("2006-01-02 15:04:05"),
		baseTime.Add(4*time.Hour).Format("2006-01-02 15:04:05"),
		240, "Running", "DAY", "20240101")

	insertStateEvents(t, adapter, ctx, "STRAND-01",
		baseTime.Add(4*time.Hour).Format("2006-01-02 15:04:05"),
		baseTime.Add(8*time.Hour).Format("2006-01-02 15:04:05"),
		240, "Blocked", "DAY", "20240101")

	// Former runs 2 hours, then starved (buffer empty)
	insertStateEvents(t, adapter, ctx, "FORMER-01",
		baseTime.Format("2006-01-02 15:04:05"),
		baseTime.Add(2*time.Hour).Format("2006-01-02 15:04:05"),
		120, "Running", "DAY", "20240101")

	insertStateEvents(t, adapter, ctx, "FORMER-01",
		baseTime.Add(2*time.Hour).Format("2006-01-02 15:04:05"),
		baseTime.Add(8*time.Hour).Format("2006-01-02 15:04:05"),
		360, "Starved", "DAY", "20240101")

	executeStarvationBlockingSQL(t, adapter, ctx)

	// Verify propagation analysis identifies dryer as root cause of both upstream/downstream issues
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			equipment_id,
			starved_event_count,
			blocked_event_count,
			total_starved_time_min,
			total_blocked_time_min,
			root_cause_equipment
		FROM starvation_blocking_analysis
		WHERE equipment_id IN ('STRAND-01', 'FORMER-01')
		ORDER BY equipment_id
	`)
	if err != nil {
		t.Fatalf("Failed to query propagation analysis: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Fatalf("Expected 2 records (FORMER and STRAND), got %d", len(result.Rows))
	}

	// First row: FORMER-01 (starved)
	formerStarved := getFloat(result.Rows[0][1])
	formerStarvedTime := getFloat(result.Rows[0][3])
	formerRootCause := result.Rows[0][5].(string)

	if formerStarved < 1.0 {
		t.Errorf("Expected FORMER-01 starvation event, got %.0f", formerStarved)
	}

	if formerRootCause != "DRYER-01" {
		t.Errorf("Expected FORMER starvation root cause DRYER-01, got %s", formerRootCause)
	}

	// Second row: STRAND-01 (blocked)
	strandBlocked := getFloat(result.Rows[1][2])
	strandBlockedTime := getFloat(result.Rows[1][4])
	strandRootCause := result.Rows[1][5].(string)

	if strandBlocked < 1.0 {
		t.Errorf("Expected STRAND-01 blocking event, got %.0f", strandBlocked)
	}

	if strandRootCause != "DRYER-01" {
		t.Errorf("Expected STRAND blocking root cause DRYER-01, got %s", strandRootCause)
	}

	t.Logf("Propagation: DRYER-01 failure → STRAND-01 blocked %.0f min, FORMER-01 starved %.0f min",
		strandBlockedTime, formerStarvedTime)
}

// TestOSBBufferSizingImpact validates analysis of "what if" buffer capacity changes
func TestOSBBufferSizingImpact(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupBufferConstraintTest(t)
	defer cleanup()

	// Scenario: Current buffer = 2 hours capacity
	// Simulate doubling buffer capacity and analyze blocking time reduction

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Press down for 4 hours
	insertStateEvents(t, adapter, ctx, "PRESS-01",
		baseTime.Format("2006-01-02 15:04:05"),
		baseTime.Add(4*time.Hour).Format("2006-01-02 15:04:05"),
		240, "Unplanned Downtime", "DAY", "20240101")

	// Former runs 1 hour then blocked for 3 hours (current 1-hour buffer)
	insertStateEvents(t, adapter, ctx, "FORMER-01",
		baseTime.Format("2006-01-02 15:04:05"),
		baseTime.Add(1*time.Hour).Format("2006-01-02 15:04:05"),
		60, "Running", "DAY", "20240101")

	insertStateEvents(t, adapter, ctx, "FORMER-01",
		baseTime.Add(1*time.Hour).Format("2006-01-02 15:04:05"),
		baseTime.Add(4*time.Hour).Format("2006-01-02 15:04:05"),
		180, "Blocked", "DAY", "20240101")

	executeConstraintAnalysisSQL(t, adapter, ctx)

	// Verify buffer sizing recommendation
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			buffer_name,
			current_capacity_hours,
			blocked_hours_observed,
			recommended_capacity_hours,
			estimated_blocking_reduction_hours
		FROM constraint_analysis
		WHERE analysis_type = 'Buffer Sizing'
			AND buffer_name LIKE '%Forming to Pressing%'
	`)
	if err != nil {
		t.Fatalf("Failed to query buffer sizing analysis: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected buffer sizing recommendation, got none")
	}

	currentCapacity := getFloat(result.Rows[0][1])
	blockedHours := getFloat(result.Rows[0][2])
	recommendedCapacity := getFloat(result.Rows[0][3])
	estimatedReduction := getFloat(result.Rows[0][4])

	// Current capacity should be 0.5 hours (from dim_production_area setup)
	if currentCapacity != 0.5 {
		t.Errorf("Expected current capacity 0.5 hours, got %.1f", currentCapacity)
	}

	// Blocked for ~3 hours observed
	if blockedHours < 2.5 || blockedHours > 3.5 {
		t.Errorf("Expected ~3 hours blocking, got %.1f", blockedHours)
	}

	// Recommended capacity should be higher
	if recommendedCapacity <= currentCapacity {
		t.Errorf("Expected recommended capacity > current (%.1f), got %.1f", currentCapacity, recommendedCapacity)
	}

	// Reduction should be positive
	if estimatedReduction <= 0 {
		t.Errorf("Expected positive blocking reduction, got %.1f hours", estimatedReduction)
	}

	t.Logf("Buffer sizing: current=%.1fh, blocked=%.1fh → recommend=%.1fh, reduction=%.1fh",
		currentCapacity, blockedHours, recommendedCapacity, estimatedReduction)
}

// TestOSBConstraintIdentification validates identification of system constraint (highest utilization + causing most downstream starvation)
func TestOSBConstraintIdentification(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupBufferConstraintTest(t)
	defer cleanup()

	// Scenario: Press is the constraint (bottleneck)
	// - Press: 95% utilization (runs almost continuously)
	// - Former: 80% utilization (occasionally blocked by press)
	// - Dryer: 70% utilization (faster than press, occasionally starved)

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Press: 23 hours running, 1 hour maintenance (95% utilization)
	insertStateEvents(t, adapter, ctx, "PRESS-01",
		baseTime.Format("2006-01-02 15:04:05"),
		baseTime.Add(23*time.Hour).Format("2006-01-02 15:04:05"),
		1380, "Running", "DAY", "20240101")

	insertStateEvents(t, adapter, ctx, "PRESS-01",
		baseTime.Add(23*time.Hour).Format("2006-01-02 15:04:05"),
		baseTime.Add(24*time.Hour).Format("2006-01-02 15:04:05"),
		60, "Planned Downtime", "NIGHT", "20240101")

	// Former: 19 hours running, 5 hours blocked (79% utilization)
	insertStateEvents(t, adapter, ctx, "FORMER-01",
		baseTime.Format("2006-01-02 15:04:05"),
		baseTime.Add(19*time.Hour).Format("2006-01-02 15:04:05"),
		1140, "Running", "DAY", "20240101")

	insertStateEvents(t, adapter, ctx, "FORMER-01",
		baseTime.Add(19*time.Hour).Format("2006-01-02 15:04:05"),
		baseTime.Add(24*time.Hour).Format("2006-01-02 15:04:05"),
		300, "Blocked", "NIGHT", "20240101")

	// Dryer: 17 hours running, 7 hours idle (71% utilization)
	insertStateEvents(t, adapter, ctx, "DRYER-01",
		baseTime.Format("2006-01-02 15:04:05"),
		baseTime.Add(17*time.Hour).Format("2006-01-02 15:04:05"),
		1020, "Running", "DAY", "20240101")

	insertStateEvents(t, adapter, ctx, "DRYER-01",
		baseTime.Add(17*time.Hour).Format("2006-01-02 15:04:05"),
		baseTime.Add(24*time.Hour).Format("2006-01-02 15:04:05"),
		420, "Idle", "NIGHT", "20240101")

	executeConstraintAnalysisSQL(t, adapter, ctx)

	// Verify constraint identified as press
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			equipment_id,
			utilization_pct,
			downstream_starvation_hours,
			constraint_score,
			is_system_constraint
		FROM constraint_analysis
		WHERE analysis_type = 'Constraint Identification'
		ORDER BY constraint_score DESC
		LIMIT 1
	`)
	if err != nil {
		t.Fatalf("Failed to query constraint identification: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected constraint identification record, got none")
	}

	constraintEquipment := result.Rows[0][0].(string)
	utilization := getFloat(result.Rows[0][1])
	downstreamStarvation := getFloat(result.Rows[0][2])
	isConstraint := result.Rows[0][4]

	// Verify press identified as constraint
	if constraintEquipment != "PRESS-01" {
		t.Errorf("Expected constraint PRESS-01, got %s", constraintEquipment)
	}

	// Verify high utilization (>90%)
	if utilization < 90.0 {
		t.Errorf("Expected utilization >90%%, got %.1f%%", utilization)
	}

	// Verify is_system_constraint flag
	if isConstraint != int64(1) && isConstraint != true {
		t.Errorf("Expected is_system_constraint=true, got %v", isConstraint)
	}

	t.Logf("Constraint identified: %s (%.1f%% utilization, %.1fh downstream starvation)",
		constraintEquipment, utilization, downstreamStarvation)
}

// TestOSBThroughputCalculation validates plant throughput limited by constraint resource
func TestOSBThroughputCalculation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupBufferConstraintTest(t)
	defer cleanup()

	// Scenario: Plant throughput = constraint capacity × constraint utilization
	// Dryer rated at 10 tons/hr, runs 24 hours/day = 240 tons/day (100% utilization)
	// Press rated at 18 tons/hr, runs 22 hours/day = 396 tons/day (91.67% utilization)
	// Since dryer comes before press in flow and has lower capacity + higher utilization, it's the constraint

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Press: 22 hours at 18 tons/hr (not the constraint)
	insertStateEvents(t, adapter, ctx, "PRESS-01",
		baseTime.Format("2006-01-02 15:04:05"),
		baseTime.Add(22*time.Hour).Format("2006-01-02 15:04:05"),
		1320, "Running", "DAY", "20240101")

	insertStateEvents(t, adapter, ctx, "PRESS-01",
		baseTime.Add(22*time.Hour).Format("2006-01-02 15:04:05"),
		baseTime.Add(24*time.Hour).Format("2006-01-02 15:04:05"),
		120, "Unplanned Downtime", "NIGHT", "20240101")

	// Dryer: 24 hours at 10 tons/hr (this is the constraint - limits plant throughput)
	insertStateEvents(t, adapter, ctx, "DRYER-01",
		baseTime.Format("2006-01-02 15:04:05"),
		baseTime.Add(24*time.Hour).Format("2006-01-02 15:04:05"),
		1440, "Running", "DAY", "20240101")

	executeConstraintAnalysisSQL(t, adapter, ctx)

	// Verify throughput calculation
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			analysis_date,
			plant_throughput_tons,
			constraint_equipment,
			constraint_capacity_tons_hr,
			constraint_utilization_pct
		FROM constraint_analysis
		WHERE analysis_type = 'Throughput Calculation'
			AND analysis_date = '20240101'
	`)
	if err != nil {
		t.Fatalf("Failed to query throughput calculation: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected throughput calculation record, got none")
	}

	throughput := getFloat(result.Rows[0][1])
	constraintEquipment := result.Rows[0][2].(string)
	constraintCapacity := getFloat(result.Rows[0][3])
	constraintUtil := getFloat(result.Rows[0][4])

	// Verify dryer is constraint (higher utilization, lower capacity)
	if constraintEquipment != "DRYER-01" {
		t.Errorf("Expected constraint DRYER-01, got %s", constraintEquipment)
	}

	// Verify capacity 10 tons/hr
	if constraintCapacity != 10.0 {
		t.Errorf("Expected constraint capacity 10.0 tons/hr, got %.1f", constraintCapacity)
	}

	// Verify utilization ~100%
	if constraintUtil < 99.0 || constraintUtil > 100.0 {
		t.Errorf("Expected constraint utilization ~100%%, got %.1f%%", constraintUtil)
	}

	// Verify throughput = 10 tons/hr × 24 hours = 240 tons
	expectedThroughput := constraintCapacity * 24.0
	if throughput < expectedThroughput-10 || throughput > expectedThroughput+10 {
		t.Errorf("Expected throughput ~%.0f tons, got %.0f tons", expectedThroughput, throughput)
	}

	t.Logf("Plant throughput: %.0f tons/day (limited by %s at %.1f tons/hr, %.1f%% utilization)",
		throughput, constraintEquipment, constraintCapacity, constraintUtil)
}

// TestOSBCapacityGapAnalysis validates quantification of capacity gap (demand vs constraint capacity)
func TestOSBCapacityGapAnalysis(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupBufferConstraintTest(t)
	defer cleanup()

	// Scenario: Demand = 450 tons/day, Constraint capacity = 396 tons/day
	// Capacity gap = 54 tons/day (12% shortage)
	// Revenue loss = 54 tons × $250/ton = $13,500/day

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Press runs 22 hours (constraint)
	insertStateEvents(t, adapter, ctx, "PRESS-01",
		baseTime.Format("2006-01-02 15:04:05"),
		baseTime.Add(22*time.Hour).Format("2006-01-02 15:04:05"),
		1320, "Running", "DAY", "20240101")

	insertStateEvents(t, adapter, ctx, "PRESS-01",
		baseTime.Add(22*time.Hour).Format("2006-01-02 15:04:05"),
		baseTime.Add(24*time.Hour).Format("2006-01-02 15:04:05"),
		120, "Unplanned Downtime", "NIGHT", "20240101")

	// Insert demand forecast (table already created in setup)
	err := adapter.ExecuteDDL(ctx, `INSERT INTO forecast_demand VALUES ('20240101', 450.0)`)
	if err != nil {
		t.Fatalf("Failed to insert demand: %v", err)
	}

	executeConstraintAnalysisSQL(t, adapter, ctx)

	// Verify capacity gap calculation
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			analysis_date,
			demand_tons,
			actual_throughput_tons,
			capacity_gap_tons,
			capacity_gap_pct,
			estimated_revenue_loss_usd
		FROM constraint_analysis
		WHERE analysis_type = 'Capacity Gap'
			AND analysis_date = '20240101'
	`)
	if err != nil {
		t.Fatalf("Failed to query capacity gap analysis: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected capacity gap record, got none")
	}

	demand := getFloat(result.Rows[0][1])
	actualThroughput := getFloat(result.Rows[0][2])
	capacityGap := getFloat(result.Rows[0][3])
	gapPct := getFloat(result.Rows[0][4])
	revenueLoss := getFloat(result.Rows[0][5])

	// Verify demand = 450 tons
	if demand != 450.0 {
		t.Errorf("Expected demand 450 tons, got %.0f", demand)
	}

	// Verify actual throughput ~396 tons (18 × 22)
	if actualThroughput < 390.0 || actualThroughput > 400.0 {
		t.Errorf("Expected actual throughput ~396 tons, got %.0f", actualThroughput)
	}

	// Verify gap = demand - actual
	expectedGap := demand - actualThroughput
	if capacityGap < expectedGap-5 || capacityGap > expectedGap+5 {
		t.Errorf("Expected capacity gap ~%.0f tons, got %.0f", expectedGap, capacityGap)
	}

	// Verify gap % ~12%
	if gapPct < 10.0 || gapPct > 14.0 {
		t.Errorf("Expected capacity gap ~12%%, got %.1f%%", gapPct)
	}

	// Verify revenue loss calculated
	if revenueLoss <= 0 {
		t.Errorf("Expected positive revenue loss, got $%.0f", revenueLoss)
	}

	t.Logf("Capacity gap: demand=%.0f tons, actual=%.0f tons, gap=%.0f tons (%.1f%%), revenue loss=$%.0f",
		demand, actualThroughput, capacityGap, gapPct, revenueLoss)
}
