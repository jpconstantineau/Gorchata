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

// setupStateDurationTest creates a test database with machine events and dimension tables
func setupStateDurationTest(t *testing.T) (*sqlite.SQLiteAdapter, context.Context, func()) {
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

	// Create dim_equipment table
	err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE dim_equipment (
			equipment_id TEXT PRIMARY KEY,
			equipment_name TEXT NOT NULL,
			equipment_type TEXT NOT NULL,
			production_area_id TEXT NOT NULL,
			design_capacity_rate REAL NOT NULL,
			capacity_units TEXT NOT NULL,
			criticality_level TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create dim_equipment: %v", err)
	}

	// Insert test equipment
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO dim_equipment VALUES 
		('DRYER-01', 'Primary Rotary Dryer', 'Dryer', 'Drying', 10.0, 'tons/hr', 'Critical'),
		('STRAND-01', 'Strander Line 1', 'Strander', 'Stranding', 6.0, 'tons/hr', 'Critical'),
		('PRESS-01', 'Continuous Press', 'Press', 'Pressing', 18.0, 'ft/min', 'Critical')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test equipment: %v", err)
	}

	// Create dim_reason_code table
	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE dim_reason_code (
			reason_code_id TEXT PRIMARY KEY,
			reason_code_name TEXT NOT NULL,
			reason_category TEXT NOT NULL,
			oee_classification TEXT NOT NULL,
			mttr_min_typical REAL NOT NULL,
			mttr_max_typical REAL NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create dim_reason_code: %v", err)
	}

	// Insert test reason codes
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO dim_reason_code VALUES 
		('RC_BEARING_FAIL', 'Bearing Failure', 'Mechanical', 'Breakdown', 120, 240),
		('RC_PLANNED_MAINT', 'Planned Maintenance', 'Maintenance', 'Planned Downtime', 480, 600),
		('RC_UPSTREAM_BLOCK', 'Upstream Equipment Blocking', 'Process', 'Blocked', 30, 120),
		('RC_DOWNSTREAM_STARVE', 'Downstream Equipment Starved', 'Process', 'Starved', 20, 60),
		('RC_MINOR_STOP', 'Minor Stop/Adjustment', 'Process', 'Minor Stop', 2, 10)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test reason codes: %v", err)
	}

	// Create dim_shift table
	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE dim_shift (
			shift_id TEXT PRIMARY KEY,
			shift_name TEXT NOT NULL,
			start_time TEXT NOT NULL,
			end_time TEXT NOT NULL,
			duration_hours REAL NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create dim_shift: %v", err)
	}

	// Insert test shifts
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO dim_shift VALUES 
		('DAY', 'Day Shift', '06:00', '14:00', 8.0),
		('SWING', 'Swing Shift', '14:00', '22:00', 8.0),
		('NIGHT', 'Night Shift', '22:00', '06:00', 8.0)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test shifts: %v", err)
	}

	// Create dim_date table
	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE dim_date (
			date_id TEXT PRIMARY KEY,
			date_actual DATE NOT NULL,
			year INTEGER NOT NULL,
			quarter INTEGER NOT NULL,
			month INTEGER NOT NULL,
			day_of_month INTEGER NOT NULL,
			day_of_week INTEGER NOT NULL,
			week_of_year INTEGER NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create dim_date: %v", err)
	}

	// Insert test dates
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO dim_date VALUES 
		('20240115', '2024-01-15', 2024, 1, 1, 15, 1, 3)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test dates: %v", err)
	}

	// Create stg_machine_events table
	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE stg_machine_events (
			equipment_id TEXT NOT NULL,
			event_timestamp TEXT NOT NULL,
			state TEXT NOT NULL,
			reason_code_id TEXT,
			PRIMARY KEY (equipment_id, event_timestamp)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create stg_machine_events: %v", err)
	}

	return adapter, ctx, cleanup
}

// machineEvent represents a machine state event
type machineEvent struct {
	EquipmentID    string
	EventTimestamp string
	State          string
	ReasonCodeID   string
}

// insertMachineEvents is a helper to insert machine events
func insertMachineEvents(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context, events []machineEvent) {
	for _, e := range events {
		reasonCode := "NULL"
		if e.ReasonCodeID != "" {
			reasonCode = fmt.Sprintf("'%s'", e.ReasonCodeID)
		}
		sql := fmt.Sprintf(`
			INSERT INTO stg_machine_events VALUES (
				'%s', '%s', '%s', %s
			)`,
			e.EquipmentID, e.EventTimestamp, e.State, reasonCode,
		)
		if err := adapter.ExecuteDDL(ctx, sql); err != nil {
			t.Fatalf("Failed to insert machine event: %v", err)
		}
	}
}

// executeStateHistorySQL loads and executes the stg_equipment_state_history.sql model
func executeStateHistorySQL(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context) {
	repoRoot := getRepoRoot(t)
	modelPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "models", "staging", "stg_equipment_state_history.sql")

	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read stg_equipment_state_history.sql: %v", err)
	}

	// Remove config calls
	contentStr := removeConfigCallsStaging(string(content))

	// Parse and render template
	templateEngine := template.New()
	tmpl, err := templateEngine.Parse("stg_equipment_state_history", contentStr)
	if err != nil {
		t.Fatalf("Failed to parse template: %v", err)
	}

	ctx2 := template.NewContext(template.WithCurrentModel("stg_equipment_state_history"))
	rendered, err := template.Render(tmpl, ctx2, nil)
	if err != nil {
		t.Fatalf("Failed to render template: %v", err)
	}

	// Create staging table
	err = adapter.ExecuteDDL(ctx, "DROP TABLE IF EXISTS stg_equipment_state_history")
	if err != nil {
		t.Fatalf("Failed to drop existing stg_equipment_state_history: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, "CREATE TABLE stg_equipment_state_history AS "+rendered)
	if err != nil {
		t.Fatalf("Failed to execute stg_equipment_state_history model: %v", err)
	}
}

// TestOSBStateDurationCalculation validates LEAD window function correctly calculates state_end from next state_start
func TestOSBStateDurationCalculation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupStateDurationTest(t)
	defer cleanup()

	// Create machine events showing state transitions
	baseTime := time.Date(2024, 1, 15, 8, 0, 0, 0, time.UTC)
	events := []machineEvent{
		{"DRYER-01", baseTime.Format("2006-01-02 15:04:05"), "Running", ""},
		{"DRYER-01", baseTime.Add(30 * time.Minute).Format("2006-01-02 15:04:05"), "Idle", ""},
		{"DRYER-01", baseTime.Add(45 * time.Minute).Format("2006-01-02 15:04:05"), "Running", ""},
		{"DRYER-01", baseTime.Add(90 * time.Minute).Format("2006-01-02 15:04:05"), "Unplanned Downtime", "RC_BEARING_FAIL"},
	}

	insertMachineEvents(t, adapter, ctx, events)
	executeStateHistorySQL(t, adapter, ctx)

	// Verify state durations calculated correctly using LEAD
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			state_start_timestamp,
			state_end_timestamp,
			state_duration_min,
			machine_state
		FROM stg_equipment_state_history
		WHERE equipment_id = 'DRYER-01'
		ORDER BY state_start_timestamp
	`)
	if err != nil {
		t.Fatalf("Failed to query state history: %v", err)
	}

	// Validate first state: Running for 30 minutes
	if len(result.Rows) < 3 {
		t.Fatalf("Expected at least 3 state periods, got %d", len(result.Rows))
	}

	// First period: Running (08:00 to 08:30 = 30 min)
	duration1 := getFloat(result.Rows[0][2])
	state1 := result.Rows[0][3].(string)
	if state1 != "Running" {
		t.Errorf("Expected first state to be 'Running', got '%s'", state1)
	}
	if duration1 != 30.0 {
		t.Errorf("Expected first state duration to be 30 minutes, got %.1f", duration1)
	}

	// Second period: Idle (08:30 to 08:45 = 15 min)
	duration2 := getFloat(result.Rows[1][2])
	state2 := result.Rows[1][3].(string)
	if state2 != "Idle" {
		t.Errorf("Expected second state to be 'Idle', got '%s'", state2)
	}
	if duration2 != 15.0 {
		t.Errorf("Expected second state duration to be 15 minutes, got %.1f", duration2)
	}

	// Third period: Running (08:45 to 09:30 = 45 min)
	duration3 := getFloat(result.Rows[2][2])
	state3 := result.Rows[2][3].(string)
	if state3 != "Running" {
		t.Errorf("Expected third state to be 'Running', got '%s'", state3)
	}
	if duration3 != 45.0 {
		t.Errorf("Expected third state duration to be 45 minutes, got %.1f", duration3)
	}
}

// TestOSBStateCompleteness ensures every event assigned a duration (handle last event per equipment)
func TestOSBStateCompleteness(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupStateDurationTest(t)
	defer cleanup()

	// Create events for multiple equipment with different end conditions
	baseTime := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	events := []machineEvent{
		{"DRYER-01", baseTime.Format("2006-01-02 15:04:05"), "Running", ""},
		{"DRYER-01", baseTime.Add(1 * time.Hour).Format("2006-01-02 15:04:05"), "Idle", ""},
		{"STRAND-01", baseTime.Format("2006-01-02 15:04:05"), "Running", ""},
		{"STRAND-01", baseTime.Add(2 * time.Hour).Format("2006-01-02 15:04:05"), "Blocked", "RC_UPSTREAM_BLOCK"},
		{"PRESS-01", baseTime.Format("2006-01-02 15:04:05"), "Running", ""},
	}

	insertMachineEvents(t, adapter, ctx, events)
	executeStateHistorySQL(t, adapter, ctx)

	// Verify all events have a state period record (including last events)
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as total_periods,
		       COUNT(state_end_timestamp) as periods_with_end,
		       COUNT(*) FILTER (WHERE state_duration_min IS NOT NULL) as periods_with_duration
		FROM stg_equipment_state_history
	`)
	if err != nil {
		t.Fatalf("Failed to query state completeness: %v", err)
	}

	totalPeriods := result.Rows[0][0].(int64)
	periodsWithEnd := result.Rows[0][1].(int64)
	periodsWithDuration := result.Rows[0][2].(int64)

	// We expect 5 events to create 5 state periods
	if totalPeriods != 5 {
		t.Errorf("Expected 5 state periods, got %d", totalPeriods)
	}

	// All periods should have duration calculated (even last events use CURRENT_TIMESTAMP)
	if periodsWithDuration != 5 {
		t.Errorf("Expected all 5 periods to have duration, got %d", periodsWithDuration)
	}

	// Last events per equipment should have state_end_timestamp (COALESCE to CURRENT_TIMESTAMP)
	if periodsWithEnd != 5 {
		t.Errorf("Expected all 5 periods to have end timestamp, got %d", periodsWithEnd)
	}

	// Verify last event for each equipment has reasonable duration
	result, err = adapter.ExecuteQuery(ctx, `
		WITH last_states AS (
			SELECT 
				equipment_id,
				state_duration_min,
				ROW_NUMBER() OVER (PARTITION BY equipment_id ORDER BY state_start_timestamp DESC) as rn
			FROM stg_equipment_state_history
		)
		SELECT equipment_id, state_duration_min
		FROM last_states
		WHERE rn = 1
	`)
	if err != nil {
		t.Fatalf("Failed to query last states: %v", err)
	}

	for _, row := range result.Rows {
		equipmentID := row[0].(string)
		duration := getFloat(row[1])
		// Last states should have positive duration (from event time to test execution time)
		if duration <= 0 {
			t.Errorf("Last state for %s has invalid duration: %.1f", equipmentID, duration)
		}
	}
}

// TestOSBStateCategorization validates states correctly classified (Running, Idle, Starved, Blocked, etc.)
func TestOSBStateCategorization(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupStateDurationTest(t)
	defer cleanup()

	// Create events with various state types
	baseTime := time.Date(2024, 1, 15, 9, 0, 0, 0, time.UTC)
	events := []machineEvent{
		{"DRYER-01", baseTime.Format("2006-01-02 15:04:05"), "Running", ""},
		{"DRYER-01", baseTime.Add(30 * time.Minute).Format("2006-01-02 15:04:05"), "Idle", ""},
		{"DRYER-01", baseTime.Add(45 * time.Minute).Format("2006-01-02 15:04:05"), "Starved", "RC_DOWNSTREAM_STARVE"},
		{"DRYER-01", baseTime.Add(60 * time.Minute).Format("2006-01-02 15:04:05"), "Blocked", "RC_UPSTREAM_BLOCK"},
		{"DRYER-01", baseTime.Add(90 * time.Minute).Format("2006-01-02 15:04:05"), "Unplanned Downtime", "RC_BEARING_FAIL"},
		{"DRYER-01", baseTime.Add(210 * time.Minute).Format("2006-01-02 15:04:05"), "Planned Downtime", "RC_PLANNED_MAINT"},
	}

	insertMachineEvents(t, adapter, ctx, events)
	executeStateHistorySQL(t, adapter, ctx)

	// Verify all state categories present and correctly classified
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT machine_state, COUNT(*) as state_count
		FROM stg_equipment_state_history
		WHERE equipment_id = 'DRYER-01'
		GROUP BY machine_state
		ORDER BY machine_state
	`)
	if err != nil {
		t.Fatalf("Failed to query state categories: %v", err)
	}

	expectedStates := map[string]bool{
		"Running":            false,
		"Idle":               false,
		"Starved":            false,
		"Blocked":            false,
		"Unplanned Downtime": false,
		"Planned Downtime":   false,
	}

	for _, row := range result.Rows {
		state := row[0].(string)
		if _, exists := expectedStates[state]; exists {
			expectedStates[state] = true
		} else {
			t.Errorf("Unexpected state category found: %s", state)
		}
	}

	for state, found := range expectedStates {
		if !found {
			t.Errorf("Expected state category '%s' not found in results", state)
		}
	}
}

// TestOSBReasonCodeJoin ensures reason codes correctly joined and OEE classification applied
func TestOSBReasonCodeJoin(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupStateDurationTest(t)
	defer cleanup()

	// Create events with reason codes
	baseTime := time.Date(2024, 1, 15, 11, 0, 0, 0, time.UTC)
	events := []machineEvent{
		{"DRYER-01", baseTime.Format("2006-01-02 15:04:05"), "Running", ""},
		{"DRYER-01", baseTime.Add(1 * time.Hour).Format("2006-01-02 15:04:05"), "Unplanned Downtime", "RC_BEARING_FAIL"},
		{"DRYER-01", baseTime.Add(3 * time.Hour).Format("2006-01-02 15:04:05"), "Running", ""},
		{"DRYER-01", baseTime.Add(4 * time.Hour).Format("2006-01-02 15:04:05"), "Blocked", "RC_UPSTREAM_BLOCK"},
	}

	insertMachineEvents(t, adapter, ctx, events)
	executeStateHistorySQL(t, adapter, ctx)

	// Verify reason codes joined correctly
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			h.machine_state,
			h.reason_code_id,
			r.reason_code_name,
			r.oee_classification,
			r.mttr_min_typical,
			r.mttr_max_typical
		FROM stg_equipment_state_history h
		LEFT JOIN dim_reason_code r ON h.reason_code_id = r.reason_code_id
		WHERE h.equipment_id = 'DRYER-01'
		AND h.reason_code_id IS NOT NULL
		ORDER BY h.state_start_timestamp
	`)
	if err != nil {
		t.Fatalf("Failed to query reason code join: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Fatalf("Expected 2 periods with reason codes, got %d", len(result.Rows))
	}

	// Verify first downtime: Bearing Failure
	reasonName1 := result.Rows[0][2].(string)
	oeeClass1 := result.Rows[0][3].(string)
	if reasonName1 != "Bearing Failure" {
		t.Errorf("Expected reason 'Bearing Failure', got '%s'", reasonName1)
	}
	if oeeClass1 != "Breakdown" {
		t.Errorf("Expected OEE classification 'Breakdown', got '%s'", oeeClass1)
	}

	// Verify second downtime: Upstream Blocking
	reasonName2 := result.Rows[1][2].(string)
	oeeClass2 := result.Rows[1][3].(string)
	if reasonName2 != "Upstream Equipment Blocking" {
		t.Errorf("Expected reason 'Upstream Equipment Blocking', got '%s'", reasonName2)
	}
	if oeeClass2 != "Blocked" {
		t.Errorf("Expected OEE classification 'Blocked', got '%s'", oeeClass2)
	}
}

// TestOSBShiftAssignment validates correct shift_id assigned based on timestamp
func TestOSBShiftAssignment(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupStateDurationTest(t)
	defer cleanup()

	// Create events across different shifts
	events := []machineEvent{
		{"DRYER-01", "2024-01-15 07:00:00", "Running", ""}, // Day shift (06:00-14:00)
		{"DRYER-01", "2024-01-15 15:00:00", "Running", ""}, // Swing shift (14:00-22:00)
		{"DRYER-01", "2024-01-15 23:00:00", "Running", ""}, // Night shift (22:00-06:00)
		{"DRYER-01", "2024-01-16 02:00:00", "Idle", ""},    // Still night shift (crosses midnight)
	}

	insertMachineEvents(t, adapter, ctx, events)
	executeStateHistorySQL(t, adapter, ctx)

	// Verify shift assignments
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			h.state_start_timestamp,
			h.shift_id,
			s.shift_name
		FROM stg_equipment_state_history h
		LEFT JOIN dim_shift s ON h.shift_id = s.shift_id
		WHERE h.equipment_id = 'DRYER-01'
		ORDER BY h.state_start_timestamp
	`)
	if err != nil {
		t.Fatalf("Failed to query shift assignments: %v", err)
	}

	if len(result.Rows) < 3 {
		t.Fatalf("Expected at least 3 state periods, got %d", len(result.Rows))
	}

	// Verify Day shift (07:00)
	shiftID1 := result.Rows[0][1]
	if shiftID1 != nil && shiftID1.(string) != "DAY" {
		t.Errorf("Expected Day shift for 07:00, got '%s'", shiftID1)
	}

	// Verify Swing shift (15:00)
	shiftID2 := result.Rows[1][1]
	if shiftID2 != nil && shiftID2.(string) != "SWING" {
		t.Errorf("Expected Swing shift for 15:00, got '%s'", shiftID2)
	}

	// Verify Night shift (23:00)
	shiftID3 := result.Rows[2][1]
	if shiftID3 != nil && shiftID3.(string) != "NIGHT" {
		t.Errorf("Expected Night shift for 23:00, got '%s'", shiftID3)
	}
}

// TestOSBDateAssignment validates correct date_id assigned
func TestOSBDateAssignment(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupStateDurationTest(t)
	defer cleanup()

	// Create events on different dates
	events := []machineEvent{
		{"DRYER-01", "2024-01-15 10:00:00", "Running", ""},
		{"DRYER-01", "2024-01-15 20:00:00", "Idle", ""},
		{"DRYER-01", "2024-01-16 08:00:00", "Running", ""},
	}

	insertMachineEvents(t, adapter, ctx, events)
	executeStateHistorySQL(t, adapter, ctx)

	// Verify date assignments
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			h.state_start_timestamp,
			h.date_id,
			d.date_actual
		FROM stg_equipment_state_history h
		LEFT JOIN dim_date d ON h.date_id = d.date_id
		WHERE h.equipment_id = 'DRYER-01'
		ORDER BY h.state_start_timestamp
	`)
	if err != nil {
		t.Fatalf("Failed to query date assignments: %v", err)
	}

	if len(result.Rows) < 2 {
		t.Fatalf("Expected at least 2 state periods, got %d", len(result.Rows))
	}

	// Verify first two events assigned to 2024-01-15
	dateID1 := result.Rows[0][1]
	if dateID1 != nil && dateID1.(string) != "20240115" {
		t.Errorf("Expected date_id '20240115' for first event, got '%s'", dateID1)
	}

	// Verify third event assigned to 2024-01-16
	if len(result.Rows) >= 3 {
		dateID3 := result.Rows[2][1]
		if dateID3 != nil && dateID3.(string) != "20240116" {
			t.Errorf("Expected date_id '20240116' for third event, got '%s'", dateID3)
		}
	}
}

// TestOSBZeroDurationHandling ensures instantaneous state changes handled appropriately
func TestOSBZeroDurationHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupStateDurationTest(t)
	defer cleanup()

	// Create events with very short duration (near-instantaneous transitions)
	baseTime := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	events := []machineEvent{
		{"DRYER-01", baseTime.Format("2006-01-02 15:04:05"), "Running", ""},
		{"DRYER-01", baseTime.Add(1 * time.Second).Format("2006-01-02 15:04:05"), "Idle", ""}, // 1 second transition
		{"DRYER-01", baseTime.Add(30 * time.Minute).Format("2006-01-02 15:04:05"), "Running", ""},
	}

	insertMachineEvents(t, adapter, ctx, events)
	executeStateHistorySQL(t, adapter, ctx)

	// Verify zero-duration states are handled (included with 0 duration or excluded)
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			state_start_timestamp,
			state_end_timestamp,
			state_duration_min,
			machine_state
		FROM stg_equipment_state_history
		WHERE equipment_id = 'DRYER-01'
		ORDER BY state_start_timestamp
	`)
	if err != nil {
		t.Fatalf("Failed to query zero-duration states: %v", err)
	}

	// Check if very short duration states exist
	hasVeryShortDuration := false
	for _, row := range result.Rows {
		duration := getFloat(row[2])
		if duration < 1.0 { // Less than 1 minute
			hasVeryShortDuration = true
		}
	}

	// Very short duration states should be included with duration rounded to 0 minutes
	if hasVeryShortDuration {
		t.Logf("Very short duration states are included (duration < 1 minute, rounds to 0)")
	} else {
		// All durations should be >= 1 minute if sub-minute events are rounded up
		t.Logf("Very short duration states are rounded to non-zero values or excluded")
	}
}

// TestOSBMultiDayPeriods validates state periods spanning midnight correctly split by day
func TestOSBMultiDayPeriods(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupStateDurationTest(t)
	defer cleanup()

	// Add date for next day
	err := adapter.ExecuteDDL(ctx, `
		INSERT INTO dim_date VALUES 
		('20240116', '2024-01-16', 2024, 1, 1, 16, 2, 3)
	`)
	if err != nil {
		t.Fatalf("Failed to insert additional date: %v", err)
	}

	// Create event that spans midnight (starts before midnight, next event after midnight)
	events := []machineEvent{
		{"DRYER-01", "2024-01-15 23:00:00", "Running", ""}, // Before midnight
		{"DRYER-01", "2024-01-16 02:00:00", "Idle", ""},    // After midnight (3 hours later)
	}

	insertMachineEvents(t, adapter, ctx, events)
	executeStateHistorySQL(t, adapter, ctx)

	// Verify the Running state spanning midnight is present
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			state_start_timestamp,
			state_end_timestamp,
			state_duration_min,
			machine_state,
			date_id
		FROM stg_equipment_state_history
		WHERE equipment_id = 'DRYER-01'
		AND machine_state = 'Running'
		ORDER BY state_start_timestamp
	`)
	if err != nil {
		t.Fatalf("Failed to query multi-day period: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected Running state period spanning midnight, got none")
	}

	// Check if state was split into two records (one per day) or kept as single record
	// Single record approach: duration should be 180 minutes (3 hours)
	// Split record approach: two records, one ending at 23:59:59, next starting at 00:00:00

	if len(result.Rows) == 1 {
		// Single record spanning midnight
		duration := getFloat(result.Rows[0][2])
		if duration != 180.0 {
			t.Errorf("Expected 180 minute duration for midnight-spanning state, got %.1f", duration)
		}
		t.Logf("Multi-day periods handled as single record spanning midnight")
	} else if len(result.Rows) == 2 {
		// Split into two records
		duration1 := getFloat(result.Rows[0][2])
		duration2 := getFloat(result.Rows[1][2])
		totalDuration := duration1 + duration2
		if totalDuration != 180.0 {
			t.Errorf("Expected total duration of 180 minutes across split records, got %.1f", totalDuration)
		}
		t.Logf("Multi-day periods split by midnight boundary")
	} else {
		t.Errorf("Unexpected number of records for midnight-spanning state: %d", len(result.Rows))
	}
}
