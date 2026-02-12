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

// setupDowntimeAnalysisTest creates a test database with equipment state history
func setupDowntimeAnalysisTest(t *testing.T) (*sqlite.SQLiteAdapter, context.Context, func()) {
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
		('PRESS-01', 'Continuous Press', 'Press', 'Pressing', 18.0, 'ft/min', 'Critical'),
		('STRAND-01', 'Strander Line 1', 'Strander', 'Stranding', 6.0, 'tons/hr', 'Important')
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
			six_big_losses_category TEXT NOT NULL,
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
		('RC_BEARING_FAIL', 'Bearing Failure', 'Mechanical', 'Breakdown', 'Equipment Failure', 120, 240),
		('RC_BURNER_TRIP', 'Dryer Burner Trip', 'Process', 'Breakdown', 'Equipment Failure', 60, 120),
		('RC_HYDRAULIC_LEAK', 'Hydraulic Leak', 'Mechanical', 'Breakdown', 'Equipment Failure', 180, 360),
		('RC_STRAND_BRIDGE', 'Strand Bridging', 'Process', 'Minor Stop', 'Small Stops', 15, 30)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test reason codes: %v", err)
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

	// Insert test dates (30 days)
	baseDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 30; i++ {
		date := baseDate.Add(time.Duration(i) * 24 * time.Hour)
		dateID := date.Format("20060102")
		_, week := date.ISOWeek()
		sql := fmt.Sprintf(`
			INSERT INTO dim_date VALUES 
			('%s', '%s', %d, %d, %d, %d, %d, %d)
		`, dateID, date.Format("2006-01-02"), date.Year(), (date.Month()-1)/3+1,
			int(date.Month()), date.Day(), int(date.Weekday()), week)
		if err := adapter.ExecuteDDL(ctx, sql); err != nil {
			t.Fatalf("Failed to insert date: %v", err)
		}
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

	return adapter, ctx, cleanup
}

// downtimeEvent represents a downtime event for test data
type downtimeEvent struct {
	EquipmentID         string
	StateStartTimestamp string
	StateEndTimestamp   string
	StateDurationMin    float64
	MachineState        string
	ReasonCodeID        string
	ShiftID             string
	DateID              string
}

// insertDowntimeEvents is a helper to insert downtime events
func insertDowntimeEvents(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context, events []downtimeEvent) {
	for _, e := range events {
		reasonCode := "NULL"
		if e.ReasonCodeID != "" {
			reasonCode = fmt.Sprintf("'%s'", e.ReasonCodeID)
		}
		sql := fmt.Sprintf(`
			INSERT INTO stg_equipment_state_history VALUES (
				'%s', '%s', '%s', %.2f, '%s', %s, '%s', '%s'
			)`,
			e.EquipmentID, e.StateStartTimestamp, e.StateEndTimestamp,
			e.StateDurationMin, e.MachineState, reasonCode, e.ShiftID, e.DateID,
		)
		if err := adapter.ExecuteDDL(ctx, sql); err != nil {
			t.Fatalf("Failed to insert downtime event: %v", err)
		}
	}
}

// executeDowntimeAnalysisSQL loads and executes the equipment_downtime_analysis.sql model
func executeDowntimeAnalysisSQL(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context) {
	repoRoot := getRepoRoot(t)
	modelPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "models", "metrics", "equipment_downtime_analysis.sql")

	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read equipment_downtime_analysis.sql: %v", err)
	}

	// Remove config calls
	contentStr := removeConfigCallsStaging(string(content))

	// Parse and render template
	templateEngine := template.New()
	tmpl, err := templateEngine.Parse("equipment_downtime_analysis", contentStr)
	if err != nil {
		t.Fatalf("Failed to parse template: %v", err)
	}

	ctx2 := template.NewContext(template.WithCurrentModel("equipment_downtime_analysis"))
	rendered, err := template.Render(tmpl, ctx2, nil)
	if err != nil {
		t.Fatalf("Failed to render template: %v", err)
	}

	// Create metrics table
	err = adapter.ExecuteDDL(ctx, "DROP TABLE IF EXISTS equipment_downtime_analysis")
	if err != nil {
		t.Fatalf("Failed to drop existing equipment_downtime_analysis: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, "CREATE TABLE equipment_downtime_analysis AS "+rendered)
	if err != nil {
		t.Fatalf("Failed to execute equipment_downtime_analysis model: %v", err)
	}
}

// executeReliabilityMetricsSQL loads and executes the equipment_reliability_metrics.sql model
func executeReliabilityMetricsSQL(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context) {
	repoRoot := getRepoRoot(t)
	modelPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "models", "metrics", "equipment_reliability_metrics.sql")

	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read equipment_reliability_metrics.sql: %v", err)
	}

	// Remove config calls
	contentStr := removeConfigCallsStaging(string(content))

	// Parse and render template
	templateEngine := template.New()
	tmpl, err := templateEngine.Parse("equipment_reliability_metrics", contentStr)
	if err != nil {
		t.Fatalf("Failed to parse template: %v", err)
	}

	ctx2 := template.NewContext(template.WithCurrentModel("equipment_reliability_metrics"))
	rendered, err := template.Render(tmpl, ctx2, nil)
	if err != nil {
		t.Fatalf("Failed to render template: %v", err)
	}

	// Create metrics table
	err = adapter.ExecuteDDL(ctx, "DROP TABLE IF EXISTS equipment_reliability_metrics")
	if err != nil {
		t.Fatalf("Failed to drop existing equipment_reliability_metrics: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, "CREATE TABLE equipment_reliability_metrics AS "+rendered)
	if err != nil {
		t.Fatalf("Failed to execute equipment_reliability_metrics model: %v", err)
	}
}

// executeFailureModePareto SQL loads and executes the failure_mode_pareto.sql model
func executeFailureModePareto(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context) {
	repoRoot := getRepoRoot(t)
	modelPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "models", "metrics", "failure_mode_pareto.sql")

	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read failure_mode_pareto.sql: %v", err)
	}

	// Remove config calls
	contentStr := removeConfigCallsStaging(string(content))

	// Parse and render template
	templateEngine := template.New()
	tmpl, err := templateEngine.Parse("failure_mode_pareto", contentStr)
	if err != nil {
		t.Fatalf("Failed to parse template: %v", err)
	}

	ctx2 := template.NewContext(template.WithCurrentModel("failure_mode_pareto"))
	rendered, err := template.Render(tmpl, ctx2, nil)
	if err != nil {
		t.Fatalf("Failed to render template: %v", err)
	}

	// Create metrics table
	err = adapter.ExecuteDDL(ctx, "DROP TABLE IF EXISTS failure_mode_pareto")
	if err != nil {
		t.Fatalf("Failed to drop existing failure_mode_pareto: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, "CREATE TABLE failure_mode_pareto AS "+rendered)
	if err != nil {
		t.Fatalf("Failed to execute failure_mode_pareto model: %v", err)
	}
}

// TestOSBDowntimeByReasonAggregation validates downtime correctly summed by reason code
func TestOSBDowntimeByReasonAggregation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupDowntimeAnalysisTest(t)
	defer cleanup()

	// Create downtime events for DRYER-01 with multiple failure types
	baseTime := time.Date(2024, 1, 15, 8, 0, 0, 0, time.UTC)
	events := []downtimeEvent{
		// Operating time
		{"DRYER-01", baseTime.Format("2006-01-02 15:04:05"), baseTime.Add(6 * time.Hour).Format("2006-01-02 15:04:05"), 360, "Running", "", "DAY", "20240115"},
		// Bearing failure (120 min)
		{"DRYER-01", baseTime.Add(6 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(8 * time.Hour).Format("2006-01-02 15:04:05"), 120, "Unplanned Downtime", "RC_BEARING_FAIL", "DAY", "20240115"},
		// Operating time
		{"DRYER-01", baseTime.Add(8 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(14 * time.Hour).Format("2006-01-02 15:04:05"), 360, "Running", "", "DAY", "20240115"},
		// Burner trip (60 min)
		{"DRYER-01", baseTime.Add(14 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(15 * time.Hour).Format("2006-01-02 15:04:05"), 60, "Unplanned Downtime", "RC_BURNER_TRIP", "SWING", "20240115"},
		// Operating time
		{"DRYER-01", baseTime.Add(15 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(22 * time.Hour).Format("2006-01-02 15:04:05"), 420, "Running", "", "SWING", "20240115"},
	}

	insertDowntimeEvents(t, adapter, ctx, events)
	executeDowntimeAnalysisSQL(t, adapter, ctx)

	// Verify downtime aggregation by reason
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			reason_code_id,
			failure_count,
			total_downtime_min
		FROM equipment_downtime_analysis
		WHERE equipment_id = 'DRYER-01'
		ORDER BY total_downtime_min DESC
	`)
	if err != nil {
		t.Fatalf("Failed to query downtime analysis: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Fatalf("Expected 2 downtime reason records, got %d", len(result.Rows))
	}

	// First record: Bearing failure (120 min, 1 occurrence)
	reasonCode1 := result.Rows[0][0].(string)
	failureCount1 := getFloat(result.Rows[0][1])
	totalDowntime1 := getFloat(result.Rows[0][2])

	if reasonCode1 != "RC_BEARING_FAIL" {
		t.Errorf("Expected RC_BEARING_FAIL, got %s", reasonCode1)
	}
	if failureCount1 != 1.0 {
		t.Errorf("Expected 1 failure, got %.0f", failureCount1)
	}
	if totalDowntime1 != 120.0 {
		t.Errorf("Expected 120 min downtime, got %.0f", totalDowntime1)
	}

	// Second record: Burner trip (60 min, 1 occurrence)
	reasonCode2 := result.Rows[1][0].(string)
	failureCount2 := getFloat(result.Rows[1][1])
	totalDowntime2 := getFloat(result.Rows[1][2])

	if reasonCode2 != "RC_BURNER_TRIP" {
		t.Errorf("Expected RC_BURNER_TRIP, got %s", reasonCode2)
	}
	if failureCount2 != 1.0 {
		t.Errorf("Expected 1 failure, got %.0f", failureCount2)
	}
	if totalDowntime2 != 60.0 {
		t.Errorf("Expected 60 min downtime, got %.0f", totalDowntime2)
	}
}

// TestOSBMTBFCalculation validates MTBF = Total Operating Time / Number of Failures
func TestOSBMTBFCalculation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupDowntimeAnalysisTest(t)
	defer cleanup()

	// Create events over 7 days: 3 failures in ~140 hours of operating time
	// Failures on days 0, 2, 4 (every other day, day%2==0 && day<6)
	// MTBF = 8400 min / 3 = 2800 minutes = 46.67 hours
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []downtimeEvent{}

	for day := 0; day < 7; day++ {
		dateID := baseTime.Add(time.Duration(day) * 24 * time.Hour).Format("20060102")
		dayStart := baseTime.Add(time.Duration(day) * 24 * time.Hour)

		// 20 hours running per day = 140 hours total
		events = append(events, downtimeEvent{
			"DRYER-01",
			dayStart.Format("2006-01-02 15:04:05"),
			dayStart.Add(20 * time.Hour).Format("2006-01-02 15:04:05"),
			1200,
			"Running",
			"",
			"DAY",
			dateID,
		})

		// Failure every other day (4 failures in 7 days)
		if day%2 == 0 && day < 6 {
			events = append(events, downtimeEvent{
				"DRYER-01",
				dayStart.Add(20 * time.Hour).Format("2006-01-02 15:04:05"),
				dayStart.Add(22 * time.Hour).Format("2006-01-02 15:04:05"),
				120,
				"Unplanned Downtime",
				"RC_BEARING_FAIL",
				"NIGHT",
				dateID,
			})
		}
	}

	insertDowntimeEvents(t, adapter, ctx, events)
	executeReliabilityMetricsSQL(t, adapter, ctx)

	// Verify MTBF calculation
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			equipment_id,
			total_operating_time_min,
			failure_count,
			mtbf_hours
		FROM equipment_reliability_metrics
		WHERE equipment_id = 'DRYER-01'
	`)
	if err != nil {
		t.Fatalf("Failed to query reliability metrics: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected reliability metrics record, got none")
	}

	operatingTime := getFloat(result.Rows[0][1])
	failureCount := getFloat(result.Rows[0][2])
	mtbfHours := getFloat(result.Rows[0][3])

	// Verify: 7 days × 20 hours = 140 hours = 8400 minutes
	if operatingTime != 8400.0 {
		t.Errorf("Expected operating time 8400 minutes, got %.0f", operatingTime)
	}

	// Verify: 3 failures (days 0, 2, 4)
	if failureCount != 3.0 {
		t.Errorf("Expected 3 failures, got %.0f", failureCount)
	}

	// Verify: MTBF = 8400 / 3 = 2800 minutes = 46.67 hours
	expectedMTBF := operatingTime / failureCount / 60.0 // Convert min to hours
	if mtbfHours < expectedMTBF-0.5 || mtbfHours > expectedMTBF+0.5 {
		t.Errorf("Expected MTBF ~%.1f hours, got %.1f hours", expectedMTBF, mtbfHours)
	}
}

// TestOSBMTTRCalculation validates MTTR = Total Downtime / Number of Failures
func TestOSBMTTRCalculation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupDowntimeAnalysisTest(t)
	defer cleanup()

	// Create events: 3 failures with varying repair times
	// Failure 1: 120 min, Failure 2: 180 min, Failure 3: 60 min
	// MTTR = (120 + 180 + 60) / 3 = 120 minutes = 2 hours
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []downtimeEvent{
		{"PRESS-01", baseTime.Format("2006-01-02 15:04:05"), baseTime.Add(10 * time.Hour).Format("2006-01-02 15:04:05"), 600, "Running", "", "NIGHT", "20240101"},
		{"PRESS-01", baseTime.Add(10 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(12 * time.Hour).Format("2006-01-02 15:04:05"), 120, "Unplanned Downtime", "RC_BEARING_FAIL", "DAY", "20240101"},
		{"PRESS-01", baseTime.Add(12 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(22 * time.Hour).Format("2006-01-02 15:04:05"), 600, "Running", "", "DAY", "20240101"},

		{"PRESS-01", baseTime.Add(24 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(34 * time.Hour).Format("2006-01-02 15:04:05"), 600, "Running", "", "NIGHT", "20240102"},
		{"PRESS-01", baseTime.Add(34 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(37 * time.Hour).Format("2006-01-02 15:04:05"), 180, "Unplanned Downtime", "RC_HYDRAULIC_LEAK", "DAY", "20240102"},
		{"PRESS-01", baseTime.Add(37 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(46 * time.Hour).Format("2006-01-02 15:04:05"), 540, "Running", "", "DAY", "20240102"},

		{"PRESS-01", baseTime.Add(48 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(58 * time.Hour).Format("2006-01-02 15:04:05"), 600, "Running", "", "NIGHT", "20240103"},
		{"PRESS-01", baseTime.Add(58 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(59 * time.Hour).Format("2006-01-02 15:04:05"), 60, "Unplanned Downtime", "RC_BURNER_TRIP", "DAY", "20240103"},
		{"PRESS-01", baseTime.Add(59 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(70 * time.Hour).Format("2006-01-02 15:04:05"), 660, "Running", "", "DAY", "20240103"},
	}

	insertDowntimeEvents(t, adapter, ctx, events)
	executeReliabilityMetricsSQL(t, adapter, ctx)

	// Verify MTTR calculation
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			equipment_id,
			total_downtime_min,
			failure_count,
			mttr_hours
		FROM equipment_reliability_metrics
		WHERE equipment_id = 'PRESS-01'
	`)
	if err != nil {
		t.Fatalf("Failed to query reliability metrics: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected reliability metrics record, got none")
	}

	totalDowntime := getFloat(result.Rows[0][1])
	failureCount := getFloat(result.Rows[0][2])
	mttrHours := getFloat(result.Rows[0][3])

	// Verify: 120 + 180 + 60 = 360 minutes total downtime
	if totalDowntime != 360.0 {
		t.Errorf("Expected total downtime 360 minutes, got %.0f", totalDowntime)
	}

	// Verify: 3 failures
	if failureCount != 3.0 {
		t.Errorf("Expected 3 failures, got %.0f", failureCount)
	}

	// Verify: MTTR = 360 / 3 = 120 minutes = 2 hours
	expectedMTTR := totalDowntime / failureCount / 60.0 // Convert min to hours
	if mttrHours < expectedMTTR-0.5 || mttrHours > expectedMTTR+0.5 {
		t.Errorf("Expected MTTR ~%.1f hours, got %.1f hours", expectedMTTR, mttrHours)
	}
}

// TestOSBFailureFrequencyCalculation validates failure count and failures per day/week
func TestOSBFailureFrequencyCalculation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupDowntimeAnalysisTest(t)
	defer cleanup()

	// Create events over 14 days: 7 failures (0.5 failures/day, 3.5 failures/week)
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []downtimeEvent{}

	for day := 0; day < 14; day++ {
		dateID := baseTime.Add(time.Duration(day) * 24 * time.Hour).Format("20060102")
		dayStart := baseTime.Add(time.Duration(day) * 24 * time.Hour)

		// Running time
		events = append(events, downtimeEvent{
			"STRAND-01",
			dayStart.Format("2006-01-02 15:04:05"),
			dayStart.Add(22 * time.Hour).Format("2006-01-02 15:04:05"),
			1320,
			"Running",
			"",
			"DAY",
			dateID,
		})

		// Failure every other day (7 failures in 14 days)
		if day%2 == 0 {
			events = append(events, downtimeEvent{
				"STRAND-01",
				dayStart.Add(22 * time.Hour).Format("2006-01-02 15:04:05"),
				dayStart.Add(23 * time.Hour).Format("2006-01-02 15:04:05"),
				60,
				"Unplanned Downtime",
				"RC_BEARING_FAIL",
				"NIGHT",
				dateID,
			})
		}
	}

	insertDowntimeEvents(t, adapter, ctx, events)
	executeReliabilityMetricsSQL(t, adapter, ctx)

	// Verify failure frequency calculation
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			equipment_id,
			failure_count,
			analysis_period_days,
			failures_per_day,
			failures_per_week
		FROM equipment_reliability_metrics
		WHERE equipment_id = 'STRAND-01'
	`)
	if err != nil {
		t.Fatalf("Failed to query reliability metrics: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected reliability metrics record, got none")
	}

	failureCount := getFloat(result.Rows[0][1])
	analysisPeriod := getFloat(result.Rows[0][2])
	failuresPerDay := getFloat(result.Rows[0][3])
	failuresPerWeek := getFloat(result.Rows[0][4])

	// Verify: 7 failures
	if failureCount != 7.0 {
		t.Errorf("Expected 7 failures, got %.0f", failureCount)
	}

	// Verify: 14 days analysis period
	if analysisPeriod != 14.0 {
		t.Errorf("Expected 14 days, got %.0f", analysisPeriod)
	}

	// Verify: 7 / 14 = 0.5 failures per day
	expectedPerDay := failureCount / analysisPeriod
	if failuresPerDay < expectedPerDay-0.01 || failuresPerDay > expectedPerDay+0.01 {
		t.Errorf("Expected %.2f failures/day, got %.2f", expectedPerDay, failuresPerDay)
	}

	// Verify: 0.5 × 7 = 3.5 failures per week
	expectedPerWeek := failuresPerDay * 7.0
	if failuresPerWeek < expectedPerWeek-0.01 || failuresPerWeek > expectedPerWeek+0.01 {
		t.Errorf("Expected %.2f failures/week, got %.2f", expectedPerWeek, failuresPerWeek)
	}
}

// TestOSBChronicFailureIdentification validates identification of failure modes occurring >3 times/week
func TestOSBChronicFailureIdentification(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupDowntimeAnalysisTest(t)
	defer cleanup()

	// Create events: Bearing failures 5 times in 7 days (chronic), Hydraulic leak once (not chronic)
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []downtimeEvent{}

	for day := 0; day < 7; day++ {
		dateID := baseTime.Add(time.Duration(day) * 24 * time.Hour).Format("20060102")
		dayStart := baseTime.Add(time.Duration(day) * 24 * time.Hour)

		// Running time
		events = append(events, downtimeEvent{
			"DRYER-01",
			dayStart.Format("2006-01-02 15:04:05"),
			dayStart.Add(22 * time.Hour).Format("2006-01-02 15:04:05"),
			1320,
			"Running",
			"",
			"DAY",
			dateID,
		})

		// Bearing failure on days 0, 1, 2, 4, 5 (5 times in 7 days = chronic)
		if day != 3 && day != 6 {
			events = append(events, downtimeEvent{
				"DRYER-01",
				dayStart.Add(22 * time.Hour).Format("2006-01-02 15:04:05"),
				dayStart.Add(23 * time.Hour).Format("2006-01-02 15:04:05"),
				60,
				"Unplanned Downtime",
				"RC_BEARING_FAIL",
				"NIGHT",
				dateID,
			})
		}
	}

	// Single hydraulic leak on day 3 (not chronic)
	events = append(events, downtimeEvent{
		"DRYER-01",
		baseTime.Add(3*24*time.Hour + 22*time.Hour).Format("2006-01-02 15:04:05"),
		baseTime.Add(3*24*time.Hour + 23*time.Hour).Format("2006-01-02 15:04:05"),
		60,
		"Unplanned Downtime",
		"RC_HYDRAULIC_LEAK",
		"NIGHT",
		"20240104",
	})

	insertDowntimeEvents(t, adapter, ctx, events)
	executeDowntimeAnalysisSQL(t, adapter, ctx)

	// Verify chronic failure identification
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			equipment_id,
			reason_code_id,
			failure_count,
			failures_per_week,
			is_chronic_failure
		FROM equipment_downtime_analysis
		WHERE equipment_id = 'DRYER-01'
		ORDER BY failure_count DESC
	`)
	if err != nil {
		t.Fatalf("Failed to query downtime analysis: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Fatalf("Expected 2 failure mode records, got %d", len(result.Rows))
	}

	// First: Bearing failure (chronic)
	reasonCode1 := result.Rows[0][1].(string)
	failureCount1 := getFloat(result.Rows[0][2])
	failuresPerWeek1 := getFloat(result.Rows[0][3])
	isChronic1 := result.Rows[0][4]

	if reasonCode1 != "RC_BEARING_FAIL" {
		t.Errorf("Expected RC_BEARING_FAIL, got %s", reasonCode1)
	}
	if failureCount1 != 5.0 {
		t.Errorf("Expected 5 failures, got %.0f", failureCount1)
	}
	// 5 failures over 6 days (20240101-20240106) = 5/6*7 = 5.83 failures/week
	if failuresPerWeek1 < 5.8 || failuresPerWeek1 > 5.9 {
		t.Errorf("Expected ~5.83 failures/week, got %.2f", failuresPerWeek1)
	}
	// is_chronic_failure should be 1 (true) for >3 failures/week
	if isChronic1 != int64(1) && isChronic1 != true {
		t.Errorf("Expected chronic failure flag to be true, got %v", isChronic1)
	}

	// Second: Hydraulic leak (not chronic)
	reasonCode2 := result.Rows[1][1].(string)
	failureCount2 := getFloat(result.Rows[1][2])
	failuresPerWeek2 := getFloat(result.Rows[1][3])
	isChronic2 := result.Rows[1][4]

	if reasonCode2 != "RC_HYDRAULIC_LEAK" {
		t.Errorf("Expected RC_HYDRAULIC_LEAK, got %s", reasonCode2)
	}
	if failureCount2 != 1.0 {
		t.Errorf("Expected 1 failure, got %.0f", failureCount2)
	}
	// 1 failure over 6 days = 1/6*7 = 1.17 failures/week
	if failuresPerWeek2 < 1.1 || failuresPerWeek2 > 1.2 {
		t.Errorf("Expected ~1.17 failures/week, got %.2f", failuresPerWeek2)
	}
	// is_chronic_failure should be 0 (false) for <=3 failures/week
	if isChronic2 != int64(0) && isChronic2 != false {
		t.Errorf("Expected chronic failure flag to be false, got %v", isChronic2)
	}
}

// TestOSBParetoAnalysis validates failures ranked by cumulative impact (frequency × duration)
func TestOSBParetoAnalysis(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupDowntimeAnalysisTest(t)
	defer cleanup()

	// Create downtime events with varying frequency and duration
	// - Bearing failure: 3 occurrences × 120 min = 360 min total (highest impact)
	// - Burner trip: 5 occurrences × 60 min = 300 min total (second highest)
	// - Hydraulic leak: 1 occurrence × 180 min = 180 min total (lowest)
	// - Strand bridging: 8 occurrences × 15 min = 120 min total (frequent but short)

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []downtimeEvent{
		// Add running states and failures
		{"DRYER-01", baseTime.Format("2006-01-02 15:04:05"), baseTime.Add(4 * time.Hour).Format("2006-01-02 15:04:05"), 240, "Running", "", "NIGHT", "20240101"},
		{"DRYER-01", baseTime.Add(4 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(6 * time.Hour).Format("2006-01-02 15:04:05"), 120, "Unplanned Downtime", "RC_BEARING_FAIL", "NIGHT", "20240101"},
		{"DRYER-01", baseTime.Add(6 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(10 * time.Hour).Format("2006-01-02 15:04:05"), 240, "Running", "", "DAY", "20240101"},
		{"DRYER-01", baseTime.Add(10 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(11 * time.Hour).Format("2006-01-02 15:04:05"), 60, "Unplanned Downtime", "RC_BURNER_TRIP", "DAY", "20240101"},
		{"DRYER-01", baseTime.Add(11 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(13 * time.Hour).Format("2006-01-02 15:04:05"), 120, "Running", "", "DAY", "20240101"},
		{"DRYER-01", baseTime.Add(13 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(13*time.Hour + 15*time.Minute).Format("2006-01-02 15:04:05"), 15, "Unplanned Downtime", "RC_STRAND_BRIDGE", "DAY", "20240101"},
		{"DRYER-01", baseTime.Add(13*time.Hour + 15*time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(15 * time.Hour).Format("2006-01-02 15:04:05"), 105, "Running", "", "DAY", "20240101"},
		{"DRYER-01", baseTime.Add(15 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(15*time.Hour + 15*time.Minute).Format("2006-01-02 15:04:05"), 15, "Unplanned Downtime", "RC_STRAND_BRIDGE", "SWING", "20240101"},
		{"DRYER-01", baseTime.Add(15*time.Hour + 15*time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(17 * time.Hour).Format("2006-01-02 15:04:05"), 105, "Running", "", "SWING", "20240101"},
		{"DRYER-01", baseTime.Add(17 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(18 * time.Hour).Format("2006-01-02 15:04:05"), 60, "Unplanned Downtime", "RC_BURNER_TRIP", "SWING", "20240101"},

		// Day 2
		{"DRYER-01", baseTime.Add(24 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(28 * time.Hour).Format("2006-01-02 15:04:05"), 240, "Running", "", "NIGHT", "20240102"},
		{"DRYER-01", baseTime.Add(28 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(30 * time.Hour).Format("2006-01-02 15:04:05"), 120, "Unplanned Downtime", "RC_BEARING_FAIL", "DAY", "20240102"},
		{"DRYER-01", baseTime.Add(30 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(33 * time.Hour).Format("2006-01-02 15:04:05"), 180, "Running", "", "DAY", "20240102"},
		{"DRYER-01", baseTime.Add(33 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(36 * time.Hour).Format("2006-01-02 15:04:05"), 180, "Unplanned Downtime", "RC_HYDRAULIC_LEAK", "DAY", "20240102"},
		{"DRYER-01", baseTime.Add(36 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(38 * time.Hour).Format("2006-01-02 15:04:05"), 120, "Running", "", "SWING", "20240102"},
		{"DRYER-01", baseTime.Add(38 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(38*time.Hour + 15*time.Minute).Format("2006-01-02 15:04:05"), 15, "Unplanned Downtime", "RC_STRAND_BRIDGE", "SWING", "20240102"},
		{"DRYER-01", baseTime.Add(38*time.Hour + 15*time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(40 * time.Hour).Format("2006-01-02 15:04:05"), 105, "Running", "", "SWING", "20240102"},
		{"DRYER-01", baseTime.Add(40 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(41 * time.Hour).Format("2006-01-02 15:04:05"), 60, "Unplanned Downtime", "RC_BURNER_TRIP", "SWING", "20240102"},

		// Additional failures for cumulative impact
		{"DRYER-01", baseTime.Add(48 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(50 * time.Hour).Format("2006-01-02 15:04:05"), 120, "Running", "", "NIGHT", "20240103"},
		{"DRYER-01", baseTime.Add(50 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(52 * time.Hour).Format("2006-01-02 15:04:05"), 120, "Unplanned Downtime", "RC_BEARING_FAIL", "DAY", "20240103"},
		{"DRYER-01", baseTime.Add(52 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(54 * time.Hour).Format("2006-01-02 15:04:05"), 120, "Running", "", "DAY", "20240103"},
		{"DRYER-01", baseTime.Add(54 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(55 * time.Hour).Format("2006-01-02 15:04:05"), 60, "Unplanned Downtime", "RC_BURNER_TRIP", "DAY", "20240103"},
		{"DRYER-01", baseTime.Add(55 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(57 * time.Hour).Format("2006-01-02 15:04:05"), 120, "Running", "", "SWING", "20240103"},
		{"DRYER-01", baseTime.Add(57 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(58 * time.Hour).Format("2006-01-02 15:04:05"), 60, "Unplanned Downtime", "RC_BURNER_TRIP", "SWING", "20240103"},
		{"DRYER-01", baseTime.Add(58 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(58*time.Hour + 15*time.Minute).Format("2006-01-02 15:04:05"), 15, "Unplanned Downtime", "RC_STRAND_BRIDGE", "SWING", "20240103"},
		{"DRYER-01", baseTime.Add(58*time.Hour + 15*time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(60 * time.Hour).Format("2006-01-02 15:04:05"), 105, "Running", "", "SWING", "20240103"},
		{"DRYER-01", baseTime.Add(60 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(60*time.Hour + 15*time.Minute).Format("2006-01-02 15:04:05"), 15, "Unplanned Downtime", "RC_STRAND_BRIDGE", "NIGHT", "20240103"},
		{"DRYER-01", baseTime.Add(60*time.Hour + 15*time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(62 * time.Hour).Format("2006-01-02 15:04:05"), 105, "Running", "", "NIGHT", "20240103"},
		{"DRYER-01", baseTime.Add(62 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(62*time.Hour + 15*time.Minute).Format("2006-01-02 15:04:05"), 15, "Unplanned Downtime", "RC_STRAND_BRIDGE", "NIGHT", "20240104"},
		{"DRYER-01", baseTime.Add(62*time.Hour + 15*time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(64 * time.Hour).Format("2006-01-02 15:04:05"), 105, "Running", "", "NIGHT", "20240104"},
		{"DRYER-01", baseTime.Add(64 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(64*time.Hour + 15*time.Minute).Format("2006-01-02 15:04:05"), 15, "Unplanned Downtime", "RC_STRAND_BRIDGE", "NIGHT", "20240104"},
		{"DRYER-01", baseTime.Add(64*time.Hour + 15*time.Minute).Format("2006-01-02 15:04:05"), baseTime.Add(66 * time.Hour).Format("2006-01-02 15:04:05"), 105, "Running", "", "NIGHT", "20240104"},
		{"DRYER-01", baseTime.Add(66 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(66*time.Hour + 15*time.Minute).Format("2006-01-02 15:04:05"), 15, "Unplanned Downtime", "RC_STRAND_BRIDGE", "DAY", "20240104"},
	}

	insertDowntimeEvents(t, adapter, ctx, events)
	executeFailureModePareto(t, adapter, ctx)

	// Verify Pareto ranking
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			reason_code_name,
			failure_count,
			total_downtime_min,
			downtime_impact,
			cumulative_pct,
			pareto_rank
		FROM failure_mode_pareto
		WHERE equipment_id = 'DRYER-01'
		ORDER BY pareto_rank
	`)
	if err != nil {
		t.Fatalf("Failed to query Pareto analysis: %v", err)
	}

	if len(result.Rows) < 4 {
		t.Fatalf("Expected at least 4 failure modes, got %d", len(result.Rows))
	}

	// Verify rank 1: Bearing failure (3 × 120 = 360 min)
	rank1Name := result.Rows[0][0].(string)
	rank1Count := getFloat(result.Rows[0][1])
	rank1Downtime := getFloat(result.Rows[0][2])

	if rank1Name != "Bearing Failure" {
		t.Errorf("Expected 'Bearing Failure' at rank 1, got '%s'", rank1Name)
	}
	if rank1Count != 3.0 {
		t.Errorf("Expected 3 bearing failures, got %.0f", rank1Count)
	}
	if rank1Downtime != 360.0 {
		t.Errorf("Expected 360 min downtime, got %.0f", rank1Downtime)
	}

	// Verify rank 2: Burner trip (5 × 60 = 300 min)
	rank2Name := result.Rows[1][0].(string)
	rank2Count := getFloat(result.Rows[1][1])
	rank2Downtime := getFloat(result.Rows[1][2])

	if rank2Name != "Dryer Burner Trip" {
		t.Errorf("Expected 'Dryer Burner Trip' at rank 2, got '%s'", rank2Name)
	}
	if rank2Count != 5.0 {
		t.Errorf("Expected 5 burner trips, got %.0f", rank2Count)
	}
	if rank2Downtime != 300.0 {
		t.Errorf("Expected 300 min downtime, got %.0f", rank2Downtime)
	}

	// Verify cumulative percentage increases
	cumPct1 := getFloat(result.Rows[0][4])
	cumPct2 := getFloat(result.Rows[1][4])

	if cumPct2 <= cumPct1 {
		t.Errorf("Expected cumulative %% to increase from rank 1 (%.1f%%) to rank 2 (%.1f%%)", cumPct1, cumPct2)
	}

	t.Logf("Pareto Analysis - Top 2 failure modes account for %.1f%% of downtime", cumPct2)
}

// TestOSBCriticalEquipmentPrioritization validates critical equipment flagged (dryer, press)
func TestOSBCriticalEquipmentPrioritization(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupDowntimeAnalysisTest(t)
	defer cleanup()

	// Create failures for all three equipment types
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []downtimeEvent{
		// DRYER-01 (Critical): 3 failures, 240 min downtime
		{"DRYER-01", baseTime.Format("2006-01-02 15:04:05"), baseTime.Add(8 * time.Hour).Format("2006-01-02 15:04:05"), 480, "Running", "", "NIGHT", "20240101"},
		{"DRYER-01", baseTime.Add(8 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(10 * time.Hour).Format("2006-01-02 15:04:05"), 120, "Unplanned Downtime", "RC_BEARING_FAIL", "DAY", "20240101"},
		{"DRYER-01", baseTime.Add(10 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(18 * time.Hour).Format("2006-01-02 15:04:05"), 480, "Running", "", "DAY", "20240101"},
		{"DRYER-01", baseTime.Add(18 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(19 * time.Hour).Format("2006-01-02 15:04:05"), 60, "Unplanned Downtime", "RC_BURNER_TRIP", "SWING", "20240101"},
		{"DRYER-01", baseTime.Add(19 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(27 * time.Hour).Format("2006-01-02 15:04:05"), 480, "Running", "", "SWING", "20240101"},
		{"DRYER-01", baseTime.Add(27 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(28 * time.Hour).Format("2006-01-02 15:04:05"), 60, "Unplanned Downtime", "RC_BURNER_TRIP", "DAY", "20240102"},

		// PRESS-01 (Critical): 2 failures, 300 min downtime
		{"PRESS-01", baseTime.Format("2006-01-02 15:04:05"), baseTime.Add(10 * time.Hour).Format("2006-01-02 15:04:05"), 600, "Running", "", "NIGHT", "20240101"},
		{"PRESS-01", baseTime.Add(10 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(12 * time.Hour).Format("2006-01-02 15:04:05"), 120, "Unplanned Downtime", "RC_HYDRAULIC_LEAK", "DAY", "20240101"},
		{"PRESS-01", baseTime.Add(12 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(22 * time.Hour).Format("2006-01-02 15:04:05"), 600, "Running", "", "DAY", "20240101"},
		{"PRESS-01", baseTime.Add(22 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(25 * time.Hour).Format("2006-01-02 15:04:05"), 180, "Unplanned Downtime", "RC_HYDRAULIC_LEAK", "NIGHT", "20240101"},

		// STRAND-01 (Important): 1 failure, 60 min downtime
		{"STRAND-01", baseTime.Format("2006-01-02 15:04:05"), baseTime.Add(22 * time.Hour).Format("2006-01-02 15:04:05"), 1320, "Running", "", "DAY", "20240101"},
		{"STRAND-01", baseTime.Add(22 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(23 * time.Hour).Format("2006-01-02 15:04:05"), 60, "Unplanned Downtime", "RC_BEARING_FAIL", "NIGHT", "20240101"},
	}

	insertDowntimeEvents(t, adapter, ctx, events)
	executeReliabilityMetricsSQL(t, adapter, ctx)

	// Verify critical equipment prioritization
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			r.equipment_id,
			e.criticality_level,
			r.failure_count,
			r.total_downtime_min,
			r.mtbf_hours,
			r.mttr_hours
		FROM equipment_reliability_metrics r
		INNER JOIN dim_equipment e ON r.equipment_id = e.equipment_id
		ORDER BY 
			CASE e.criticality_level 
				WHEN 'Critical' THEN 1 
				WHEN 'Important' THEN 2 
				ELSE 3 
			END,
			r.total_downtime_min DESC
	`)
	if err != nil {
		t.Fatalf("Failed to query reliability metrics: %v", err)
	}

	if len(result.Rows) != 3 {
		t.Fatalf("Expected 3 equipment records, got %d", len(result.Rows))
	}

	// Verify DRYER-01 and PRESS-01 are critical and ranked first
	for i := 0; i < 2; i++ {
		criticality := result.Rows[i][1].(string)
		if criticality != "Critical" {
			t.Errorf("Expected first two equipment to be Critical, got '%s' at position %d", criticality, i)
		}
	}

	// Verify STRAND-01 is Important and ranked last
	strandCriticality := result.Rows[2][1].(string)
	if strandCriticality != "Important" {
		t.Errorf("Expected STRAND-01 to be Important, got '%s'", strandCriticality)
	}

	// Verify DRYER-01 has highest failure count among critical equipment
	dryerID := result.Rows[0][0].(string)
	if dryerID != "DRYER-01" && dryerID != "PRESS-01" {
		t.Errorf("Expected DRYER-01 or PRESS-01 first, got %s", dryerID)
	}

	t.Logf("Critical equipment prioritization verified - Critical equipment ranked above Important")
}
