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

// setupAdvancedAnalyticsTest creates test database with equipment, downtime, and reliability data
func setupAdvancedAnalyticsTest(t *testing.T) (*sqlite.SQLiteAdapter, context.Context, func()) {
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
			production_area TEXT NOT NULL,
			rated_capacity_units_hr REAL NOT NULL,
			criticality_level TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create dim_equipment: %v", err)
	}

	// Insert test equipment
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO dim_equipment VALUES 
		('DRYER-01', 'Primary Rotary Dryer', 'Dryer', 'AREA_DRYING', 10.0, 'Critical'),
		('PRESS-01', 'Continuous Press', 'Press', 'AREA_PRESSING', 18.0, 'Critical'),
		('STRAND-01', 'Strander Line 1', 'Strander', 'AREA_STRANDING', 6.0, 'Important'),
		('FORMER-01', 'Mat Former Station', 'Former', 'AREA_FORMING', 8.0, 'Important'),
		('SAW-01', 'Panel Saw Station', 'Saw', 'AREA_FINISHING', 20.0, 'Standard')
	`)
	if err != nil {
		t.Fatalf("Failed to insert equipment: %v", err)
	}

	// Create equipment_reliability_metrics table
	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE equipment_reliability_metrics (
			equipment_id TEXT PRIMARY KEY,
			equipment_name TEXT NOT NULL,
			total_operating_time_min REAL NOT NULL,
			total_downtime_min REAL NOT NULL,
			failure_count INTEGER NOT NULL,
			mtbf_hours REAL,
			mttr_hours REAL,
			analysis_period_days INTEGER NOT NULL,
			failures_per_day REAL NOT NULL,
			failures_per_week REAL NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create equipment_reliability_metrics: %v", err)
	}

	// Create equipment_downtime_analysis table
	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE equipment_downtime_analysis (
			equipment_id TEXT NOT NULL,
			equipment_name TEXT NOT NULL,
			reason_code_id TEXT NOT NULL,
			reason_code_name TEXT NOT NULL,
			failure_count INTEGER NOT NULL,
			total_downtime_min REAL NOT NULL,
			avg_downtime_per_event_min REAL NOT NULL,
			min_downtime_min REAL NOT NULL,
			max_downtime_min REAL NOT NULL,
			analysis_period_days INTEGER NOT NULL,
			failures_per_day REAL NOT NULL,
			failures_per_week REAL NOT NULL,
			is_chronic_failure INTEGER NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create equipment_downtime_analysis: %v", err)
	}

	// Create dim_shift table
	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE dim_shift (
			shift_id TEXT PRIMARY KEY,
			shift_name TEXT NOT NULL,
			start_hour INTEGER NOT NULL,
			end_hour INTEGER NOT NULL,
			shift_hours REAL NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create dim_shift: %v", err)
	}

	// Insert shifts
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO dim_shift VALUES 
		('DAY', 'Day Shift', 6, 14, 8.0),
		('SWING', 'Swing Shift', 14, 22, 8.0),
		('NIGHT', 'Night Shift', 22, 6, 8.0)
	`)
	if err != nil {
		t.Fatalf("Failed to insert shifts: %v", err)
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
			date_actual DATE NOT NULL,
			year INTEGER NOT NULL,
			month INTEGER NOT NULL,
			week_of_year INTEGER NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create dim_date: %v", err)
	}

	// Insert test dates (30 days for trending)
	baseDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 30; i++ {
		date := baseDate.Add(time.Duration(i) * 24 * time.Hour)
		dateID := date.Format("20060102")
		_, week := date.ISOWeek()
		sql := fmt.Sprintf(`INSERT INTO dim_date VALUES ('%s', '%s', %d, %d, %d)`,
			dateID, date.Format("2006-01-02"), date.Year(), int(date.Month()), week)
		if err := adapter.ExecuteDDL(ctx, sql); err != nil {
			t.Fatalf("Failed to insert date: %v", err)
		}
	}

	return adapter, ctx, cleanup
}

// executeBadActorSQL loads and executes the bad_actor_prioritization.sql model
func executeBadActorSQL(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context) {
	repoRoot := getRepoRoot(t)
	modelPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "models", "analytics", "bad_actor_prioritization.sql")

	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read bad_actor_prioritization.sql: %v", err)
	}

	contentStr := removeConfigCallsStaging(string(content))

	templateEngine := template.New()
	tmpl, err := templateEngine.Parse("bad_actor_prioritization", contentStr)
	if err != nil {
		t.Fatalf("Failed to parse template: %v", err)
	}

	ctx2 := template.NewContext(template.WithCurrentModel("bad_actor_prioritization"))
	rendered, err := template.Render(tmpl, ctx2, nil)
	if err != nil {
		t.Fatalf("Failed to render template: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, "DROP TABLE IF EXISTS bad_actor_prioritization")
	if err != nil {
		t.Fatalf("Failed to drop existing bad_actor_prioritization: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, "CREATE TABLE bad_actor_prioritization AS "+rendered)
	if err != nil {
		t.Fatalf("Failed to execute bad_actor_prioritization model: %v", err)
	}
}

// executeShiftPerformanceSQL loads and executes the shift_performance_comparison.sql model
func executeShiftPerformanceSQL(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context) {
	repoRoot := getRepoRoot(t)
	modelPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "models", "analytics", "shift_performance_comparison.sql")

	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read shift_performance_comparison.sql: %v", err)
	}

	contentStr := removeConfigCallsStaging(string(content))

	templateEngine := template.New()
	tmpl, err := templateEngine.Parse("shift_performance_comparison", contentStr)
	if err != nil {
		t.Fatalf("Failed to parse template: %v", err)
	}

	ctx2 := template.NewContext(template.WithCurrentModel("shift_performance_comparison"))
	rendered, err := template.Render(tmpl, ctx2, nil)
	if err != nil {
		t.Fatalf("Failed to render template: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, "DROP TABLE IF EXISTS shift_performance_comparison")
	if err != nil {
		t.Fatalf("Failed to drop existing shift_performance_comparison: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, "CREATE TABLE shift_performance_comparison AS "+rendered)
	if err != nil {
		t.Fatalf("Failed to execute shift_performance_comparison model: %v", err)
	}
}

// TestOSBBadActorScoring validates equipment scored by impact (downtime × frequency × criticality)
func TestOSBBadActorScoring(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupAdvancedAnalyticsTest(t)
	defer cleanup()

	// Scenario: DRYER-01 (Critical) has highest impact due to frequency × severity × criticality
	// - DRYER-01: 5 failures, 720 min downtime, Critical (3×) → impact = 720 × 5 × 3 / 60 = 180
	// - PRESS-01: 2 failures, 360 min downtime, Critical (3×) → impact = 360 × 2 × 3 / 60 = 36
	// - STRAND-01: 3 failures, 180 min downtime, Important (2×) → impact = 180 × 3 × 2 / 60 = 18

	// Insert reliability metrics
	err := adapter.ExecuteDDL(ctx, `
		INSERT INTO equipment_reliability_metrics VALUES 
		('DRYER-01', 'Primary Rotary Dryer', 14400, 720, 5, 48.0, 2.4, 14, 0.36, 2.5),
		('PRESS-01', 'Continuous Press', 16800, 360, 2, 140.0, 3.0, 14, 0.14, 1.0),
		('STRAND-01', 'Strander Line 1', 18000, 180, 3, 100.0, 1.0, 14, 0.21, 1.5)
	`)
	if err != nil {
		t.Fatalf("Failed to insert reliability metrics: %v", err)
	}

	// Insert downtime analysis
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO equipment_downtime_analysis VALUES 
		('DRYER-01', 'Primary Rotary Dryer', 'RC_BEARING_FAIL', 'Bearing Failure', 3, 360, 120, 90, 150, 14, 0.21, 1.5, 0),
		('DRYER-01', 'Primary Rotary Dryer', 'RC_BURNER_TRIP', 'Burner Trip', 2, 360, 180, 150, 210, 14, 0.14, 1.0, 0),
		('PRESS-01', 'Continuous Press', 'RC_HYDRAULIC_LEAK', 'Hydraulic Leak', 2, 360, 180, 150, 210, 14, 0.14, 1.0, 0),
		('STRAND-01', 'Strander Line 1', 'RC_BEARING_FAIL', 'Bearing Failure', 3, 180, 60, 45, 75, 14, 0.21, 1.5, 0)
	`)
	if err != nil {
		t.Fatalf("Failed to insert downtime analysis: %v", err)
	}

	executeBadActorSQL(t, adapter, ctx)

	// Verify bad actor prioritization
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			equipment_id,
			equipment_name,
			criticality_level,
			total_failures,
			total_downtime_hours,
			mtbf_hours,
			mttr_hours,
			impact_score,
			priority_rank
		FROM bad_actor_prioritization
		ORDER BY priority_rank
		LIMIT 3
	`)
	if err != nil {
		t.Fatalf("Failed to query bad actor prioritization: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected bad actor results, got none")
	}

	// Verify DRYER-01 is ranked #1 (highest impact)
	rank1Equipment := result.Rows[0][0].(string)
	rank1Impact := getFloat(result.Rows[0][7])
	rank1Priority := getFloat(result.Rows[0][8])

	if rank1Equipment != "DRYER-01" {
		t.Errorf("Expected DRYER-01 as top bad actor, got %s", rank1Equipment)
	}

	if rank1Priority != 1.0 {
		t.Errorf("Expected priority rank 1, got %.0f", rank1Priority)
	}

	// Verify impact score is highest for DRYER-01
	if rank1Impact < 100.0 {
		t.Errorf("Expected high impact score for DRYER-01, got %.1f", rank1Impact)
	}

	t.Logf("Top bad actor: %s (impact score: %.1f, MTBF: %.1fh, MTTR: %.1fh)",
		rank1Equipment, rank1Impact, getFloat(result.Rows[0][5]), getFloat(result.Rows[0][6]))
}

// TestOSBShiftPerformanceComparison validates OEE and downtime metrics compared across shifts
func TestOSBShiftPerformanceComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupAdvancedAnalyticsTest(t)
	defer cleanup()

	// Scenario: Day shift has better performance than Swing shift
	// Day shift: 7/8 hours running (87.5% availability)
	// Swing shift: 6/8 hours running (75% availability)
	// Night shift: 7.5/8 hours running (93.75% availability)

	baseTime := time.Date(2024, 1, 1, 6, 0, 0, 0, time.UTC)

	// Day shift: 7 hours running, 1 hour downtime
	insertStateEvents(t, adapter, ctx, "PRESS-01",
		baseTime.Format("2006-01-02 15:04:05"),
		baseTime.Add(7*time.Hour).Format("2006-01-02 15:04:05"),
		420, "Running", "DAY", "20240101")
	insertStateEvents(t, adapter, ctx, "PRESS-01",
		baseTime.Add(7*time.Hour).Format("2006-01-02 15:04:05"),
		baseTime.Add(8*time.Hour).Format("2006-01-02 15:04:05"),
		60, "Unplanned Downtime", "DAY", "20240101")

	// Swing shift: 6 hours running, 2 hours downtime
	insertStateEvents(t, adapter, ctx, "PRESS-01",
		baseTime.Add(8*time.Hour).Format("2006-01-02 15:04:05"),
		baseTime.Add(14*time.Hour).Format("2006-01-02 15:04:05"),
		360, "Running", "SWING", "20240101")
	insertStateEvents(t, adapter, ctx, "PRESS-01",
		baseTime.Add(14*time.Hour).Format("2006-01-02 15:04:05"),
		baseTime.Add(16*time.Hour).Format("2006-01-02 15:04:05"),
		120, "Unplanned Downtime", "SWING", "20240101")

	// Night shift: 7.5 hours running, 0.5 hour downtime
	insertStateEvents(t, adapter, ctx, "PRESS-01",
		baseTime.Add(16*time.Hour).Format("2006-01-02 15:04:05"),
		baseTime.Add(23*time.Hour+30*time.Minute).Format("2006-01-02 15:04:05"),
		450, "Running", "NIGHT", "20240101")
	insertStateEvents(t, adapter, ctx, "PRESS-01",
		baseTime.Add(23*time.Hour+30*time.Minute).Format("2006-01-02 15:04:05"),
		baseTime.Add(24*time.Hour).Format("2006-01-02 15:04:05"),
		30, "Unplanned Downtime", "NIGHT", "20240101")

	executeShiftPerformanceSQL(t, adapter, ctx)

	// Verify shift performance comparison
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			shift_id,
			shift_name,
			total_operating_time_hours,
			total_downtime_hours,
			availability_pct,
			shift_rank
		FROM shift_performance_comparison
		ORDER BY shift_rank
	`)
	if err != nil {
		t.Fatalf("Failed to query shift performance: %v", err)
	}

	if len(result.Rows) != 3 {
		t.Fatalf("Expected 3 shifts, got %d", len(result.Rows))
	}

	// Verify Night shift is best (#1)
	bestShift := result.Rows[0][1].(string)
	bestAvailability := getFloat(result.Rows[0][4])

	if bestShift != "Night Shift" {
		t.Errorf("Expected Night Shift as best, got %s", bestShift)
	}

	if bestAvailability < 90.0 {
		t.Errorf("Expected Night shift availability >90%%, got %.1f%%", bestAvailability)
	}

	// Verify Swing shift is worst (#3)
	worstShift := result.Rows[2][1].(string)
	worstAvailability := getFloat(result.Rows[2][4])

	if worstShift != "Swing Shift" {
		t.Errorf("Expected Swing Shift as worst, got %s", worstShift)
	}

	if worstAvailability > 80.0 {
		t.Errorf("Expected Swing shift availability <80%%, got %.1f%%", worstAvailability)
	}

	t.Logf("Best shift: %s (%.1f%% availability), Worst shift: %s (%.1f%% availability)",
		bestShift, bestAvailability, worstShift, worstAvailability)
}

// TestOSBTrendAnalysis validates trending of key metrics (MTBF, downtime) over time
func TestOSBTrendAnalysis(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupAdvancedAnalyticsTest(t)
	defer cleanup()

	// Scenario: MTBF improving over 4 weeks (reliability improvement program working)
	// Week 1: MTBF 40h, Week 2: 50h, Week 3: 60h, Week 4: 70h (positive trend)

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Week 1: More frequent failures (poor MTBF)
	for day := 0; day < 7; day++ {
		date := baseTime.Add(time.Duration(day) * 24 * time.Hour)
		dateID := date.Format("20060102")

		if day%2 == 0 { // Failure every 2 days
			insertStateEvents(t, adapter, ctx, "DRYER-01",
				date.Format("2006-01-02 06:00:00"),
				date.Add(2*time.Hour).Format("2006-01-02 15:04:05"),
				120, "Unplanned Downtime", "DAY", dateID)
			insertStateEvents(t, adapter, ctx, "DRYER-01",
				date.Add(2*time.Hour).Format("2006-01-02 15:04:05"),
				date.Add(24*time.Hour).Format("2006-01-02 15:04:05"),
				1320, "Running", "DAY", dateID)
		} else {
			insertStateEvents(t, adapter, ctx, "DRYER-01",
				date.Format("2006-01-02 06:00:00"),
				date.Add(24*time.Hour).Format("2006-01-02 15:04:05"),
				1440, "Running", "DAY", dateID)
		}
	}

	// Week 2-4: Fewer failures (improving MTBF)
	for day := 7; day < 28; day++ {
		date := baseTime.Add(time.Duration(day) * 24 * time.Hour)
		dateID := date.Format("20060102")

		if day%4 == 0 { // Failure every 4 days (less frequent)
			insertStateEvents(t, adapter, ctx, "DRYER-01",
				date.Format("2006-01-02 06:00:00"),
				date.Add(1*time.Hour).Format("2006-01-02 15:04:05"),
				60, "Unplanned Downtime", "DAY", dateID)
			insertStateEvents(t, adapter, ctx, "DRYER-01",
				date.Add(1*time.Hour).Format("2006-01-02 15:04:05"),
				date.Add(24*time.Hour).Format("2006-01-02 15:04:05"),
				1380, "Running", "DAY", dateID)
		} else {
			insertStateEvents(t, adapter, ctx, "DRYER-01",
				date.Format("2006-01-02 06:00:00"),
				date.Add(24*time.Hour).Format("2006-01-02 15:04:05"),
				1440, "Running", "DAY", dateID)
		}
	}

	// Query weekly aggregates to validate trend
	result, err := adapter.ExecuteQuery(ctx, `
		WITH weekly_metrics AS (
			SELECT 
				d.week_of_year,
				SUM(CASE WHEN s.machine_state = 'Running' THEN s.state_duration_min ELSE 0 END) AS operating_time_min,
				SUM(CASE WHEN s.machine_state = 'Unplanned Downtime' THEN 1 ELSE 0 END) AS failure_count,
				SUM(CASE WHEN s.machine_state = 'Unplanned Downtime' THEN s.state_duration_min ELSE 0 END) AS downtime_min
			FROM stg_equipment_state_history s
			INNER JOIN dim_date d ON s.date_id = d.date_id
			WHERE s.equipment_id = 'DRYER-01'
			GROUP BY d.week_of_year
		)
		SELECT 
			week_of_year,
			operating_time_min / 60.0 AS operating_hours,
			failure_count,
			downtime_min / 60.0 AS downtime_hours,
			CASE 
				WHEN failure_count > 0 
				THEN operating_time_min / 60.0 / failure_count 
				ELSE NULL 
			END AS mtbf_hours
		FROM weekly_metrics
		ORDER BY week_of_year
	`)
	if err != nil {
		t.Fatalf("Failed to query weekly trends: %v", err)
	}

	if len(result.Rows) < 4 {
		t.Fatalf("Expected at least 4 weeks of data, got %d", len(result.Rows))
	}

	// Verify MTBF is improving over time
	week1MTBF := getFloat(result.Rows[0][4])
	week4MTBF := getFloat(result.Rows[len(result.Rows)-1][4])

	if week4MTBF <= week1MTBF {
		t.Errorf("Expected MTBF to improve from Week 1 (%.1fh) to Week 4 (%.1fh), but it didn't",
			week1MTBF, week4MTBF)
	}

	// Verify downtime is decreasing
	week1Downtime := getFloat(result.Rows[0][3])
	week4Downtime := getFloat(result.Rows[len(result.Rows)-1][3])

	if week4Downtime >= week1Downtime {
		t.Errorf("Expected downtime to decrease from Week 1 (%.1fh) to Week 4 (%.1fh), but it didn't",
			week1Downtime, week4Downtime)
	}

	t.Logf("Trend analysis: MTBF improved from %.1fh (Week 1) to %.1fh (Week 4), downtime reduced from %.1fh to %.1fh",
		week1MTBF, week4MTBF, week1Downtime, week4Downtime)
}

// TestOSBQualityIssueCorrelation validates quality defects correlated with equipment states and process parameters
func TestOSBQualityIssueCorrelation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupAdvancedAnalyticsTest(t)
	defer cleanup()

	// Create quality defects table
	err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE stg_quality_defects (
			defect_id TEXT PRIMARY KEY,
			timestamp TEXT NOT NULL,
			equipment_id TEXT NOT NULL,
			defect_type TEXT NOT NULL,
			defect_severity TEXT NOT NULL,
			shift_id TEXT NOT NULL,
			date_id TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create quality defects table: %v", err)
	}

	// Create process parameters table
	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE stg_process_parameters (
			timestamp TEXT NOT NULL,
			equipment_id TEXT NOT NULL,
			parameter_name TEXT NOT NULL,
			parameter_value REAL NOT NULL,
			date_id TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create process parameters table: %v", err)
	}

	// Scenario: Thickness defects correlate with PRESS-01 performance parameters
	// When press temperature is low (<150C), more thickness defects occur

	baseTime := time.Date(2024, 1, 1, 6, 0, 0, 0, time.UTC)

	// Insert defects during low temperature periods
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO stg_quality_defects VALUES 
		('DEF-001', '2024-01-01 08:00:00', 'PRESS-01', 'Thickness Deviation', 'Major', 'DAY', '20240101'),
		('DEF-002', '2024-01-01 09:00:00', 'PRESS-01', 'Thickness Deviation', 'Major', 'DAY', '20240101'),
		('DEF-003', '2024-01-01 10:00:00', 'PRESS-01', 'Delamination', 'Minor', 'DAY', '20240101'),
		('DEF-004', '2024-01-02 08:00:00', 'PRESS-01', 'Thickness Deviation', 'Major', 'DAY', '20240102')
	`)
	if err != nil {
		t.Fatalf("Failed to insert quality defects: %v", err)
	}

	// Insert process parameters (low temp during defect periods)
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO stg_process_parameters VALUES 
		('2024-01-01 08:00:00', 'PRESS-01', 'press_temperature_c', 145.0, '20240101'),
		('2024-01-01 09:00:00', 'PRESS-01', 'press_temperature_c', 142.0, '20240101'),
		('2024-01-01 10:00:00', 'PRESS-01', 'press_temperature_c', 148.0, '20240101'),
		('2024-01-01 14:00:00', 'PRESS-01', 'press_temperature_c', 165.0, '20240101'),
		('2024-01-02 08:00:00', 'PRESS-01', 'press_temperature_c', 143.0, '20240102'),
		('2024-01-02 14:00:00', 'PRESS-01', 'press_temperature_c', 168.0, '20240102')
	`)
	if err != nil {
		t.Fatalf("Failed to insert process parameters: %v", err)
	}

	// Insert equipment state during defect periods
	insertStateEvents(t, adapter, ctx, "PRESS-01",
		baseTime.Format("2006-01-02 06:00:00"),
		baseTime.Add(8*time.Hour).Format("2006-01-02 15:04:05"),
		480, "Running", "DAY", "20240101")

	// Query quality correlation
	result, err := adapter.ExecuteQuery(ctx, `
		WITH defect_windows AS (
			SELECT 
				d.defect_type,
				d.equipment_id,
				d.timestamp,
				d.shift_id
			FROM stg_quality_defects d
		),
		parameter_at_defect AS (
			SELECT 
				w.defect_type,
				w.equipment_id,
				w.timestamp AS defect_timestamp,
				p.parameter_name,
				p.parameter_value,
				w.shift_id
			FROM defect_windows w
			INNER JOIN stg_process_parameters p 
				ON w.equipment_id = p.equipment_id 
				AND w.timestamp = p.timestamp
		)
		SELECT 
			defect_type,
			equipment_id,
			parameter_name,
			COUNT(*) AS defect_count,
			AVG(parameter_value) AS avg_parameter_value,
			MIN(parameter_value) AS min_parameter_value,
			MAX(parameter_value) AS max_parameter_value
		FROM parameter_at_defect
		WHERE parameter_name = 'press_temperature_c'
		GROUP BY defect_type, equipment_id, parameter_name
		ORDER BY defect_count DESC
	`)
	if err != nil {
		t.Fatalf("Failed to query quality correlation: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected quality correlation results, got none")
	}

	// Verify thickness defects correlate with low temperature
	defectType := result.Rows[0][0].(string)
	defectCount := getFloat(result.Rows[0][3])
	avgTemp := getFloat(result.Rows[0][4])

	if defectType != "Thickness Deviation" {
		t.Errorf("Expected 'Thickness Deviation' as most common defect, got %s", defectType)
	}

	if defectCount < 3.0 {
		t.Errorf("Expected at least 3 thickness defects, got %.0f", defectCount)
	}

	if avgTemp > 150.0 {
		t.Errorf("Expected average temperature <150C during defects, got %.1fC", avgTemp)
	}

	t.Logf("Quality correlation: %s defects (n=%.0f) correlated with low press temperature (avg=%.1fC)",
		defectType, defectCount, avgTemp)
}

// TestOSBMaintenanceEffectivenessAnalysis validates PM vs breakdown ratio and cost analysis
func TestOSBMaintenanceEffectivenessAnalysis(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupAdvancedAnalyticsTest(t)
	defer cleanup()

	// Create maintenance work orders table
	err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE stg_maintenance_work_orders (
			work_order_id TEXT PRIMARY KEY,
			equipment_id TEXT NOT NULL,
			work_order_type TEXT NOT NULL,
			scheduled_date TEXT,
			actual_date TEXT NOT NULL,
			duration_hours REAL NOT NULL,
			cost_usd REAL NOT NULL,
			date_id TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create work orders table: %v", err)
	}

	// Scenario: DRYER-01 has poor PM adherence and high breakdown costs
	// 2 PM events (total $2000), 5 breakdown events (total $15000)
	// PM effectiveness ratio: 2/(2+5) = 28.6% (target >50%)

	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO stg_maintenance_work_orders VALUES 
		('WO-PM-001', 'DRYER-01', 'Planned Maintenance', '2024-01-05', '2024-01-05', 2.0, 800, '20240105'),
		('WO-PM-002', 'DRYER-01', 'Planned Maintenance', '2024-01-15', '2024-01-15', 3.0, 1200, '20240115'),
		('WO-BD-001', 'DRYER-01', 'Breakdown', NULL, '2024-01-03', 4.0, 3500, '20240103'),
		('WO-BD-002', 'DRYER-01', 'Breakdown', NULL, '2024-01-08', 2.5, 2800, '20240108'),
		('WO-BD-003', 'DRYER-01', 'Breakdown', NULL, '2024-01-12', 3.0, 3200, '20240112'),
		('WO-BD-004', 'DRYER-01', 'Breakdown', NULL, '2024-01-18', 2.0, 2700, '20240118'),
		('WO-BD-005', 'DRYER-01', 'Breakdown', NULL, '2024-01-22', 3.5, 2800, '20240122'),
		('WO-PM-003', 'PRESS-01', 'Planned Maintenance', '2024-01-10', '2024-01-10', 2.5, 1000, '20240110'),
		('WO-PM-004', 'PRESS-01', 'Planned Maintenance', '2024-01-20', '2024-01-20', 2.0, 900, '20240120'),
		('WO-BD-006', 'PRESS-01', 'Breakdown', NULL, '2024-01-15', 1.5, 1500, '20240115')
	`)
	if err != nil {
		t.Fatalf("Failed to insert work orders: %v", err)
	}

	// Query maintenance effectiveness
	result, err := adapter.ExecuteQuery(ctx, `
		WITH maintenance_summary AS (
			SELECT 
				equipment_id,
				COUNT(CASE WHEN work_order_type = 'Planned Maintenance' THEN 1 END) AS pm_count,
				COUNT(CASE WHEN work_order_type = 'Breakdown' THEN 1 END) AS breakdown_count,
				SUM(CASE WHEN work_order_type = 'Planned Maintenance' THEN cost_usd ELSE 0 END) AS pm_cost,
				SUM(CASE WHEN work_order_type = 'Breakdown' THEN cost_usd ELSE 0 END) AS breakdown_cost,
				SUM(CASE WHEN work_order_type = 'Planned Maintenance' THEN duration_hours ELSE 0 END) AS pm_hours,
				SUM(CASE WHEN work_order_type = 'Breakdown' THEN duration_hours ELSE 0 END) AS breakdown_hours
			FROM stg_maintenance_work_orders
			GROUP BY equipment_id
		)
		SELECT 
			equipment_id,
			pm_count,
			breakdown_count,
			pm_cost,
			breakdown_cost,
			pm_hours,
			breakdown_hours,
			ROUND(100.0 * pm_count / (pm_count + breakdown_count), 1) AS pm_ratio_pct,
			ROUND(breakdown_cost / NULLIF(pm_cost, 0), 2) AS breakdown_to_pm_cost_ratio
		FROM maintenance_summary
		ORDER BY pm_ratio_pct ASC
	`)
	if err != nil {
		t.Fatalf("Failed to query maintenance effectiveness: %v", err)
	}

	if len(result.Rows) < 2 {
		t.Fatalf("Expected at least 2 equipment records, got %d", len(result.Rows))
	}

	// Verify DRYER-01 has poor PM ratio (<50%)
	equipment := result.Rows[0][0].(string)
	pmRatio := getFloat(result.Rows[0][7])
	breakdownCost := getFloat(result.Rows[0][4])
	pmCost := getFloat(result.Rows[0][3])
	costRatio := getFloat(result.Rows[0][8])

	if equipment != "DRYER-01" {
		t.Errorf("Expected DRYER-01 to have worst PM ratio, got %s", equipment)
	}

	if pmRatio > 50.0 {
		t.Errorf("Expected PM ratio <50%% for DRYER-01, got %.1f%%", pmRatio)
	}

	if costRatio < 5.0 {
		t.Errorf("Expected breakdown cost >> PM cost (ratio >5), got %.2f", costRatio)
	}

	t.Logf("Maintenance effectiveness: %s has %.1f%% PM ratio, breakdown costs ($%.0f) are %.1fx PM costs ($%.0f)",
		equipment, pmRatio, breakdownCost, costRatio, pmCost)
}

// TestOSBImprovementROICalculation validates ROI for proposed improvements (MTBF increases, capacity additions)
func TestOSBImprovementROICalculation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupAdvancedAnalyticsTest(t)
	defer cleanup()

	// Insert baseline reliability metrics
	err := adapter.ExecuteDDL(ctx, `
		INSERT INTO equipment_reliability_metrics VALUES 
		('DRYER-01', 'Primary Rotary Dryer', 14400, 720, 5, 48.0, 2.4, 14, 0.36, 2.5)
	`)
	if err != nil {
		t.Fatalf("Failed to insert reliability metrics: %v", err)
	}

	// Scenario: Calculate ROI for improving DRYER-01 MTBF from 48h to 96h (doubling)
	// Current: 5 failures per 14 days (10 hrs downtime each) = 50 hrs/2 weeks = 25 hrs/week = 1,300 hrs/year
	// Improved: 2.5 failures per 14 days = 25 hrs/2 weeks = 12.5 hrs/week = 650 hrs/year
	// Avoided downtime: 650 hrs/year × $1000/hr revenue = $650,000/year
	// Investment: $100,000 (better bearings, proactive maintenance program)
	// ROI: ($650,000 - $100,000) / $100,000 = 550%, Payback: 1.8 months

	// Query ROI calculation
	result, err := adapter.ExecuteQuery(ctx, `
		WITH baseline_metrics AS (
			SELECT 
				equipment_id,
				equipment_name,
				mtbf_hours AS current_mtbf,
				mttr_hours,
				failure_count,
				total_downtime_min / 60.0 AS total_downtime_hours,
				analysis_period_days
			FROM equipment_reliability_metrics
			WHERE equipment_id = 'DRYER-01'
		),
		improvement_scenarios AS (
			SELECT 
				equipment_id,
				equipment_name,
				current_mtbf,
				current_mtbf * 2.0 AS target_mtbf,
				total_downtime_hours,
				analysis_period_days,
				-- Calculate annual downtime reduction
				(total_downtime_hours / analysis_period_days * 365) AS current_annual_downtime_hours,
				(total_downtime_hours / 2.0 / analysis_period_days * 365) AS improved_annual_downtime_hours
			FROM baseline_metrics
		),
		roi_calculation AS (
			SELECT 
				equipment_id,
				equipment_name,
				current_mtbf,
				target_mtbf,
				current_annual_downtime_hours,
				improved_annual_downtime_hours,
				current_annual_downtime_hours - improved_annual_downtime_hours AS avoided_downtime_hours,
				(current_annual_downtime_hours - improved_annual_downtime_hours) * 1000.0 AS annual_revenue_recovery,
				100000.0 AS improvement_investment,
				ROUND(((current_annual_downtime_hours - improved_annual_downtime_hours) * 1000.0 - 100000.0) / 100000.0 * 100.0, 1) AS roi_pct,
				ROUND(100000.0 / ((current_annual_downtime_hours - improved_annual_downtime_hours) * 1000.0 / 12.0), 1) AS payback_months
			FROM improvement_scenarios
		)
		SELECT 
			equipment_id,
			equipment_name,
			current_mtbf,
			target_mtbf,
			avoided_downtime_hours,
			annual_revenue_recovery,
			improvement_investment,
			roi_pct,
			payback_months
		FROM roi_calculation
	`)
	if err != nil {
		t.Fatalf("Failed to query ROI calculation: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected ROI calculation results, got none")
	}

	// Verify ROI calculation
	currentMTBF := getFloat(result.Rows[0][2])
	targetMTBF := getFloat(result.Rows[0][3])
	avoidedDowntime := getFloat(result.Rows[0][4])
	revenueRecovery := getFloat(result.Rows[0][5])
	investment := getFloat(result.Rows[0][6])
	roiPct := getFloat(result.Rows[0][7])
	paybackMonths := getFloat(result.Rows[0][8])

	if targetMTBF != currentMTBF*2.0 {
		t.Errorf("Expected target MTBF to be 2× current (%.1fh × 2 = %.1fh), got %.1fh",
			currentMTBF, currentMTBF*2.0, targetMTBF)
	}

	if avoidedDowntime < 100.0 {
		t.Errorf("Expected avoided downtime >100 hours/year, got %.1f", avoidedDowntime)
	}

	if roiPct < 50.0 {
		t.Errorf("Expected ROI >50%%, got %.1f%%", roiPct)
	}

	if paybackMonths > 12.0 {
		t.Errorf("Expected payback <12 months, got %.1f", paybackMonths)
	}

	t.Logf("Improvement ROI: MTBF %.1fh→%.1fh avoids %.0f hrs/year downtime, recovers $%.0f/year, %.1f%% ROI, %.1f month payback on $%.0f investment",
		currentMTBF, targetMTBF, avoidedDowntime, revenueRecovery, roiPct, paybackMonths, investment)
}
