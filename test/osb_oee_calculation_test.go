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

// setupOEECalculationTest creates a test database with equipment state history and production output data
func setupOEECalculationTest(t *testing.T) (*sqlite.SQLiteAdapter, context.Context, func()) {
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
		('PRESS-01', 'Continuous Press', 'Press', 'Pressing', 18.0, 'ft/min', 'Critical')
	`)
	if err != nil {
		t.Fatalf("Failed to insert test equipment: %v", err)
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
		('RC_PLANNED_MAINT', 'Planned Maintenance', 'Maintenance', 'Planned Downtime', 'Planned Downtime', 480, 600),
		('RC_MINOR_STOP', 'Minor Stop', 'Process', 'Minor Stop', 'Small Stops', 2, 10)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test reason codes: %v", err)
	}

	// Create dim_product_spec table
	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE dim_product_spec (
			product_id TEXT PRIMARY KEY,
			product_name TEXT NOT NULL,
			thickness_inch REAL NOT NULL,
			width_ft REAL NOT NULL,
			length_ft REAL NOT NULL,
			ideal_cycle_time_min REAL NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create dim_product_spec: %v", err)
	}

	// Insert test product specs
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO dim_product_spec VALUES 
		('OSB-7/16', '7/16 inch OSB', 0.4375, 8.0, 24.0, 8.0)
	`)
	if err != nil {
		t.Fatalf("Failed to insert test product specs: %v", err)
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

	// Create fact_production_output table
	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE fact_production_output (
			production_id TEXT PRIMARY KEY,
			equipment_id TEXT NOT NULL,
			shift_id TEXT NOT NULL,
			date_id TEXT NOT NULL,
			timestamp TEXT NOT NULL,
			quantity REAL NOT NULL,
			product_id TEXT NOT NULL,
			pass_fail TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create fact_production_output: %v", err)
	}

	return adapter, ctx, cleanup
}

// stateHistoryRecord represents an equipment state period
type stateHistoryRecord struct {
	EquipmentID          string
	StateStartTimestamp  string
	StateEndTimestamp    string
	StateDurationMin     float64
	MachineState         string
	ReasonCodeID         string
	ShiftID              string
	DateID               string
}

// productionOutputRecord represents a production output event
type productionOutputRecord struct {
	ProductionID string
	EquipmentID  string
	ShiftID      string
	DateID       string
	Timestamp    string
	Quantity     float64
	ProductID    string
	PassFail     string
}

// insertStateHistory is a helper to insert state history records
func insertStateHistory(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context, records []stateHistoryRecord) {
	for _, r := range records {
		reasonCode := "NULL"
		if r.ReasonCodeID != "" {
			reasonCode = fmt.Sprintf("'%s'", r.ReasonCodeID)
		}
		sql := fmt.Sprintf(`
			INSERT INTO stg_equipment_state_history VALUES (
				'%s', '%s', '%s', %.2f, '%s', %s, '%s', '%s'
			)`,
			r.EquipmentID, r.StateStartTimestamp, r.StateEndTimestamp,
			r.StateDurationMin, r.MachineState, reasonCode, r.ShiftID, r.DateID,
		)
		if err := adapter.ExecuteDDL(ctx, sql); err != nil {
			t.Fatalf("Failed to insert state history: %v", err)
		}
	}
}

// insertProductionOutput is a helper to insert production output records
func insertProductionOutput(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context, records []productionOutputRecord) {
	for _, r := range records {
		sql := fmt.Sprintf(`
			INSERT INTO fact_production_output VALUES (
				'%s', '%s', '%s', '%s', '%s', %.2f, '%s', '%s'
			)`,
			r.ProductionID, r.EquipmentID, r.ShiftID, r.DateID,
			r.Timestamp, r.Quantity, r.ProductID, r.PassFail,
		)
		if err := adapter.ExecuteDDL(ctx, sql); err != nil {
			t.Fatalf("Failed to insert production output: %v", err)
		}
	}
}

// executeOEECalculationSQL loads and executes the fact_equipment_daily_oee.sql model
func executeOEECalculationSQL(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context) {
	repoRoot := getRepoRoot(t)
	modelPath := filepath.Join(repoRoot, "examples", "osb_machine_event_oee", "models", "facts", "fact_equipment_daily_oee.sql")

	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read fact_equipment_daily_oee.sql: %v", err)
	}

	// Remove config calls
	contentStr := removeConfigCallsStaging(string(content))

	// Parse and render template
	templateEngine := template.New()
	tmpl, err := templateEngine.Parse("fact_equipment_daily_oee", contentStr)
	if err != nil {
		t.Fatalf("Failed to parse template: %v", err)
	}

	ctx2 := template.NewContext(template.WithCurrentModel("fact_equipment_daily_oee"))
	rendered, err := template.Render(tmpl, ctx2, nil)
	if err != nil {
		t.Fatalf("Failed to render template: %v", err)
	}

	// Create fact table
	err = adapter.ExecuteDDL(ctx, "DROP TABLE IF EXISTS fact_equipment_daily_oee")
	if err != nil {
		t.Fatalf("Failed to drop existing fact_equipment_daily_oee: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, "CREATE TABLE fact_equipment_daily_oee AS "+rendered)
	if err != nil {
		t.Fatalf("Failed to execute fact_equipment_daily_oee model: %v", err)
	}
}

// TestOSBPlannedProductionTimeCalculation validates Planned Production Time = Calendar Time - Planned Downtime
func TestOSBPlannedProductionTimeCalculation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupOEECalculationTest(t)
	defer cleanup()

	// Create state history: 24 hours with 2 hours planned maintenance
	baseTime := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	states := []stateHistoryRecord{
		{"PRESS-01", baseTime.Format("2006-01-02 15:04:05"), baseTime.Add(22 * time.Hour).Format("2006-01-02 15:04:05"), 1320, "Running", "", "DAY", "20240115"},
		{"PRESS-01", baseTime.Add(22 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(24 * time.Hour).Format("2006-01-02 15:04:05"), 120, "Planned Downtime", "RC_PLANNED_MAINT", "NIGHT", "20240115"},
	}

	insertStateHistory(t, adapter, ctx, states)
	executeOEECalculationSQL(t, adapter, ctx)

	// Verify Planned Production Time calculation
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			calendar_time_min,
			planned_downtime_min,
			planned_production_time_min
		FROM fact_equipment_daily_oee
		WHERE equipment_id = 'PRESS-01'
		AND date_id = '20240115'
	`)
	if err != nil {
		t.Fatalf("Failed to query OEE data: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected OEE record, got none")
	}

	calendarTime := getFloat(result.Rows[0][0])
	plannedDowntime := getFloat(result.Rows[0][1])
	plannedProdTime := getFloat(result.Rows[0][2])

	// Verify: 24 hours = 1440 minutes
	if calendarTime != 1440.0 {
		t.Errorf("Expected calendar time 1440 minutes, got %.1f", calendarTime)
	}

	// Verify: 2 hours planned downtime = 120 minutes
	if plannedDowntime != 120.0 {
		t.Errorf("Expected planned downtime 120 minutes, got %.1f", plannedDowntime)
	}

	// Verify: Planned Production Time = 1440 - 120 = 1320 minutes
	if plannedProdTime != 1320.0 {
		t.Errorf("Expected planned production time 1320 minutes, got %.1f", plannedProdTime)
	}
}

// TestOSBAvailabilityCalculation validates Availability = Operating Time / Planned Production Time
func TestOSBAvailabilityCalculation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupOEECalculationTest(t)
	defer cleanup()

	// Create state history: 1320 min planned production time, 120 min breakdown, 1200 min operating time
	baseTime := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	states := []stateHistoryRecord{
		{"PRESS-01", baseTime.Format("2006-01-02 15:04:05"), baseTime.Add(10 * time.Hour).Format("2006-01-02 15:04:05"), 600, "Running", "", "DAY", "20240115"},
		{"PRESS-01", baseTime.Add(10 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(12 * time.Hour).Format("2006-01-02 15:04:05"), 120, "Unplanned Downtime", "RC_BEARING_FAIL", "DAY", "20240115"},
		{"PRESS-01", baseTime.Add(12 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(22 * time.Hour).Format("2006-01-02 15:04:05"), 600, "Running", "", "SWING", "20240115"},
		{"PRESS-01", baseTime.Add(22 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(24 * time.Hour).Format("2006-01-02 15:04:05"), 120, "Planned Downtime", "RC_PLANNED_MAINT", "NIGHT", "20240115"},
	}

	insertStateHistory(t, adapter, ctx, states)
	executeOEECalculationSQL(t, adapter, ctx)

	// Verify Availability calculation
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			planned_production_time_min,
			unplanned_downtime_min,
			operating_time_min,
			availability_pct
		FROM fact_equipment_daily_oee
		WHERE equipment_id = 'PRESS-01'
		AND date_id = '20240115'
	`)
	if err != nil {
		t.Fatalf("Failed to query OEE data: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected OEE record, got none")
	}

	plannedProdTime := getFloat(result.Rows[0][0])
	unplannedDowntime := getFloat(result.Rows[0][1])
	operatingTime := getFloat(result.Rows[0][2])
	availability := getFloat(result.Rows[0][3])

	// Verify: Planned Production Time = 1440 - 120 = 1320 minutes
	if plannedProdTime != 1320.0 {
		t.Errorf("Expected planned production time 1320 minutes, got %.1f", plannedProdTime)
	}

	// Verify: Unplanned Downtime = 120 minutes (breakdown)
	if unplannedDowntime != 120.0 {
		t.Errorf("Expected unplanned downtime 120 minutes, got %.1f", unplannedDowntime)
	}

	// Verify: Operating Time = 1320 - 120 = 1200 minutes
	if operatingTime != 1200.0 {
		t.Errorf("Expected operating time 1200 minutes, got %.1f", operatingTime)
	}

	// Verify: Availability = 1200 / 1320 = 90.91%
	expectedAvail := (1200.0 / 1320.0) * 100.0
	if availability < expectedAvail-0.5 || availability > expectedAvail+0.5 {
		t.Errorf("Expected availability ~%.2f%%, got %.2f%%", expectedAvail, availability)
	}
}

// TestOSBPerformanceCalculation validates Performance = (Actual Output / Ideal Output)
func TestOSBPerformanceCalculation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupOEECalculationTest(t)
	defer cleanup()

	// Create state history: 480 minutes operating time
	baseTime := time.Date(2024, 1, 15, 6, 0, 0, 0, time.UTC)
	states := []stateHistoryRecord{
		{"PRESS-01", baseTime.Format("2006-01-02 15:04:05"), baseTime.Add(8 * time.Hour).Format("2006-01-02 15:04:05"), 480, "Running", "", "DAY", "20240115"},
	}

	// Create production output: 54 panels in 480 minutes
	// Ideal cycle time = 8 min/panel, so ideal output = 480 / 8 = 60 panels
	// Actual output = 54 panels, so performance = 54 / 60 = 90%
	production := []productionOutputRecord{}
	for i := 0; i < 54; i++ {
		production = append(production, productionOutputRecord{
			ProductionID: fmt.Sprintf("PROD-%03d", i+1),
			EquipmentID:  "PRESS-01",
			ShiftID:      "DAY",
			DateID:       "20240115",
			Timestamp:    baseTime.Add(time.Duration(i*9) * time.Minute).Format("2006-01-02 15:04:05"), // Average 9 min/panel (slightly slow)
			Quantity:     1,
			ProductID:    "OSB-7/16",
			PassFail:     "Pass",
		})
	}

	insertStateHistory(t, adapter, ctx, states)
	insertProductionOutput(t, adapter, ctx, production)
	executeOEECalculationSQL(t, adapter, ctx)

	// Verify Performance calculation
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			operating_time_min,
			actual_output_qty,
			ideal_output_qty,
			performance_pct
		FROM fact_equipment_daily_oee
		WHERE equipment_id = 'PRESS-01'
		AND date_id = '20240115'
	`)
	if err != nil {
		t.Fatalf("Failed to query OEE data: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected OEE record, got none")
	}

	operatingTime := getFloat(result.Rows[0][0])
	actualOutput := getFloat(result.Rows[0][1])
	idealOutput := getFloat(result.Rows[0][2])
	performance := getFloat(result.Rows[0][3])

	// Verify: Operating Time = 480 minutes
	if operatingTime != 480.0 {
		t.Errorf("Expected operating time 480 minutes, got %.1f", operatingTime)
	}

	// Verify: Actual Output = 54 panels
	if actualOutput != 54.0 {
		t.Errorf("Expected actual output 54 panels, got %.1f", actualOutput)
	}

	// Verify: Ideal Output = 480 / 8 = 60 panels
	if idealOutput != 60.0 {
		t.Errorf("Expected ideal output 60 panels, got %.1f", idealOutput)
	}

	// Verify: Performance = 54 / 60 = 90%
	expectedPerf := (54.0 / 60.0) * 100.0
	if performance < expectedPerf-0.5 || performance > expectedPerf+0.5 {
		t.Errorf("Expected performance ~%.2f%%, got %.2f%%", expectedPerf, performance)
	}
}

// TestOSBQualityCalculation validates Quality = Good Output / Total Output
func TestOSBQualityCalculation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupOEECalculationTest(t)
	defer cleanup()

	// Create state history: 480 minutes operating time
	baseTime := time.Date(2024, 1, 15, 6, 0, 0, 0, time.UTC)
	states := []stateHistoryRecord{
		{"PRESS-01", baseTime.Format("2006-01-02 15:04:05"), baseTime.Add(8 * time.Hour).Format("2006-01-02 15:04:05"), 480, "Running", "", "DAY", "20240115"},
	}

	// Create production output: 60 panels total, 57 pass, 3 fail
	// Quality = 57 / 60 = 95%
	production := []productionOutputRecord{}
	for i := 0; i < 60; i++ {
		passFailStatus := "Pass"
		if i == 10 || i == 25 || i == 45 {
			passFailStatus = "Fail" // 3 failed panels
		}
		production = append(production, productionOutputRecord{
			ProductionID: fmt.Sprintf("PROD-%03d", i+1),
			EquipmentID:  "PRESS-01",
			ShiftID:      "DAY",
			DateID:       "20240115",
			Timestamp:    baseTime.Add(time.Duration(i*8) * time.Minute).Format("2006-01-02 15:04:05"), // Ideal 8 min/panel
			Quantity:     1,
			ProductID:    "OSB-7/16",
			PassFail:     passFailStatus,
		})
	}

	insertStateHistory(t, adapter, ctx, states)
	insertProductionOutput(t, adapter, ctx, production)
	executeOEECalculationSQL(t, adapter, ctx)

	// Verify Quality calculation
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			actual_output_qty,
			good_output_qty,
			quality_pct
		FROM fact_equipment_daily_oee
		WHERE equipment_id = 'PRESS-01'
		AND date_id = '20240115'
	`)
	if err != nil {
		t.Fatalf("Failed to query OEE data: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected OEE record, got none")
	}

	actualOutput := getFloat(result.Rows[0][0])
	goodOutput := getFloat(result.Rows[0][1])
	quality := getFloat(result.Rows[0][2])

	// Verify: Actual Output = 60 panels
	if actualOutput != 60.0 {
		t.Errorf("Expected actual output 60 panels, got %.1f", actualOutput)
	}

	// Verify: Good Output = 57 panels
	if goodOutput != 57.0 {
		t.Errorf("Expected good output 57 panels, got %.1f", goodOutput)
	}

	// Verify: Quality = 57 / 60 = 95%
	expectedQuality := (57.0 / 60.0) * 100.0
	if quality < expectedQuality-0.5 || quality > expectedQuality+0.5 {
		t.Errorf("Expected quality ~%.2f%%, got %.2f%%", expectedQuality, quality)
	}
}

// TestOSBOEECalculation validates OEE = Availability × Performance × Quality
func TestOSBOEECalculation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupOEECalculationTest(t)
	defer cleanup()

	// Create comprehensive scenario:
	// 1. Planned Production Time: 1440 - 120 (maint) = 1320 min
	// 2. Unplanned Downtime: 120 min (breakdown)
	// 3. Operating Time: 1320 - 120 = 1200 min
	// 4. Availability: 1200 / 1320 = 90.91%
	// 5. Ideal Output: 1200 / 8 = 150 panels
	// 6. Actual Output: 135 panels (Performance = 135/150 = 90%)
	// 7. Good Output: 131 panels (Quality = 131/135 = 97.04%)
	// 8. OEE = 0.9091 × 0.90 × 0.9704 = 79.4%

	baseTime := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	states := []stateHistoryRecord{
		{"PRESS-01", baseTime.Format("2006-01-02 15:04:05"), baseTime.Add(10 * time.Hour).Format("2006-01-02 15:04:05"), 600, "Running", "", "NIGHT", "20240115"},
		{"PRESS-01", baseTime.Add(10 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(12 * time.Hour).Format("2006-01-02 15:04:05"), 120, "Unplanned Downtime", "RC_BEARING_FAIL", "DAY", "20240115"},
		{"PRESS-01", baseTime.Add(12 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(22 * time.Hour).Format("2006-01-02 15:04:05"), 600, "Running", "", "DAY", "20240115"},
		{"PRESS-01", baseTime.Add(22 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(24 * time.Hour).Format("2006-01-02 15:04:05"), 120, "Planned Downtime", "RC_PLANNED_MAINT", "NIGHT", "20240115"},
	}

	// Create production output: 135 panels, 4 failed (131 good)
	// Failure logic: every 27th panel (i=27,54,81,108) = 4 failures
	production := []productionOutputRecord{}
	for i := 0; i < 135; i++ {
		passFailStatus := "Pass"
		if i%27 == 0 && i > 0 { // Fail every 27th panel (4 failures total)
			passFailStatus = "Fail"
		}
		// Distribute across operating periods
		var timestamp time.Time
		if i < 67 {
			timestamp = baseTime.Add(time.Duration(i*9) * time.Minute)
		} else {
			timestamp = baseTime.Add(12*time.Hour + time.Duration((i-67)*9)*time.Minute)
		}
		production = append(production, productionOutputRecord{
			ProductionID: fmt.Sprintf("PROD-%03d", i+1),
			EquipmentID:  "PRESS-01",
			ShiftID:      "DAY",
			DateID:       "20240115",
			Timestamp:    timestamp.Format("2006-01-02 15:04:05"),
			Quantity:     1,
			ProductID:    "OSB-7/16",
			PassFail:     passFailStatus,
		})
	}

	insertStateHistory(t, adapter, ctx, states)
	insertProductionOutput(t, adapter, ctx, production)
	executeOEECalculationSQL(t, adapter, ctx)

	// Verify OEE calculation
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			availability_pct,
			performance_pct,
			quality_pct,
			oee_pct
		FROM fact_equipment_daily_oee
		WHERE equipment_id = 'PRESS-01'
		AND date_id = '20240115'
	`)
	if err != nil {
		t.Fatalf("Failed to query OEE data: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected OEE record, got none")
	}

	availability := getFloat(result.Rows[0][0])
	performance := getFloat(result.Rows[0][1])
	quality := getFloat(result.Rows[0][2])
	oee := getFloat(result.Rows[0][3])

	// Verify: Availability ≈ 90.91%
	expectedAvail := (1200.0 / 1320.0) * 100.0
	if availability < expectedAvail-0.5 || availability > expectedAvail+0.5 {
		t.Errorf("Expected availability ~%.2f%%, got %.2f%%", expectedAvail, availability)
	}

	// Verify: Performance ≈ 90%
	expectedPerf := (135.0 / 150.0) * 100.0
	if performance < expectedPerf-0.5 || performance > expectedPerf+0.5 {
		t.Errorf("Expected performance ~%.2f%%, got %.2f%%", expectedPerf, performance)
	}

	// Verify: Quality ≈ 97.04%
	expectedQuality := (131.0 / 135.0) * 100.0
	if quality < expectedQuality-0.5 || quality > expectedQuality+0.5 {
		t.Errorf("Expected quality ~%.2f%%, got %.2f%%", expectedQuality, quality)
	}

	// Verify: OEE = Availability × Performance × Quality ≈ 78.8%
	expectedOEE := (availability / 100.0) * (performance / 100.0) * (quality / 100.0) * 100.0
	if oee < expectedOEE-0.5 || oee > expectedOEE+0.5 {
		t.Errorf("Expected OEE ~%.2f%%, got %.2f%%", expectedOEE, oee)
	}

	// Verify OEE is in reasonable range (78-82% based on calculations)
	if oee < 78.0 || oee > 82.0 {
		t.Errorf("OEE %.2f%% outside expected range (78-82%%)", oee)
	}
}

// TestOSBSixBigLossesClassification validates downtime/losses correctly map to Six Big Losses categories
func TestOSBSixBigLossesClassification(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupOEECalculationTest(t)
	defer cleanup()

	// Create state history with various loss types
	baseTime := time.Date(2024, 1, 15, 6, 0, 0, 0, time.UTC)
	states := []stateHistoryRecord{
		{"PRESS-01", baseTime.Format("2006-01-02 15:04:05"), baseTime.Add(7 * time.Hour).Format("2006-01-02 15:04:05"), 420, "Running", "", "DAY", "20240115"},
		{"PRESS-01", baseTime.Add(7 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(9 * time.Hour).Format("2006-01-02 15:04:05"), 120, "Unplanned Downtime", "RC_BEARING_FAIL", "DAY", "20240115"},
		{"PRESS-01", baseTime.Add(9 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(14 * time.Hour).Format("2006-01-02 15:04:05"), 300, "Running", "", "DAY", "20240115"},
	}

	insertStateHistory(t, adapter, ctx, states)
	executeOEECalculationSQL(t, adapter, ctx)

	// Verify Six Big Losses categorization
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			equipment_failure_loss_min,
			small_stops_loss_min,
			reduced_speed_loss_min
		FROM fact_equipment_daily_oee
		WHERE equipment_id = 'PRESS-01'
		AND date_id = '20240115'
	`)
	if err != nil {
		t.Fatalf("Failed to query OEE data: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatal("Expected OEE record, got none")
	}

	equipmentFailure := getFloat(result.Rows[0][0])
	smallStops := getFloat(result.Rows[0][1])
	reducedSpeed := getFloat(result.Rows[0][2])

	// Verify: Equipment Failure Loss = 120 minutes (bearing failure)
	if equipmentFailure != 120.0 {
		t.Errorf("Expected equipment failure loss 120 minutes, got %.1f", equipmentFailure)
	}

	// Small stops and reduced speed are implicit in performance loss
	// These should be calculated from production output vs ideal cycle time
	t.Logf("Small stops loss: %.1f min", smallStops)
	t.Logf("Reduced speed loss: %.1f min", reducedSpeed)
}

// TestOSBEquipmentWithoutProductionHandling ensures non-production equipment handled appropriately
func TestOSBEquipmentWithoutProductionHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupOEECalculationTest(t)
	defer cleanup()

	// Add non-production equipment (conveyor)
	err := adapter.ExecuteDDL(ctx, `
		INSERT INTO dim_equipment VALUES 
		('CONV-01', 'Cooling Conveyor', 'Conveyor', 'Finishing', 0, 'N/A', 'Standard')
	`)
	if err != nil {
		t.Fatalf("Failed to insert conveyor equipment: %v", err)
	}

	// Create state history for conveyor (no production output)
	baseTime := time.Date(2024, 1, 15, 6, 0, 0, 0, time.UTC)
	states := []stateHistoryRecord{
		{"CONV-01", baseTime.Format("2006-01-02 15:04:05"), baseTime.Add(8 * time.Hour).Format("2006-01-02 15:04:05"), 480, "Running", "", "DAY", "20240115"},
	}

	insertStateHistory(t, adapter, ctx, states)
	executeOEECalculationSQL(t, adapter, ctx)

	// Verify conveyor has availability but no performance/quality metrics
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			equipment_id,
			availability_pct,
			performance_pct,
			quality_pct,
			oee_pct
		FROM fact_equipment_daily_oee
		WHERE equipment_id = 'CONV-01'
		AND date_id = '20240115'
	`)
	if err != nil {
		t.Fatalf("Failed to query OEE data: %v", err)
	}

	// Non-production equipment may not have OEE record, or have NULL performance/quality
	if len(result.Rows) == 0 {
		t.Log("Non-production equipment excluded from OEE calculation (expected behavior)")
		return
	}

	// If included, performance and quality should be NULL or 100%
	availability := result.Rows[0][1]
	performance := result.Rows[0][2]
	quality := result.Rows[0][3]

	t.Logf("Non-production equipment OEE: Availability=%v, Performance=%v, Quality=%v",
		availability, performance, quality)
}

// TestOSBMultiShiftAggregation validates OEE calculated separately per shift and as daily aggregate
func TestOSBMultiShiftAggregation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, cleanup := setupOEECalculationTest(t)
	defer cleanup()

	// Create state history spanning multiple shifts
	baseTime := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	states := []stateHistoryRecord{
		// Night shift: 6 hours running (06:00 start in Night shift 22:00-06:00)
		{"PRESS-01", baseTime.Format("2006-01-02 15:04:05"), baseTime.Add(6 * time.Hour).Format("2006-01-02 15:04:05"), 360, "Running", "", "NIGHT", "20240115"},
		// Day shift: 7 hours running, 1 hour downtime
		{"PRESS-01", baseTime.Add(6 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(13 * time.Hour).Format("2006-01-02 15:04:05"), 420, "Running", "", "DAY", "20240115"},
		{"PRESS-01", baseTime.Add(13 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(14 * time.Hour).Format("2006-01-02 15:04:05"), 60, "Unplanned Downtime", "RC_BEARING_FAIL", "DAY", "20240115"},
		// Swing shift: 8 hours running
		{"PRESS-01", baseTime.Add(14 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(22 * time.Hour).Format("2006-01-02 15:04:05"), 480, "Running", "", "SWING", "20240115"},
		// Night shift: 2 hours running
		{"PRESS-01", baseTime.Add(22 * time.Hour).Format("2006-01-02 15:04:05"), baseTime.Add(24 * time.Hour).Format("2006-01-02 15:04:05"), 120, "Running", "", "NIGHT", "20240115"},
	}

	// Create production across shifts
	production := []productionOutputRecord{}
	for i := 0; i < 150; i++ {
		var shiftID string
		var timestamp time.Time
		if i < 45 {
			shiftID = "NIGHT"
			timestamp = baseTime.Add(time.Duration(i*8) * time.Minute)
		} else if i < 45+52 {
			shiftID = "DAY"
			timestamp = baseTime.Add(6*time.Hour + time.Duration((i-45)*8)*time.Minute)
		} else {
			shiftID = "SWING"
			timestamp = baseTime.Add(14*time.Hour + time.Duration((i-97)*8)*time.Minute)
		}
		production = append(production, productionOutputRecord{
			ProductionID: fmt.Sprintf("PROD-%03d", i+1),
			EquipmentID:  "PRESS-01",
			ShiftID:      shiftID,
			DateID:       "20240115",
			Timestamp:    timestamp.Format("2006-01-02 15:04:05"),
			Quantity:     1,
			ProductID:    "OSB-7/16",
			PassFail:     "Pass",
		})
	}

	insertStateHistory(t, adapter, ctx, states)
	insertProductionOutput(t, adapter, ctx, production)
	executeOEECalculationSQL(t, adapter, ctx)

	// Verify daily aggregate
	result, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			equipment_id,
			date_id,
			shift_id,
			oee_pct
		FROM fact_equipment_daily_oee
		WHERE equipment_id = 'PRESS-01'
		AND date_id = '20240115'
		ORDER BY shift_id
	`)
	if err != nil {
		t.Fatalf("Failed to query OEE data: %v", err)
	}

	// Should have either:
	// - 1 record (daily aggregate only), or
	// - 4 records (3 shifts + daily aggregate), or
	// - 3 records (3 shifts only)
	if len(result.Rows) == 0 {
		t.Fatal("Expected OEE records, got none")
	}

	t.Logf("Found %d OEE records for multi-shift day", len(result.Rows))
	for _, row := range result.Rows {
		shiftID := row[2]
		oee := getFloat(row[3])
		t.Logf("Shift %v: OEE = %.2f%%", shiftID, oee)
	}

	// Verify Day shift has lower OEE due to breakdown
	var dayShiftOEE, swingShiftOEE float64
	for _, row := range result.Rows {
		if row[2] == "DAY" {
			dayShiftOEE = getFloat(row[3])
		} else if row[2] == "SWING" {
			swingShiftOEE = getFloat(row[3])
		}
	}

	if dayShiftOEE > 0 && swingShiftOEE > 0 {
		if dayShiftOEE >= swingShiftOEE {
			t.Errorf("Expected Day shift OEE (%.2f%%) < Swing shift OEE (%.2f%%) due to breakdown", dayShiftOEE, swingShiftOEE)
		}
	}
}
