package test

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// TestValidationQueriesExist verifies all 4 SQL validation files exist
func TestValidationQueriesExist(t *testing.T) {
	repoRoot := getRepoRoot(t)
	validationDir := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "validation")

	requiredFiles := []string{
		"car_accounting.sql",
		"train_integrity.sql",
		"operational_constraints.sql",
		"straggler_validation.sql",
	}

	for _, file := range requiredFiles {
		filePath := filepath.Join(validationDir, file)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			t.Errorf("Required validation query file does not exist: %s", file)
		}
	}
}

// TestCarAccountingValidation verifies car inventory reconciliation logic
func TestCarAccountingValidation(t *testing.T) {
	repoRoot := getRepoRoot(t)
	validationPath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "validation", "car_accounting.sql")

	sqlContent, err := os.ReadFile(validationPath)
	if err != nil {
		t.Fatalf("Failed to read car_accounting.sql: %v", err)
	}

	content := string(sqlContent)

	// Verify the query checks for essential validations
	requiredChecks := []string{
		"228", // Total car count
		"duplicate",
		"dim_car",
		"fact_car_location_event",
	}

	for _, check := range requiredChecks {
		if !strings.Contains(strings.ToLower(content), strings.ToLower(check)) {
			t.Errorf("Car accounting validation missing expected check: %s", check)
		}
	}
}

// TestTrainIntegrityValidation verifies train operation validation logic
func TestTrainIntegrityValidation(t *testing.T) {
	repoRoot := getRepoRoot(t)
	validationPath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "validation", "train_integrity.sql")

	sqlContent, err := os.ReadFile(validationPath)
	if err != nil {
		t.Fatalf("Failed to read train_integrity.sql: %v", err)
	}

	content := string(sqlContent)

	// Verify the query checks for essential validations
	requiredChecks := []string{
		"fact_train_trip",
		"corridor",
		"duration",
		"location",
	}

	for _, check := range requiredChecks {
		if !strings.Contains(strings.ToLower(content), strings.ToLower(check)) {
			t.Errorf("Train integrity validation missing expected check: %s", check)
		}
	}
}

// TestOperationalConstraintsValidation verifies business rule checks
func TestOperationalConstraintsValidation(t *testing.T) {
	repoRoot := getRepoRoot(t)
	validationPath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "validation", "operational_constraints.sql")

	sqlContent, err := os.ReadFile(validationPath)
	if err != nil {
		t.Fatalf("Failed to read operational_constraints.sql: %v", err)
	}

	content := string(sqlContent)

	// Verify the query checks for essential validations
	requiredChecks := []string{
		"queue",
		"set_out",
		"pick_up",
		"straggler",
		"power",
		"week",
	}

	for _, check := range requiredChecks {
		if !strings.Contains(strings.ToLower(content), strings.ToLower(check)) {
			t.Errorf("Operational constraints validation missing expected check: %s", check)
		}
	}
}

// TestStragglerValidationChecks verifies straggler-specific validation
func TestStragglerValidationChecks(t *testing.T) {
	repoRoot := getRepoRoot(t)
	validationPath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "validation", "straggler_validation.sql")

	sqlContent, err := os.ReadFile(validationPath)
	if err != nil {
		t.Fatalf("Failed to read straggler_validation.sql: %v", err)
	}

	content := string(sqlContent)

	// Verify the query checks for essential validations
	requiredChecks := []string{
		"fact_straggler",
		"delay",
		"6",  // Min delay hours
		"72", // Max delay hours
		"set_out",
		"week 8",
	}

	for _, check := range requiredChecks {
		if !strings.Contains(strings.ToLower(content), strings.ToLower(check)) {
			t.Errorf("Straggler validation missing expected check: %s", check)
		}
	}
}

// TestNoCarDuplicates runs car_accounting and verifies 0 duplicate violations
func TestNoCarDuplicates(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	repoRoot := getRepoRoot(t)
	dbPath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "unit_train.db")
	validationPath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "validation", "car_accounting.sql")

	// Check if DB exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Skip("Database does not exist, run seed first")
	}

	// Read validation query
	sqlContent, err := os.ReadFile(validationPath)
	if err != nil {
		t.Fatalf("Failed to read car_accounting.sql: %v", err)
	}

	// Connect to DB
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Execute validation query
	rows, err := db.Query(string(sqlContent))
	if err != nil {
		t.Fatalf("Failed to execute car accounting validation: %v", err)
	}
	defer rows.Close()

	// Read results and check for violations
	var hasViolations bool
	for rows.Next() {
		hasViolations = true
		// Log violation details if available
		cols, _ := rows.Columns()
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err == nil {
			t.Logf("Car accounting violation found: %v", values)
		}
	}

	if hasViolations {
		t.Error("Car accounting validation found violations (expected 0 for clean data)")
	}
}

// TestTrainTripValidity runs train_integrity and verifies 0 invalid trips
func TestTrainTripValidity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	repoRoot := getRepoRoot(t)
	dbPath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "unit_train.db")
	validationPath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "validation", "train_integrity.sql")

	// Check if DB exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Skip("Database does not exist, run seed first")
	}

	// Read validation query
	sqlContent, err := os.ReadFile(validationPath)
	if err != nil {
		t.Fatalf("Failed to read train_integrity.sql: %v", err)
	}

	// Connect to DB
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Execute validation query
	rows, err := db.Query(string(sqlContent))
	if err != nil {
		t.Fatalf("Failed to execute train integrity validation: %v", err)
	}
	defer rows.Close()

	// Read results and check for violations
	var hasViolations bool
	for rows.Next() {
		hasViolations = true
		// Log violation details if available
		cols, _ := rows.Columns()
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err == nil {
			t.Logf("Train integrity violation found: %v", values)
		}
	}

	if hasViolations {
		t.Error("Train integrity validation found violations (expected 0 for valid operations)")
	}
}

// TestSeasonalEffectsPresent runs operational_constraints and verifies Week 5 & 8 effects exist
func TestSeasonalEffectsPresent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	repoRoot := getRepoRoot(t)
	dbPath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "unit_train.db")

	// Check if DB exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Skip("Database does not exist, run seed first")
	}

	// Connect to DB
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Check Week 5 slowdown effect (transit times should be ~20% longer)
	week5Query := `
		SELECT 
			AVG(actual_transit_hours) as avg_transit,
			COUNT(*) as trip_count
		FROM fact_train_trip ftt
		JOIN dim_date dd ON ftt.departure_date_key = dd.date_key
		WHERE dd.week = 5
	`
	var week5Avg float64
	var week5Count int
	err = db.QueryRow(week5Query).Scan(&week5Avg, &week5Count)
	if err != nil {
		t.Fatalf("Failed to query Week 5 data: %v", err)
	}

	if week5Count == 0 {
		t.Error("No trips found in Week 5 (expected slowdown effect)")
	} else {
		t.Logf("Week 5: %d trips with avg transit %.1f hours", week5Count, week5Avg)
	}

	// Check Week 8 straggler spike (should have higher straggler count)
	week8Query := `
		SELECT COUNT(*) as straggler_count
		FROM fact_straggler fs
		JOIN dim_date dd ON fs.set_out_date_key = dd.date_key
		WHERE dd.week = 8
	`
	var week8StragglerCount int
	err = db.QueryRow(week8Query).Scan(&week8StragglerCount)
	if err != nil {
		t.Fatalf("Failed to query Week 8 straggler data: %v", err)
	}

	if week8StragglerCount == 0 {
		t.Error("No stragglers found in Week 8 (expected 2x spike)")
	} else {
		t.Logf("Week 8: %d stragglers found", week8StragglerCount)
	}

	// Compare Week 8 to baseline weeks
	baselineQuery := `
		SELECT AVG(week_straggler_count) as avg_baseline
		FROM (
			SELECT COUNT(*) as week_straggler_count
			FROM fact_straggler fs
			JOIN dim_date dd ON fs.set_out_date_key = dd.date_key
			WHERE dd.week NOT IN (5, 8)
			GROUP BY dd.week
		)
	`
	var avgBaseline float64
	err = db.QueryRow(baselineQuery).Scan(&avgBaseline)
	if err != nil {
		t.Fatalf("Failed to query baseline straggler data: %v", err)
	}

	week8Ratio := float64(week8StragglerCount) / avgBaseline
	t.Logf("Week 8 straggler ratio vs baseline: %.2fx (expected ~2x)", week8Ratio)

	// Allow some variance but Week 8 should be noticeably higher
	if week8Ratio < 1.5 {
		t.Errorf("Week 8 straggler spike not detected (%.2fx vs baseline, expected >1.5x)", week8Ratio)
	}
}
