// Phase 4 Build Tool
// Go-based tool to build intermediate layer models
package main

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"

	_ "modernc.org/sqlite"
)

type Model struct {
	Name string
	Path string
}

func main() {
	fmt.Println("=== Precision Railroading Phase 4: Intermediate Layer Build ===")
	fmt.Println()

	dbPath := "target/precision_railroading.db"

	// Check if database exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		fmt.Println("❌ Database not found. Run build_phase3.ps1 first to create staging tables.")
		os.Exit(1)
	}

	// Open database connection
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		fmt.Printf("❌ Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Drop existing intermediate tables
	fmt.Println("Dropping existing intermediate tables...")
	dropTables := []string{
		"DROP TABLE IF EXISTS int_cycle_classification",
		"DROP TABLE IF EXISTS int_trip_segments",
		"DROP TABLE IF EXISTS int_state_intervals",
	}
	for _, dropSQL := range dropTables {
		if _, err := db.Exec(dropSQL); err != nil {
			fmt.Printf("❌ Failed to drop table: %v\n", err)
			os.Exit(1)
		}
	}

	// Models to build in dependency order
	models := []Model{
		{Name: "int_state_intervals", Path: "models/intermediate/int_state_intervals.sql"},
		{Name: "int_trip_segments", Path: "models/intermediate/int_trip_segments.sql"},
		{Name: "int_cycle_classification", Path: "models/intermediate/int_cycle_classification.sql"},
	}

	fmt.Println("Building intermediate models...")

	for _, model := range models {
		fmt.Printf("  Building %s... ", model.Name)

		// Read SQL file
		sqlBytes, err := os.ReadFile(model.Path)
		if err != nil {
			fmt.Printf("❌ FAILED\n")
			fmt.Printf("Error reading file: %v\n", err)
			os.Exit(1)
		}

		sqlContent := string(sqlBytes)

		// Process Jinja2-like templates
		sqlContent = processTemplate(sqlContent)

		// Create table
		createSQL := fmt.Sprintf("CREATE TABLE %s AS\n%s", model.Name, sqlContent)

		if _, err := db.Exec(createSQL); err != nil {
			fmt.Printf("❌ FAILED\n")
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		}

		fmt.Println("✓ OK")
	}

	// Verify row counts
	fmt.Println("\nVerifying intermediate tables...")

	intervals, _ := getRowCount(db, "int_state_intervals")
	trips, _ := getRowCount(db, "int_trip_segments")
	cycles, _ := getRowCount(db, "int_cycle_classification")

	fmt.Printf("  int_state_intervals: %d rows\n", intervals)
	fmt.Printf("  int_trip_segments: %d rows\n", trips)
	fmt.Printf("  int_cycle_classification: %d rows\n", cycles)

	// Verify data quality metrics
	fmt.Println("\nVerifying data quality...")

	// Interval stats
	var totalIntervals, openIntervals int
	var avgDuration float64
	err = db.QueryRow(`
		SELECT 
			COUNT(*) as total_intervals,
			SUM(CASE WHEN end_timestamp IS NULL THEN 1 ELSE 0 END) as open_intervals,
			AVG(duration_minutes) as avg_duration_minutes
		FROM int_state_intervals
	`).Scan(&totalIntervals, &openIntervals, &avgDuration)
	if err != nil {
		fmt.Printf("  Warning: Could not get interval stats: %v\n", err)
	} else {
		fmt.Printf("  Total intervals: %d\n", totalIntervals)
		fmt.Printf("  Open intervals: %d\n", openIntervals)
		fmt.Printf("  Avg duration: %.2f minutes\n", avgDuration)
	}

	// Trip classification
	var loadedTrips, emptyTrips int
	err = db.QueryRow(`
		SELECT 
			SUM(CASE WHEN is_loaded_trip = 1 THEN 1 ELSE 0 END) as loaded_trips,
			SUM(CASE WHEN is_loaded_trip = 0 THEN 1 ELSE 0 END) as empty_trips
		FROM int_trip_segments
	`).Scan(&loadedTrips, &emptyTrips)
	if err != nil {
		fmt.Printf("  Warning: Could not get trip stats: %v\n", err)
	} else {
		fmt.Printf("  Loaded trips: %d\n", loadedTrips)
		fmt.Printf("  Empty trips: %d\n", emptyTrips)
	}

	// Cycle statistics
	var avgCycleDays, minCycleDays, maxCycleDays float64
	err = db.QueryRow(`
		SELECT 
			AVG(cycle_duration_days) as avg_cycle_days,
			MIN(cycle_duration_days) as min_cycle_days,
			MAX(cycle_duration_days) as max_cycle_days
		FROM int_cycle_classification
	`).Scan(&avgCycleDays, &minCycleDays, &maxCycleDays)
	if err != nil {
		fmt.Printf("  Warning: Could not get cycle stats: %v\n", err)
	} else {
		fmt.Printf("  Avg cycle duration: %.2f days\n", avgCycleDays)
		fmt.Printf("  Min cycle duration: %.2f days\n", minCycleDays)
		fmt.Printf("  Max cycle duration: %.2f days\n", maxCycleDays)
	}

	fmt.Println("\n✓ Phase 4 build complete!")
	fmt.Println("Run test_phase4.ps1 to execute data quality tests.")
}

func processTemplate(sql string) string {
	// Remove config directives
	re := regexp.MustCompile(`\{\{\s*config\s*"materialized"\s*"(table|view)"\s*\}\}`)
	sql = re.ReplaceAllString(sql, "")

	// Replace ref() calls
	refPatterns := map[string]string{
		`\{\{\s*ref\s*"stg_clm_enriched"\s*\}\}`:         "stg_clm_enriched",
		`\{\{\s*ref\s*"int_state_intervals"\s*\}\}`:      "int_state_intervals",
		`\{\{\s*ref\s*"int_trip_segments"\s*\}\}`:        "int_trip_segments",
		`\{\{\s*ref\s*"int_cycle_classification"\s*\}\}`: "int_cycle_classification",
		`\{\{\s*ref\s*"dim_railcar"\s*\}\}`:              "dim_railcar",
	}

	for pattern, replacement := range refPatterns {
		re := regexp.MustCompile(pattern)
		sql = re.ReplaceAllString(sql, replacement)
	}

	return strings.TrimSpace(sql)
}

func getRowCount(db *sql.DB, tableName string) (int, error) {
	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	err := db.QueryRow(query).Scan(&count)
	return count, err
}
