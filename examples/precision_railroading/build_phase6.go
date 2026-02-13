// Phase 6 Build Tool
// Go-based tool to build fact tables
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
	fmt.Println("=== Precision Railroading Phase 6: Fact Tables & Stop Classification ===")
	fmt.Println()

	dbPath := "target/precision_railroading.db"

	// Check if database exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		fmt.Println("❌ Database not found. Run build_phase5.ps1 first to create intermediate tables.")
		os.Exit(1)
	}

	// Open database connection
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		fmt.Printf("❌ Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Drop existing Phase 6 tables
	fmt.Println("Dropping existing Phase 6 tables...")
	dropTables := []string{
		"DROP TABLE IF EXISTS fact_corridor_transit",
		"DROP TABLE IF EXISTS fact_stop_classification",
		"DROP TABLE IF EXISTS fact_dwell",
		"DROP TABLE IF EXISTS fact_trip",
	}
	for _, dropSQL := range dropTables {
		if _, err := db.Exec(dropSQL); err != nil {
			fmt.Printf("❌ Failed to drop table: %v\n", err)
			os.Exit(1)
		}
	}

	// Models to build in dependency order
	models := []Model{
		{Name: "fact_trip", Path: "models/facts/fact_trip.sql"},
		{Name: "fact_dwell", Path: "models/facts/fact_dwell.sql"},
		{Name: "fact_stop_classification", Path: "models/facts/fact_stop_classification.sql"},
		{Name: "fact_corridor_transit", Path: "models/facts/fact_corridor_transit.sql"},
	}

	fmt.Println("Building Phase 6 models...")

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
	fmt.Println("\nVerifying Phase 6 tables...")

	factTrip, _ := getRowCount(db, "fact_trip")
	factDwell, _ := getRowCount(db, "fact_dwell")
	factStopClass, _ := getRowCount(db, "fact_stop_classification")
	factCorridorTransit, _ := getRowCount(db, "fact_corridor_transit")

	fmt.Printf("  fact_trip: %d rows\n", factTrip)
	fmt.Printf("  fact_dwell: %d rows\n", factDwell)
	fmt.Printf("  fact_stop_classification: %d rows\n", factStopClass)
	fmt.Printf("  fact_corridor_transit: %d rows\n", factCorridorTransit)

	// Verify data quality metrics
	fmt.Println("\nVerifying data quality...")

	// Trip stats
	var tripLoadedCount, tripEmptyCount int
	err = db.QueryRow(`
		SELECT 
			SUM(CASE WHEN trip_type = 'loaded' THEN 1 ELSE 0 END) as loaded_count,
			SUM(CASE WHEN trip_type = 'empty' THEN 1 ELSE 0 END) as empty_count
		FROM fact_trip
	`).Scan(&tripLoadedCount, &tripEmptyCount)
	if err != nil {
		fmt.Printf("  Warning: Could not get trip stats: %v\n", err)
	} else {
		fmt.Printf("  Loaded trips: %d\n", tripLoadedCount)
		fmt.Printf("  Empty trips: %d\n", tripEmptyCount)
	}

	// Shadow yard stats
	var shadowYardStops int
	err = db.QueryRow(`
		SELECT COUNT(*)
		FROM fact_dwell
		WHERE shadow_yard_flag = 1
	`).Scan(&shadowYardStops)
	if err != nil {
		fmt.Printf("  Warning: Could not get shadow yard stats: %v\n", err)
	} else {
		fmt.Printf("  Shadow yard stops: %d\n", shadowYardStops)
	}

	// Stop classification breakdown
	fmt.Println("\nStop classification breakdown:")
	classRows, err := db.Query(`
		SELECT dwell_classification, COUNT(*) as count
		FROM fact_dwell
		GROUP BY dwell_classification
		ORDER BY count DESC
	`)
	if err != nil {
		fmt.Printf("  Warning: Could not get classification breakdown: %v\n", err)
	} else {
		defer classRows.Close()
		for classRows.Next() {
			var classification string
			var count int
			if err := classRows.Scan(&classification, &count); err == nil {
				fmt.Printf("  %s: %d\n", classification, count)
			}
		}
	}

	fmt.Println("\n✓ Phase 6 build complete!")
	fmt.Println("Run test_phase6.ps1 to execute data quality tests.")
}

func processTemplate(sql string) string {
	// Remove config directives
	re := regexp.MustCompile(`\{\{\s*config\s*"materialized"\s*"(table|view)"\s*\}\}`)
	sql = re.ReplaceAllString(sql, "")

	// Replace ref() calls
	refPatterns := map[string]string{
		`\{\{\s*ref\s*"int_trip_segments"\s*\}\}`:        "int_trip_segments",
		`\{\{\s*ref\s*"int_velocity_vectors"\s*\}\}`:     "int_velocity_vectors",
		`\{\{\s*ref\s*"int_nodal_dwell"\s*\}\}`:          "int_nodal_dwell",
		`\{\{\s*ref\s*"int_dwell_classification"\s*\}\}`: "int_dwell_classification",
		`\{\{\s*ref\s*"dim_railcar"\s*\}\}`:              "dim_railcar",
		`\{\{\s*ref\s*"dim_location"\s*\}\}`:             "dim_location",
		`\{\{\s*ref\s*"dim_train"\s*\}\}`:                "dim_train",
		`\{\{\s*ref\s*"dim_corridor"\s*\}\}`:             "dim_corridor",
		`\{\{\s*ref\s*"dim_date"\s*\}\}`:                 "dim_date",
		`\{\{\s*ref\s*"fact_trip"\s*\}\}`:                "fact_trip",
		`\{\{\s*ref\s*"fact_dwell"\s*\}\}`:               "fact_dwell",
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
