// Phase 5 Build Tool
// Go-based tool to build velocity and dwell analysis models
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
	fmt.Println("=== Precision Railroading Phase 5: Velocity & Dwell Analysis ===")
	fmt.Println()

	dbPath := "target/precision_railroading.db"

	// Check if database exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		fmt.Println("❌ Database not found. Run build_phase4.ps1 first to create intermediate tables.")
		os.Exit(1)
	}

	// Open database connection
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		fmt.Printf("❌ Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Drop existing Phase 5 tables
	fmt.Println("Dropping existing Phase 5 tables...")
	dropTables := []string{
		"DROP TABLE IF EXISTS int_dwell_classification",
		"DROP TABLE IF EXISTS int_nodal_dwell",
		"DROP TABLE IF EXISTS int_velocity_vectors",
	}
	for _, dropSQL := range dropTables {
		if _, err := db.Exec(dropSQL); err != nil {
			fmt.Printf("❌ Failed to drop table: %v\n", err)
			os.Exit(1)
		}
	}

	// Models to build in dependency order
	models := []Model{
		{Name: "int_velocity_vectors", Path: "models/intermediate/int_velocity_vectors.sql"},
		{Name: "int_nodal_dwell", Path: "models/intermediate/int_nodal_dwell.sql"},
		{Name: "int_dwell_classification", Path: "models/intermediate/int_dwell_classification.sql"},
	}

	fmt.Println("Building Phase 5 models...")

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
	fmt.Println("\nVerifying Phase 5 tables...")

	velocityVectors, _ := getRowCount(db, "int_velocity_vectors")
	nodalDwell, _ := getRowCount(db, "int_nodal_dwell")
	dwellClassification, _ := getRowCount(db, "int_dwell_classification")

	fmt.Printf("  int_velocity_vectors: %d rows\n", velocityVectors)
	fmt.Printf("  int_nodal_dwell: %d rows\n", nodalDwell)
	fmt.Printf("  int_dwell_classification: %d rows\n", dwellClassification)

	// Verify data quality metrics
	fmt.Println("\nVerifying data quality...")

	// Velocity stats
	var avgVelocity, minVelocity, maxVelocity float64
	err = db.QueryRow(`
		SELECT 
			AVG(velocity_mph) as avg_velocity,
			MIN(velocity_mph) as min_velocity,
			MAX(velocity_mph) as max_velocity
		FROM int_velocity_vectors
	`).Scan(&avgVelocity, &minVelocity, &maxVelocity)
	if err != nil {
		fmt.Printf("  Warning: Could not get velocity stats: %v\n", err)
	} else {
		fmt.Printf("  Avg velocity: %.2f mph\n", avgVelocity)
		fmt.Printf("  Min velocity: %.2f mph\n", minVelocity)
		fmt.Printf("  Max velocity: %.2f mph\n", maxVelocity)
	}

	// Dwell stats
	var avgDwell, minDwell, maxDwell float64
	err = db.QueryRow(`
		SELECT 
			AVG(dwell_duration_minutes) as avg_dwell,
			MIN(dwell_duration_minutes) as min_dwell,
			MAX(dwell_duration_minutes) as max_dwell
		FROM int_nodal_dwell
	`).Scan(&avgDwell, &minDwell, &maxDwell)
	if err != nil {
		fmt.Printf("  Warning: Could not get dwell stats: %v\n", err)
	} else {
		fmt.Printf("  Avg dwell: %.2f minutes\n", avgDwell)
		fmt.Printf("  Min dwell: %.2f minutes\n", minDwell)
		fmt.Printf("  Max dwell: %.2f minutes\n", maxDwell)
	}

	// Shadow yard detection
	var shadowYardCount int
	err = db.QueryRow(`
		SELECT COUNT(DISTINCT location_id)
		FROM int_dwell_classification
		WHERE shadow_yard_flag = 1
	`).Scan(&shadowYardCount)
	if err != nil {
		fmt.Printf("  Warning: Could not get shadow yard count: %v\n", err)
	} else {
		fmt.Printf("  Shadow yard locations detected: %d\n", shadowYardCount)
	}

	// Classification breakdown
	fmt.Println("\nDwell classification breakdown:")
	classRows, err := db.Query(`
		SELECT dwell_classification, COUNT(*) as count
		FROM int_dwell_classification
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

	fmt.Println("\n✓ Phase 5 build complete!")
	fmt.Println("Run test_phase5.ps1 to execute data quality tests.")
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
		`\{\{\s*ref\s*"int_velocity_vectors"\s*\}\}`:     "int_velocity_vectors",
		`\{\{\s*ref\s*"int_nodal_dwell"\s*\}\}`:          "int_nodal_dwell",
		`\{\{\s*ref\s*"int_dwell_classification"\s*\}\}`: "int_dwell_classification",
		`\{\{\s*ref\s*"dim_railcar"\s*\}\}`:              "dim_railcar",
		`\{\{\s*ref\s*"dim_location"\s*\}\}`:             "dim_location",
		`\{\{\s*ref\s*"dim_corridor"\s*\}\}`:             "dim_corridor",
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
