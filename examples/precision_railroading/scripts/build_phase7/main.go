// Phase 7 Build Tool
// Go-based tool to build metrics aggregation tables
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
	fmt.Println("=== Precision Railroading Phase 7: Metrics & Aggregations ===")
	fmt.Println()

	dbPath := "target/precision_railroading.db"

	// Check if database exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		fmt.Println("❌ Database not found. Run build_phase6.ps1 first to create fact tables.")
		os.Exit(1)
	}

	// Open database connection
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		fmt.Printf("❌ Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Drop existing Phase 7 tables
	fmt.Println("Dropping existing Phase 7 tables...")
	dropTables := []string{
		"DROP TABLE IF EXISTS agg_psr_evolution",
		"DROP TABLE IF EXISTS agg_corridor_weekly_performance",
		"DROP TABLE IF EXISTS agg_directional_asymmetry",
		"DROP TABLE IF EXISTS agg_buffer_consumption",
		"DROP TABLE IF EXISTS agg_shadow_yards",
		"DROP TABLE IF EXISTS agg_slot_adherence",
		"DROP TABLE IF EXISTS agg_network_fluidity",
	}
	for _, dropSQL := range dropTables {
		if _, err := db.Exec(dropSQL); err != nil {
			fmt.Printf("❌ Failed to drop table: %v\n", err)
			os.Exit(1)
		}
	}

	// Models to build in dependency order
	models := []Model{
		{Name: "agg_network_fluidity", Path: "models/metrics/agg_network_fluidity.sql"},
		{Name: "agg_slot_adherence", Path: "models/metrics/agg_slot_adherence.sql"},
		{Name: "agg_shadow_yards", Path: "models/metrics/agg_shadow_yards.sql"},
		{Name: "agg_buffer_consumption", Path: "models/metrics/agg_buffer_consumption.sql"},
		{Name: "agg_directional_asymmetry", Path: "models/metrics/agg_directional_asymmetry.sql"},
		{Name: "agg_corridor_weekly_performance", Path: "models/metrics/agg_corridor_weekly_performance.sql"},
		{Name: "agg_psr_evolution", Path: "models/metrics/agg_psr_evolution.sql"},
	}

	fmt.Println("Building Phase 7 models...")

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
	fmt.Println("\nVerifying Phase 7 tables...")

	aggNetworkFluidity, _ := getRowCount(db, "agg_network_fluidity")
	aggSlotAdherence, _ := getRowCount(db, "agg_slot_adherence")
	aggShadowYards, _ := getRowCount(db, "agg_shadow_yards")
	aggBufferConsumption, _ := getRowCount(db, "agg_buffer_consumption")
	aggDirectionalAsymmetry, _ := getRowCount(db, "agg_directional_asymmetry")
	aggCorridorWeeklyPerformance, _ := getRowCount(db, "agg_corridor_weekly_performance")
	aggPsrEvolution, _ := getRowCount(db, "agg_psr_evolution")

	fmt.Printf("  agg_network_fluidity: %d rows\n", aggNetworkFluidity)
	fmt.Printf("  agg_slot_adherence: %d rows\n", aggSlotAdherence)
	fmt.Printf("  agg_shadow_yards: %d rows\n", aggShadowYards)
	fmt.Printf("  agg_buffer_consumption: %d rows\n", aggBufferConsumption)
	fmt.Printf("  agg_directional_asymmetry: %d rows\n", aggDirectionalAsymmetry)
	fmt.Printf("  agg_corridor_weekly_performance: %d rows\n", aggCorridorWeeklyPerformance)
	fmt.Printf("  agg_psr_evolution: %d rows\n", aggPsrEvolution)

	// Verify specific metrics
	fmt.Println("\nVerifying key metrics...")

	// PSR Evolution check
	var prePsr, transition, mature int
	db.QueryRow(`
		SELECT 
			SUM(CASE WHEN psr_period = 'pre-PSR' THEN 1 ELSE 0 END),
			SUM(CASE WHEN psr_period = 'transition' THEN 1 ELSE 0 END),
			SUM(CASE WHEN psr_period = 'mature' THEN 1 ELSE 0 END)
		FROM agg_psr_evolution
	`).Scan(&prePsr, &transition, &mature)
	fmt.Printf("  PSR periods: pre-PSR=%d, transition=%d, mature=%d\n", prePsr, transition, mature)

	// Shadow yards detection
	var shadowYardLocations int
	db.QueryRow(`
		SELECT COUNT(*)
		FROM agg_shadow_yards
		WHERE shadow_yard_percentage > 30
	`).Scan(&shadowYardLocations)
	fmt.Printf("  Shadow yard locations (>30%%): %d\n", shadowYardLocations)

	fmt.Println("\n✓ Phase 7 build complete!")
	fmt.Println("Run test_phase7.ps1 to execute data quality tests.")
}

func processTemplate(sql string) string {
	// Remove config directives
	re := regexp.MustCompile(`\{\{\s*config\s*"materialized"\s*"(table|view)"\s*\}\}`)
	sql = re.ReplaceAllString(sql, "")

	// Replace ref() calls
	refPatterns := map[string]string{
		`\{\{\s*ref\s*"fact_trip"\s*\}\}`:                "fact_trip",
		`\{\{\s*ref\s*"fact_dwell"\s*\}\}`:               "fact_dwell",
		`\{\{\s*ref\s*"fact_corridor_transit"\s*\}\}`:    "fact_corridor_transit",
		`\{\{\s*ref\s*"fact_stop_classification"\s*\}\}`: "fact_stop_classification",
		`\{\{\s*ref\s*"dim_location"\s*\}\}`:             "dim_location",
		`\{\{\s*ref\s*"dim_corridor"\s*\}\}`:             "dim_corridor",
		`\{\{\s*ref\s*"dim_date"\s*\}\}`:                 "dim_date",
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
