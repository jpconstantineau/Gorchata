// Phase 8 Build Tool
// Go-based tool to build analytics query tables
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
	fmt.Println("=== Precision Railroading Phase 8: Analytics Queries ===")
	fmt.Println()

	dbPath := "target/precision_railroading.db"

	// Check if database exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		fmt.Println("❌ Database not found. Run build_phase7.ps1 first to create metrics tables.")
		os.Exit(1)
	}

	// Open database connection
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		fmt.Printf("❌ Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Drop existing Phase 8 tables
	fmt.Println("Dropping existing Phase 8 tables...")
	dropTables := []string{
		"DROP TABLE IF EXISTS directional_efficiency_analysis",
		"DROP TABLE IF EXISTS network_congestion_hotspots",
		"DROP TABLE IF EXISTS psr_strategy_shifts",
		"DROP TABLE IF EXISTS seasonal_performance_trends",
		"DROP TABLE IF EXISTS shadow_yard_identification",
		"DROP TABLE IF EXISTS worst_performing_corridors",
	}
	for _, dropSQL := range dropTables {
		if _, err := db.Exec(dropSQL); err != nil {
			fmt.Printf("❌ Failed to drop table: %v\n", err)
			os.Exit(1)
		}
	}

	// Models to build in dependency order
	models := []Model{
		{Name: "worst_performing_corridors", Path: "models/analytics/worst_performing_corridors.sql"},
		{Name: "shadow_yard_identification", Path: "models/analytics/shadow_yard_identification.sql"},
		{Name: "seasonal_performance_trends", Path: "models/analytics/seasonal_performance_trends.sql"},
		{Name: "psr_strategy_shifts", Path: "models/analytics/psr_strategy_shifts.sql"},
		{Name: "network_congestion_hotspots", Path: "models/analytics/network_congestion_hotspots.sql"},
		{Name: "directional_efficiency_analysis", Path: "models/analytics/directional_efficiency_analysis.sql"},
	}

	fmt.Println("Building Phase 8 models...")

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
	fmt.Println("\nVerifying Phase 8 tables...")

	worstCorridors, _ := getRowCount(db, "worst_performing_corridors")
	shadowYards, _ := getRowCount(db, "shadow_yard_identification")
	seasonalTrends, _ := getRowCount(db, "seasonal_performance_trends")
	psrShifts, _ := getRowCount(db, "psr_strategy_shifts")
	congestionHotspots, _ := getRowCount(db, "network_congestion_hotspots")
	directionalEfficiency, _ := getRowCount(db, "directional_efficiency_analysis")

	fmt.Printf("  worst_performing_corridors: %d rows\n", worstCorridors)
	fmt.Printf("  shadow_yard_identification: %d rows\n", shadowYards)
	fmt.Printf("  seasonal_performance_trends: %d rows\n", seasonalTrends)
	fmt.Printf("  psr_strategy_shifts: %d rows\n", psrShifts)
	fmt.Printf("  network_congestion_hotspots: %d rows\n", congestionHotspots)
	fmt.Printf("  directional_efficiency_analysis: %d rows\n", directionalEfficiency)

	// Verify specific analytics
	fmt.Println("\nVerifying key analytics...")

	// PSR shifts check
	var prePsr, transition, mature int
	db.QueryRow(`
		SELECT 
			SUM(CASE WHEN psr_period = 'pre-PSR' THEN 1 ELSE 0 END),
			SUM(CASE WHEN psr_period = 'transition' THEN 1 ELSE 0 END),
			SUM(CASE WHEN psr_period = 'mature' THEN 1 ELSE 0 END)
		FROM psr_strategy_shifts
	`).Scan(&prePsr, &transition, &mature)
	fmt.Printf("  PSR periods analyzed: pre-PSR=%d, transition=%d, mature=%d\n", prePsr, transition, mature)

	// Shadow yards flagged
	var shadowYardsFlagged int
	db.QueryRow(`
		SELECT COUNT(*)
		FROM shadow_yard_identification
		WHERE shadow_yard_flag = 1
	`).Scan(&shadowYardsFlagged)
	fmt.Printf("  Shadow yards flagged: %d locations\n", shadowYardsFlagged)

	// Congestion hotspots by severity
	var critical, high, moderate int
	db.QueryRow(`
		SELECT 
			SUM(CASE WHEN congestion_severity = 'Critical' THEN 1 ELSE 0 END),
			SUM(CASE WHEN congestion_severity = 'High' THEN 1 ELSE 0 END),
			SUM(CASE WHEN congestion_severity = 'Moderate' THEN 1 ELSE 0 END)
		FROM network_congestion_hotspots
	`).Scan(&critical, &high, &moderate)
	fmt.Printf("  Congestion severity: Critical=%d, High=%d, Moderate=%d\n", critical, high, moderate)

	fmt.Println("\n✓ Phase 8 build complete!")
	fmt.Println("Run test_phase8.ps1 to execute data quality tests.")
}

func processTemplate(sql string) string {
	// Remove config directives
	re := regexp.MustCompile(`\{\{\s*config\s*"materialized"\s*"(table|view)"\s*\}\}`)
	sql = re.ReplaceAllString(sql, "")

	// Replace ref() calls for all tables
	refPatterns := map[string]string{
		// Analytics references (for cross-analytics dependencies if any)
		`\{\{\s*ref\s*"worst_performing_corridors"\s*\}\}`:      "worst_performing_corridors",
		`\{\{\s*ref\s*"shadow_yard_identification"\s*\}\}`:      "shadow_yard_identification",
		`\{\{\s*ref\s*"seasonal_performance_trends"\s*\}\}`:     "seasonal_performance_trends",
		`\{\{\s*ref\s*"psr_strategy_shifts"\s*\}\}`:             "psr_strategy_shifts",
		`\{\{\s*ref\s*"network_congestion_hotspots"\s*\}\}`:     "network_congestion_hotspots",
		`\{\{\s*ref\s*"directional_efficiency_analysis"\s*\}\}`: "directional_efficiency_analysis",
		// Metrics references
		`\{\{\s*ref\s*"agg_network_fluidity"\s*\}\}`:            "agg_network_fluidity",
		`\{\{\s*ref\s*"agg_slot_adherence"\s*\}\}`:              "agg_slot_adherence",
		`\{\{\s*ref\s*"agg_shadow_yards"\s*\}\}`:                "agg_shadow_yards",
		`\{\{\s*ref\s*"agg_buffer_consumption"\s*\}\}`:          "agg_buffer_consumption",
		`\{\{\s*ref\s*"agg_directional_asymmetry"\s*\}\}`:       "agg_directional_asymmetry",
		`\{\{\s*ref\s*"agg_corridor_weekly_performance"\s*\}\}`: "agg_corridor_weekly_performance",
		`\{\{\s*ref\s*"agg_psr_evolution"\s*\}\}`:               "agg_psr_evolution",
		// Fact and dimension references
		`\{\{\s*ref\s*"fact_trip"\s*\}\}`:                "fact_trip",
		`\{\{\s*ref\s*"fact_dwell"\s*\}\}`:               "fact_dwell",
		`\{\{\s*ref\s*"fact_corridor_transit"\s*\}\}`:    "fact_corridor_transit",
		`\{\{\s*ref\s*"fact_stop_classification"\s*\}\}`: "fact_stop_classification",
		`\{\{\s*ref\s*"dim_location"\s*\}\}`:             "dim_location",
		`\{\{\s*ref\s*"dim_corridor"\s*\}\}`:             "dim_corridor",
		`\{\{\s*ref\s*"dim_date"\s*\}\}`:                 "dim_date",
		`\{\{\s*ref\s*"dim_railcar"\s*\}\}`:              "dim_railcar",
		`\{\{\s*ref\s*"dim_train"\s*\}\}`:                "dim_train",
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
