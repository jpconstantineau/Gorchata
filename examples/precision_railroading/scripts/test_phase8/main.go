// Phase 8 Test Tool
// Go-based tool to test analytics queries
package main

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"

	_ "modernc.org/sqlite"
)

type TestFile struct {
	Name string
	Path string
}

type TestResult struct {
	TestName       string
	ViolationCount int
	Description    string
	Status         string
}

func main() {
	fmt.Println("=== Precision Railroading Phase 8: Analytics Queries Tests ===")
	fmt.Println()

	dbPath := "target/precision_railroading.db"

	// Check if database exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		fmt.Println("❌ Database not found. Run build_phase7.ps1 first.")
		os.Exit(1)
	}

	// Open database connection
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		fmt.Printf("❌ Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Test files to execute
	testFiles := []TestFile{
		{Name: "worst_performing_corridors", Path: "tests/analytics/test_worst_performing_corridors.sql"},
		{Name: "shadow_yard_identification", Path: "tests/analytics/test_shadow_yard_identification.sql"},
		{Name: "seasonal_performance_trends", Path: "tests/analytics/test_seasonal_performance_trends.sql"},
		{Name: "psr_strategy_shifts", Path: "tests/analytics/test_psr_strategy_shifts.sql"},
		{Name: "network_congestion_hotspots", Path: "tests/analytics/test_network_congestion_hotspots.sql"},
		{Name: "directional_efficiency_analysis", Path: "tests/analytics/test_directional_efficiency_analysis.sql"},
	}

	allTestsPassed := true
	totalTests := 0
	passedTests := 0
	failedTests := 0

	for _, testFile := range testFiles {
		fmt.Printf("Running tests for %s...\n", testFile.Name)
		fmt.Println()

		// Read test SQL file
		sqlBytes, err := os.ReadFile(testFile.Path)
		if err != nil {
			fmt.Printf("❌ Error reading test file: %v\n\n", err)
			allTestsPassed = false
			continue
		}

		sqlContent := string(sqlBytes)

		// Process template
		sqlContent = processTemplate(sqlContent)

		// Execute test query
		rows, err := db.Query(sqlContent)
		if err != nil {
			fmt.Printf("❌ Error executing test: %v\n\n", err)
			allTestsPassed = false
			continue
		}

		// Read test results
		for rows.Next() {
			var result TestResult
			if err := rows.Scan(&result.TestName, &result.ViolationCount, &result.Description, &result.Status); err != nil {
				fmt.Printf("❌ Error reading test result: %v\n", err)
				continue
			}

			totalTests++

			if result.Status == "PASS" {
				passedTests++
				fmt.Printf("  ✓ %s\n", result.TestName)
				fmt.Printf("    %s\n", result.Description)
			} else {
				failedTests++
				allTestsPassed = false
				fmt.Printf("  ✗ %s\n", result.TestName)
				fmt.Printf("    %s\n", result.Description)
				fmt.Printf("    Violations: %d\n", result.ViolationCount)
			}
		}
		rows.Close()
		fmt.Println()
	}

	// Summary
	fmt.Println("=== Test Summary ===")
	fmt.Printf("Total tests: %d\n", totalTests)
	fmt.Printf("Passed: %d\n", passedTests)
	fmt.Printf("Failed: %d\n", failedTests)
	fmt.Println()

	if allTestsPassed {
		fmt.Println("✓ All tests passed!")
		os.Exit(0)
	} else {
		fmt.Println("❌ Some tests failed. Review the output above.")
		os.Exit(1)
	}
}

func processTemplate(sql string) string {
	// Replace ref() calls for analytics queries
	refPatterns := map[string]string{
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
		`\{\{\s*ref\s*"fact_trip"\s*\}\}`:             "fact_trip",
		`\{\{\s*ref\s*"fact_dwell"\s*\}\}`:            "fact_dwell",
		`\{\{\s*ref\s*"fact_corridor_transit"\s*\}\}`: "fact_corridor_transit",
		`\{\{\s*ref\s*"dim_location"\s*\}\}`:          "dim_location",
		`\{\{\s*ref\s*"dim_corridor"\s*\}\}`:          "dim_corridor",
		`\{\{\s*ref\s*"dim_date"\s*\}\}`:              "dim_date",
	}

	for pattern, replacement := range refPatterns {
		re := regexp.MustCompile(pattern)
		sql = re.ReplaceAllString(sql, replacement)
	}

	return strings.TrimSpace(sql)
}
