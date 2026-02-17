// Phase 7 Test Tool
// Go-based tool to test metrics aggregation tables
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
	fmt.Println("=== Precision Railroading Phase 7: Metrics & Aggregations Tests ===")
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
		{Name: "agg_network_fluidity", Path: "tests/metrics/test_agg_network_fluidity.sql"},
		{Name: "agg_slot_adherence", Path: "tests/metrics/test_agg_slot_adherence.sql"},
		{Name: "agg_shadow_yards", Path: "tests/metrics/test_agg_shadow_yards.sql"},
		{Name: "agg_buffer_consumption", Path: "tests/metrics/test_agg_buffer_consumption.sql"},
		{Name: "agg_directional_asymmetry", Path: "tests/metrics/test_agg_directional_asymmetry.sql"},
		{Name: "agg_corridor_weekly_performance", Path: "tests/metrics/test_agg_corridor_weekly_performance.sql"},
		{Name: "agg_psr_evolution", Path: "tests/metrics/test_agg_psr_evolution.sql"},
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
	// Replace ref() calls
	refPatterns := map[string]string{
		`\{\{\s*ref\s*"agg_network_fluidity"\s*\}\}`:            "agg_network_fluidity",
		`\{\{\s*ref\s*"agg_slot_adherence"\s*\}\}`:              "agg_slot_adherence",
		`\{\{\s*ref\s*"agg_shadow_yards"\s*\}\}`:                "agg_shadow_yards",
		`\{\{\s*ref\s*"agg_buffer_consumption"\s*\}\}`:          "agg_buffer_consumption",
		`\{\{\s*ref\s*"agg_directional_asymmetry"\s*\}\}`:       "agg_directional_asymmetry",
		`\{\{\s*ref\s*"agg_corridor_weekly_performance"\s*\}\}`: "agg_corridor_weekly_performance",
		`\{\{\s*ref\s*"agg_psr_evolution"\s*\}\}`:               "agg_psr_evolution",
		`\{\{\s*ref\s*"fact_trip"\s*\}\}`:                       "fact_trip",
		`\{\{\s*ref\s*"fact_dwell"\s*\}\}`:                      "fact_dwell",
		`\{\{\s*ref\s*"fact_corridor_transit"\s*\}\}`:           "fact_corridor_transit",
		`\{\{\s*ref\s*"dim_location"\s*\}\}`:                    "dim_location",
		`\{\{\s*ref\s*"dim_corridor"\s*\}\}`:                    "dim_corridor",
	}

	for pattern, replacement := range refPatterns {
		re := regexp.MustCompile(pattern)
		sql = re.ReplaceAllString(sql, replacement)
	}

	return strings.TrimSpace(sql)
}
