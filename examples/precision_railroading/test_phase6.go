// Phase 6 Test Tool
// Go-based tool to test fact tables
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
	fmt.Println("=== Precision Railroading Phase 6: Fact Tables & Stop Classification Tests ===")
	fmt.Println()

	dbPath := "target/precision_railroading.db"

	// Check if database exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		fmt.Println("❌ Database not found. Run build_phase6.ps1 first.")
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
		{Name: "fact_trip", Path: "tests/facts/test_fact_trip.sql"},
		{Name: "fact_dwell", Path: "tests/facts/test_fact_dwell.sql"},
		{Name: "fact_stop_classification", Path: "tests/facts/test_fact_stop_classification.sql"},
		{Name: "fact_corridor_transit", Path: "tests/facts/test_fact_corridor_transit.sql"},
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
		`\{\{\s*ref\s*"fact_stop_classification"\s*\}\}`: "fact_stop_classification",
		`\{\{\s*ref\s*"fact_corridor_transit"\s*\}\}`:    "fact_corridor_transit",
	}

	for pattern, replacement := range refPatterns {
		re := regexp.MustCompile(pattern)
		sql = re.ReplaceAllString(sql, replacement)
	}

	return strings.TrimSpace(sql)
}
