package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "modernc.org/sqlite"
)

// verify_phase3.go - Go program to verify Phase 3 staging layer data quality
// Usage: go run verify_phase3.go
// Uses pure Go SQLite driver (no CGO required)

func main() {
	dbPath := "target/precision_railroading.db"

	// Check if database exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		fmt.Println("❌ Database not found. Run build_phase3.ps1 first.")
		os.Exit(1)
	}

	// Open database
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		fmt.Printf("❌ Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	fmt.Println("=== Phase 3 Verification Report ===")
	fmt.Println()

	// Check row counts
	fmt.Println("📊 Row Counts:")
	if err := checkRowCount(db, "stg_clm_events"); err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		os.Exit(1)
	}
	if err := checkRowCount(db, "stg_clm_enriched"); err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println()

	// Check dimension join success rates
	fmt.Println("🔗 Dimension Join Success:")
	if err := checkDimensionJoins(db); err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println()

	// Check event type distribution
	fmt.Println("📈 Event Type Distribution:")
	if err := checkEventTypes(db); err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println()

	// Check derived field correctness
	fmt.Println("✅ Derived Fields:")
	if err := checkDerivedFields(db); err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println()

	// Check temporal ordering
	fmt.Println("⏱️  Temporal Ordering:")
	if err := checkTemporalOrdering(db); err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println()
	fmt.Println("✅ Phase 3 verification complete!")
}

func checkRowCount(db *sql.DB, tableName string) error {
	var count int
	err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to count %s: %w", tableName, err)
	}
	fmt.Printf("  %s: %d rows\n", tableName, count)
	return nil
}

func checkDimensionJoins(db *sql.DB) error {
	query := `
	SELECT 
		COUNT(*) as total,
		SUM(CASE WHEN location_id IS NOT NULL THEN 1 ELSE 0 END) as has_location,
		SUM(CASE WHEN railcar_id IS NOT NULL THEN 1 ELSE 0 END) as has_railcar,
		SUM(CASE WHEN date_id IS NOT NULL THEN 1 ELSE 0 END) as has_date
	FROM stg_clm_enriched
	`

	var total, hasLocation, hasRailcar, hasDate int
	err := db.QueryRow(query).Scan(&total, &hasLocation, &hasRailcar, &hasDate)
	if err != nil {
		return fmt.Errorf("failed to check dimension joins: %w", err)
	}

	fmt.Printf("  Location: %d/%d (%.1f%%)\n", hasLocation, total, float64(hasLocation)/float64(total)*100)
	fmt.Printf("  Railcar:  %d/%d (%.1f%%)\n", hasRailcar, total, float64(hasRailcar)/float64(total)*100)
	fmt.Printf("  Date:     %d/%d (%.1f%%)\n", hasDate, total, float64(hasDate)/float64(total)*100)

	if hasLocation != total || hasRailcar != total || hasDate != total {
		fmt.Println("  ⚠️  Warning: Not all events have complete dimension joins")
	}

	return nil
}

func checkEventTypes(db *sql.DB) error {
	query := `
	SELECT event_type, COUNT(*) as cnt
	FROM stg_clm_enriched
	GROUP BY event_type
	ORDER BY cnt DESC
	`

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to check event types: %w", err)
	}
	defer rows.Close()

	var total int
	types := make(map[string]int)

	for rows.Next() {
		var eventType string
		var count int
		if err := rows.Scan(&eventType, &count); err != nil {
			return err
		}
		types[eventType] = count
		total += count
	}

	for eventType, count := range types {
		pct := float64(count) / float64(total) * 100
		fmt.Printf("  %s: %d (%.1f%%)\n", eventType, count, pct)
	}

	return nil
}

func checkDerivedFields(db *sql.DB) error {
	// Check is_loaded_event logic
	query := `
	SELECT 
		SUM(CASE WHEN event_type = 'PLAC' AND is_loaded_event = 1 THEN 1 ELSE 0 END) as plac_correct,
		SUM(CASE WHEN event_type = 'PLAC' THEN 1 ELSE 0 END) as plac_total,
		SUM(CASE WHEN event_type = 'PULL' AND is_loaded_event = 0 THEN 1 ELSE 0 END) as pull_correct,
		SUM(CASE WHEN event_type = 'PULL' THEN 1 ELSE 0 END) as pull_total
	FROM stg_clm_enriched
	`

	var placCorrect, placTotal, pullCorrect, pullTotal int
	err := db.QueryRow(query).Scan(&placCorrect, &placTotal, &pullCorrect, &pullTotal)
	if err != nil {
		return fmt.Errorf("failed to check derived fields: %w", err)
	}

	fmt.Printf("  is_loaded_event (PLAC): %d/%d correct\n", placCorrect, placTotal)
	fmt.Printf("  is_loaded_event (PULL): %d/%d correct\n", pullCorrect, pullTotal)

	if placCorrect == placTotal && pullCorrect == pullTotal {
		fmt.Println("  ✅ Loaded event logic is correct")
	} else {
		fmt.Println("  ⚠️  Warning: Loaded event logic has issues")
	}

	return nil
}

func checkTemporalOrdering(db *sql.DB) error {
	query := `
	SELECT COUNT(*) FROM (
		SELECT 
			car_number,
			timestamp,
			event_sequence,
			LAG(timestamp) OVER (PARTITION BY car_number ORDER BY event_sequence) AS prev_timestamp
		FROM stg_clm_enriched
	) WHERE prev_timestamp IS NOT NULL AND timestamp < prev_timestamp
	`

	var violations int
	err := db.QueryRow(query).Scan(&violations)
	if err != nil {
		return fmt.Errorf("failed to check temporal ordering: %w", err)
	}

	if violations == 0 {
		fmt.Println("  ✅ All events are in correct temporal order per railcar")
	} else {
		fmt.Printf("  ⚠️  Warning: %d temporal ordering violations found\n", violations)
	}

	return nil
}
