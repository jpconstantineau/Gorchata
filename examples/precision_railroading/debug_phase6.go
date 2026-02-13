// Debug script to investigate Phase 6 test failures
package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "modernc.org/sqlite"
)

func main() {
	dbPath := "target/precision_railroading.db"

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		fmt.Printf("Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	fmt.Println("=== Investigating Phase 6 Test Failures ===")
	fmt.Println()

	// Issue 1: train_id FK violations
	fmt.Println("1. Train ID Investigation:")
	var tripTrains, dimTrains int
	db.QueryRow("SELECT COUNT(DISTINCT train_id) FROM fact_trip WHERE train_id IS NOT NULL").Scan(&tripTrains)
	db.QueryRow("SELECT COUNT(*) FROM dim_train").Scan(&dimTrains)
	fmt.Printf("   Distinct train_ids in fact_trip: %d\n", tripTrains)
	fmt.Printf("   Rows in dim_train: %d\n", dimTrains)

	// Sample train_ids from fact_trip
	fmt.Println("   Sample train_ids from fact_trip:")
	rows, _ := db.Query("SELECT DISTINCT train_id FROM fact_trip WHERE train_id IS NOT NULL LIMIT 5")
	defer rows.Close()
	for rows.Next() {
		var trainID string
		rows.Scan(&trainID)
		fmt.Printf("     %s\n", trainID)
	}

	// Sample train_ids from dim_train
	fmt.Println("   Sample train_ids from dim_train:")
	rows2, _ := db.Query("SELECT train_id FROM dim_train LIMIT 5")
	defer rows2.Close()
	for rows2.Next() {
		var trainID int
		rows2.Scan(&trainID)
		fmt.Printf("     %d\n", trainID)
	}

	fmt.Println()

	// Issue 2: psr_period values
	fmt.Println("2. PSR Period Investigation:")
	fmt.Println("   Distinct psr_period values in fact_trip:")
	rows3, _ := db.Query("SELECT DISTINCT psr_period FROM fact_trip")
	defer rows3.Close()
	for rows3.Next() {
		var psrPeriod sql.NullString
		rows3.Scan(&psrPeriod)
		if psrPeriod.Valid {
			fmt.Printf("     '%s'\n", psrPeriod.String)
		} else {
			fmt.Printf("     NULL\n")
		}
	}

	fmt.Println("   Distinct psr_period values in fact_stop_classification:")
	rows4, _ := db.Query("SELECT DISTINCT psr_period FROM fact_stop_classification")
	defer rows4.Close()
	for rows4.Next() {
		var psrPeriod sql.NullString
		rows4.Scan(&psrPeriod)
		if psrPeriod.Valid {
			fmt.Printf("     '%s'\n", psrPeriod.String)
		} else {
			fmt.Printf("     NULL\n")
		}
	}

	// Check sample timestamps
	fmt.Println()
	fmt.Println("3. Sample trip timestamps (to verify PSR period derivation):")
	rows5, _ := db.Query("SELECT trip_start_timestamp, psr_period FROM fact_trip LIMIT 5")
	defer rows5.Close()
	for rows5.Next() {
		var timestamp string
		var psrPeriod sql.NullString
		rows5.Scan(&timestamp, &psrPeriod)
		fmt.Printf("     %s -> %v\n", timestamp, psrPeriod)
	}

	fmt.Println()
	fmt.Println("4. PSR period values in int_trip_segments:")
	rows6, _ := db.Query("SELECT DISTINCT psr_period FROM int_trip_segments")
	defer rows6.Close()
	for rows6.Next() {
		var psrPeriod sql.NullString
		rows6.Scan(&psrPeriod)
		if psrPeriod.Valid {
			fmt.Printf("     '%s'\n", psrPeriod.String)
		} else {
			fmt.Printf("     NULL\n")
		}
	}

	fmt.Println()
	fmt.Println("5. Corridor assignment in fact_trip:")
	var nullCount, nonNullCount int
	db.QueryRow("SELECT COUNT(*) FROM fact_trip WHERE corridor_id IS NULL").Scan(&nullCount)
	db.QueryRow("SELECT COUNT(*) FROM fact_trip WHERE corridor_id IS NOT NULL").Scan(&nonNullCount)
	fmt.Printf("   Trips with NULL corridor_id: %d\n", nullCount)
	fmt.Printf("   Trips with corridor_id: %d\n", nonNullCount)

	if nonNullCount > 0 {
		fmt.Println("   Sample trips with corridor_id:")
		rows7, _ := db.Query("SELECT trip_segment_id, corridor_id, origin_location_id, destination_location_id FROM fact_trip WHERE corridor_id IS NOT NULL LIMIT 3")
		defer rows7.Close()
		for rows7.Next() {
			var tripID, corridorID, originID, destID int
			rows7.Scan(&tripID, &corridorID, &originID, &destID)
			fmt.Printf("     Trip %d: Corridor %d (Origin %d -> Dest %d)\n", tripID, corridorID, originID, destID)
		}
	}
}
