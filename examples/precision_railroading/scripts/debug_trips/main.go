// Debug tool to inspect trip segments and cycle matching
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
		fmt.Printf("❌ Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	fmt.Println("=== Trip Segments Analysis ===")

	// Check trip segments by car
	rows, err := db.Query(`
		SELECT 
			car_number,
			trip_segment_id,
			trip_start_timestamp,
			is_loaded_trip,
			origin_splc_code,
			destination_splc_code
		FROM int_trip_segments
		ORDER BY car_number, trip_start_timestamp
		LIMIT 50
	`)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Trip Segments (first 50):")
	for rows.Next() {
		var carNumber string
		var tripID int
		var timestamp string
		var isLoaded int
		var origin, dest string
		rows.Scan(&carNumber, &tripID, &timestamp, &isLoaded, &origin, &dest)
		tripType := "Empty"
		if isLoaded == 1 {
			tripType = "Loaded"
		}
		fmt.Printf("  Car %s: Trip %d at %s - %s: %s → %s\n",
			carNumber, tripID, timestamp, tripType, origin, dest)
	}
	rows.Close()

	// Count loaded vs empty by car
	fmt.Println("\n=== Trips by Type per Car ===")
	rows, err = db.Query(`
		SELECT 
			car_number,
			SUM(CASE WHEN is_loaded_trip = 1 THEN 1 ELSE 0 END) as loaded_count,
			SUM(CASE WHEN is_loaded_trip = 0 THEN 1 ELSE 0 END) as empty_count
		FROM int_trip_segments
		GROUP BY car_number
		LIMIT 20
	`)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	for rows.Next() {
		var carNumber string
		var loadedCount, emptyCount int
		rows.Scan(&carNumber, &loadedCount, &emptyCount)
		fmt.Printf("  Car %s: %d loaded, %d empty\n", carNumber, loadedCount, emptyCount)
	}
	rows.Close()

	// Check raw event types
	fmt.Println("\n=== Event Type Distribution ===")
	rows, err = db.Query(`
		SELECT 
			event_type,
			COUNT(*) as count
		FROM stg_clm_events
		GROUP BY event_type
		ORDER BY event_type
	`)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	for rows.Next() {
		var eventType string
		var count int
		rows.Scan(&eventType, &count)
		fmt.Printf("  %s: %d events\n", eventType, count)
	}
	rows.Close()
}
