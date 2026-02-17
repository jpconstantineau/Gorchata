// Debug tool to check trip patterns
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

	// Check same origin/destination percentage
	var sameLocTrips, totalTrips int
	err = db.QueryRow(`
		SELECT 
			SUM(CASE WHEN origin_location_id = destination_location_id THEN 1 ELSE 0 END),
			COUNT(*)
		FROM int_trip_segments
	`).Scan(&sameLocTrips, &totalTrips)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Trip segments with same origin/destination: %d/%d (%.1f%%)\n\n",
		sameLocTrips, totalTrips, float64(sameLocTrips)/float64(totalTrips)*100)

	// Check cycle endpoint matching
	var mismatchOrigin, mismatchDest, totalCycles int
	err = db.QueryRow(`
		SELECT 
			SUM(CASE WHEN empty_origin_splc != loaded_destination_splc THEN 1 ELSE 0 END),
			SUM(CASE WHEN empty_destination_splc != loaded_origin_splc THEN 1 ELSE 0 END),
			COUNT(*)
		FROM int_cycle_classification
	`).Scan(&mismatchOrigin, &mismatchDest, &totalCycles)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Cycle endpoint analysis:\n")
	fmt.Printf("  Empty origin != loaded destination: %d/%d (%.1f%%)\n",
		mismatchOrigin, totalCycles, float64(mismatchOrigin)/float64(totalCycles)*100)
	fmt.Printf("  Empty destination != loaded origin: %d/%d (%.1f%%)\n\n",
		mismatchDest, totalCycles, float64(mismatchDest)/float64(totalCycles)*100)

	// Sample a few cycles to see the pattern
	fmt.Println("Sample cycles:")
	rows, _ := db.Query(`
		SELECT 
			car_number,
			loaded_origin_splc,
			loaded_destination_splc,
			empty_origin_splc,
			empty_destination_splc
		FROM int_cycle_classification
		LIMIT 5
	`)
	for rows.Next() {
		var car, loadedOrig, loadedDest, emptyOrig, emptyDest string
		rows.Scan(&car, &loadedOrig, &loadedDest, &emptyOrig, &emptyDest)
		fmt.Printf("  %s: Loaded %s→%s, Empty %s→%s\n",
			car, loadedOrig, loadedDest, emptyOrig, emptyDest)
	}
	rows.Close()
}
