// Diagnostic script to investigate velocity vector issue
package main

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

func main() {
	db, err := sql.Open("sqlite", "target/precision_railroading.db")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer db.Close()

	// Check trip segments
	var tripCount, minDuration, maxDuration int
	err = db.QueryRow(`
		SELECT COUNT(*), MIN(trip_duration_minutes), MAX(trip_duration_minutes)
		FROM int_trip_segments
	`).Scan(&tripCount, &minDuration, &maxDuration)
	if err != nil {
		fmt.Printf("Error getting trip segments: %v\n", err)
	} else {
		fmt.Printf("Trip segments: %d (duration: %d-%d minutes)\n", tripCount, minDuration, maxDuration)
	}

	// Check if any trips have origin != destination
	var differentLocations int
	err = db.QueryRow(`
		SELECT COUNT(*)
		FROM int_trip_segments
		WHERE origin_location_id != destination_location_id
	`).Scan(&differentLocations)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("Trips with different origin/destination: %d\n", differentLocations)
	}

	// Check corridor matches
	var corridorMatches int
	err = db.QueryRow(`
		SELECT COUNT(*)
		FROM int_trip_segments ts
		JOIN dim_corridor c
		  ON ts.origin_splc_code = c.origin_splc
		  AND ts.destination_splc_code = c.destination_splc
	`).Scan(&corridorMatches)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("Trips with corridor matches: %d\n", corridorMatches)
	}

	// Sample trip segment
	fmt.Println("\nSample trip segment:")
	rows, err := db.Query(`
		SELECT 
			trip_segment_id,
			origin_location_id,
			destination_location_id,
			trip_duration_minutes,
			origin_splc_code,
			destination_splc_code
		FROM int_trip_segments
		LIMIT 5
	`)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		defer rows.Close()
		for rows.Next() {
			var id, orig, dest, dur int
			var origSplc, destSplc string
			if err := rows.Scan(&id, &orig, &dest, &dur, &origSplc, &destSplc); err == nil {
				fmt.Printf("  Trip %d: %s (%d) -> %s (%d), duration: %d min\n",
					id, origSplc, orig, destSplc, dest, dur)
			}
		}
	}
}
