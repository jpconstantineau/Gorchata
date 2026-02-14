package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "modernc.org/sqlite"
)

func main() {
	db, err := sql.Open("sqlite", "target/precision_railroading.db")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	rows, err := db.Query("SELECT psr_period, trip_count, avg_duration_minutes FROM psr_strategy_shifts ORDER BY psr_period")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()

	fmt.Println("PSR Period | Trip Count | Avg Duration")
	fmt.Println("-----------|------------|-------------")
	for rows.Next() {
		var period string
		var tripCount int
		var avgDuration float64
		rows.Scan(&period, &tripCount, &avgDuration)
		fmt.Printf("%-10s | %10d | %12.2f\n", period, tripCount, avgDuration)
	}
}
