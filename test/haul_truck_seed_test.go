package test

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// TestHaulTruckSeedConfiguration validates seed YAML parses (if needed)
func TestHaulTruckSeedConfiguration(t *testing.T) {
	repoRoot := getRepoRoot(t)
	seedPath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "seeds", "seed.yml")

	// Check if seed.yml exists
	if _, err := os.Stat(seedPath); os.IsNotExist(err) {
		t.Skip("seed.yml not required for static CSV seeds")
		return
	}

	// Parse seed YAML
	data, err := os.ReadFile(seedPath)
	if err != nil {
		t.Fatalf("Failed to read seed.yml: %v", err)
	}

	var seedConfig map[string]interface{}
	if err := yaml.Unmarshal(data, &seedConfig); err != nil {
		t.Fatalf("Failed to parse seed.yml: %v", err)
	}

	// Verify version exists
	if _, ok := seedConfig["version"]; !ok {
		t.Error("seed.yml missing version field")
	}
}

// TestFleetComposition ensures 12 trucks: 4×100-ton, 6×200-ton, 2×400-ton
func TestFleetComposition(t *testing.T) {
	repoRoot := getRepoRoot(t)
	truckPath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "seeds", "dim_truck.csv")

	trucks := readCSV(t, truckPath)
	if len(trucks) == 0 {
		t.Fatal("dim_truck.csv has no data rows")
	}

	// Count by fleet class
	classCounts := make(map[string]int)
	for _, truck := range trucks {
		fleetClass := getField(truck, "fleet_class")
		classCounts[fleetClass]++
	}

	// Verify fleet composition
	if classCounts["100-ton"] != 4 {
		t.Errorf("Expected 4 trucks in 100-ton class, got %d", classCounts["100-ton"])
	}
	if classCounts["200-ton"] != 6 {
		t.Errorf("Expected 6 trucks in 200-ton class, got %d", classCounts["200-ton"])
	}
	if classCounts["400-ton"] != 2 {
		t.Errorf("Expected 2 trucks in 400-ton class, got %d", classCounts["400-ton"])
	}

	// Verify total count
	totalTrucks := len(trucks)
	if totalTrucks != 12 {
		t.Errorf("Expected 12 total trucks, got %d", totalTrucks)
	}

	// Verify payload capacities match fleet classes
	for _, truck := range trucks {
		capacity, err := strconv.ParseFloat(getField(truck, "payload_capacity_tons"), 64)
		if err != nil {
			t.Errorf("Invalid payload_capacity_tons for truck %s", getField(truck, "truck_id"))
			continue
		}

		fleetClass := getField(truck, "fleet_class")
		switch fleetClass {
		case "100-ton":
			if capacity < 90 || capacity > 110 {
				t.Errorf("100-ton truck %s has invalid capacity: %.1f", getField(truck, "truck_id"), capacity)
			}
		case "200-ton":
			if capacity < 180 || capacity > 220 {
				t.Errorf("200-ton truck %s has invalid capacity: %.1f", getField(truck, "truck_id"), capacity)
			}
		case "400-ton":
			if capacity < 360 || capacity > 440 {
				t.Errorf("400-ton truck %s has invalid capacity: %.1f", getField(truck, "truck_id"), capacity)
			}
		}
	}
}

// TestShovelCapacityMatching validates shovel bucket sizes match truck capacities (3-6 passes rule)
func TestShovelCapacityMatching(t *testing.T) {
	repoRoot := getRepoRoot(t)
	shovelPath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "seeds", "dim_shovel.csv")

	shovels := readCSV(t, shovelPath)
	if len(shovels) != 3 {
		t.Errorf("Expected 3 shovels, got %d", len(shovels))
	}

	// Verify bucket sizes are appropriate
	bucketSizes := make(map[string]float64)
	for _, shovel := range shovels {
		bucketSize, err := strconv.ParseFloat(getField(shovel, "bucket_size_m3"), 64)
		if err != nil {
			t.Errorf("Invalid bucket_size_m3 for shovel %s", getField(shovel, "shovel_id"))
			continue
		}
		bucketSizes[getField(shovel, "shovel_id")] = bucketSize
	}

	// Verify we have buckets suitable for each truck class
	// 100-ton trucks need ~20m³ bucket (4-5 passes @ 2.5 tons/m³ density)
	// 200-ton trucks need ~35m³ bucket (5-6 passes)
	// 400-ton trucks need ~60m³ bucket (6-7 passes)
	hasSmallBucket := false
	hasMediumBucket := false
	hasLargeBucket := false

	for _, size := range bucketSizes {
		if size >= 15 && size <= 25 {
			hasSmallBucket = true
		}
		if size >= 30 && size <= 40 {
			hasMediumBucket = true
		}
		if size >= 55 && size <= 70 {
			hasLargeBucket = true
		}
	}

	if !hasSmallBucket {
		t.Error("Missing small bucket (15-25 m³) for 100-ton trucks")
	}
	if !hasMediumBucket {
		t.Error("Missing medium bucket (30-40 m³) for 200-ton trucks")
	}
	if !hasLargeBucket {
		t.Error("Missing large bucket (55-70 m³) for 400-ton trucks")
	}
}

// TestCrusherSingleBottleneck ensures only 1 crusher receiving all trucks
func TestCrusherSingleBottleneck(t *testing.T) {
	repoRoot := getRepoRoot(t)
	crusherPath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "seeds", "dim_crusher.csv")

	crushers := readCSV(t, crusherPath)
	if len(crushers) != 1 {
		t.Errorf("Expected exactly 1 crusher (bottleneck scenario), got %d", len(crushers))
	}

	// Verify crusher has reasonable capacity
	if len(crushers) > 0 {
		capacity, err := strconv.ParseFloat(getField(crushers[0], "capacity_tph"), 64)
		if err != nil {
			t.Errorf("Invalid capacity_tph: %v", err)
		} else if capacity < 2000 || capacity > 5000 {
			t.Errorf("Crusher capacity should be 2000-5000 TPH, got %.1f", capacity)
		}
	}
}

// TestPayloadDistribution validates payloads are realistic (placeholder for Phase 3 telemetry generation)
func TestPayloadDistribution(t *testing.T) {
	t.Skip("Payload distribution testing will be in Phase 3 (telemetry generation)")
}

// TestCycleTimeRealism ensures cycle times are realistic (placeholder for Phase 3 telemetry generation)
func TestCycleTimeRealism(t *testing.T) {
	t.Skip("Cycle time testing will be in Phase 3 (telemetry generation)")
}

// TestShiftBoundaries validates 12-hour shift patterns
func TestShiftBoundaries(t *testing.T) {
	repoRoot := getRepoRoot(t)
	shiftPath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "seeds", "dim_shift.csv")

	shifts := readCSV(t, shiftPath)
	if len(shifts) != 2 {
		t.Errorf("Expected 2 shifts (Day/Night), got %d", len(shifts))
	}

	// Verify shift names
	shiftNames := make(map[string]bool)
	for _, shift := range shifts {
		name := getField(shift, "shift_name")
		shiftNames[name] = true

		// Verify start and end times exist
		startTime := getField(shift, "start_time")
		endTime := getField(shift, "end_time")

		if startTime == "" || endTime == "" {
			t.Errorf("Shift %s missing start_time or end_time", name)
		}

		// Parse times and verify 12-hour duration
		start, err1 := time.Parse("15:04", startTime)
		end, err2 := time.Parse("15:04", endTime)
		if err1 != nil || err2 != nil {
			t.Errorf("Invalid time format for shift %s", name)
			continue
		}

		// Handle day boundary for night shift
		var duration time.Duration
		if end.Before(start) {
			duration = 24*time.Hour - start.Sub(end)
		} else {
			duration = end.Sub(start)
		}

		if duration != 12*time.Hour {
			t.Errorf("Shift %s should be 12 hours, got %v", name, duration)
		}
	}

	if !shiftNames["Day"] {
		t.Error("Missing 'Day' shift")
	}
	if !shiftNames["Night"] {
		t.Error("Missing 'Night' shift")
	}
}

// TestOperatorAssignment ensures operators exist for shifts
func TestOperatorAssignment(t *testing.T) {
	repoRoot := getRepoRoot(t)
	operatorPath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "seeds", "dim_operator.csv")

	operators := readCSV(t, operatorPath)
	if len(operators) < 8 {
		t.Errorf("Expected at least 8 operators (to cover 12 trucks across 2 shifts), got %d", len(operators))
	}

	// Verify experience levels
	levelCounts := make(map[string]int)
	for _, operator := range operators {
		level := getField(operator, "experience_level")
		if level != "Junior" && level != "Intermediate" && level != "Senior" {
			t.Errorf("Invalid experience_level for operator %s: %s", getField(operator, "operator_id"), level)
		}
		levelCounts[level]++
	}

	// Verify we have a mix of experience levels
	if len(levelCounts) < 2 {
		t.Error("Operators should have varied experience levels (Junior, Intermediate, Senior)")
	}
}

// TestDateDimension validates 30 days of date dimension data
func TestDateDimension(t *testing.T) {
	repoRoot := getRepoRoot(t)
	datePath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "seeds", "dim_date.csv")

	dates := readCSV(t, datePath)
	if len(dates) != 30 {
		t.Errorf("Expected 30 days of date dimension, got %d", len(dates))
	}

	// Verify required fields
	for i, date := range dates {
		dateKey := getField(date, "date_key")
		fullDate := getField(date, "full_date")
		year := getField(date, "year")
		quarter := getField(date, "quarter")
		month := getField(date, "month")
		week := getField(date, "week")
		dayOfWeek := getField(date, "day_of_week")

		if dateKey == "" || fullDate == "" || year == "" || quarter == "" || month == "" || week == "" || dayOfWeek == "" {
			t.Errorf("Date row %d missing required fields", i)
		}

		// Verify quarter is 1-4
		q, _ := strconv.Atoi(quarter)
		if q < 1 || q > 4 {
			t.Errorf("Invalid quarter %s for date %s", quarter, fullDate)
		}

		// Verify month is 1-12
		m, _ := strconv.Atoi(month)
		if m < 1 || m > 12 {
			t.Errorf("Invalid month %s for date %s", month, fullDate)
		}

		// Verify day_of_week is 1-7
		dow, _ := strconv.Atoi(dayOfWeek)
		if dow < 1 || dow > 7 {
			t.Errorf("Invalid day_of_week %s for date %s", dayOfWeek, fullDate)
		}
	}

	// Verify dates are consecutive
	if len(dates) > 1 {
		firstDate, err := time.Parse("2006-01-02", getField(dates[0], "full_date"))
		if err != nil {
			t.Fatalf("Failed to parse first date: %v", err)
		}

		lastDate, err := time.Parse("2006-01-02", getField(dates[len(dates)-1], "full_date"))
		if err != nil {
			t.Fatalf("Failed to parse last date: %v", err)
		}

		expectedDuration := 29 * 24 * time.Hour // 30 days = 29 day difference
		actualDuration := lastDate.Sub(firstDate)
		if actualDuration != expectedDuration {
			t.Errorf("Expected 30 consecutive days, got duration of %v", actualDuration)
		}
	}
}

// TestRefuelingSpotDelays validates refueling is modeled (placeholder for Phase 3)
func TestRefuelingSpotDelays(t *testing.T) {
	t.Skip("Refueling spot delay testing will be in Phase 3 (telemetry generation)")
}

// Helper functions

// readCSV reads a CSV file and returns rows as maps (header -> value)
func readCSV(t *testing.T, path string) []map[string]string {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Failed to open CSV file %s: %v", path, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read CSV file %s: %v", path, err)
	}

	if len(records) < 2 {
		t.Fatalf("CSV file %s has no data rows", path)
	}

	// First row is header
	header := records[0]
	var rows []map[string]string

	for _, record := range records[1:] {
		row := make(map[string]string)
		for i, value := range record {
			if i < len(header) {
				row[header[i]] = value
			}
		}
		rows = append(rows, row)
	}

	return rows
}

// getField safely gets a field from a map, returning empty string if not found
func getField(row map[string]string, field string) string {
	value, ok := row[field]
	if !ok {
		return ""
	}
	return strings.TrimSpace(value)
}
