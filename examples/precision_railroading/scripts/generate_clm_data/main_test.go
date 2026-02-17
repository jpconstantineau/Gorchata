package main

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestCLMDataGenerator validates basic event structure
func TestCLMDataGenerator(t *testing.T) {
	// Create temp config and output
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "test_config.yml")
	outputPath := filepath.Join(tmpDir, "test_output.csv")

	// Write minimal test config
	testConfig := `version: 1
fleet:
  total_cars: 100
  railroads:
    - BNSF
    - UP
    - CSX
    - NS
locations:
  total_locations: 20
  terminals: 3
  interchanges: 5
  yards: 6
  customer_sites: 4
  sidings: 2
  shadow_yards:
    count: 2
time_window:
  start_date: "2024-01-01"
  duration_days: 30
psr:
  pre_psr_end: "2017-12-31"
  transition_end: "2020-12-31"
  mature_start: "2021-01-01"
seasonal:
  variation_percent: 25
output:
  filename: ` + outputPath + `
  headers:
    - event_id
    - car_number
    - timestamp
    - event_type
    - splc_code
    - train_id
    - location_name
`
	if err := os.WriteFile(configPath, []byte(testConfig), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	// Generate events
	config, err := loadConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	generator := NewPSREventGenerator(config, 42)
	events, err := generator.GenerateEvents()
	if err != nil {
		t.Fatalf("Failed to generate events: %v", err)
	}

	// Validate basic structure
	if len(events) == 0 {
		t.Fatal("Expected non-empty events list")
	}

	// Check first event has required fields
	first := events[0]
	if first.EventID == "" {
		t.Error("Event missing event_id")
	}
	if first.CarNumber == "" {
		t.Error("Event missing car_number")
	}
	if first.Timestamp.IsZero() {
		t.Error("Event missing timestamp")
	}
	if first.EventType == "" {
		t.Error("Event missing event_type")
	}
	if first.SPLCCode == "" {
		t.Error("Event missing splc_code")
	}
}

// TestEventTypeDistribution validates event type ratios
func TestEventTypeDistribution(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "test_config.yml")

	testConfig := `version: 1
fleet:
  total_cars: 50
  railroads:
    - BNSF
    - UP
locations:
  total_locations: 10
  terminals: 2
  interchanges: 3
  yards: 3
  customer_sites: 1
  sidings: 1
  shadow_yards:
    count: 1
time_window:
  start_date: "2024-01-01"
  duration_days: 7
psr:
  pre_psr_end: "2017-12-31"
  transition_end: "2020-12-31"
  mature_start: "2021-01-01"
seasonal:
  variation_percent: 25
output:
  filename: test.csv
  headers:
    - event_id
    - car_number
    - timestamp
    - event_type
    - splc_code
    - train_id
    - location_name
`
	if err := os.WriteFile(configPath, []byte(testConfig), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	config, err := loadConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	generator := NewPSREventGenerator(config, 42)
	events, err := generator.GenerateEvents()
	if err != nil {
		t.Fatalf("Failed to generate events: %v", err)
	}

	// Count event types
	eventTypes := make(map[string]int)
	for _, event := range events {
		eventTypes[event.EventType]++
	}

	// Validate all required event types present
	requiredTypes := []string{"DEPA", "ARRI", "PULL", "PLAC"}
	for _, et := range requiredTypes {
		if eventTypes[et] == 0 {
			t.Errorf("Missing event type: %s", et)
		}
	}

	// Check reasonable distribution (ARRI should roughly match DEPA, PULL should roughly match PLAC)
	arriCount := eventTypes["ARRI"]
	depaCount := eventTypes["DEPA"]
	if arriCount == 0 || depaCount == 0 {
		t.Error("ARRI and DEPA counts should be non-zero")
	}

	// Allow 50% variance (reasonable for small sample)
	ratio := float64(arriCount) / float64(depaCount)
	if ratio < 0.5 || ratio > 1.5 {
		t.Errorf("ARRI/DEPA ratio out of range: %.2f (ARRI=%d, DEPA=%d)", ratio, arriCount, depaCount)
	}
}

// TestTemporalConsistency validates chronological ordering and minute precision
func TestTemporalConsistency(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "test_config.yml")

	testConfig := `version: 1
fleet:
  total_cars: 20
  railroads:
    - BNSF
locations:
  total_locations: 5
  terminals: 1
  interchanges: 2
  yards: 1
  customer_sites: 1
  sidings: 0
  shadow_yards:
    count: 0
time_window:
  start_date: "2024-01-01"
  duration_days: 5
psr:
  pre_psr_end: "2017-12-31"
  transition_end: "2020-12-31"
  mature_start: "2021-01-01"
seasonal:
  variation_percent: 25
output:
  filename: test.csv
  headers:
    - event_id
    - car_number
    - timestamp
    - event_type
    - splc_code
    - train_id
    - location_name
`
	if err := os.WriteFile(configPath, []byte(testConfig), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	config, err := loadConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	generator := NewPSREventGenerator(config, 42)
	events, err := generator.GenerateEvents()
	if err != nil {
		t.Fatalf("Failed to generate events: %v", err)
	}

	// Group events by car
	carEvents := make(map[string][]CLMEvent)
	for _, event := range events {
		carEvents[event.CarNumber] = append(carEvents[event.CarNumber], event)
	}

	// Check each car's events are chronologically ordered
	for carNum, evts := range carEvents {
		if len(evts) < 2 {
			continue
		}

		for i := 1; i < len(evts); i++ {
			if evts[i].Timestamp.Before(evts[i-1].Timestamp) {
				t.Errorf("Car %s events out of order: %v before %v",
					carNum, evts[i-1].Timestamp, evts[i].Timestamp)
			}
		}

		// Check minute precision (no seconds)
		for _, evt := range evts {
			if evt.Timestamp.Second() != 0 {
				t.Errorf("Car %s event has seconds component: %v", carNum, evt.Timestamp)
			}
		}

		// Check no duplicate timestamps for same car
		timestamps := make(map[time.Time]bool)
		for _, evt := range evts {
			if timestamps[evt.Timestamp] {
				t.Errorf("Car %s has duplicate timestamp: %v", carNum, evt.Timestamp)
			}
			timestamps[evt.Timestamp] = true
		}
	}
}

// TestSeasonalVariation validates 25% seasonal effects
func TestSeasonalVariation(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "test_config.yml")

	// Generate data covering full year to capture seasonal effects
	testConfig := `version: 1
fleet:
  total_cars: 100
  railroads:
    - BNSF
    - UP
locations:
  total_locations: 10
  terminals: 2
  interchanges: 3
  yards: 3
  customer_sites: 1
  sidings: 1
  shadow_yards:
    count: 1
time_window:
  start_date: "2024-01-01"
  duration_days: 365
psr:
  pre_psr_end: "2017-12-31"
  transition_end: "2020-12-31"
  mature_start: "2021-01-01"
seasonal:
  variation_percent: 25
output:
  filename: test.csv
  headers:
    - event_id
    - car_number
    - timestamp
    - event_type
    - splc_code
    - train_id
    - location_name
`
	if err := os.WriteFile(configPath, []byte(testConfig), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	config, err := loadConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	generator := NewPSREventGenerator(config, 42)
	events, err := generator.GenerateEvents()
	if err != nil {
		t.Fatalf("Failed to generate events: %v", err)
	}

	// Calculate average trip duration by season
	type Trip struct {
		start time.Time
		end   time.Time
	}

	carTrips := make(map[string][]Trip)

	// Simple heuristic: PLAC to next PULL is a trip
	carLastPLAC := make(map[string]time.Time)
	for _, event := range events {
		if event.EventType == "PLAC" {
			carLastPLAC[event.CarNumber] = event.Timestamp
		} else if event.EventType == "PULL" {
			if start, ok := carLastPLAC[event.CarNumber]; ok {
				carTrips[event.CarNumber] = append(carTrips[event.CarNumber], Trip{start, event.Timestamp})
				delete(carLastPLAC, event.CarNumber)
			}
		}
	}

	// Group trip durations by season
	winterDurations := []float64{}
	summerDurations := []float64{}

	for _, trips := range carTrips {
		for _, trip := range trips {
			duration := trip.end.Sub(trip.start).Hours()
			month := trip.start.Month()

			// Winter: Dec, Jan, Feb
			if month == 12 || month == 1 || month == 2 {
				winterDurations = append(winterDurations, duration)
			}
			// Summer: Jun, Jul, Aug
			if month == 6 || month == 7 || month == 8 {
				summerDurations = append(summerDurations, duration)
			}
		}
	}

	if len(winterDurations) == 0 || len(summerDurations) == 0 {
		t.Skip("Insufficient seasonal data for comparison")
	}

	// Calculate averages
	avgWinter := average(winterDurations)
	avgSummer := average(summerDurations)

	// Winter should be slower (higher duration) than summer
	if avgWinter <= avgSummer {
		t.Errorf("Expected winter trips to be slower than summer: winter=%.2fh, summer=%.2fh",
			avgWinter, avgSummer)
	}

	// Check variation is in reasonable range (should be close to 25% configured)
	variation := ((avgWinter - avgSummer) / avgSummer) * 100
	if variation < 10 || variation > 40 {
		t.Errorf("Seasonal variation %.1f%% outside expected range (10-40%%)", variation)
	}
}

// TestPSRGradualAdoption validates three-period evolution
func TestPSRGradualAdoption(t *testing.T) {
	// Test three different periods
	periods := []struct {
		name      string
		startDate string
		days      int
	}{
		{"pre_psr", "2016-06-01", 180},
		{"transition", "2019-06-01", 180},
		{"mature", "2023-06-01", 180},
	}

	results := make(map[string]struct {
		avgVelocity   float64
		dwellVariance float64
		avgDwellTime  float64
	})

	for _, period := range periods {
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "test_config.yml")

		testConfig := `version: 1
fleet:
  total_cars: 100
  railroads:
    - BNSF
    - UP
locations:
  total_locations: 20
  terminals: 3
  interchanges: 5
  yards: 6
  customer_sites: 4
  sidings: 2
  shadow_yards:
    count: 2
time_window:
  start_date: "` + period.startDate + `"
  duration_days: ` + strconv.Itoa(period.days) + `
psr:
  pre_psr_end: "2017-12-31"
  transition_end: "2020-12-31"
  mature_start: "2021-01-01"
seasonal:
  variation_percent: 25
output:
  filename: test.csv
  headers:
    - event_id
    - car_number
    - timestamp
    - event_type
    - splc_code
    - train_id
    - location_name
`
		if err := os.WriteFile(configPath, []byte(testConfig), 0644); err != nil {
			t.Fatalf("Failed to write test config: %v", err)
		}

		config, err := loadConfig(configPath)
		if err != nil {
			t.Fatalf("Failed to load config: %v", err)
		}

		generator := NewPSREventGenerator(config, 42)
		events, err := generator.GenerateEvents()
		if err != nil {
			t.Fatalf("Failed to generate events: %v", err)
		}

		// Calculate velocity (distance/time) and dwell times
		velocities := []float64{}
		dwellTimes := []float64{}

		// Simple metric: time between consecutive events for same car as proxy for velocity
		carLastEvent := make(map[string]time.Time)
		for _, event := range events {
			if lastTime, ok := carLastEvent[event.CarNumber]; ok {
				hoursDiff := event.Timestamp.Sub(lastTime).Hours()
				if hoursDiff > 0 && hoursDiff < 168 { // Within a week
					// Assume typical distance between events is ~200 miles
					velocity := 200.0 / hoursDiff
					velocities = append(velocities, velocity)

					// Dwell time proxy: ARRI to DEPA at same location
					if event.EventType == "DEPA" {
						dwellTimes = append(dwellTimes, hoursDiff)
					}
				}
			}
			carLastEvent[event.CarNumber] = event.Timestamp
		}

		if len(velocities) > 0 && len(dwellTimes) > 0 {
			results[period.name] = struct {
				avgVelocity   float64
				dwellVariance float64
				avgDwellTime  float64
			}{
				avgVelocity:   average(velocities),
				dwellVariance: variance(dwellTimes),
				avgDwellTime:  average(dwellTimes),
			}
		}
	}

	// Validate evolution: mature should have higher velocity and lower dwell variance
	if len(results) < 3 {
		t.Fatal("Failed to generate results for all periods")
	}

	prePSR := results["pre_psr"]
	transition := results["transition"]
	mature := results["mature"]

	// Velocity should improve over time
	if mature.avgVelocity <= prePSR.avgVelocity {
		t.Errorf("Expected mature PSR velocity (%.2f mph) > pre-PSR velocity (%.2f mph)",
			mature.avgVelocity, prePSR.avgVelocity)
	}

	// Transition should be between pre-PSR and mature
	if transition.avgVelocity < prePSR.avgVelocity || transition.avgVelocity > mature.avgVelocity {
		t.Logf("Warning: Transition velocity (%.2f) not between pre-PSR (%.2f) and mature (%.2f)",
			transition.avgVelocity, prePSR.avgVelocity, mature.avgVelocity)
	}

	// Dwell variance should decrease over time
	if mature.dwellVariance >= prePSR.dwellVariance {
		t.Errorf("Expected mature PSR dwell variance (%.2f) < pre-PSR variance (%.2f)",
			mature.dwellVariance, prePSR.dwellVariance)
	}
}

// Helper functions
func average(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func variance(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	avg := average(values)
	sumSquares := 0.0
	for _, v := range values {
		diff := v - avg
		sumSquares += diff * diff
	}
	return sumSquares / float64(len(values))
}

// TestCSVOutput validates that CSV file is written correctly
func TestCSVOutput(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "test_config.yml")
	outputPath := filepath.Join(tmpDir, "output.csv")

	testConfig := `version: 1
fleet:
  total_cars: 10
  railroads:
    - BNSF
locations:
  total_locations: 5
  terminals: 1
  interchanges: 2
  yards: 1
  customer_sites: 1
  sidings: 0
  shadow_yards:
    count: 0
time_window:
  start_date: "2024-01-01"
  duration_days: 2
psr:
  pre_psr_end: "2017-12-31"
  transition_end: "2020-12-31"
  mature_start: "2021-01-01"
seasonal:
  variation_percent: 25
output:
  filename: ` + outputPath + `
  headers:
    - event_id
    - car_number
    - timestamp
    - event_type
    - splc_code
    - train_id
    - location_name
`
	if err := os.WriteFile(configPath, []byte(testConfig), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	config, err := loadConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	generator := NewPSREventGenerator(config, 42)
	events, err := generator.GenerateEvents()
	if err != nil {
		t.Fatalf("Failed to generate events: %v", err)
	}

	// Write CSV
	if err := writeCSV(outputPath, config.Output.Headers, events); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	// Verify CSV can be read
	file, err := os.Open(outputPath)
	if err != nil {
		t.Fatalf("Failed to open output CSV: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read CSV: %v", err)
	}

	if len(records) < 2 {
		t.Fatal("CSV should have header and at least one data row")
	}

	// Verify header
	expectedHeaders := []string{"event_id", "car_number", "timestamp", "event_type", "splc_code", "train_id", "location_name"}
	if len(records[0]) != len(expectedHeaders) {
		t.Errorf("Expected %d columns, got %d", len(expectedHeaders), len(records[0]))
	}

	// Verify data row has correct number of columns
	if len(records[1]) != len(expectedHeaders) {
		t.Errorf("Data row has %d columns, expected %d", len(records[1]), len(expectedHeaders))
	}

	// Verify timestamp format (should be minute precision)
	timestamp := records[1][2]
	if strings.Contains(timestamp, ":") {
		parts := strings.Split(timestamp, ":")
		if len(parts) == 3 { // Has seconds
			t.Error("Timestamp should not have seconds component")
		}
	}
}
