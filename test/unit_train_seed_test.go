package test

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

// UnitTrainSeedConfig represents the seed configuration for unit train CLM generation
type UnitTrainSeedConfig struct {
	Version    int              `yaml:"version"`
	Fleet      FleetConfig      `yaml:"fleet"`
	Locations  LocationsConfig  `yaml:"locations"`
	Corridors  []CorridorConfig `yaml:"corridors"`
	Trains     TrainConfig      `yaml:"trains"`
	Operations OperationsConfig `yaml:"operations"`
	Stragglers StragglerConfig  `yaml:"stragglers"`
	Seasonal   SeasonalConfig   `yaml:"seasonal"`
	TimeWindow TimeWindowConfig `yaml:"time_window"`
	Output     OutputConfig     `yaml:"output"`
}

type FleetConfig struct {
	TotalCars    int    `yaml:"total_cars"`
	CarType      string `yaml:"car_type"`
	Commodity    string `yaml:"commodity"`
	CapacityTons int    `yaml:"capacity_tons"`
}

type LocationsConfig struct {
	Origins      []LocationDef `yaml:"origins"`
	Destinations []LocationDef `yaml:"destinations"`
}

type LocationDef struct {
	ID                string  `yaml:"id"`
	Name              string  `yaml:"name"`
	LoadingHoursMin   float64 `yaml:"loading_hours_min,omitempty"`
	LoadingHoursMax   float64 `yaml:"loading_hours_max,omitempty"`
	UnloadingHoursMin float64 `yaml:"unloading_hours_min,omitempty"`
	UnloadingHoursMax float64 `yaml:"unloading_hours_max,omitempty"`
	QueueCapacity     int     `yaml:"queue_capacity"`
}

type CorridorConfig struct {
	ID                string  `yaml:"id"`
	OriginID          string  `yaml:"origin_id"`
	DestinationID     string  `yaml:"destination_id"`
	TransitDaysMin    int     `yaml:"transit_days_min"`
	TransitDaysMax    int     `yaml:"transit_days_max"`
	StationCount      int     `yaml:"station_count"`
	DistanceMiles     int     `yaml:"distance_miles"`
	EmptyReturnFactor float64 `yaml:"empty_return_factor"`
}

type TrainConfig struct {
	CarsPerTrain   int `yaml:"cars_per_train"`
	ParallelTrains int `yaml:"parallel_trains"`
}

type OperationsConfig struct {
	OriginQueue      QueueConfig `yaml:"origin_queue"`
	DestinationQueue QueueConfig `yaml:"destination_queue"`
}

type QueueConfig struct {
	MaxConcurrent int `yaml:"max_concurrent"`
}

type StragglerConfig struct {
	RatePerTrainPerDay float64 `yaml:"rate_per_train_per_day"`
	DelayHoursMin      float64 `yaml:"delay_hours_min"`
	DelayHoursMax      float64 `yaml:"delay_hours_max"`
}

type SeasonalConfig struct {
	SlowCorridorWeek    int     `yaml:"slow_corridor_week"`
	SlowCorridorID      string  `yaml:"slow_corridor_id"`
	SlowdownFactor      float64 `yaml:"slowdown_factor"`
	HighStragglerWeek   int     `yaml:"high_straggler_week"`
	StragglerMultiplier float64 `yaml:"straggler_multiplier"`
}

type TimeWindowConfig struct {
	StartDate    string `yaml:"start_date"`
	DurationDays int    `yaml:"duration_days"`
}

type OutputConfig struct {
	Format   string   `yaml:"format"`
	Filename string   `yaml:"filename"`
	Headers  []string `yaml:"headers"`
}

// TestUnitTrainSeedConfiguration validates seed YAML parses
func TestUnitTrainSeedConfiguration(t *testing.T) {
	repoRoot := getRepoRoot(t)
	seedPath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "seeds", "clm_generation_config.yml")

	// Verify seed config file exists
	if _, err := os.Stat(seedPath); os.IsNotExist(err) {
		t.Fatal("Seed configuration file does not exist: seeds/clm_generation_config.yml")
	}

	// Parse seed configuration
	data, err := os.ReadFile(seedPath)
	if err != nil {
		t.Fatalf("Failed to read seed config: %v", err)
	}

	var config UnitTrainSeedConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		t.Fatalf("Failed to parse seed config: %v", err)
	}

	// Verify version
	if config.Version != 1 {
		t.Errorf("Expected seed config version 1, got %d", config.Version)
	}
}

// TestCarFleetAllocation ensures 225+ cars available (3 trains × 75 cars with buffer)
func TestCarFleetAllocation(t *testing.T) {
	config := loadSeedConfig(t)

	minRequired := config.Trains.CarsPerTrain * config.Trains.ParallelTrains
	if config.Fleet.TotalCars < minRequired {
		t.Errorf("Fleet has %d cars but needs at least %d for %d trains with %d cars each",
			config.Fleet.TotalCars, minRequired, config.Trains.ParallelTrains, config.Trains.CarsPerTrain)
	}

	// Should have buffer
	expectedWithBuffer := 250
	if config.Fleet.TotalCars < expectedWithBuffer {
		t.Errorf("Expected at least %d cars (including buffer), got %d", expectedWithBuffer, config.Fleet.TotalCars)
	}
}

// TestTrainFormationLogic validates 75 cars per train constraint
func TestTrainFormationLogic(t *testing.T) {
	config := loadSeedConfig(t)

	expectedCarsPerTrain := 75
	if config.Trains.CarsPerTrain != expectedCarsPerTrain {
		t.Errorf("Expected %d cars per train, got %d", expectedCarsPerTrain, config.Trains.CarsPerTrain)
	}

	// Verify single car type for simplicity
	if config.Fleet.CarType == "" {
		t.Error("Fleet car type not specified")
	}

	// Verify single commodity
	if config.Fleet.Commodity == "" {
		t.Error("Fleet commodity not specified")
	}
}

// TestOriginDestinationPairs verifies 2 origins × 3 destinations = 6 corridors
func TestOriginDestinationPairs(t *testing.T) {
	config := loadSeedConfig(t)

	expectedOrigins := 2
	if len(config.Locations.Origins) != expectedOrigins {
		t.Errorf("Expected %d origins, got %d", expectedOrigins, len(config.Locations.Origins))
	}

	expectedDestinations := 3
	if len(config.Locations.Destinations) != expectedDestinations {
		t.Errorf("Expected %d destinations, got %d", expectedDestinations, len(config.Locations.Destinations))
	}

	expectedCorridors := expectedOrigins * expectedDestinations
	if len(config.Corridors) != expectedCorridors {
		t.Errorf("Expected %d corridors (2 origins × 3 destinations), got %d",
			expectedCorridors, len(config.Corridors))
	}

	// Verify each corridor references valid origin and destination
	originMap := make(map[string]bool)
	for _, origin := range config.Locations.Origins {
		originMap[origin.ID] = true
	}

	destMap := make(map[string]bool)
	for _, dest := range config.Locations.Destinations {
		destMap[dest.ID] = true
	}

	for _, corridor := range config.Corridors {
		if !originMap[corridor.OriginID] {
			t.Errorf("Corridor %s references unknown origin: %s", corridor.ID, corridor.OriginID)
		}
		if !destMap[corridor.DestinationID] {
			t.Errorf("Corridor %s references unknown destination: %s", corridor.ID, corridor.DestinationID)
		}
	}
}

// TestOriginQueueLogic ensures only 1 train loading at origin at a time (12-18 hours)
func TestOriginQueueLogic(t *testing.T) {
	config := loadSeedConfig(t)

	// Verify queue capacity of 1
	expectedCapacity := 1
	if config.Operations.OriginQueue.MaxConcurrent != expectedCapacity {
		t.Errorf("Expected origin queue capacity %d, got %d",
			expectedCapacity, config.Operations.OriginQueue.MaxConcurrent)
	}

	// Verify each origin has queue capacity defined
	for _, origin := range config.Locations.Origins {
		if origin.QueueCapacity != expectedCapacity {
			t.Errorf("Origin %s should have queue capacity %d, got %d",
				origin.ID, expectedCapacity, origin.QueueCapacity)
		}

		// Verify loading time range (12-18 hours)
		if origin.LoadingHoursMin < 12 || origin.LoadingHoursMin > 18 {
			t.Errorf("Origin %s loading hours min %.1f outside expected range [12, 18]",
				origin.ID, origin.LoadingHoursMin)
		}
		if origin.LoadingHoursMax < 12 || origin.LoadingHoursMax > 18 {
			t.Errorf("Origin %s loading hours max %.1f outside expected range [12, 18]",
				origin.ID, origin.LoadingHoursMax)
		}
		if origin.LoadingHoursMin > origin.LoadingHoursMax {
			t.Errorf("Origin %s loading hours min %.1f > max %.1f",
				origin.ID, origin.LoadingHoursMin, origin.LoadingHoursMax)
		}
	}
}

// TestDestinationQueueLogic ensures only 1 train unloading at destination at a time (8-12 hours)
func TestDestinationQueueLogic(t *testing.T) {
	config := loadSeedConfig(t)

	// Verify queue capacity of 1
	expectedCapacity := 1
	if config.Operations.DestinationQueue.MaxConcurrent != expectedCapacity {
		t.Errorf("Expected destination queue capacity %d, got %d",
			expectedCapacity, config.Operations.DestinationQueue.MaxConcurrent)
	}

	// Verify each destination has queue capacity defined
	for _, dest := range config.Locations.Destinations {
		if dest.QueueCapacity != expectedCapacity {
			t.Errorf("Destination %s should have queue capacity %d, got %d",
				dest.ID, expectedCapacity, dest.QueueCapacity)
		}

		// Verify unloading time range (8-12 hours)
		if dest.UnloadingHoursMin < 8 || dest.UnloadingHoursMin > 12 {
			t.Errorf("Destination %s unloading hours min %.1f outside expected range [8, 12]",
				dest.ID, dest.UnloadingHoursMin)
		}
		if dest.UnloadingHoursMax < 8 || dest.UnloadingHoursMax > 12 {
			t.Errorf("Destination %s unloading hours max %.1f outside expected range [8, 12]",
				dest.ID, dest.UnloadingHoursMax)
		}
		if dest.UnloadingHoursMin > dest.UnloadingHoursMax {
			t.Errorf("Destination %s unloading hours min %.1f > max %.1f",
				dest.ID, dest.UnloadingHoursMin, dest.UnloadingHoursMax)
		}
	}
}

// TestTransitTimeDistribution ensures 2-4 day variation with 5-10 stations
func TestTransitTimeDistribution(t *testing.T) {
	config := loadSeedConfig(t)

	for _, corridor := range config.Corridors {
		// Verify transit time range (2-4 days)
		if corridor.TransitDaysMin < 2 || corridor.TransitDaysMin > 4 {
			t.Errorf("Corridor %s transit days min %d outside expected range [2, 4]",
				corridor.ID, corridor.TransitDaysMin)
		}
		if corridor.TransitDaysMax < 2 || corridor.TransitDaysMax > 4 {
			t.Errorf("Corridor %s transit days max %d outside expected range [2, 4]",
				corridor.ID, corridor.TransitDaysMax)
		}
		if corridor.TransitDaysMin > corridor.TransitDaysMax {
			t.Errorf("Corridor %s transit days min %d > max %d",
				corridor.ID, corridor.TransitDaysMin, corridor.TransitDaysMax)
		}

		// Verify station count (5-10 stations)
		if corridor.StationCount < 5 || corridor.StationCount > 10 {
			t.Errorf("Corridor %s station count %d outside expected range [5, 10]",
				corridor.ID, corridor.StationCount)
		}

		// Verify empty return factor (should be faster, ~0.7)
		if corridor.EmptyReturnFactor <= 0 || corridor.EmptyReturnFactor >= 1 {
			t.Errorf("Corridor %s empty return factor %.2f should be between 0 and 1",
				corridor.ID, corridor.EmptyReturnFactor)
		}
	}
}

// TestStragglerDelayRange validates straggler delay between 6 hours and 3 days before resuming transit
func TestStragglerDelayRange(t *testing.T) {
	config := loadSeedConfig(t)

	minExpected := 6.0  // 6 hours
	maxExpected := 72.0 // 3 days = 72 hours

	if config.Stragglers.DelayHoursMin < minExpected {
		t.Errorf("Straggler delay min %.1f hours is less than expected minimum %.1f hours",
			config.Stragglers.DelayHoursMin, minExpected)
	}

	if config.Stragglers.DelayHoursMax > maxExpected {
		t.Errorf("Straggler delay max %.1f hours exceeds expected maximum %.1f hours",
			config.Stragglers.DelayHoursMax, maxExpected)
	}

	if config.Stragglers.DelayHoursMin > config.Stragglers.DelayHoursMax {
		t.Errorf("Straggler delay min %.1f > max %.1f",
			config.Stragglers.DelayHoursMin, config.Stragglers.DelayHoursMax)
	}
}

// TestStragglerIndependentTravel validates stragglers travel to destination alone after delay, then join next returning train
func TestStragglerIndependentTravel(t *testing.T) {
	config := loadSeedConfig(t)

	// This test validates the configuration supports independent travel logic
	// The actual behavior will be tested in Phase 3 generation logic tests

	// Verify delay configuration exists
	if config.Stragglers.DelayHoursMin <= 0 {
		t.Error("Straggler delay configuration missing or invalid")
	}

	// Verify corridors have return trip configuration (empty return factor)
	for _, corridor := range config.Corridors {
		if corridor.EmptyReturnFactor <= 0 {
			t.Errorf("Corridor %s missing empty return factor needed for straggler rejoin logic",
				corridor.ID)
		}
	}
}

// TestStragglerGeneration validates 1 car per train per day in transit (both directions), doubles during specific week
func TestStragglerGeneration(t *testing.T) {
	config := loadSeedConfig(t)

	expectedRate := 1.0 // 1 car per train per day in transit
	if config.Stragglers.RatePerTrainPerDay != expectedRate {
		t.Errorf("Expected straggler rate %.1f cars per train per day, got %.1f",
			expectedRate, config.Stragglers.RatePerTrainPerDay)
	}

	// Verify seasonal multiplier is 2x
	expectedMultiplier := 2.0
	if config.Seasonal.StragglerMultiplier != expectedMultiplier {
		t.Errorf("Expected straggler multiplier %.1fx during high week, got %.1fx",
			expectedMultiplier, config.Seasonal.StragglerMultiplier)
	}

	// Verify high straggler week is defined
	if config.Seasonal.HighStragglerWeek <= 0 {
		t.Error("High straggler week not defined")
	}
}

// TestParallelTrainOperations confirms 3 trains can operate simultaneously
func TestParallelTrainOperations(t *testing.T) {
	config := loadSeedConfig(t)

	expectedTrains := 3
	if config.Trains.ParallelTrains != expectedTrains {
		t.Errorf("Expected %d parallel trains, got %d", expectedTrains, config.Trains.ParallelTrains)
	}

	// Verify fleet size supports parallel operations
	minRequired := config.Trains.ParallelTrains * config.Trains.CarsPerTrain
	if config.Fleet.TotalCars < minRequired {
		t.Errorf("Fleet size %d insufficient for %d trains with %d cars each (need %d)",
			config.Fleet.TotalCars, config.Trains.ParallelTrains, config.Trains.CarsPerTrain, minRequired)
	}
}

// TestSeasonalSlowdown validates 1 corridor slower for 1 week
func TestSeasonalSlowdown(t *testing.T) {
	config := loadSeedConfig(t)

	// Verify slow corridor week is defined
	if config.Seasonal.SlowCorridorWeek <= 0 {
		t.Error("Slow corridor week not defined")
	}

	// Verify slow corridor ID is specified
	if config.Seasonal.SlowCorridorID == "" {
		t.Error("Slow corridor ID not specified")
	}

	// Verify slowdown factor (should be 1.2 for 20% slower)
	expectedSlowdown := 1.2
	if config.Seasonal.SlowdownFactor != expectedSlowdown {
		t.Errorf("Expected slowdown factor %.1f (20%% slower), got %.1f",
			expectedSlowdown, config.Seasonal.SlowdownFactor)
	}

	// Verify the slow corridor exists in corridor list
	corridorExists := false
	for _, corridor := range config.Corridors {
		if corridor.ID == config.Seasonal.SlowCorridorID {
			corridorExists = true
			break
		}
	}
	if !corridorExists {
		t.Errorf("Slow corridor ID %s not found in corridor list", config.Seasonal.SlowCorridorID)
	}

	// Verify slow week is within analysis period
	analysisWeeks := config.TimeWindow.DurationDays / 7
	if config.Seasonal.SlowCorridorWeek > analysisWeeks {
		t.Errorf("Slow corridor week %d exceeds analysis period of %d weeks",
			config.Seasonal.SlowCorridorWeek, analysisWeeks)
	}
}

// TestCSVFormatOutput ensures CLM messages output as valid CSV
func TestCSVFormatOutput(t *testing.T) {
	config := loadSeedConfig(t)

	// Verify output format is CSV
	if config.Output.Format != "csv" {
		t.Errorf("Expected output format 'csv', got '%s'", config.Output.Format)
	}

	// Verify headers are defined
	if len(config.Output.Headers) == 0 {
		t.Error("CSV headers not defined")
	}

	// Verify required CLM headers present
	requiredHeaders := []string{
		"event_id",
		"event_timestamp",
		"car_id",
		"train_id",
		"location_id",
		"event_type",
	}

	headerMap := make(map[string]bool)
	for _, header := range config.Output.Headers {
		headerMap[header] = true
	}

	for _, required := range requiredHeaders {
		if !headerMap[required] {
			t.Errorf("Required CSV header missing: %s", required)
		}
	}

	// Verify output filename is defined
	if config.Output.Filename == "" {
		t.Error("Output filename not specified")
	}
}

// Helper function to load seed configuration
func loadSeedConfig(t *testing.T) UnitTrainSeedConfig {
	t.Helper()

	repoRoot := getRepoRoot(t)
	seedPath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "seeds", "clm_generation_config.yml")

	data, err := os.ReadFile(seedPath)
	if err != nil {
		t.Fatalf("Failed to read seed config: %v", err)
	}

	var config UnitTrainSeedConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		t.Fatalf("Failed to parse seed config: %v", err)
	}

	return config
}
