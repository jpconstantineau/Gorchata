package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/jpconstantineau/gorchata/internal/domain"
	"gopkg.in/yaml.v3"
)

// Config structures (same as test)
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

func main() {
	// Load configuration
	configPath := filepath.Join("examples", "unit_train_analytics", "seeds", "clm_generation_config.yml")
	data, err := os.ReadFile(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading config: %v\n", err)
		os.Exit(1)
	}

	var config UnitTrainSeedConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing config: %v\n", err)
		os.Exit(1)
	}

	// Convert to domain config
	domainConfig := convertToDomainConfig(config)

	// Generate events
	fmt.Println("Generating CLM events...")
	generator := domain.NewUnitTrainEventGenerator(domainConfig, 42)
	events, err := generator.GenerateEvents()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating events: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Generated %d events\n", len(events))

	// Write CSV file
	outputPath := filepath.Join("examples", "unit_train_analytics", "seeds", config.Output.Filename)
	fmt.Printf("Writing to %s...\n", outputPath)

	csvFile, err := os.Create(outputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating CSV file: %v\n", err)
		os.Exit(1)
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	// Write headers
	if err := writer.Write(config.Output.Headers); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing headers: %v\n", err)
		os.Exit(1)
	}

	// Write events
	eventID := 1
	for _, event := range events {
		// Create one row per car in the event
		for _, carID := range event.CarIDs {
			row := []string{
				strconv.Itoa(eventID),
				event.Timestamp.Format("2006-01-02 15:04:05"),
				carID,
				event.TrainID,
				event.LocationID,
				event.EventType,
				strconv.FormatBool(event.LoadedFlag),
				event.Commodity,
				strconv.Itoa(event.WeightTons / len(event.CarIDs)), // Weight per car
			}
			if err := writer.Write(row); err != nil {
				fmt.Fprintf(os.Stderr, "Error writing row: %v\n", err)
				os.Exit(1)
			}
			eventID++
		}
	}

	fmt.Printf("Successfully wrote %d rows to %s\n", eventID-1, outputPath)
}

func convertToDomainConfig(config UnitTrainSeedConfig) domain.UnitTrainConfig {
	domainConfig := domain.UnitTrainConfig{
		Fleet: domain.FleetConfig{
			TotalCars:    config.Fleet.TotalCars,
			CarType:      config.Fleet.CarType,
			Commodity:    config.Fleet.Commodity,
			CapacityTons: config.Fleet.CapacityTons,
		},
		Locations: domain.LocationsConfig{
			Origins:      make([]domain.LocationDef, len(config.Locations.Origins)),
			Destinations: make([]domain.LocationDef, len(config.Locations.Destinations)),
		},
		Corridors: make([]domain.CorridorConfig, len(config.Corridors)),
		Trains: domain.TrainConfig{
			CarsPerTrain:   config.Trains.CarsPerTrain,
			ParallelTrains: config.Trains.ParallelTrains,
		},
		Operations: domain.OperationsConfig{
			OriginQueue: domain.QueueConfig{
				MaxConcurrent: config.Operations.OriginQueue.MaxConcurrent,
			},
			DestinationQueue: domain.QueueConfig{
				MaxConcurrent: config.Operations.DestinationQueue.MaxConcurrent,
			},
		},
		Stragglers: domain.StragglerConfig{
			RatePerTrainPerDay: config.Stragglers.RatePerTrainPerDay,
			DelayHoursMin:      config.Stragglers.DelayHoursMin,
			DelayHoursMax:      config.Stragglers.DelayHoursMax,
		},
		Seasonal: domain.SeasonalConfig{
			SlowCorridorWeek:    config.Seasonal.SlowCorridorWeek,
			SlowCorridorID:      config.Seasonal.SlowCorridorID,
			SlowdownFactor:      config.Seasonal.SlowdownFactor,
			HighStragglerWeek:   config.Seasonal.HighStragglerWeek,
			StragglerMultiplier: config.Seasonal.StragglerMultiplier,
		},
		TimeWindow: domain.TimeWindowConfig{
			StartDate:    config.TimeWindow.StartDate,
			DurationDays: config.TimeWindow.DurationDays,
		},
		Output: domain.OutputConfig{
			Format:   config.Output.Format,
			Filename: config.Output.Filename,
			Headers:  config.Output.Headers,
		},
	}

	for i, origin := range config.Locations.Origins {
		domainConfig.Locations.Origins[i] = domain.LocationDef{
			ID:                origin.ID,
			Name:              origin.Name,
			LoadingHoursMin:   origin.LoadingHoursMin,
			LoadingHoursMax:   origin.LoadingHoursMax,
			UnloadingHoursMin: origin.UnloadingHoursMin,
			UnloadingHoursMax: origin.UnloadingHoursMax,
			QueueCapacity:     origin.QueueCapacity,
		}
	}

	for i, dest := range config.Locations.Destinations {
		domainConfig.Locations.Destinations[i] = domain.LocationDef{
			ID:                dest.ID,
			Name:              dest.Name,
			LoadingHoursMin:   dest.LoadingHoursMin,
			LoadingHoursMax:   dest.LoadingHoursMax,
			UnloadingHoursMin: dest.UnloadingHoursMin,
			UnloadingHoursMax: dest.UnloadingHoursMax,
			QueueCapacity:     dest.QueueCapacity,
		}
	}

	for i, corridor := range config.Corridors {
		domainConfig.Corridors[i] = domain.CorridorConfig{
			ID:                corridor.ID,
			OriginID:          corridor.OriginID,
			DestinationID:     corridor.DestinationID,
			TransitDaysMin:    corridor.TransitDaysMin,
			TransitDaysMax:    corridor.TransitDaysMax,
			StationCount:      corridor.StationCount,
			DistanceMiles:     corridor.DistanceMiles,
			EmptyReturnFactor: corridor.EmptyReturnFactor,
		}
	}

	return domainConfig
}
