package main

import (
	"encoding/csv"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// Config structures
type PSRSeedConfig struct {
	Version    int              `yaml:"version"`
	Fleet      FleetConfig      `yaml:"fleet"`
	Locations  LocationsConfig  `yaml:"locations"`
	TimeWindow TimeWindowConfig `yaml:"time_window"`
	PSR        PSRConfig        `yaml:"psr"`
	Seasonal   SeasonalConfig   `yaml:"seasonal"`
	Output     OutputConfig     `yaml:"output"`
}

type FleetConfig struct {
	TotalCars int      `yaml:"total_cars"`
	Railroads []string `yaml:"railroads"`
}

type LocationsConfig struct {
	TotalLocations int               `yaml:"total_locations"`
	Terminals      int               `yaml:"terminals"`
	Interchanges   int               `yaml:"interchanges"`
	Yards          int               `yaml:"yards"`
	CustomerSites  int               `yaml:"customer_sites"`
	Sidings        int               `yaml:"sidings"`
	ShadowYards    ShadowYardsConfig `yaml:"shadow_yards"`
}

type ShadowYardsConfig struct {
	Count int `yaml:"count"`
}

type TimeWindowConfig struct {
	StartDate    string `yaml:"start_date"`
	DurationDays int    `yaml:"duration_days"`
}

type PSRConfig struct {
	PrePSREnd     string `yaml:"pre_psr_end"`
	TransitionEnd string `yaml:"transition_end"`
	MatureStart   string `yaml:"mature_start"`
}

type SeasonalConfig struct {
	VariationPercent int `yaml:"variation_percent"`
}

type OutputConfig struct {
	Filename string   `yaml:"filename"`
	Headers  []string `yaml:"headers"`
}

// Domain structures
type CLMEvent struct {
	EventID      string
	CarNumber    string
	Timestamp    time.Time
	EventType    string
	SPLCCode     string
	TrainID      string
	LocationName string
}

type Location struct {
	ID           string
	Name         string
	SPLCCode     string
	Type         string
	IsShadowYard bool
	Latitude     float64
	Longitude    float64
}

type PSREventGenerator struct {
	config      PSRSeedConfig
	rng         *rand.Rand
	locations   []Location
	cars        []string
	prePSREnd   time.Time
	transEnd    time.Time
	matureStart time.Time
}

func NewPSREventGenerator(config PSRSeedConfig, seed int64) *PSREventGenerator {
	gen := &PSREventGenerator{
		config: config,
		rng:    rand.New(rand.NewSource(seed)),
	}

	// Parse PSR period dates
	var err error
	gen.prePSREnd, err = time.Parse("2006-01-02", config.PSR.PrePSREnd)
	if err != nil {
		gen.prePSREnd = time.Date(2017, 12, 31, 0, 0, 0, 0, time.UTC)
	}
	gen.transEnd, err = time.Parse("2006-01-02", config.PSR.TransitionEnd)
	if err != nil {
		gen.transEnd = time.Date(2020, 12, 31, 0, 0, 0, 0, time.UTC)
	}
	gen.matureStart, err = time.Parse("2006-01-02", config.PSR.MatureStart)
	if err != nil {
		gen.matureStart = time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	}

	gen.initializeLocations()
	gen.initializeCars()
	return gen
}

func (g *PSREventGenerator) initializeLocations() {
	g.locations = make([]Location, 0, g.config.Locations.TotalLocations)
	locID := 1

	// Create terminals
	for i := 0; i < g.config.Locations.Terminals; i++ {
		g.locations = append(g.locations, Location{
			ID:           fmt.Sprintf("TERM_%03d", locID),
			Name:         fmt.Sprintf("Terminal %d", locID),
			SPLCCode:     fmt.Sprintf("T%05d", 10000+locID),
			Type:         "TERMINAL",
			IsShadowYard: false,
			Latitude:     35.0 + g.rng.Float64()*10,
			Longitude:    -100.0 + g.rng.Float64()*20,
		})
		locID++
	}

	// Create interchanges
	for i := 0; i < g.config.Locations.Interchanges; i++ {
		g.locations = append(g.locations, Location{
			ID:           fmt.Sprintf("INTX_%03d", locID),
			Name:         fmt.Sprintf("Interchange %d", locID),
			SPLCCode:     fmt.Sprintf("I%05d", 20000+locID),
			Type:         "INTERCHANGE",
			IsShadowYard: false,
			Latitude:     35.0 + g.rng.Float64()*10,
			Longitude:    -100.0 + g.rng.Float64()*20,
		})
		locID++
	}

	// Create yards (some will be shadow yards)
	shadowYardIndices := make(map[int]bool)
	if g.config.Locations.ShadowYards.Count > 0 {
		// Select random yards to be shadow yards
		for len(shadowYardIndices) < g.config.Locations.ShadowYards.Count {
			idx := g.rng.Intn(g.config.Locations.Yards)
			shadowYardIndices[idx] = true
		}
	}

	for i := 0; i < g.config.Locations.Yards; i++ {
		g.locations = append(g.locations, Location{
			ID:           fmt.Sprintf("YARD_%03d", locID),
			Name:         fmt.Sprintf("Yard %d", locID),
			SPLCCode:     fmt.Sprintf("Y%05d", 30000+locID),
			Type:         "YARD",
			IsShadowYard: shadowYardIndices[i],
			Latitude:     35.0 + g.rng.Float64()*10,
			Longitude:    -100.0 + g.rng.Float64()*20,
		})
		locID++
	}

	// Create customer sites
	for i := 0; i < g.config.Locations.CustomerSites; i++ {
		g.locations = append(g.locations, Location{
			ID:           fmt.Sprintf("CUST_%03d", locID),
			Name:         fmt.Sprintf("Customer Site %d", locID),
			SPLCCode:     fmt.Sprintf("C%05d", 40000+locID),
			Type:         "CUSTOMER",
			IsShadowYard: false,
			Latitude:     35.0 + g.rng.Float64()*10,
			Longitude:    -100.0 + g.rng.Float64()*20,
		})
		locID++
	}

	// Create sidings
	for i := 0; i < g.config.Locations.Sidings; i++ {
		g.locations = append(g.locations, Location{
			ID:           fmt.Sprintf("SIDG_%03d", locID),
			Name:         fmt.Sprintf("Siding %d", locID),
			SPLCCode:     fmt.Sprintf("S%05d", 50000+locID),
			Type:         "SIDING",
			IsShadowYard: false,
			Latitude:     35.0 + g.rng.Float64()*10,
			Longitude:    -100.0 + g.rng.Float64()*20,
		})
		locID++
	}
}

func (g *PSREventGenerator) initializeCars() {
	g.cars = make([]string, 0, g.config.Fleet.TotalCars)
	railroads := g.config.Fleet.Railroads
	carsPerRR := g.config.Fleet.TotalCars / len(railroads)

	for _, rr := range railroads {
		for i := 0; i < carsPerRR; i++ {
			carNum := fmt.Sprintf("%s%06d", rr, 100000+len(g.cars))
			g.cars = append(g.cars, carNum)
		}
	}

	// Add remaining cars to first railroad
	for len(g.cars) < g.config.Fleet.TotalCars {
		carNum := fmt.Sprintf("%s%06d", railroads[0], 100000+len(g.cars))
		g.cars = append(g.cars, carNum)
	}
}

func (g *PSREventGenerator) GenerateEvents() ([]CLMEvent, error) {
	startDate, err := time.Parse("2006-01-02", g.config.TimeWindow.StartDate)
	if err != nil {
		return nil, fmt.Errorf("invalid start date: %w", err)
	}

	endDate := startDate.AddDate(0, 0, g.config.TimeWindow.DurationDays)
	events := make([]CLMEvent, 0)

	// Generate events for each car
	for _, carNum := range g.cars {
		carEvents := g.generateCarJourney(carNum, startDate, endDate)
		events = append(events, carEvents...)
	}

	return events, nil
}

// GenerateEventsStreaming generates events and writes them directly to CSV
func (g *PSREventGenerator) GenerateEventsStreaming(writer *csv.Writer) (int, error) {
	startDate, err := time.Parse("2006-01-02", g.config.TimeWindow.StartDate)
	if err != nil {
		return 0, fmt.Errorf("invalid start date: %w", err)
	}

	endDate := startDate.AddDate(0, 0, g.config.TimeWindow.DurationDays)
	totalEvents := 0

	// Generate events for each car and write immediately
	for i, carNum := range g.cars {
		if i%1000 == 0 && i > 0 {
			fmt.Printf("  Processed %d/%d cars...\n", i, len(g.cars))
		}

		carEvents := g.generateCarJourney(carNum, startDate, endDate)

		// Write events immediately
		for _, event := range carEvents {
			row := []string{
				strconv.Itoa(totalEvents + 1),
				event.CarNumber,
				event.Timestamp.Format("2006-01-02 15:04"),
				event.EventType,
				event.SPLCCode,
				event.TrainID,
				event.LocationName,
			}
			if err := writer.Write(row); err != nil {
				return totalEvents, fmt.Errorf("error writing event: %w", err)
			}
			totalEvents++
		}

		// Flush periodically to avoid buffering too much
		if i%100 == 0 {
			writer.Flush()
		}
	}

	writer.Flush()
	return totalEvents, nil
}

func (g *PSREventGenerator) generateCarJourney(carNum string, startDate, endDate time.Time) []CLMEvent {
	events := make([]CLMEvent, 0)
	currentTime := startDate
	trainCounter := 1

	for currentTime.Before(endDate) {
		// Pick random origin and destination
		origin := g.locations[g.rng.Intn(len(g.locations))]
		dest := g.locations[g.rng.Intn(len(g.locations))]
		if origin.ID == dest.ID {
			continue
		}

		trainID := fmt.Sprintf("TRAIN_%s_%04d", carNum[0:4], trainCounter)

		// PLAC event (loading)
		placTime := g.roundToMinute(currentTime)
		events = append(events, CLMEvent{
			EventID:      fmt.Sprintf("EVT_%s_%d", carNum, len(events)),
			CarNumber:    carNum,
			Timestamp:    placTime,
			EventType:    "PLAC",
			SPLCCode:     origin.SPLCCode,
			TrainID:      trainID,
			LocationName: origin.Name,
		})

		// Loading time
		loadingHours := 2.0 + g.rng.Float64()*4.0
		currentTime = placTime.Add(time.Duration(loadingHours * float64(time.Hour)))

		// DEPA event (departure)
		depaTime := g.roundToMinute(currentTime)
		events = append(events, CLMEvent{
			EventID:      fmt.Sprintf("EVT_%s_%d", carNum, len(events)),
			CarNumber:    carNum,
			Timestamp:    depaTime,
			EventType:    "DEPA",
			SPLCCode:     origin.SPLCCode,
			TrainID:      trainID,
			LocationName: origin.Name,
		})

		// Transit time (affected by PSR period and season)
		transitHours := g.calculateTransitTime(depaTime, 200.0) // Assume 200 miles average
		currentTime = depaTime.Add(time.Duration(transitHours * float64(time.Hour)))

		// ARRI event (arrival)
		arriTime := g.roundToMinute(currentTime)
		events = append(events, CLMEvent{
			EventID:      fmt.Sprintf("EVT_%s_%d", carNum, len(events)),
			CarNumber:    carNum,
			Timestamp:    arriTime,
			EventType:    "ARRI",
			SPLCCode:     dest.SPLCCode,
			TrainID:      trainID,
			LocationName: dest.Name,
		})

		// Dwell time at destination
		dwellHours := g.calculateDwellTime(arriTime, dest.IsShadowYard)
		currentTime = arriTime.Add(time.Duration(dwellHours * float64(time.Hour)))

		// PULL event (unloading)
		pullTime := g.roundToMinute(currentTime)
		events = append(events, CLMEvent{
			EventID:      fmt.Sprintf("EVT_%s_%d", carNum, len(events)),
			CarNumber:    carNum,
			Timestamp:    pullTime,
			EventType:    "PULL",
			SPLCCode:     dest.SPLCCode,
			TrainID:      trainID,
			LocationName: dest.Name,
		})

		// Unloading time
		unloadingHours := 1.5 + g.rng.Float64()*3.0
		currentTime = pullTime.Add(time.Duration(unloadingHours * float64(time.Hour)))

		trainCounter++
	}

	return events
}

func (g *PSREventGenerator) calculateTransitTime(timestamp time.Time, distanceMiles float64) float64 {
	// Base velocity depends on PSR period
	var baseVelocity float64
	if timestamp.Before(g.prePSREnd) {
		// Pre-PSR: 15-18 mph
		baseVelocity = 15.0 + g.rng.Float64()*3.0
	} else if timestamp.Before(g.transEnd) {
		// Transition: 18-22 mph
		baseVelocity = 18.0 + g.rng.Float64()*4.0
	} else {
		// Mature PSR: 22-25 mph
		baseVelocity = 22.0 + g.rng.Float64()*3.0
	}

	// Apply seasonal variation
	seasonalFactor := g.getSeasonalFactor(timestamp)
	adjustedVelocity := baseVelocity * seasonalFactor

	// Calculate transit time in hours
	return distanceMiles / adjustedVelocity
}

func (g *PSREventGenerator) calculateDwellTime(timestamp time.Time, isShadowYard bool) float64 {
	// Base dwell time with variance depending on PSR period
	var baseDwell, variance float64
	if timestamp.Before(g.prePSREnd) {
		// Pre-PSR: higher dwell, higher variance
		baseDwell = 24.0 + g.rng.Float64()*12.0
		variance = g.rng.Float64() * 8.0
	} else if timestamp.Before(g.transEnd) {
		// Transition: improving
		baseDwell = 18.0 + g.rng.Float64()*8.0
		variance = g.rng.Float64() * 6.0
	} else {
		// Mature PSR: lower dwell, lower variance
		baseDwell = 12.0 + g.rng.Float64()*6.0
		variance = g.rng.Float64() * 4.0
	}

	dwellTime := baseDwell + variance

	// Shadow yards have artificially low dwell (gaming metrics)
	if isShadowYard {
		dwellTime = dwellTime * 0.5
	}

	return dwellTime
}

func (g *PSREventGenerator) getSeasonalFactor(timestamp time.Time) float64 {
	month := timestamp.Month()
	variationFactor := float64(g.config.Seasonal.VariationPercent) / 100.0

	switch month {
	case 12, 1, 2: // Winter - slower
		return 1.0 - variationFactor*(0.8+g.rng.Float64()*0.2)
	case 6, 7, 8: // Summer - faster
		return 1.0 + variationFactor*(0.4+g.rng.Float64()*0.2)
	default: // Spring/Fall - normal
		return 1.0 + (g.rng.Float64()-0.5)*variationFactor*0.2
	}
}

func (g *PSREventGenerator) roundToMinute(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
}

// Helper functions for main and tests
func loadConfig(configPath string) (PSRSeedConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return PSRSeedConfig{}, err
	}

	var config PSRSeedConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return PSRSeedConfig{}, err
	}

	return config, nil
}

func writeCSV(outputPath string, headers []string, events []CLMEvent) error {
	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write headers
	if err := writer.Write(headers); err != nil {
		return err
	}

	// Write events
	for i, event := range events {
		row := []string{
			strconv.Itoa(i + 1),
			event.CarNumber,
			event.Timestamp.Format("2006-01-02 15:04"),
			event.EventType,
			event.SPLCCode,
			event.TrainID,
			event.LocationName,
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return nil
}

func main() {
	// Load configuration
	configPath := filepath.Join("examples", "precision_railroading", "seeds", "clm_generation_config.yml")
	config, err := loadConfig(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading config: %v\n", err)
		os.Exit(1)
	}

	// Generate events using streaming approach
	fmt.Println("Generating PSR CLM events...")
	generator := NewPSREventGenerator(config, 42)

	// Prepare output file
	outputPath := filepath.Join("examples", "precision_railroading", "seeds", config.Output.Filename)
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

	// Generate and write events in streaming fashion
	totalEvents, err := generator.GenerateEventsStreaming(writer)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating events: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\nSuccessfully wrote %d events to %s\n", totalEvents, outputPath)

	// Note: Statistics are not available in streaming mode
	fmt.Println("Generation complete!")
}

// Suppress unused warning
var _ = math.Abs
