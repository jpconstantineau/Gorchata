package domain

import (
	"fmt"
	"math/rand"
	"sort"
	"time"
)

// UnitTrainEventGenerator generates CLM events for unit train operations
type UnitTrainEventGenerator struct {
	Config          UnitTrainConfig
	QueueManager    *QueueManager
	StragglerEngine *StragglerEngine
	Random          *rand.Rand
}

// UnitTrainConfig contains configuration for unit train operations
type UnitTrainConfig struct {
	Fleet      FleetConfig
	Locations  LocationsConfig
	Corridors  []CorridorConfig
	Trains     TrainConfig
	Operations OperationsConfig
	Stragglers StragglerConfig
	Seasonal   SeasonalConfig
	TimeWindow TimeWindowConfig
	Output     OutputConfig
}

// FleetConfig defines the car fleet
type FleetConfig struct {
	TotalCars    int
	CarType      string
	Commodity    string
	CapacityTons int
}

// LocationsConfig defines origins and destinations
type LocationsConfig struct {
	Origins      []LocationDef
	Destinations []LocationDef
}

// LocationDef defines a single location
type LocationDef struct {
	ID                string
	Name              string
	LoadingHoursMin   float64
	LoadingHoursMax   float64
	UnloadingHoursMin float64
	UnloadingHoursMax float64
	QueueCapacity     int
}

// CorridorConfig defines a corridor between origin and destination
type CorridorConfig struct {
	ID                string
	OriginID          string
	DestinationID     string
	TransitDaysMin    int
	TransitDaysMax    int
	StationCount      int
	DistanceMiles     int
	EmptyReturnFactor float64
}

// TrainConfig defines train composition
type TrainConfig struct {
	CarsPerTrain   int
	ParallelTrains int
}

// OperationsConfig defines operational constraints
type OperationsConfig struct {
	OriginQueue      QueueConfig
	DestinationQueue QueueConfig
}

// QueueConfig defines queue constraints
type QueueConfig struct {
	MaxConcurrent int
}

// StragglerConfig defines straggler behavior
type StragglerConfig struct {
	RatePerTrainPerDay float64
	DelayHoursMin      float64
	DelayHoursMax      float64
}

// SeasonalConfig defines seasonal effects
type SeasonalConfig struct {
	SlowCorridorWeek    int
	SlowCorridorID      string
	SlowdownFactor      float64
	HighStragglerWeek   int
	StragglerMultiplier float64
}

// TimeWindowConfig defines simulation time window
type TimeWindowConfig struct {
	StartDate    string
	DurationDays int
}

// OutputConfig defines output format
type OutputConfig struct {
	Format   string
	Filename string
	Headers  []string
}

// CLMEvent represents a single car location message event
type CLMEvent struct {
	EventID    string
	Timestamp  time.Time
	CarIDs     []string
	TrainID    string
	LocationID string
	EventType  string
	LoadedFlag bool
	Commodity  string
	WeightTons int
}

// TrainState tracks the state of a single train
type TrainState struct {
	TrainID           string
	CarIDs            []string
	CurrentLocation   string
	CorridorID        string
	LoadedFlag        bool
	FormationTime     time.Time
	NextEventTime     time.Time
	Phase             string // "forming", "loading", "transit_loaded", "unloading", "transit_empty", "idle"
	LoadedTransitDays int    // Track loaded transit days for empty return comparison
}

// NewUnitTrainEventGenerator creates a new event generator
func NewUnitTrainEventGenerator(config UnitTrainConfig, seed int64) *UnitTrainEventGenerator {
	r := rand.New(rand.NewSource(seed))

	return &UnitTrainEventGenerator{
		Config:          config,
		QueueManager:    NewQueueManager(config.Operations),
		StragglerEngine: NewStragglerEngine(config.Stragglers, r),
		Random:          r,
	}
}

// GenerateEvents generates all CLM events for the configured time window
func (g *UnitTrainEventGenerator) GenerateEvents() ([]CLMEvent, error) {
	events := make([]CLMEvent, 0, 10000)
	eventIDCounter := 1

	// Parse start date
	startDate, err := time.Parse("2006-01-02", g.Config.TimeWindow.StartDate)
	if err != nil {
		return nil, fmt.Errorf("invalid start date: %w", err)
	}

	endDate := startDate.Add(time.Duration(g.Config.TimeWindow.DurationDays) * 24 * time.Hour)

	// Initialize trains
	trains := make([]*TrainState, g.Config.Trains.ParallelTrains)
	for i := 0; i < g.Config.Trains.ParallelTrains; i++ {
		trains[i] = &TrainState{
			TrainID:       fmt.Sprintf("TRAIN_%03d", i+1),
			Phase:         "idle",
			NextEventTime: startDate.Add(time.Duration(i*6) * time.Hour), // Stagger initial starts
		}
	}

	// Allocate cars - ensure enough for all trains
	allCars := make([]string, 0, g.Config.Fleet.TotalCars)
	for i := 1; i <= g.Config.Fleet.TotalCars; i++ {
		allCars = append(allCars, fmt.Sprintf("CAR_%05d", i))
	}

	// Shuffle all cars
	g.Random.Shuffle(len(allCars), func(i, j int) {
		allCars[i], allCars[j] = allCars[j], allCars[i]
	})

	// Track available cars (cars not currently assigned to any train)
	availableCars := make([]string, len(allCars))
	copy(availableCars, allCars)
	assignedCars := make(map[string]bool) // Track which cars are currently assigned

	// Simulation loop - process trains in time order
	for {
		// Find next train to process
		var nextTrain *TrainState
		for _, train := range trains {
			if nextTrain == nil || train.NextEventTime.Before(nextTrain.NextEventTime) {
				nextTrain = train
			}
		}

		// Check if simulation complete
		if nextTrain.NextEventTime.After(endDate) {
			break
		}

		// Process next event for this train
		currentTime := nextTrain.NextEventTime

		switch nextTrain.Phase {
		case "idle":
			// Form train - allocate cars from available pool
			neededCars := g.Config.Trains.CarsPerTrain

			// Count unassigned cars
			unassignedCount := 0
			for _, car := range availableCars {
				if !assignedCars[car] {
					unassignedCount++
				}
			}

			if unassignedCount < neededCars {
				// Not enough cars available - skip this formation
				nextTrain.NextEventTime = currentTime.Add(24 * time.Hour)
				continue
			}

			// Select origin and destination
			origin := g.selectOrigin()
			destination := g.selectDestination()
			corridor := g.findCorridor(origin.ID, destination.ID)

			if corridor == nil {
				// No corridor found - skip
				nextTrain.NextEventTime = currentTime.Add(24 * time.Hour)
				continue
			}

			// Allocate cars from available pool
			nextTrain.CarIDs = make([]string, 0, neededCars)
			for _, car := range availableCars {
				if !assignedCars[car] {
					nextTrain.CarIDs = append(nextTrain.CarIDs, car)
					assignedCars[car] = true
					if len(nextTrain.CarIDs) >= neededCars {
						break
					}
				}
			}

			nextTrain.CurrentLocation = origin.ID
			nextTrain.CorridorID = corridor.ID
			nextTrain.FormationTime = currentTime

			// Generate FORM_TRAIN event
			events = append(events, CLMEvent{
				EventID:    fmt.Sprintf("EVT_%08d", eventIDCounter),
				Timestamp:  currentTime,
				CarIDs:     nextTrain.CarIDs,
				TrainID:    nextTrain.TrainID,
				LocationID: origin.ID,
				EventType:  "FORM_TRAIN",
				LoadedFlag: false,
				Commodity:  g.Config.Fleet.Commodity,
				WeightTons: 0,
			})
			eventIDCounter++

			// Move to loading phase - wait for queue (add small delay after formation)
			nextTrain.Phase = "waiting_load"
			nextTrain.NextEventTime = currentTime.Add(1 * time.Minute)

		case "waiting_load":
			// Check if loading queue available
			origin := g.findLocationByID(nextTrain.CurrentLocation)
			if origin == nil {
				return nil, fmt.Errorf("origin not found: %s", nextTrain.CurrentLocation)
			}

			loadDuration := g.randomDuration(origin.LoadingHoursMin, origin.LoadingHoursMax)

			if g.QueueManager.CanStartLoading(nextTrain.CurrentLocation, currentTime, currentTime.Add(loadDuration)) {
				// Start loading
				g.QueueManager.StartLoading(nextTrain.CurrentLocation, nextTrain.TrainID, currentTime, currentTime.Add(loadDuration))

				events = append(events, CLMEvent{
					EventID:    fmt.Sprintf("EVT_%08d", eventIDCounter),
					Timestamp:  currentTime,
					CarIDs:     nextTrain.CarIDs,
					TrainID:    nextTrain.TrainID,
					LocationID: nextTrain.CurrentLocation,
					EventType:  "LOAD_START",
					LoadedFlag: false,
					Commodity:  g.Config.Fleet.Commodity,
					WeightTons: 0,
				})
				eventIDCounter++

				nextTrain.Phase = "loading"
				nextTrain.NextEventTime = currentTime.Add(loadDuration)
			} else {
				// Wait and retry
				nextTrain.NextEventTime = currentTime.Add(1 * time.Hour)
			}

		case "loading":
			// Complete loading
			g.QueueManager.CompleteLoading(nextTrain.CurrentLocation, nextTrain.TrainID)

			events = append(events, CLMEvent{
				EventID:    fmt.Sprintf("EVT_%08d", eventIDCounter),
				Timestamp:  currentTime,
				CarIDs:     nextTrain.CarIDs,
				TrainID:    nextTrain.TrainID,
				LocationID: nextTrain.CurrentLocation,
				EventType:  "LOAD_COMPLETE",
				LoadedFlag: true,
				Commodity:  g.Config.Fleet.Commodity,
				WeightTons: g.Config.Fleet.CapacityTons * len(nextTrain.CarIDs),
			})
			eventIDCounter++

			nextTrain.LoadedFlag = true

			// Depart origin
			events = append(events, CLMEvent{
				EventID:    fmt.Sprintf("EVT_%08d", eventIDCounter),
				Timestamp:  currentTime.Add(30 * time.Minute),
				CarIDs:     nextTrain.CarIDs,
				TrainID:    nextTrain.TrainID,
				LocationID: nextTrain.CurrentLocation,
				EventType:  "DEPART_ORIGIN",
				LoadedFlag: true,
				Commodity:  g.Config.Fleet.Commodity,
				WeightTons: g.Config.Fleet.CapacityTons * len(nextTrain.CarIDs),
			})
			eventIDCounter++

			nextTrain.Phase = "transit_loaded"
			nextTrain.NextEventTime = currentTime.Add(30 * time.Minute)

		case "transit_loaded":
			// Generate station events and travel to destination
			corridor := g.findCorridorByID(nextTrain.CorridorID)
			if corridor == nil {
				return nil, fmt.Errorf("corridor not found: %s", nextTrain.CorridorID)
			}

			// Calculate transit time (with seasonal effects)
			transitDays := g.randomInt(corridor.TransitDaysMin, corridor.TransitDaysMax)
			nextTrain.LoadedTransitDays = transitDays // Store for empty return
			transitDuration := time.Duration(transitDays) * 24 * time.Hour

			// Apply seasonal slowdown if applicable
			if g.isSlowWeek(currentTime) && corridor.ID == g.Config.Seasonal.SlowCorridorID {
				transitDuration = time.Duration(float64(transitDuration) * g.Config.Seasonal.SlowdownFactor)
			}

			// Generate station events
			stationEvents := g.generateStationEvents(nextTrain, corridor, currentTime, transitDuration, true, &eventIDCounter)
			events = append(events, stationEvents...)

			// Check for stragglers
			stragglerEvents := g.StragglerEngine.GenerateStragglerEvents(
				nextTrain, corridor, currentTime, transitDuration, true, &eventIDCounter, g.isHighStragglerWeek(currentTime),
			)
			if len(stragglerEvents) > 0 {
				events = append(events, stragglerEvents...)
				// Remove stragglers from train
				for _, se := range stragglerEvents {
					if se.EventType == "SET_OUT" {
						nextTrain.CarIDs = removeCarFromList(nextTrain.CarIDs, se.CarIDs[0])
					}
				}
			}

			// Arrive at destination
			arrivalTime := currentTime.Add(transitDuration)
			destination := g.findLocationByID(g.getDestinationFromCorridor(corridor))

			events = append(events, CLMEvent{
				EventID:    fmt.Sprintf("EVT_%08d", eventIDCounter),
				Timestamp:  arrivalTime,
				CarIDs:     nextTrain.CarIDs,
				TrainID:    nextTrain.TrainID,
				LocationID: destination.ID,
				EventType:  "ARRIVE_DESTINATION",
				LoadedFlag: true,
				Commodity:  g.Config.Fleet.Commodity,
				WeightTons: g.Config.Fleet.CapacityTons * len(nextTrain.CarIDs),
			})
			eventIDCounter++

			nextTrain.CurrentLocation = destination.ID
			nextTrain.Phase = "waiting_unload"
			nextTrain.NextEventTime = arrivalTime

		case "waiting_unload":
			// Check if unloading queue available
			destination := g.findLocationByID(nextTrain.CurrentLocation)
			if destination == nil {
				return nil, fmt.Errorf("destination not found: %s", nextTrain.CurrentLocation)
			}

			unloadDuration := g.randomDuration(destination.UnloadingHoursMin, destination.UnloadingHoursMax)

			if g.QueueManager.CanStartUnloading(nextTrain.CurrentLocation, currentTime, currentTime.Add(unloadDuration)) {
				// Start unloading
				g.QueueManager.StartUnloading(nextTrain.CurrentLocation, nextTrain.TrainID, currentTime, currentTime.Add(unloadDuration))

				events = append(events, CLMEvent{
					EventID:    fmt.Sprintf("EVT_%08d", eventIDCounter),
					Timestamp:  currentTime,
					CarIDs:     nextTrain.CarIDs,
					TrainID:    nextTrain.TrainID,
					LocationID: nextTrain.CurrentLocation,
					EventType:  "UNLOAD_START",
					LoadedFlag: true,
					Commodity:  g.Config.Fleet.Commodity,
					WeightTons: g.Config.Fleet.CapacityTons * len(nextTrain.CarIDs),
				})
				eventIDCounter++

				nextTrain.Phase = "unloading"
				nextTrain.NextEventTime = currentTime.Add(unloadDuration)
			} else {
				// Wait and retry
				nextTrain.NextEventTime = currentTime.Add(1 * time.Hour)
			}

		case "unloading":
			// Complete unloading
			g.QueueManager.CompleteUnloading(nextTrain.CurrentLocation, nextTrain.TrainID)

			events = append(events, CLMEvent{
				EventID:    fmt.Sprintf("EVT_%08d", eventIDCounter),
				Timestamp:  currentTime,
				CarIDs:     nextTrain.CarIDs,
				TrainID:    nextTrain.TrainID,
				LocationID: nextTrain.CurrentLocation,
				EventType:  "UNLOAD_COMPLETE",
				LoadedFlag: false,
				Commodity:  g.Config.Fleet.Commodity,
				WeightTons: 0,
			})
			eventIDCounter++

			nextTrain.LoadedFlag = false

			// Depart destination (empty)
			events = append(events, CLMEvent{
				EventID:    fmt.Sprintf("EVT_%08d", eventIDCounter),
				Timestamp:  currentTime.Add(30 * time.Minute),
				CarIDs:     nextTrain.CarIDs,
				TrainID:    nextTrain.TrainID,
				LocationID: nextTrain.CurrentLocation,
				EventType:  "DEPART_DESTINATION",
				LoadedFlag: false,
				Commodity:  g.Config.Fleet.Commodity,
				WeightTons: 0,
			})
			eventIDCounter++

			nextTrain.Phase = "transit_empty"
			nextTrain.NextEventTime = currentTime.Add(30 * time.Minute)

		case "transit_empty":
			// Return to origin (empty, faster)
			corridor := g.findCorridorByID(nextTrain.CorridorID)
			if corridor == nil {
				return nil, fmt.Errorf("corridor not found: %s", nextTrain.CorridorID)
			}

			// Calculate empty return time (faster) - use same base transit days as loaded trip
			transitDays := nextTrain.LoadedTransitDays
			if transitDays == 0 {
				// Fallback if not set (shouldn't happen)
				transitDays = g.randomInt(corridor.TransitDaysMin, corridor.TransitDaysMax)
			}
			transitDuration := time.Duration(float64(transitDays)*24*corridor.EmptyReturnFactor) * time.Hour

			// Generate station events (empty return)
			stationEvents := g.generateStationEvents(nextTrain, corridor, currentTime, transitDuration, false, &eventIDCounter)
			events = append(events, stationEvents...)

			// Check for stragglers on empty return
			stragglerEvents := g.StragglerEngine.GenerateStragglerEvents(
				nextTrain, corridor, currentTime, transitDuration, false, &eventIDCounter, g.isHighStragglerWeek(currentTime),
			)
			if len(stragglerEvents) > 0 {
				events = append(events, stragglerEvents...)
				// Remove stragglers from train
				for _, se := range stragglerEvents {
					if se.EventType == "SET_OUT" {
						nextTrain.CarIDs = removeCarFromList(nextTrain.CarIDs, se.CarIDs[0])
					}
				}
			}

			// Arrive at origin (empty)
			arrivalTime := currentTime.Add(transitDuration)
			origin := g.findLocationByID(g.getOriginFromCorridor(corridor))

			events = append(events, CLMEvent{
				EventID:    fmt.Sprintf("EVT_%08d", eventIDCounter),
				Timestamp:  arrivalTime,
				CarIDs:     nextTrain.CarIDs,
				TrainID:    nextTrain.TrainID,
				LocationID: origin.ID,
				EventType:  "ARRIVE_ORIGIN",
				LoadedFlag: false,
				Commodity:  g.Config.Fleet.Commodity,
				WeightTons: 0,
			})
			eventIDCounter++

			// Return cars to available pool
			for _, carID := range nextTrain.CarIDs {
				delete(assignedCars, carID)
			}

			// Turnaround time - power inference marker
			turnaroundTime := g.generateTurnaroundTime()

			nextTrain.CarIDs = nil
			nextTrain.CurrentLocation = ""
			nextTrain.CorridorID = ""
			nextTrain.Phase = "idle"
			nextTrain.NextEventTime = arrivalTime.Add(turnaroundTime)
		}
	}

	// Sort events by timestamp
	sort.Slice(events, func(i, j int) bool {
		return events[i].Timestamp.Before(events[j].Timestamp)
	})

	return events, nil
}

// Helper methods

func (g *UnitTrainEventGenerator) selectOrigin() LocationDef {
	idx := g.Random.Intn(len(g.Config.Locations.Origins))
	return g.Config.Locations.Origins[idx]
}

func (g *UnitTrainEventGenerator) selectDestination() LocationDef {
	idx := g.Random.Intn(len(g.Config.Locations.Destinations))
	return g.Config.Locations.Destinations[idx]
}

func (g *UnitTrainEventGenerator) findCorridor(originID, destID string) *CorridorConfig {
	for i := range g.Config.Corridors {
		if g.Config.Corridors[i].OriginID == originID && g.Config.Corridors[i].DestinationID == destID {
			return &g.Config.Corridors[i]
		}
	}
	return nil
}

func (g *UnitTrainEventGenerator) findCorridorByID(corridorID string) *CorridorConfig {
	for i := range g.Config.Corridors {
		if g.Config.Corridors[i].ID == corridorID {
			return &g.Config.Corridors[i]
		}
	}
	return nil
}

func (g *UnitTrainEventGenerator) findLocationByID(locationID string) *LocationDef {
	for i := range g.Config.Locations.Origins {
		if g.Config.Locations.Origins[i].ID == locationID {
			return &g.Config.Locations.Origins[i]
		}
	}
	for i := range g.Config.Locations.Destinations {
		if g.Config.Locations.Destinations[i].ID == locationID {
			return &g.Config.Locations.Destinations[i]
		}
	}
	return nil
}

func (g *UnitTrainEventGenerator) getOriginFromCorridor(corridor *CorridorConfig) string {
	return corridor.OriginID
}

func (g *UnitTrainEventGenerator) getDestinationFromCorridor(corridor *CorridorConfig) string {
	return corridor.DestinationID
}

func (g *UnitTrainEventGenerator) randomDuration(minHours, maxHours float64) time.Duration {
	hours := minHours + g.Random.Float64()*(maxHours-minHours)
	return time.Duration(hours * float64(time.Hour))
}

func (g *UnitTrainEventGenerator) randomInt(min, max int) int {
	if min == max {
		return min
	}
	return min + g.Random.Intn(max-min+1)
}

func (g *UnitTrainEventGenerator) generateStationEvents(
	train *TrainState,
	corridor *CorridorConfig,
	startTime time.Time,
	totalDuration time.Duration,
	loaded bool,
	eventIDCounter *int,
) []CLMEvent {
	events := make([]CLMEvent, 0, corridor.StationCount*2)

	// Divide transit time among stations
	timePerStation := totalDuration / time.Duration(corridor.StationCount+1)

	currentTime := startTime
	for i := 1; i <= corridor.StationCount; i++ {
		currentTime = currentTime.Add(timePerStation)

		stationID := fmt.Sprintf("%s_STN_%03d", corridor.ID, i)

		// Arrive at station
		events = append(events, CLMEvent{
			EventID:    fmt.Sprintf("EVT_%08d", *eventIDCounter),
			Timestamp:  currentTime,
			CarIDs:     train.CarIDs,
			TrainID:    train.TrainID,
			LocationID: stationID,
			EventType:  "ARRIVE_STATION",
			LoadedFlag: loaded,
			Commodity:  g.Config.Fleet.Commodity,
			WeightTons: func() int {
				if loaded {
					return g.Config.Fleet.CapacityTons * len(train.CarIDs)
				}
				return 0
			}(),
		})
		*eventIDCounter++

		// Dwell at station (2-4 hours for inspection/crew change)
		dwellTime := g.randomDuration(2.0, 4.0)
		currentTime = currentTime.Add(dwellTime)

		// Depart station
		events = append(events, CLMEvent{
			EventID:    fmt.Sprintf("EVT_%08d", *eventIDCounter),
			Timestamp:  currentTime,
			CarIDs:     train.CarIDs,
			TrainID:    train.TrainID,
			LocationID: stationID,
			EventType:  "DEPART_STATION",
			LoadedFlag: loaded,
			Commodity:  g.Config.Fleet.Commodity,
			WeightTons: func() int {
				if loaded {
					return g.Config.Fleet.CapacityTons * len(train.CarIDs)
				}
				return 0
			}(),
		})
		*eventIDCounter++
	}

	return events
}

func (g *UnitTrainEventGenerator) generateTurnaroundTime() time.Duration {
	// Generate turnaround time with variation
	// Quick turnaround (<1 hour) suggests same power
	// Longer turnaround (>1 hour) suggests power change
	if g.Random.Float64() < 0.4 {
		// 40% quick turnaround
		return time.Duration(30+g.Random.Intn(30)) * time.Minute
	}
	// 60% longer turnaround
	return time.Duration(1+g.Random.Intn(4)) * time.Hour
}

func (g *UnitTrainEventGenerator) isSlowWeek(currentTime time.Time) bool {
	startDate, _ := time.Parse("2006-01-02", g.Config.TimeWindow.StartDate)
	weekNum := int(currentTime.Sub(startDate).Hours()/(24*7)) + 1
	return weekNum == g.Config.Seasonal.SlowCorridorWeek
}

func (g *UnitTrainEventGenerator) isHighStragglerWeek(currentTime time.Time) bool {
	startDate, _ := time.Parse("2006-01-02", g.Config.TimeWindow.StartDate)
	weekNum := int(currentTime.Sub(startDate).Hours()/(24*7)) + 1
	return weekNum == g.Config.Seasonal.HighStragglerWeek
}

func removeCarFromList(carIDs []string, carID string) []string {
	result := make([]string, 0, len(carIDs))
	for _, c := range carIDs {
		if c != carID {
			result = append(result, c)
		}
	}
	return result
}
