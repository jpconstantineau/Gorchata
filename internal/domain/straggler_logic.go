package domain

import (
	"fmt"
	"math/rand"
	"time"
)

// StragglerEngine handles straggler car logic
type StragglerEngine struct {
	config StragglerConfig
	random *rand.Rand
}

// NewStragglerEngine creates a new straggler engine
func NewStragglerEngine(config StragglerConfig, r *rand.Rand) *StragglerEngine {
	return &StragglerEngine{
		config: config,
		random: r,
	}
}

// GenerateStragglerEvents generates straggler events if applicable
func (se *StragglerEngine) GenerateStragglerEvents(
	train *TrainState,
	corridor *CorridorConfig,
	startTime time.Time,
	transitDuration time.Duration,
	loaded bool,
	eventIDCounter *int,
	isHighStragglerWeek bool,
) []CLMEvent {
	events := make([]CLMEvent, 0)

	if len(train.CarIDs) == 0 {
		return events
	}

	// Calculate probability of straggler
	// Rate is per train per day in transit
	transitDays := transitDuration.Hours() / 24.0
	baseRate := se.config.RatePerTrainPerDay * transitDays

	// Apply seasonal multiplier if in high straggler week
	if isHighStragglerWeek {
		baseRate *= 2.0
	}

	// Probabilistic: should we generate a straggler?
	if se.random.Float64() > baseRate/float64(len(train.CarIDs)) {
		return events
	}

	// Select a random car to be a straggler
	carIdx := se.random.Intn(len(train.CarIDs))
	carID := train.CarIDs[carIdx]

	// Determine set-out point (somewhere in first half of transit)
	setOutTime := startTime.Add(time.Duration(se.random.Float64() * 0.5 * float64(transitDuration)))

	// Generate car_set_out event
	setOutLocation := fmt.Sprintf("%s_STN_%03d", corridor.ID, 1+se.random.Intn(corridor.StationCount))

	events = append(events, CLMEvent{
		EventID:    fmt.Sprintf("EVT_%08d", *eventIDCounter),
		Timestamp:  setOutTime,
		CarIDs:     []string{carID},
		TrainID:    train.TrainID,
		LocationID: setOutLocation,
		EventType:  "car_set_out",
		LoadedFlag: loaded,
		Commodity:  train.CarIDs[0], // Use as placeholder for commodity
		WeightTons: 0,
	})
	*eventIDCounter++

	// Determine delay period (6 hours to 3 days)
	delayHours := se.config.DelayHoursMin + se.random.Float64()*(se.config.DelayHoursMax-se.config.DelayHoursMin)
	delayDuration := time.Duration(delayHours) * time.Hour

	// Generate car_picked_up event
	resumeTime := setOutTime.Add(delayDuration)

	events = append(events, CLMEvent{
		EventID:    fmt.Sprintf("EVT_%08d", *eventIDCounter),
		Timestamp:  resumeTime,
		CarIDs:     []string{carID},
		TrainID:    "", // Independent travel - no train ID
		LocationID: setOutLocation,
		EventType:  "car_picked_up",
		LoadedFlag: loaded,
		Commodity:  train.CarIDs[0], // Placeholder
		WeightTons: 0,
	})
	*eventIDCounter++

	// Calculate remaining transit time to destination
	var destination string
	if loaded {
		destination = corridor.DestinationID
	} else {
		destination = corridor.OriginID
	}

	// Straggler travels independently to destination
	remainingTransitTime := transitDuration - setOutTime.Sub(startTime)
	arrivalTime := resumeTime.Add(remainingTransitTime)

	// Generate straggler arrival at destination
	events = append(events, CLMEvent{
		EventID:    fmt.Sprintf("EVT_%08d", *eventIDCounter),
		Timestamp:  arrivalTime,
		CarIDs:     []string{carID},
		TrainID:    "",
		LocationID: destination,
		EventType:  "arrived_destination",
		LoadedFlag: loaded,
		Commodity:  train.CarIDs[0], // Placeholder
		WeightTons: 0,
	})
	*eventIDCounter++

	// TODO: Generate JOIN_TRAIN event when next returning train is available
	// For now, we'll leave this as a future enhancement
	// The car will rejoin the available pool when train reaches origin

	return events
}
