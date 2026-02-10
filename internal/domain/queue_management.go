package domain

import (
	"time"
)

// QueueManager manages loading and unloading queues at origins and destinations
type QueueManager struct {
	config         OperationsConfig
	loadingSlots   map[string]*QueueSlot
	unloadingSlots map[string]*QueueSlot
}

// QueueSlot represents a single slot in a queue
type QueueSlot struct {
	TrainID   string
	StartTime time.Time
	EndTime   time.Time
}

// NewQueueManager creates a new queue manager
func NewQueueManager(config OperationsConfig) *QueueManager {
	return &QueueManager{
		config:         config,
		loadingSlots:   make(map[string]*QueueSlot),
		unloadingSlots: make(map[string]*QueueSlot),
	}
}

// CanStartLoading checks if a train can start loading at an origin
func (qm *QueueManager) CanStartLoading(locationID string, startTime, endTime time.Time) bool {
	// Check if there's an existing slot occupying this time
	slot, exists := qm.loadingSlots[locationID]
	if !exists {
		return true
	}

	// Check for time overlap
	// No overlap if new start is after existing end, or new end is before existing start
	if startTime.After(slot.EndTime) || startTime.Equal(slot.EndTime) {
		return true
	}
	if endTime.Before(slot.StartTime) || endTime.Equal(slot.StartTime) {
		return true
	}

	return false
}

// StartLoading marks a loading slot as occupied
func (qm *QueueManager) StartLoading(locationID, trainID string, startTime, endTime time.Time) {
	qm.loadingSlots[locationID] = &QueueSlot{
		TrainID:   trainID,
		StartTime: startTime,
		EndTime:   endTime,
	}
}

// CompleteLoading frees up a loading slot
func (qm *QueueManager) CompleteLoading(locationID, trainID string) {
	delete(qm.loadingSlots, locationID)
}

// CanStartUnloading checks if a train can start unloading at a destination
func (qm *QueueManager) CanStartUnloading(locationID string, startTime, endTime time.Time) bool {
	// Check if there's an existing slot occupying this time
	slot, exists := qm.unloadingSlots[locationID]
	if !exists {
		return true
	}

	// Check for time overlap
	if startTime.After(slot.EndTime) || startTime.Equal(slot.EndTime) {
		return true
	}
	if endTime.Before(slot.StartTime) || endTime.Equal(slot.StartTime) {
		return true
	}

	return false
}

// StartUnloading marks an unloading slot as occupied
func (qm *QueueManager) StartUnloading(locationID, trainID string, startTime, endTime time.Time) {
	qm.unloadingSlots[locationID] = &QueueSlot{
		TrainID:   trainID,
		StartTime: startTime,
		EndTime:   endTime,
	}
}

// CompleteUnloading frees up an unloading slot
func (qm *QueueManager) CompleteUnloading(locationID, trainID string) {
	delete(qm.unloadingSlots, locationID)
}
