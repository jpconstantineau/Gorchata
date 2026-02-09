package seeds

import "time"

// Status constants for seed execution results
const (
	StatusSuccess = "success"
	StatusFailed  = "failed"
	StatusRunning = "running"
)

// SeedResult represents the result of a seed execution
type SeedResult struct {
	SeedID     string    // Unique identifier for the seed
	Status     string    // Execution status (success, failed, running)
	StartTime  time.Time // When execution started
	EndTime    time.Time // When execution completed
	RowsLoaded int       // Number of rows successfully loaded
	Error      string    // Error message if execution failed
}
