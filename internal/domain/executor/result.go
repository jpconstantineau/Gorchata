package executor

import (
	"time"
)

// ExecutionStatus represents the status of execution
type ExecutionStatus string

const (
	// StatusPending indicates execution has not started
	StatusPending ExecutionStatus = "pending"
	// StatusRunning indicates execution is in progress
	StatusRunning ExecutionStatus = "running"
	// StatusSuccess indicates execution completed successfully
	StatusSuccess ExecutionStatus = "success"
	// StatusFailed indicates execution failed
	StatusFailed ExecutionStatus = "failed"
)

// ExecutionResult captures the results of executing one or more models
type ExecutionResult struct {
	// Status is the overall execution status
	Status ExecutionStatus

	// StartTime is when execution began
	StartTime time.Time

	// EndTime is when execution completed
	EndTime time.Time

	// ModelResults contains results for each model executed
	ModelResults []ModelResult
}

// ModelResult captures the result of executing a single model
type ModelResult struct {
	// ModelID is the identifier of the model
	ModelID string

	// Status is the execution status for this model
	Status ExecutionStatus

	// StartTime is when model execution began
	StartTime time.Time

	// EndTime is when model execution completed
	EndTime time.Time

	// Error contains error message if execution failed
	Error string

	// RowsAffected is the number of rows affected (if applicable)
	RowsAffected int64

	// SQLStatements are the SQL statements that were executed
	SQLStatements []string
}

// NewExecutionResult creates a new ExecutionResult with initial values
func NewExecutionResult() *ExecutionResult {
	return &ExecutionResult{
		Status:       StatusPending,
		StartTime:    time.Now(),
		ModelResults: []ModelResult{},
	}
}

// AddModelResult adds a model result to the execution result
func (r *ExecutionResult) AddModelResult(result ModelResult) {
	r.ModelResults = append(r.ModelResults, result)
}

// Complete marks the execution as complete and sets the final status
func (r *ExecutionResult) Complete() {
	r.EndTime = time.Now()

	// Determine overall status based on model results
	hasFailures := false
	for _, mr := range r.ModelResults {
		if mr.Status == StatusFailed {
			hasFailures = true
			break
		}
	}

	if hasFailures {
		r.Status = StatusFailed
	} else {
		r.Status = StatusSuccess
	}
}

// SuccessCount returns the number of successfully executed models
func (r *ExecutionResult) SuccessCount() int {
	count := 0
	for _, mr := range r.ModelResults {
		if mr.Status == StatusSuccess {
			count++
		}
	}
	return count
}

// FailureCount returns the number of failed models
func (r *ExecutionResult) FailureCount() int {
	count := 0
	for _, mr := range r.ModelResults {
		if mr.Status == StatusFailed {
			count++
		}
	}
	return count
}

// Duration returns the total execution duration
func (r *ExecutionResult) Duration() time.Duration {
	if r.EndTime.IsZero() {
		return time.Since(r.StartTime)
	}
	return r.EndTime.Sub(r.StartTime)
}

// Duration returns the execution duration for a model
func (mr *ModelResult) Duration() time.Duration {
	if mr.EndTime.IsZero() {
		return time.Since(mr.StartTime)
	}
	return mr.EndTime.Sub(mr.StartTime)
}
