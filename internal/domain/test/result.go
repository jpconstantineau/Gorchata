package test

import (
	"fmt"
	"time"
)

// TestResult captures the result of executing a single test
type TestResult struct {
	// TestID is the identifier of the test
	TestID string

	// Status is the execution status
	Status TestStatus

	// StartTime is when test execution began
	StartTime time.Time

	// EndTime is when test execution completed
	EndTime time.Time

	// FailureCount is the number of rows that failed the test
	FailureCount int64

	// ErrorMessage contains error details if the test failed
	ErrorMessage string

	// FailedRows contains sample failed rows for debugging (optional)
	FailedRows []map[string]interface{}
}

// NewTestResult creates a new TestResult with initial values
func NewTestResult(testID string, status TestStatus) *TestResult {
	return &TestResult{
		TestID:       testID,
		Status:       status,
		StartTime:    time.Now(),
		FailureCount: 0,
		ErrorMessage: "",
		FailedRows:   []map[string]interface{}{},
	}
}

// Validate checks if the TestResult is valid
func (tr *TestResult) Validate() error {
	if tr.TestID == "" {
		return fmt.Errorf("test ID cannot be empty")
	}
	if tr.Status == "" {
		return fmt.Errorf("status cannot be empty")
	}
	if tr.StartTime.IsZero() {
		return fmt.Errorf("start time cannot be zero")
	}
	return nil
}

// Duration returns the duration of the test execution
func (tr *TestResult) Duration() time.Duration {
	if tr.EndTime.IsZero() {
		return 0
	}
	return tr.EndTime.Sub(tr.StartTime)
}

// Complete marks the test as complete and sets final status and results
func (tr *TestResult) Complete(status TestStatus, failureCount int64, errorMessage string) {
	tr.Status = status
	tr.EndTime = time.Now()
	tr.FailureCount = failureCount
	tr.ErrorMessage = errorMessage
}

// AddFailedRows adds failed rows to the result for debugging
func (tr *TestResult) AddFailedRows(rows []map[string]interface{}) {
	tr.FailedRows = append(tr.FailedRows, rows...)
}

// TestSummary captures aggregate results of multiple test executions
type TestSummary struct {
	// TotalTests is the total number of tests executed
	TotalTests int

	// PassedTests is the number of tests that passed
	PassedTests int

	// FailedTests is the number of tests that failed
	FailedTests int

	// WarningTests is the number of tests with warnings
	WarningTests int

	// SkippedTests is the number of tests that were skipped
	SkippedTests int

	// StartTime is when test execution began
	StartTime time.Time

	// EndTime is when test execution completed
	EndTime time.Time

	// TestResults contains individual test results
	TestResults []*TestResult
}

// NewTestSummary creates a new TestSummary with initial values
func NewTestSummary() *TestSummary {
	return &TestSummary{
		TotalTests:   0,
		PassedTests:  0,
		FailedTests:  0,
		WarningTests: 0,
		SkippedTests: 0,
		StartTime:    time.Now(),
		TestResults:  []*TestResult{},
	}
}

// AddResult adds a test result and updates counters
func (ts *TestSummary) AddResult(result *TestResult) {
	ts.TestResults = append(ts.TestResults, result)
	ts.TotalTests++

	switch result.Status {
	case StatusPassed:
		ts.PassedTests++
	case StatusFailed:
		ts.FailedTests++
	case StatusWarning:
		ts.WarningTests++
	case StatusSkipped:
		ts.SkippedTests++
	}
}

// Complete marks the summary as complete
func (ts *TestSummary) Complete() {
	ts.EndTime = time.Now()
}

// Duration returns the total duration of test execution
func (ts *TestSummary) Duration() time.Duration {
	if ts.EndTime.IsZero() {
		return 0
	}
	return ts.EndTime.Sub(ts.StartTime)
}

// HasFailures returns true if any tests failed
func (ts *TestSummary) HasFailures() bool {
	return ts.FailedTests > 0
}
