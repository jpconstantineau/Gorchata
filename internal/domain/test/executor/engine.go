package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/jpconstantineau/gorchata/internal/domain/test"
	"github.com/jpconstantineau/gorchata/internal/platform"
	"github.com/jpconstantineau/gorchata/internal/template"
)

// TestEngine executes data quality tests against a database
type TestEngine struct {
	adapter        platform.DatabaseAdapter
	templateEngine *template.Engine
	sampler        *Sampler
}

// NewTestEngine creates a new test execution engine
func NewTestEngine(adapter platform.DatabaseAdapter, templateEngine *template.Engine) (*TestEngine, error) {
	if adapter == nil {
		return nil, fmt.Errorf("database adapter cannot be nil")
	}

	return &TestEngine{
		adapter:        adapter,
		templateEngine: templateEngine,
		sampler:        NewSampler(adapter),
	}, nil
}

// ExecuteTests executes multiple tests in sequence and returns aggregated results
func (e *TestEngine) ExecuteTests(ctx context.Context, tests []*test.Test) (*test.TestSummary, error) {
	summary := test.NewTestSummary()

	for _, t := range tests {
		result, err := e.ExecuteTest(ctx, t)
		if err != nil {
			// Log error but continue with other tests
			result = test.NewTestResult(t.ID, test.StatusFailed)
			result.Complete(test.StatusFailed, 0, err.Error())
		}

		summary.AddResult(result)
	}

	summary.EndTime = time.Now()
	return summary, nil
}

// ExecuteTest executes a single test and returns the result
func (e *TestEngine) ExecuteTest(ctx context.Context, t *test.Test) (*test.TestResult, error) {
	result := test.NewTestResult(t.ID, test.StatusRunning)

	// Get SQL to execute (with potential sampling)
	sql := t.SQLTemplate

	// Apply adaptive sampling if needed
	if t.ModelName != "" {
		rowCount, err := e.sampler.GetTableRowCount(ctx, t.ModelName)
		if err == nil {
			shouldSample, sampleSize := e.sampler.ShouldSample(t, rowCount)
			if shouldSample {
				sql = e.sampler.ApplySampling(sql, sampleSize)
			}
		}
		// Ignore sampling errors, continue with original query
	}

	// Execute test query
	queryResult, err := e.adapter.ExecuteQuery(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to execute test query: %w", err)
	}

	// Count failing rows
	failureCount := int64(len(queryResult.Rows))

	// Determine test status based on failures and thresholds
	status := e.determineStatus(t, failureCount)

	// Complete the result
	result.Complete(status, failureCount, "")

	return result, nil
}

// determineStatus determines the final test status based on failure count and thresholds
func (e *TestEngine) determineStatus(t *test.Test, failureCount int64) test.TestStatus {
	// No failures = pass
	if failureCount == 0 {
		return test.StatusPassed
	}

	// Check conditional thresholds
	if t.Config.ErrorIf != nil && t.Config.ErrorIf.Evaluate(failureCount) {
		return test.StatusFailed
	}

	if t.Config.WarnIf != nil && t.Config.WarnIf.Evaluate(failureCount) {
		return test.StatusWarning
	}

	// Default behavior based on severity
	if t.Config.Severity == test.SeverityWarn {
		return test.StatusWarning
	}

	return test.StatusFailed
}
