package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jpconstantineau/gorchata/internal/domain/test"
	"github.com/jpconstantineau/gorchata/internal/domain/test/storage"
	"github.com/jpconstantineau/gorchata/internal/platform"
	"github.com/jpconstantineau/gorchata/internal/template"
)

// TestEngine executes data quality tests against a database
type TestEngine struct {
	adapter        platform.DatabaseAdapter
	templateEngine *template.Engine
	sampler        *Sampler
	failureStore   storage.FailureStore
}

// NewTestEngine creates a new test execution engine
func NewTestEngine(adapter platform.DatabaseAdapter, templateEngine *template.Engine, failureStore storage.FailureStore) (*TestEngine, error) {
	if adapter == nil {
		return nil, fmt.Errorf("database adapter cannot be nil")
	}

	return &TestEngine{
		adapter:        adapter,
		templateEngine: templateEngine,
		sampler:        NewSampler(adapter),
		failureStore:   failureStore,
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

	// Get SQL to execute (render template if template engine is available)
	sql := t.SQLTemplate

	// Render template if template engine is available
	if e.templateEngine != nil {
		tmpl, err := e.templateEngine.Parse(t.ID, sql)
		if err != nil {
			// If template parsing fails, fall back to original SQL
			// This allows tests without templates to work
			sql = t.SQLTemplate
		} else {
			// Create a context for template rendering
			templateCtx := template.NewContext()
			rendered, err := template.Render(tmpl, templateCtx, nil)
			if err != nil {
				// If rendering fails, fall back to original SQL
				sql = t.SQLTemplate
			} else {
				sql = rendered
			}
		}
	}

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

	// Store failures if enabled and test failed
	if t.Config.StoreFailures && status == test.StatusFailed && e.failureStore != nil && len(queryResult.Rows) > 0 {
		testRunID := generateTestRunID()
		failingRows := e.captureFailingRows(queryResult)
		failures := convertToFailureRows(testRunID, t, failingRows)

		if err := e.failureStore.StoreFailures(ctx, t, testRunID, failures); err != nil {
			// Log warning but don't fail test execution
			result.Complete(status, failureCount, fmt.Sprintf("Test failed. Warning: could not store failures: %v", err))
		} else {
			result.Complete(status, failureCount, fmt.Sprintf("Test failed. %d failures stored", len(failures)))
		}
	} else {
		// Complete the result normally
		result.Complete(status, failureCount, "")
	}

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

// captureFailingRows converts query results to a slice of maps
func (e *TestEngine) captureFailingRows(queryResult *platform.QueryResult) []map[string]interface{} {
	rows := make([]map[string]interface{}, 0, len(queryResult.Rows))

	// Limit to 1000 rows for performance
	maxRows := len(queryResult.Rows)
	if maxRows > 1000 {
		maxRows = 1000
	}

	for i := 0; i < maxRows; i++ {
		row := make(map[string]interface{})
		for j, col := range queryResult.Columns {
			if j < len(queryResult.Rows[i]) {
				row[col] = queryResult.Rows[i][j]
			}
		}
		rows = append(rows, row)
	}

	return rows
}

// generateTestRunID generates a unique ID for this test run
func generateTestRunID() string {
	return uuid.New().String()
}

// convertToFailureRows converts query result rows to FailureRow instances
func convertToFailureRows(testRunID string, t *test.Test, rows []map[string]interface{}) []storage.FailureRow {
	failures := make([]storage.FailureRow, 0, len(rows))
	now := time.Now()

	for _, row := range rows {
		failure := storage.FailureRow{
			TestID:        t.ID,
			TestRunID:     testRunID,
			FailedAt:      now,
			FailureReason: fmt.Sprintf("Test '%s' failed", t.Name),
			RowData:       row,
		}
		failures = append(failures, failure)
	}

	return failures
}
