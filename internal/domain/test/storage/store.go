package storage

import (
	"context"
	"time"

	"github.com/jpconstantineau/gorchata/internal/domain/test"
)

// FailureRow represents a single failing row captured during test execution
type FailureRow struct {
	TestID        string
	TestRunID     string // UUID for this test run
	FailedAt      time.Time
	FailureReason string                 // Optional: why this row failed
	RowData       map[string]interface{} // The actual failing row data
}

// FailureStore persists test failure details to database
type FailureStore interface {
	// Initialize creates the dbt_test__audit schema and required tables
	Initialize(ctx context.Context) error

	// StoreFailures persists failing rows for a test
	StoreFailures(ctx context.Context, test *test.Test, testRunID string, failures []FailureRow) error

	// CleanupOldFailures removes failure records older than retention period
	CleanupOldFailures(ctx context.Context, retentionDays int) error

	// GetFailures retrieves stored failures for a test
	GetFailures(ctx context.Context, testID string, limit int) ([]FailureRow, error)
}
