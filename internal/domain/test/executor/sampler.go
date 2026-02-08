package executor

import (
	"context"
	"fmt"

	"github.com/jpconstantineau/gorchata/internal/domain/test"
	"github.com/jpconstantineau/gorchata/internal/platform"
)

const (
	// DefaultSampleThreshold is the row count threshold for automatic sampling
	DefaultSampleThreshold = 1000000

	// DefaultSampleSize is the default number of rows to sample
	DefaultSampleSize = 100000
)

// Sampler handles adaptive sampling for large tables
type Sampler struct {
	adapter platform.DatabaseAdapter
}

// NewSampler creates a new sampler
func NewSampler(adapter platform.DatabaseAdapter) *Sampler {
	return &Sampler{
		adapter: adapter,
	}
}

// GetTableRowCount retrieves the row count for a table
func (s *Sampler) GetTableRowCount(ctx context.Context, tableName string) (int, error) {
	if s.adapter == nil {
		return 0, fmt.Errorf("adapter not configured")
	}

	sql := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	result, err := s.adapter.ExecuteQuery(ctx, sql)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %w", err)
	}

	if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		return 0, nil
	}

	// Extract count from result
	count := int64(0)
	switch v := result.Rows[0][0].(type) {
	case int:
		count = int64(v)
	case int32:
		count = int64(v)
	case int64:
		count = v
	default:
		return 0, fmt.Errorf("unexpected count type: %T", v)
	}

	return int(count), nil
}

// ShouldSample determines if a test should use sampling and what size
func (s *Sampler) ShouldSample(t *test.Test, rowCount int) (bool, int) {
	// If explicit sample size is set in config
	if t.Config.SampleSize > 0 {
		return true, t.Config.SampleSize
	}

	// If sample size explicitly set to 0, no sampling
	if t.Config.SampleSize == 0 {
		// Check if it was explicitly set (we can't distinguish from default in this simple implementation)
		// For now, treat 0 as "use default behavior"
	}

	// Automatic sampling for large tables
	if rowCount >= DefaultSampleThreshold {
		return true, DefaultSampleSize
	}

	return false, 0
}

// ApplySampling wraps SQL with sampling logic
func (s *Sampler) ApplySampling(sql string, sampleSize int) string {
	if sql == "" {
		return ""
	}

	// Wrap original query with LIMIT clause
	// For SQLite, we use ORDER BY RANDOM() for sampling
	sampledSQL := fmt.Sprintf("SELECT * FROM (\n%s\n) ORDER BY RANDOM() LIMIT %d", sql, sampleSize)
	return sampledSQL
}
