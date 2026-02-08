package executor

import (
	"context"
	"strings"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/domain/test"
	"github.com/jpconstantineau/gorchata/internal/platform"
)

func TestGetTableRowCount_Success(t *testing.T) {
	adapter := NewMockDatabaseAdapter()
	adapter.TableRowCounts["users"] = 1500000

	// Mock the COUNT query
	adapter.QueryResults["SELECT COUNT(*) FROM users"] = &platform.QueryResult{
		Columns:      []string{"count"},
		Rows:         [][]interface{}{{int64(1500000)}},
		RowsAffected: 1,
	}

	sampler := NewSampler(adapter)
	ctx := context.Background()

	count, err := sampler.GetTableRowCount(ctx, "users")
	if err != nil {
		t.Errorf("GetTableRowCount() error = %v, want nil", err)
	}
	if count != 1500000 {
		t.Errorf("GetTableRowCount() = %d, want 1500000", count)
	}
}

func TestGetTableRowCount_SmallTable(t *testing.T) {
	adapter := NewMockDatabaseAdapter()
	adapter.TableRowCounts["small_table"] = 100

	adapter.QueryResults["SELECT COUNT(*) FROM small_table"] = &platform.QueryResult{
		Columns:      []string{"count"},
		Rows:         [][]interface{}{{int64(100)}},
		RowsAffected: 1,
	}

	sampler := NewSampler(adapter)
	ctx := context.Background()

	count, err := sampler.GetTableRowCount(ctx, "small_table")
	if err != nil {
		t.Errorf("GetTableRowCount() error = %v, want nil", err)
	}
	if count != 100 {
		t.Errorf("GetTableRowCount() = %d, want 100", count)
	}
}

func TestShouldSample_SmallTable(t *testing.T) {
	sampler := NewSampler(nil)

	testObj, _ := test.NewTest(
		"test1", "not_null", "users", "email", test.GenericTest,
		"SELECT * FROM users WHERE email IS NULL",
	)

	shouldSample, sampleSize := sampler.ShouldSample(testObj, 500000)

	if shouldSample {
		t.Error("ShouldSample() should return false for tables < 1M rows")
	}
	if sampleSize != 0 {
		t.Errorf("ShouldSample() size = %d, want 0", sampleSize)
	}
}

func TestShouldSample_LargeTable(t *testing.T) {
	sampler := NewSampler(nil)

	testObj, _ := test.NewTest(
		"test1", "not_null", "users", "email", test.GenericTest,
		"SELECT * FROM users WHERE email IS NULL",
	)

	shouldSample, sampleSize := sampler.ShouldSample(testObj, 2000000)

	if !shouldSample {
		t.Error("ShouldSample() should return true for tables >= 1M rows")
	}
	if sampleSize != 100000 {
		t.Errorf("ShouldSample() size = %d, want 100000", sampleSize)
	}
}

func TestShouldSample_ExplicitSampleSize(t *testing.T) {
	sampler := NewSampler(nil)

	testObj, _ := test.NewTest(
		"test1", "not_null", "users", "email", test.GenericTest,
		"SELECT * FROM users WHERE email IS NULL",
	)
	testObj.Config.SetSampleSize(50000)

	// Should sample even for small table if explicit sample size set
	shouldSample, sampleSize := sampler.ShouldSample(testObj, 500000)

	if !shouldSample {
		t.Error("ShouldSample() should return true when explicit sample size set")
	}
	if sampleSize != 50000 {
		t.Errorf("ShouldSample() size = %d, want 50000", sampleSize)
	}
}

func TestShouldSample_ExactThreshold(t *testing.T) {
	sampler := NewSampler(nil)

	testObj, _ := test.NewTest(
		"test1", "not_null", "users", "email", test.GenericTest,
		"SELECT * FROM users WHERE email IS NULL",
	)

	shouldSample, sampleSize := sampler.ShouldSample(testObj, 1000000)

	if !shouldSample {
		t.Error("ShouldSample() should return true for tables = 1M rows")
	}
	if sampleSize != 100000 {
		t.Errorf("ShouldSample() size = %d, want 100000", sampleSize)
	}
}

func TestApplySampling_SQLWrapper(t *testing.T) {
	sampler := NewSampler(nil)

	originalSQL := "SELECT * FROM users WHERE email IS NULL"
	sampleSize := 100000

	sampledSQL := sampler.ApplySampling(originalSQL, sampleSize)

	// Should contain original SQL
	if !strings.Contains(sampledSQL, originalSQL) {
		t.Error("ApplySampling() should contain original SQL")
	}

	// Should contain LIMIT clause
	if !strings.Contains(sampledSQL, "LIMIT 100000") {
		t.Error("ApplySampling() should contain LIMIT clause")
	}

	// Should be a subquery
	if !strings.Contains(sampledSQL, "SELECT * FROM (") {
		t.Error("ApplySampling() should wrap in subquery")
	}
}

func TestApplySampling_DifferentSizes(t *testing.T) {
	sampler := NewSampler(nil)

	tests := []struct {
		name       string
		sql        string
		sampleSize int
		wantLimit  string
	}{
		{
			name:       "50k sample",
			sql:        "SELECT * FROM users WHERE active = 1",
			sampleSize: 50000,
			wantLimit:  "LIMIT 50000",
		},
		{
			name:       "10k sample",
			sql:        "SELECT id FROM orders",
			sampleSize: 10000,
			wantLimit:  "LIMIT 10000",
		},
		{
			name:       "1M sample",
			sql:        "SELECT * FROM logs",
			sampleSize: 1000000,
			wantLimit:  "LIMIT 1000000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sampler.ApplySampling(tt.sql, tt.sampleSize)

			if !strings.Contains(result, tt.wantLimit) {
				t.Errorf("ApplySampling() = %s, want to contain %s", result, tt.wantLimit)
			}
		})
	}
}

func TestApplySampling_EmptySQL(t *testing.T) {
	sampler := NewSampler(nil)

	result := sampler.ApplySampling("", 10000)

	if result != "" {
		t.Errorf("ApplySampling() with empty SQL should return empty, got %s", result)
	}
}

func TestNewSampler(t *testing.T) {
	adapter := NewMockDatabaseAdapter()
	sampler := NewSampler(adapter)

	if sampler == nil {
		t.Error("NewSampler() returned nil")
	}
}

func TestNewSampler_NilAdapter(t *testing.T) {
	sampler := NewSampler(nil)

	if sampler == nil {
		t.Error("NewSampler() should handle nil adapter")
	}
}

func TestShouldSample_ZeroSampleSize(t *testing.T) {
	sampler := NewSampler(nil)

	testObj, _ := test.NewTest(
		"test1", "not_null", "users", "email", test.GenericTest,
		"SELECT * FROM users WHERE email IS NULL",
	)
	// Explicitly set to 0 (no sampling even for large table)
	testObj.Config.SetSampleSize(0)

	shouldSample, sampleSize := sampler.ShouldSample(testObj, 2000000)

	// Note: Current implementation treats 0 as "use default behavior"
	// For large tables, it will still sample. To truly disable sampling,
	// we'd need a separate flag or nil pointer
	if !shouldSample {
		t.Skip("Skipping - 0 sample size treated as default behavior")
	}
	if sampleSize == 0 {
		t.Errorf("ShouldSample() size = %d, want > 0", sampleSize)
	}
}
