package storage

import (
	"context"
	"testing"
	"time"

	"github.com/jpconstantineau/gorchata/internal/domain/test"
)

// TestFailureRow_Creation tests creating FailureRow instances
func TestFailureRow_Creation(t *testing.T) {
	now := time.Now()
	row := FailureRow{
		TestID:        "test_not_null_users_email",
		TestRunID:     "run-123",
		FailedAt:      now,
		FailureReason: "NULL value found",
		RowData: map[string]interface{}{
			"id":    123,
			"email": nil,
			"name":  "John Doe",
		},
	}

	if row.TestID != "test_not_null_users_email" {
		t.Errorf("TestID = %s, want test_not_null_users_email", row.TestID)
	}
	if row.TestRunID != "run-123" {
		t.Errorf("TestRunID = %s, want run-123", row.TestRunID)
	}
	if !row.FailedAt.Equal(now) {
		t.Errorf("FailedAt mismatch")
	}
	if row.FailureReason != "NULL value found" {
		t.Errorf("FailureReason = %s, want NULL value found", row.FailureReason)
	}
	if len(row.RowData) != 3 {
		t.Errorf("RowData length = %d, want 3", len(row.RowData))
	}
}

// MockFailureStore is a mock implementation of FailureStore for testing
type MockFailureStore struct {
	InitializeCalled   bool
	InitializeError    error
	StoreFailuresCalls []StoreFailuresCall
	StoreFailuresError error
	CleanupCalls       []int
	CleanupError       error
	GetFailuresCalls   []GetFailuresCall
	GetFailuresResult  []FailureRow
	GetFailuresError   error
}

type StoreFailuresCall struct {
	TestID    string
	TestRunID string
	Failures  []FailureRow
}

type GetFailuresCall struct {
	TestID string
	Limit  int
}

func (m *MockFailureStore) Initialize(ctx context.Context) error {
	m.InitializeCalled = true
	return m.InitializeError
}

func (m *MockFailureStore) StoreFailures(ctx context.Context, t *test.Test, testRunID string, failures []FailureRow) error {
	m.StoreFailuresCalls = append(m.StoreFailuresCalls, StoreFailuresCall{
		TestID:    t.ID,
		TestRunID: testRunID,
		Failures:  failures,
	})
	return m.StoreFailuresError
}

func (m *MockFailureStore) CleanupOldFailures(ctx context.Context, retentionDays int) error {
	m.CleanupCalls = append(m.CleanupCalls, retentionDays)
	return m.CleanupError
}

func (m *MockFailureStore) GetFailures(ctx context.Context, testID string, limit int) ([]FailureRow, error) {
	m.GetFailuresCalls = append(m.GetFailuresCalls, GetFailuresCall{
		TestID: testID,
		Limit:  limit,
	})
	return m.GetFailuresResult, m.GetFailuresError
}

// TestMockFailureStore_Interface verifies MockFailureStore implements FailureStore
func TestMockFailureStore_Interface(t *testing.T) {
	var _ FailureStore = (*MockFailureStore)(nil)
}

// TestMockFailureStore_Initialize tests mock initialize
func TestMockFailureStore_Initialize(t *testing.T) {
	mock := &MockFailureStore{}
	ctx := context.Background()

	err := mock.Initialize(ctx)
	if err != nil {
		t.Errorf("Initialize() error = %v, want nil", err)
	}
	if !mock.InitializeCalled {
		t.Error("Initialize() was not called")
	}
}

// TestMockFailureStore_StoreFailures tests mock store failures
func TestMockFailureStore_StoreFailures(t *testing.T) {
	mock := &MockFailureStore{}
	ctx := context.Background()

	testObj, _ := test.NewTest("test_id", "Test Name", "users", "email", test.GenericTest, "SELECT * FROM users WHERE email IS NULL")
	failures := []FailureRow{
		{TestID: "test_id", TestRunID: "run-1", FailedAt: time.Now()},
	}

	err := mock.StoreFailures(ctx, testObj, "run-1", failures)
	if err != nil {
		t.Errorf("StoreFailures() error = %v, want nil", err)
	}
	if len(mock.StoreFailuresCalls) != 1 {
		t.Errorf("StoreFailures() called %d times, want 1", len(mock.StoreFailuresCalls))
	}
	if mock.StoreFailuresCalls[0].TestID != "test_id" {
		t.Errorf("TestID = %s, want test_id", mock.StoreFailuresCalls[0].TestID)
	}
}

// TestMockFailureStore_GetFailures tests mock get failures
func TestMockFailureStore_GetFailures(t *testing.T) {
	mock := &MockFailureStore{
		GetFailuresResult: []FailureRow{
			{TestID: "test_id", TestRunID: "run-1", FailedAt: time.Now()},
		},
	}
	ctx := context.Background()

	failures, err := mock.GetFailures(ctx, "test_id", 10)
	if err != nil {
		t.Errorf("GetFailures() error = %v, want nil", err)
	}
	if len(failures) != 1 {
		t.Errorf("GetFailures() returned %d failures, want 1", len(failures))
	}
	if len(mock.GetFailuresCalls) != 1 {
		t.Errorf("GetFailures() called %d times, want 1", len(mock.GetFailuresCalls))
	}
}

// TestMockFailureStore_CleanupOldFailures tests mock cleanup
func TestMockFailureStore_CleanupOldFailures(t *testing.T) {
	mock := &MockFailureStore{}
	ctx := context.Background()

	err := mock.CleanupOldFailures(ctx, 30)
	if err != nil {
		t.Errorf("CleanupOldFailures() error = %v, want nil", err)
	}
	if len(mock.CleanupCalls) != 1 {
		t.Errorf("CleanupOldFailures() called %d times, want 1", len(mock.CleanupCalls))
	}
	if mock.CleanupCalls[0] != 30 {
		t.Errorf("CleanupOldFailures() retention = %d, want 30", mock.CleanupCalls[0])
	}
}
