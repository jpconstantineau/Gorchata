package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/jpconstantineau/gorchata/internal/domain/test"
	"github.com/jpconstantineau/gorchata/internal/platform"
	"github.com/jpconstantineau/gorchata/internal/platform/sqlite"
)

// createTestDatabase creates a temporary SQLite database for testing
func createTestDatabase(t *testing.T) platform.DatabaseAdapter {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	config := &platform.ConnectionConfig{
		DatabasePath: dbPath,
	}

	adapter := sqlite.NewSQLiteAdapter(config)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect to test database: %v", err)
	}

	t.Cleanup(func() {
		adapter.Close()
	})

	return adapter
}

// TestSQLiteStore_Interface verifies SQLiteFailureStore implements FailureStore
func TestSQLiteStore_Interface(t *testing.T) {
	var _ FailureStore = (*SQLiteFailureStore)(nil)
}

// TestNewSQLiteFailureStore tests store creation
func TestNewSQLiteFailureStore(t *testing.T) {
	adapter := createTestDatabase(t)
	store := NewSQLiteFailureStore(adapter)

	if store == nil {
		t.Fatal("NewSQLiteFailureStore() returned nil")
	}
}

// TestNewSQLiteFailureStore_NilAdapter tests store creation with nil adapter
func TestNewSQLiteFailureStore_NilAdapter(t *testing.T) {
	// Should not panic
	store := NewSQLiteFailureStore(nil)
	if store != nil {
		t.Error("NewSQLiteFailureStore(nil) should return nil or handle gracefully")
	}
}

// TestSQLiteStore_Initialize tests schema initialization
func TestSQLiteStore_Initialize(t *testing.T) {
	adapter := createTestDatabase(t)
	store := NewSQLiteFailureStore(adapter)
	ctx := context.Background()

	err := store.Initialize(ctx)
	if err != nil {
		t.Fatalf("Initialize() error = %v, want nil", err)
	}

	// Initialize should be idempotent
	err = store.Initialize(ctx)
	if err != nil {
		t.Fatalf("Second Initialize() error = %v, want nil", err)
	}
}

// TestSQLiteStore_StoreFailures_Basic tests storing failures with default table name
func TestSQLiteStore_StoreFailures_Basic(t *testing.T) {
	adapter := createTestDatabase(t)
	store := NewSQLiteFailureStore(adapter)
	ctx := context.Background()

	// Initialize store
	if err := store.Initialize(ctx); err != nil {
		t.Fatalf("Initialize() failed: %v", err)
	}

	// Create a test
	testObj, err := test.NewTest(
		"not_null_users_email",
		"Not Null Test",
		"users",
		"email",
		test.GenericTest,
		"SELECT * FROM users WHERE email IS NULL",
	)
	if err != nil {
		t.Fatalf("NewTest() failed: %v", err)
	}

	// Create failure rows
	failures := []FailureRow{
		{
			TestID:        testObj.ID,
			TestRunID:     "run-123",
			FailedAt:      time.Now(),
			FailureReason: "NULL value found",
			RowData: map[string]interface{}{
				"id":    123,
				"email": nil,
				"name":  "John Doe",
			},
		},
		{
			TestID:        testObj.ID,
			TestRunID:     "run-123",
			FailedAt:      time.Now(),
			FailureReason: "NULL value found",
			RowData: map[string]interface{}{
				"id":    456,
				"email": nil,
				"name":  "Jane Smith",
			},
		},
	}

	// Store failures
	err = store.StoreFailures(ctx, testObj, "run-123", failures)
	if err != nil {
		t.Fatalf("StoreFailures() error = %v, want nil", err)
	}

	// Verify failures were stored
	retrieved, err := store.GetFailures(ctx, testObj.ID, 10)
	if err != nil {
		t.Fatalf("GetFailures() error = %v, want nil", err)
	}
	if len(retrieved) != 2 {
		t.Errorf("GetFailures() returned %d failures, want 2", len(retrieved))
	}
}

// TestSQLiteStore_StoreFailures_CustomTableName tests storing with custom table name
func TestSQLiteStore_StoreFailures_CustomTableName(t *testing.T) {
	adapter := createTestDatabase(t)
	store := NewSQLiteFailureStore(adapter)
	ctx := context.Background()

	// Initialize store
	if err := store.Initialize(ctx); err != nil {
		t.Fatalf("Initialize() failed: %v", err)
	}

	// Create a test with custom table name
	testObj, err := test.NewTest(
		"not_null_users_email",
		"Not Null Test",
		"users",
		"email",
		test.GenericTest,
		"SELECT * FROM users WHERE email IS NULL",
	)
	if err != nil {
		t.Fatalf("NewTest() failed: %v", err)
	}

	// Set custom failure table name
	testObj.Config.StoreFailures = true
	testObj.Config.StoreFailuresAs = "custom_email_failures"

	// Create failure rows
	failures := []FailureRow{
		{
			TestID:        testObj.ID,
			TestRunID:     "run-456",
			FailedAt:      time.Now(),
			FailureReason: "NULL value found",
			RowData: map[string]interface{}{
				"id":    789,
				"email": nil,
			},
		},
	}

	// Store failures
	err = store.StoreFailures(ctx, testObj, "run-456", failures)
	if err != nil {
		t.Fatalf("StoreFailures() error = %v, want nil", err)
	}

	// Verify failures were stored
	retrieved, err := store.GetFailures(ctx, testObj.ID, 10)
	if err != nil {
		t.Fatalf("GetFailures() error = %v, want nil", err)
	}
	if len(retrieved) != 1 {
		t.Errorf("GetFailures() returned %d failures, want 1", len(retrieved))
	}
}

// TestSQLiteStore_StoreFailures_DynamicColumns tests handling different column types
func TestSQLiteStore_StoreFailures_DynamicColumns(t *testing.T) {
	adapter := createTestDatabase(t)
	store := NewSQLiteFailureStore(adapter)
	ctx := context.Background()

	// Initialize store
	if err := store.Initialize(ctx); err != nil {
		t.Fatalf("Initialize() failed: %v", err)
	}

	// Create a test
	testObj, err := test.NewTest(
		"test_various_types",
		"Various Types Test",
		"products",
		"",
		test.GenericTest,
		"SELECT * FROM products",
	)
	if err != nil {
		t.Fatalf("NewTest() failed: %v", err)
	}

	// Create failure rows with various types
	failures := []FailureRow{
		{
			TestID:        testObj.ID,
			TestRunID:     "run-789",
			FailedAt:      time.Now(),
			FailureReason: "Invalid data",
			RowData: map[string]interface{}{
				"id":          1,
				"name":        "Product A",
				"price":       29.99,
				"in_stock":    true,
				"quantity":    int64(100),
				"description": "A great product",
			},
		},
	}

	// Store failures
	err = store.StoreFailures(ctx, testObj, "run-789", failures)
	if err != nil {
		t.Fatalf("StoreFailures() error = %v, want nil", err)
	}

	// Verify failures were stored with correct types
	retrieved, err := store.GetFailures(ctx, testObj.ID, 10)
	if err != nil {
		t.Fatalf("GetFailures() error = %v, want nil", err)
	}
	if len(retrieved) != 1 {
		t.Errorf("GetFailures() returned %d failures, want 1", len(retrieved))
	}
}

// TestSQLiteStore_StoreFailures_EmptyFailures tests storing empty failure list
func TestSQLiteStore_StoreFailures_EmptyFailures(t *testing.T) {
	adapter := createTestDatabase(t)
	store := NewSQLiteFailureStore(adapter)
	ctx := context.Background()

	// Initialize store
	if err := store.Initialize(ctx); err != nil {
		t.Fatalf("Initialize() failed: %v", err)
	}

	// Create a test
	testObj, err := test.NewTest(
		"test_empty",
		"Empty Test",
		"users",
		"",
		test.GenericTest,
		"SELECT * FROM users",
	)
	if err != nil {
		t.Fatalf("NewTest() failed: %v", err)
	}

	// Store empty failures - should not error
	err = store.StoreFailures(ctx, testObj, "run-000", []FailureRow{})
	if err != nil {
		t.Errorf("StoreFailures() with empty list error = %v, want nil", err)
	}
}

// TestSQLiteStore_GetFailures_NotFound tests getting failures for non-existent test
func TestSQLiteStore_GetFailures_NotFound(t *testing.T) {
	adapter := createTestDatabase(t)
	store := NewSQLiteFailureStore(adapter)
	ctx := context.Background()

	// Initialize store
	if err := store.Initialize(ctx); err != nil {
		t.Fatalf("Initialize() failed: %v", err)
	}

	// Get failures for non-existent test
	failures, err := store.GetFailures(ctx, "non_existent_test", 10)

	// Should either return empty list or error gracefully
	if err != nil {
		// Error is acceptable if table doesn't exist
		t.Logf("GetFailures() for non-existent test error = %v (acceptable)", err)
	} else if len(failures) != 0 {
		t.Errorf("GetFailures() for non-existent test returned %d failures, want 0", len(failures))
	}
}

// TestSQLiteStore_GetFailures_Limit tests limiting returned failures
func TestSQLiteStore_GetFailures_Limit(t *testing.T) {
	adapter := createTestDatabase(t)
	store := NewSQLiteFailureStore(adapter)
	ctx := context.Background()

	// Initialize store
	if err := store.Initialize(ctx); err != nil {
		t.Fatalf("Initialize() failed: %v", err)
	}

	// Create a test
	testObj, err := test.NewTest(
		"test_limit",
		"Limit Test",
		"users",
		"",
		test.GenericTest,
		"SELECT * FROM users",
	)
	if err != nil {
		t.Fatalf("NewTest() failed: %v", err)
	}

	// Store 5 failures
	failures := make([]FailureRow, 5)
	for i := 0; i < 5; i++ {
		failures[i] = FailureRow{
			TestID:    testObj.ID,
			TestRunID: "run-limit",
			FailedAt:  time.Now(),
			RowData: map[string]interface{}{
				"id": i,
			},
		}
	}

	err = store.StoreFailures(ctx, testObj, "run-limit", failures)
	if err != nil {
		t.Fatalf("StoreFailures() error = %v, want nil", err)
	}

	// Get only 3 failures
	retrieved, err := store.GetFailures(ctx, testObj.ID, 3)
	if err != nil {
		t.Fatalf("GetFailures() error = %v, want nil", err)
	}
	if len(retrieved) > 3 {
		t.Errorf("GetFailures() returned %d failures, want at most 3", len(retrieved))
	}
}

// TestSQLiteStore_CleanupOldFailures tests cleanup of old records
func TestSQLiteStore_CleanupOldFailures(t *testing.T) {
	adapter := createTestDatabase(t)
	store := NewSQLiteFailureStore(adapter)
	ctx := context.Background()

	// Initialize store
	if err := store.Initialize(ctx); err != nil {
		t.Fatalf("Initialize() failed: %v", err)
	}

	// Create a test
	testObj, err := test.NewTest(
		"test_cleanup",
		"Cleanup Test",
		"users",
		"",
		test.GenericTest,
		"SELECT * FROM users",
	)
	if err != nil {
		t.Fatalf("NewTest() failed: %v", err)
	}

	// Store old and recent failures
	oldTime := time.Now().AddDate(0, 0, -60) // 60 days ago
	recentTime := time.Now()

	failures := []FailureRow{
		{
			TestID:    testObj.ID,
			TestRunID: "run-old",
			FailedAt:  oldTime,
			RowData:   map[string]interface{}{"id": 1},
		},
		{
			TestID:    testObj.ID,
			TestRunID: "run-recent",
			FailedAt:  recentTime,
			RowData:   map[string]interface{}{"id": 2},
		},
	}

	err = store.StoreFailures(ctx, testObj, "run-cleanup", failures)
	if err != nil {
		t.Fatalf("StoreFailures() error = %v, want nil", err)
	}

	// Cleanup failures older than 30 days
	err = store.CleanupOldFailures(ctx, 30)
	if err != nil {
		t.Fatalf("CleanupOldFailures() error = %v, want nil", err)
	}

	// Verify only recent failure remains
	retrieved, err := store.GetFailures(ctx, testObj.ID, 10)
	if err != nil {
		t.Fatalf("GetFailures() after cleanup error = %v, want nil", err)
	}

	// Should have 1 recent failure (old one cleaned up)
	if len(retrieved) != 1 {
		t.Errorf("After cleanup: GetFailures() returned %d failures, want 1", len(retrieved))
	}
}

// TestSQLiteStore_CleanupOldFailures_NoTables tests cleanup when no tables exist
func TestSQLiteStore_CleanupOldFailures_NoTables(t *testing.T) {
	adapter := createTestDatabase(t)
	store := NewSQLiteFailureStore(adapter)
	ctx := context.Background()

	// Initialize store
	if err := store.Initialize(ctx); err != nil {
		t.Fatalf("Initialize() failed: %v", err)
	}

	// Cleanup with no failure tables - should not error
	err := store.CleanupOldFailures(ctx, 30)
	if err != nil {
		t.Errorf("CleanupOldFailures() with no tables error = %v, want nil", err)
	}
}
