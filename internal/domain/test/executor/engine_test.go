package executor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jpconstantineau/gorchata/internal/domain/test"
	"github.com/jpconstantineau/gorchata/internal/domain/test/storage"
	"github.com/jpconstantineau/gorchata/internal/platform"
)

// MockDatabaseAdapter for testing
type MockDatabaseAdapter struct {
	QueryResults     map[string]*platform.QueryResult
	QueryErrors      map[string]error
	TableRowCounts   map[string]int
	ConnectError     error
	TableExistsError error
}

func NewMockDatabaseAdapter() *MockDatabaseAdapter {
	return &MockDatabaseAdapter{
		QueryResults:   make(map[string]*platform.QueryResult),
		QueryErrors:    make(map[string]error),
		TableRowCounts: make(map[string]int),
	}
}

func (m *MockDatabaseAdapter) Connect(ctx context.Context) error {
	return m.ConnectError
}

func (m *MockDatabaseAdapter) Close() error {
	return nil
}

func (m *MockDatabaseAdapter) ExecuteQuery(ctx context.Context, sql string, args ...interface{}) (*platform.QueryResult, error) {
	if err, ok := m.QueryErrors[sql]; ok {
		return nil, err
	}
	if result, ok := m.QueryResults[sql]; ok {
		return result, nil
	}
	// Default: return empty result
	return &platform.QueryResult{
		Columns:      []string{},
		Rows:         [][]interface{}{},
		RowsAffected: 0,
	}, nil
}

func (m *MockDatabaseAdapter) ExecuteDDL(ctx context.Context, sql string) error {
	return nil
}

func (m *MockDatabaseAdapter) TableExists(ctx context.Context, table string) (bool, error) {
	if m.TableExistsError != nil {
		return false, m.TableExistsError
	}
	_, exists := m.TableRowCounts[table]
	return exists, nil
}

func (m *MockDatabaseAdapter) GetTableSchema(ctx context.Context, table string) (*platform.Schema, error) {
	return nil, errors.New("not implemented")
}

func (m *MockDatabaseAdapter) CreateTableAs(ctx context.Context, table, selectSQL string) error {
	return errors.New("not implemented")
}

func (m *MockDatabaseAdapter) CreateView(ctx context.Context, view, selectSQL string) error {
	return errors.New("not implemented")
}

func (m *MockDatabaseAdapter) BeginTransaction(ctx context.Context) (platform.Transaction, error) {
	return nil, errors.New("not implemented")
}

func TestNewTestEngine(t *testing.T) {
	adapter := NewMockDatabaseAdapter()

	engine, err := NewTestEngine(adapter, nil, nil)
	if err != nil {
		t.Errorf("NewTestEngine() error = %v, want nil", err)
	}
	if engine == nil {
		t.Error("NewTestEngine() returned nil engine")
	}
}

func TestNewTestEngine_NilAdapter(t *testing.T) {
	_, err := NewTestEngine(nil, nil, nil)
	if err == nil {
		t.Error("NewTestEngine() with nil adapter should return error")
	}
}

func TestExecuteTest_PassingTest(t *testing.T) {
	adapter := NewMockDatabaseAdapter()

	// Test returns 0 rows (passes)
	adapter.QueryResults["SELECT * FROM users WHERE email IS NULL"] = &platform.QueryResult{
		Columns:      []string{"id", "email"},
		Rows:         [][]interface{}{},
		RowsAffected: 0,
	}

	engine, _ := NewTestEngine(adapter, nil, nil)

	testObj, _ := test.NewTest(
		"not_null_users_email",
		"not_null",
		"users",
		"email",
		test.GenericTest,
		"SELECT * FROM users WHERE email IS NULL",
	)

	ctx := context.Background()
	result, err := engine.ExecuteTest(ctx, testObj)

	if err != nil {
		t.Errorf("ExecuteTest() error = %v, want nil", err)
	}
	if result.Status != test.StatusPassed {
		t.Errorf("ExecuteTest() status = %v, want %v", result.Status, test.StatusPassed)
	}
	if result.FailureCount != 0 {
		t.Errorf("ExecuteTest() failure count = %d, want 0", result.FailureCount)
	}
}

func TestExecuteTest_FailingTest(t *testing.T) {
	adapter := NewMockDatabaseAdapter()

	// Test returns 3 rows (fails)
	adapter.QueryResults["SELECT * FROM users WHERE email IS NULL"] = &platform.QueryResult{
		Columns: []string{"id", "email"},
		Rows: [][]interface{}{
			{1, nil},
			{2, nil},
			{3, nil},
		},
		RowsAffected: 3,
	}

	engine, _ := NewTestEngine(adapter, nil, nil)

	testObj, _ := test.NewTest(
		"not_null_users_email",
		"not_null",
		"users",
		"email",
		test.GenericTest,
		"SELECT * FROM users WHERE email IS NULL",
	)

	ctx := context.Background()
	result, err := engine.ExecuteTest(ctx, testObj)

	if err != nil {
		t.Errorf("ExecuteTest() error = %v, want nil", err)
	}
	if result.Status != test.StatusFailed {
		t.Errorf("ExecuteTest() status = %v, want %v", result.Status, test.StatusFailed)
	}
	if result.FailureCount != 3 {
		t.Errorf("ExecuteTest() failure count = %d, want 3", result.FailureCount)
	}
}

func TestExecuteTest_WithWarnings(t *testing.T) {
	adapter := NewMockDatabaseAdapter()

	// Test returns 2 rows
	adapter.QueryResults["SELECT * FROM users WHERE email IS NULL"] = &platform.QueryResult{
		Columns: []string{"id", "email"},
		Rows: [][]interface{}{
			{1, nil},
			{2, nil},
		},
		RowsAffected: 2,
	}

	engine, _ := NewTestEngine(adapter, nil, nil)

	testObj, _ := test.NewTest(
		"not_null_users_email",
		"not_null",
		"users",
		"email",
		test.GenericTest,
		"SELECT * FROM users WHERE email IS NULL",
	)

	// Set severity to Warn
	testObj.Config.SetSeverity(test.SeverityWarn)

	ctx := context.Background()
	result, err := engine.ExecuteTest(ctx, testObj)

	if err != nil {
		t.Errorf("ExecuteTest() error = %v, want nil", err)
	}
	if result.Status != test.StatusWarning {
		t.Errorf("ExecuteTest() status = %v, want %v", result.Status, test.StatusWarning)
	}
	if result.FailureCount != 2 {
		t.Errorf("ExecuteTest() failure count = %d, want 2", result.FailureCount)
	}
}

func TestExecuteTest_WithThresholds_ErrorIf(t *testing.T) {
	adapter := NewMockDatabaseAdapter()

	// Test returns 5 rows
	adapter.QueryResults["SELECT * FROM users WHERE email IS NULL"] = &platform.QueryResult{
		Columns:      []string{"id", "email"},
		Rows:         make([][]interface{}, 5),
		RowsAffected: 5,
	}

	engine, _ := NewTestEngine(adapter, nil, nil)

	testObj, _ := test.NewTest(
		"not_null_users_email",
		"not_null",
		"users",
		"email",
		test.GenericTest,
		"SELECT * FROM users WHERE email IS NULL",
	)

	// Set threshold: error if > 2
	testObj.Config.ErrorIf = &test.ConditionalThreshold{
		Operator: test.OperatorGreaterThan,
		Value:    2,
	}

	ctx := context.Background()
	result, err := engine.ExecuteTest(ctx, testObj)

	if err != nil {
		t.Errorf("ExecuteTest() error = %v, want nil", err)
	}
	if result.Status != test.StatusFailed {
		t.Errorf("ExecuteTest() status = %v, want %v", result.Status, test.StatusFailed)
	}
}

func TestExecuteTest_WithThresholds_WarnIf(t *testing.T) {
	adapter := NewMockDatabaseAdapter()

	// Test returns 2 rows
	adapter.QueryResults["SELECT * FROM users WHERE email IS NULL"] = &platform.QueryResult{
		Columns:      []string{"id", "email"},
		Rows:         make([][]interface{}, 2),
		RowsAffected: 2,
	}

	engine, _ := NewTestEngine(adapter, nil, nil)

	testObj, _ := test.NewTest(
		"not_null_users_email",
		"not_null",
		"users",
		"email",
		test.GenericTest,
		"SELECT * FROM users WHERE email IS NULL",
	)

	// Set threshold: warn if >= 1, error if > 5
	testObj.Config.WarnIf = &test.ConditionalThreshold{
		Operator: test.OperatorGreaterThanOrEqual,
		Value:    1,
	}
	testObj.Config.ErrorIf = &test.ConditionalThreshold{
		Operator: test.OperatorGreaterThan,
		Value:    5,
	}

	ctx := context.Background()
	result, err := engine.ExecuteTest(ctx, testObj)

	if err != nil {
		t.Errorf("ExecuteTest() error = %v, want nil", err)
	}
	if result.Status != test.StatusWarning {
		t.Errorf("ExecuteTest() status = %v, want %v", result.Status, test.StatusWarning)
	}
}

func TestExecuteTests_AllPass(t *testing.T) {
	adapter := NewMockDatabaseAdapter()

	// All tests return 0 rows
	adapter.QueryResults["SELECT * FROM users WHERE email IS NULL"] = &platform.QueryResult{
		Columns: []string{"id"}, Rows: [][]interface{}{}, RowsAffected: 0,
	}
	adapter.QueryResults["SELECT * FROM orders WHERE customer_id IS NULL"] = &platform.QueryResult{
		Columns: []string{"id"}, Rows: [][]interface{}{}, RowsAffected: 0,
	}

	engine, _ := NewTestEngine(adapter, nil, nil)

	test1, _ := test.NewTest("test1", "not_null", "users", "email", test.GenericTest,
		"SELECT * FROM users WHERE email IS NULL")
	test2, _ := test.NewTest("test2", "not_null", "orders", "customer_id", test.GenericTest,
		"SELECT * FROM orders WHERE customer_id IS NULL")

	tests := []*test.Test{test1, test2}

	ctx := context.Background()
	summary, err := engine.ExecuteTests(ctx, tests)

	if err != nil {
		t.Errorf("ExecuteTests() error = %v, want nil", err)
	}
	if summary.TotalTests != 2 {
		t.Errorf("ExecuteTests() total = %d, want 2", summary.TotalTests)
	}
	if summary.PassedTests != 2 {
		t.Errorf("ExecuteTests() passed = %d, want 2", summary.PassedTests)
	}
	if summary.FailedTests != 0 {
		t.Errorf("ExecuteTests() failed = %d, want 0", summary.FailedTests)
	}
}

func TestExecuteTests_SomeFailures(t *testing.T) {
	adapter := NewMockDatabaseAdapter()

	// First test passes, second fails
	adapter.QueryResults["SELECT * FROM users WHERE email IS NULL"] = &platform.QueryResult{
		Columns: []string{"id"}, Rows: [][]interface{}{}, RowsAffected: 0,
	}
	adapter.QueryResults["SELECT * FROM orders WHERE customer_id IS NULL"] = &platform.QueryResult{
		Columns: []string{"id"}, Rows: [][]interface{}{{1}, {2}}, RowsAffected: 2,
	}

	engine, _ := NewTestEngine(adapter, nil, nil)

	test1, _ := test.NewTest("test1", "not_null", "users", "email", test.GenericTest,
		"SELECT * FROM users WHERE email IS NULL")
	test2, _ := test.NewTest("test2", "not_null", "orders", "customer_id", test.GenericTest,
		"SELECT * FROM orders WHERE customer_id IS NULL")

	tests := []*test.Test{test1, test2}

	ctx := context.Background()
	summary, err := engine.ExecuteTests(ctx, tests)

	if err != nil {
		t.Errorf("ExecuteTests() error = %v, want nil", err)
	}
	if summary.TotalTests != 2 {
		t.Errorf("ExecuteTests() total = %d, want 2", summary.TotalTests)
	}
	if summary.PassedTests != 1 {
		t.Errorf("ExecuteTests() passed = %d, want 1", summary.PassedTests)
	}
	if summary.FailedTests != 1 {
		t.Errorf("ExecuteTests() failed = %d, want 1", summary.FailedTests)
	}
}

func TestExecuteTests_WithWarnings(t *testing.T) {
	adapter := NewMockDatabaseAdapter()

	adapter.QueryResults["SELECT * FROM users WHERE email IS NULL"] = &platform.QueryResult{
		Columns: []string{"id"}, Rows: [][]interface{}{}, RowsAffected: 0,
	}
	adapter.QueryResults["SELECT * FROM orders WHERE total < 0"] = &platform.QueryResult{
		Columns: []string{"id"}, Rows: [][]interface{}{{1}}, RowsAffected: 1,
	}

	engine, _ := NewTestEngine(adapter, nil, nil)

	test1, _ := test.NewTest("test1", "not_null", "users", "email", test.GenericTest,
		"SELECT * FROM users WHERE email IS NULL")
	test2, _ := test.NewTest("test2", "positive_values", "orders", "total", test.GenericTest,
		"SELECT * FROM orders WHERE total < 0")
	test2.Config.SetSeverity(test.SeverityWarn)

	tests := []*test.Test{test1, test2}

	ctx := context.Background()
	summary, err := engine.ExecuteTests(ctx, tests)

	if err != nil {
		t.Errorf("ExecuteTests() error = %v, want nil", err)
	}
	if summary.PassedTests != 1 {
		t.Errorf("ExecuteTests() passed = %d, want 1", summary.PassedTests)
	}
	if summary.WarningTests != 1 {
		t.Errorf("ExecuteTests() warnings = %d, want 1", summary.WarningTests)
	}
}

func TestExecuteTest_QueryError(t *testing.T) {
	adapter := NewMockDatabaseAdapter()

	// Simulate SQL error
	adapter.QueryErrors["SELECT * FROM users WHERE email IS NULL"] = errors.New("table does not exist")

	engine, _ := NewTestEngine(adapter, nil, nil)

	testObj, _ := test.NewTest(
		"not_null_users_email",
		"not_null",
		"users",
		"email",
		test.GenericTest,
		"SELECT * FROM users WHERE email IS NULL",
	)

	ctx := context.Background()
	result, err := engine.ExecuteTest(ctx, testObj)

	if err == nil {
		t.Error("ExecuteTest() should return error for query failure")
	}
	if result != nil {
		t.Error("ExecuteTest() should return nil result on error")
	}
}

func TestExecuteTest_ResultTiming(t *testing.T) {
	adapter := NewMockDatabaseAdapter()
	adapter.QueryResults["SELECT * FROM users WHERE email IS NULL"] = &platform.QueryResult{
		Columns: []string{"id"}, Rows: [][]interface{}{}, RowsAffected: 0,
	}

	engine, _ := NewTestEngine(adapter, nil, nil)

	testObj, _ := test.NewTest(
		"not_null_users_email",
		"not_null",
		"users",
		"email",
		test.GenericTest,
		"SELECT * FROM users WHERE email IS NULL",
	)

	ctx := context.Background()
	start := time.Now()
	result, err := engine.ExecuteTest(ctx, testObj)
	elapsed := time.Since(start)

	if err != nil {
		t.Errorf("ExecuteTest() error = %v, want nil", err)
	}
	if result.Duration() > elapsed {
		t.Error("ExecuteTest() result duration should be less than total elapsed time")
	}
	if result.StartTime.IsZero() {
		t.Error("ExecuteTest() result start time should be set")
	}
	if result.EndTime.IsZero() {
		t.Error("ExecuteTest() result end time should be set")
	}
}

// TestNewTestEngine_WithFailureStore tests engine creation with failure store
func TestNewTestEngine_WithFailureStore(t *testing.T) {
	adapter := NewMockDatabaseAdapter()
	mockStore := &MockFailureStore{}

	engine, err := NewTestEngine(adapter, nil, mockStore)
	if err != nil {
		t.Errorf("NewTestEngine() error = %v, want nil", err)
	}
	if engine == nil {
		t.Error("NewTestEngine() returned nil engine")
	}
}

// TestExecuteTest_StoreFailures_Enabled tests storing failures when enabled
func TestExecuteTest_StoreFailures_Enabled(t *testing.T) {
	adapter := NewMockDatabaseAdapter()
	mockStore := &MockFailureStore{}

	// Setup test that will fail
	sql := "SELECT * FROM users WHERE email IS NULL"
	adapter.QueryResults[sql] = &platform.QueryResult{
		Columns: []string{"id", "email", "name"},
		Rows: [][]interface{}{
			{1, nil, "John Doe"},
			{2, nil, "Jane Smith"},
		},
		RowsAffected: 2,
	}

	engine, _ := NewTestEngine(adapter, nil, mockStore)
	ctx := context.Background()

	testObj, _ := test.NewTest("test_id", "Test Name", "users", "email", test.GenericTest, sql)
	testObj.Config.StoreFailures = true

	result, err := engine.ExecuteTest(ctx, testObj)
	if err != nil {
		t.Fatalf("ExecuteTest() error = %v, want nil", err)
	}

	// Test should fail
	if result.Status != test.StatusFailed {
		t.Errorf("ExecuteTest() status = %v, want %v", result.Status, test.StatusFailed)
	}

	// Failures should be stored
	if len(mockStore.StoreFailuresCalls) != 1 {
		t.Errorf("StoreFailures() called %d times, want 1", len(mockStore.StoreFailuresCalls))
	}

	// Check stored data
	if len(mockStore.StoreFailuresCalls) > 0 {
		call := mockStore.StoreFailuresCalls[0]
		if call.TestID != "test_id" {
			t.Errorf("Stored test ID = %s, want test_id", call.TestID)
		}
		if len(call.Failures) != 2 {
			t.Errorf("Stored %d failures, want 2", len(call.Failures))
		}
	}
}

// TestExecuteTest_StoreFailures_Disabled tests no storage when disabled
func TestExecuteTest_StoreFailures_Disabled(t *testing.T) {
	adapter := NewMockDatabaseAdapter()
	mockStore := &MockFailureStore{}

	// Setup test that will fail
	sql := "SELECT * FROM users WHERE email IS NULL"
	adapter.QueryResults[sql] = &platform.QueryResult{
		Columns: []string{"id", "email"},
		Rows: [][]interface{}{
			{1, nil},
		},
		RowsAffected: 1,
	}

	engine, _ := NewTestEngine(adapter, nil, mockStore)
	ctx := context.Background()

	testObj, _ := test.NewTest("test_id", "Test Name", "users", "email", test.GenericTest, sql)
	testObj.Config.StoreFailures = false // Disabled

	result, err := engine.ExecuteTest(ctx, testObj)
	if err != nil {
		t.Fatalf("ExecuteTest() error = %v, want nil", err)
	}

	// Test should fail
	if result.Status != test.StatusFailed {
		t.Errorf("ExecuteTest() status = %v, want %v", result.Status, test.StatusFailed)
	}

	// Failures should NOT be stored
	if len(mockStore.StoreFailuresCalls) != 0 {
		t.Errorf("StoreFailures() called %d times, want 0", len(mockStore.StoreFailuresCalls))
	}
}

// TestExecuteTest_StoreFailures_PassingTest tests no storage for passing tests
func TestExecuteTest_StoreFailures_PassingTest(t *testing.T) {
	adapter := NewMockDatabaseAdapter()
	mockStore := &MockFailureStore{}

	// Setup test that will pass (no rows returned)
	sql := "SELECT * FROM users WHERE email IS NULL"
	adapter.QueryResults[sql] = &platform.QueryResult{
		Columns:      []string{"id", "email"},
		Rows:         [][]interface{}{}, // No failing rows
		RowsAffected: 0,
	}

	engine, _ := NewTestEngine(adapter, nil, mockStore)
	ctx := context.Background()

	testObj, _ := test.NewTest("test_id", "Test Name", "users", "email", test.GenericTest, sql)
	testObj.Config.StoreFailures = true

	result, err := engine.ExecuteTest(ctx, testObj)
	if err != nil {
		t.Fatalf("ExecuteTest() error = %v, want nil", err)
	}

	// Test should pass
	if result.Status != test.StatusPassed {
		t.Errorf("ExecuteTest() status = %v, want %v", result.Status, test.StatusPassed)
	}

	// Failures should NOT be stored (test passed)
	if len(mockStore.StoreFailuresCalls) != 0 {
		t.Errorf("StoreFailures() called %d times, want 0", len(mockStore.StoreFailuresCalls))
	}
}

// TestExecuteTest_StoreFailures_StorageError tests handling of storage errors
func TestExecuteTest_StoreFailures_StorageError(t *testing.T) {
	adapter := NewMockDatabaseAdapter()
	mockStore := &MockFailureStore{
		StoreFailuresError: errors.New("storage error"),
	}

	// Setup test that will fail
	sql := "SELECT * FROM users WHERE email IS NULL"
	adapter.QueryResults[sql] = &platform.QueryResult{
		Columns: []string{"id", "email"},
		Rows: [][]interface{}{
			{1, nil},
		},
		RowsAffected: 1,
	}

	engine, _ := NewTestEngine(adapter, nil, mockStore)
	ctx := context.Background()

	testObj, _ := test.NewTest("test_id", "Test Name", "users", "email", test.GenericTest, sql)
	testObj.Config.StoreFailures = true

	result, err := engine.ExecuteTest(ctx, testObj)

	// Test execution should NOT fail even if storage fails
	if err != nil {
		t.Errorf("ExecuteTest() error = %v, want nil (storage error should not fail test)", err)
	}

	// Test should still be marked as failed
	if result.Status != test.StatusFailed {
		t.Errorf("ExecuteTest() status = %v, want %v", result.Status, test.StatusFailed)
	}

	// Storage should have been attempted
	if len(mockStore.StoreFailuresCalls) != 1 {
		t.Errorf("StoreFailures() called %d times, want 1", len(mockStore.StoreFailuresCalls))
	}

	// Result message should indicate storage error
	if result.ErrorMessage == "" {
		t.Error("ExecuteTest() result error message should indicate storage error")
	}
}

// TestExecuteTest_StoreFailures_NilStore tests execution with nil failure store
func TestExecuteTest_StoreFailures_NilStore(t *testing.T) {
	adapter := NewMockDatabaseAdapter()

	// Setup test that will fail
	sql := "SELECT * FROM users WHERE email IS NULL"
	adapter.QueryResults[sql] = &platform.QueryResult{
		Columns: []string{"id", "email"},
		Rows: [][]interface{}{
			{1, nil},
		},
		RowsAffected: 1,
	}

	engine, _ := NewTestEngine(adapter, nil, nil) // nil failure store
	ctx := context.Background()

	testObj, _ := test.NewTest("test_id", "Test Name", "users", "email", test.GenericTest, sql)
	testObj.Config.StoreFailures = true

	result, err := engine.ExecuteTest(ctx, testObj)

	// Should not panic or error when store is nil
	if err != nil {
		t.Errorf("ExecuteTest() error = %v, want nil", err)
	}

	// Test should still fail normally
	if result.Status != test.StatusFailed {
		t.Errorf("ExecuteTest() status = %v, want %v", result.Status, test.StatusFailed)
	}
}

// TestGenerateTestRunID tests test run ID generation
func TestGenerateTestRunID(t *testing.T) {
	id1 := generateTestRunID()
	id2 := generateTestRunID()

	if id1 == "" {
		t.Error("generateTestRunID() returned empty string")
	}
	if id1 == id2 {
		t.Error("generateTestRunID() should generate unique IDs")
	}
}

// TestConvertToFailureRows tests conversion of query results to FailureRow
func TestConvertToFailureRows(t *testing.T) {
	testRunID := "run-123"
	testObj, _ := test.NewTest("test_id", "Test Name", "users", "email", test.GenericTest, "SELECT * FROM users")

	rows := []map[string]interface{}{
		{"id": 1, "email": nil, "name": "John"},
		{"id": 2, "email": nil, "name": "Jane"},
	}

	failures := convertToFailureRows(testRunID, testObj, rows)

	if len(failures) != 2 {
		t.Errorf("convertToFailureRows() returned %d failures, want 2", len(failures))
	}

	for i, f := range failures {
		if f.TestID != testObj.ID {
			t.Errorf("failures[%d].TestID = %s, want %s", i, f.TestID, testObj.ID)
		}
		if f.TestRunID != testRunID {
			t.Errorf("failures[%d].TestRunID = %s, want %s", i, f.TestRunID, testRunID)
		}
		if f.FailedAt.IsZero() {
			t.Errorf("failures[%d].FailedAt is zero", i)
		}
		if len(f.RowData) == 0 {
			t.Errorf("failures[%d].RowData is empty", i)
		}
	}
}

// MockFailureStore is a test mock for storage.FailureStore
type MockFailureStore struct {
	StoreFailuresCalls []struct {
		TestID    string
		TestRunID string
		Failures  []storage.FailureRow
	}
	StoreFailuresError error
}

func (m *MockFailureStore) Initialize(ctx context.Context) error {
	return nil
}

func (m *MockFailureStore) StoreFailures(ctx context.Context, t *test.Test, testRunID string, failures []storage.FailureRow) error {
	m.StoreFailuresCalls = append(m.StoreFailuresCalls, struct {
		TestID    string
		TestRunID string
		Failures  []storage.FailureRow
	}{
		TestID:    t.ID,
		TestRunID: testRunID,
		Failures:  failures,
	})
	return m.StoreFailuresError
}

func (m *MockFailureStore) CleanupOldFailures(ctx context.Context, retentionDays int) error {
	return nil
}

func (m *MockFailureStore) GetFailures(ctx context.Context, testID string, limit int) ([]storage.FailureRow, error) {
	return nil, nil
}
