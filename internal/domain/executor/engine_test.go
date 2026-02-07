package executor

import (
	"context"
	"fmt"
	"testing"

	"github.com/pierre/gorchata/internal/domain/materialization"
	"github.com/pierre/gorchata/internal/platform"
	"github.com/pierre/gorchata/internal/template"
)

// mockAdapter is a mock DatabaseAdapter for testing
type mockAdapter struct {
	connected      bool
	closed         bool
	executedSQL    []string
	tableExists    map[string]bool
	createViewErr  error
	createTableErr error
	executeDDLErr  error
}

func newMockAdapter() *mockAdapter {
	return &mockAdapter{
		tableExists: make(map[string]bool),
		executedSQL: []string{},
	}
}

func (m *mockAdapter) Connect(ctx context.Context) error {
	m.connected = true
	return nil
}

func (m *mockAdapter) Close() error {
	m.closed = true
	return nil
}

func (m *mockAdapter) ExecuteQuery(ctx context.Context, sql string, args ...interface{}) (*platform.QueryResult, error) {
	m.executedSQL = append(m.executedSQL, sql)
	return &platform.QueryResult{}, nil
}

func (m *mockAdapter) ExecuteDDL(ctx context.Context, sql string) error {
	if m.executeDDLErr != nil {
		return m.executeDDLErr
	}
	m.executedSQL = append(m.executedSQL, sql)
	return nil
}

func (m *mockAdapter) TableExists(ctx context.Context, table string) (bool, error) {
	exists, ok := m.tableExists[table]
	return ok && exists, nil
}

func (m *mockAdapter) GetTableSchema(ctx context.Context, table string) (*platform.Schema, error) {
	return &platform.Schema{}, nil
}

func (m *mockAdapter) CreateTableAs(ctx context.Context, table, selectSQL string) error {
	if m.createTableErr != nil {
		return m.createTableErr
	}
	m.executedSQL = append(m.executedSQL, fmt.Sprintf("CREATE TABLE %s AS %s", table, selectSQL))
	return nil
}

func (m *mockAdapter) CreateView(ctx context.Context, view, selectSQL string) error {
	if m.createViewErr != nil {
		return m.createViewErr
	}
	m.executedSQL = append(m.executedSQL, fmt.Sprintf("CREATE VIEW %s AS %s", view, selectSQL))
	return nil
}

func (m *mockAdapter) BeginTransaction(ctx context.Context) (platform.Transaction, error) {
	return &mockTransaction{adapter: m}, nil
}

type mockTransaction struct {
	adapter    *mockAdapter
	committed  bool
	rolledBack bool
}

func (t *mockTransaction) Commit() error {
	t.committed = true
	return nil
}

func (t *mockTransaction) Rollback() error {
	t.rolledBack = true
	return nil
}

func (t *mockTransaction) Exec(ctx context.Context, sql string, args ...interface{}) error {
	t.adapter.executedSQL = append(t.adapter.executedSQL, sql)
	return nil
}

func TestNewEngine(t *testing.T) {
	adapter := newMockAdapter()
	engine := template.New()

	exec, err := NewEngine(adapter, engine)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if exec == nil {
		t.Fatal("expected non-nil engine")
	}
}

func TestNewEngine_NilAdapter(t *testing.T) {
	engine := template.New()

	_, err := NewEngine(nil, engine)
	if err == nil {
		t.Error("expected error for nil adapter")
	}
}

func TestNewEngine_NilTemplateEngine(t *testing.T) {
	adapter := newMockAdapter()

	_, err := NewEngine(adapter, nil)
	if err == nil {
		t.Error("expected error for nil template engine")
	}
}

func TestEngine_ExecuteModel_View(t *testing.T) {
	adapter := newMockAdapter()
	engine := template.New()

	exec, _ := NewEngine(adapter, engine)

	// Create a simple model
	model, _ := NewModel("test_view", "models/test_view.sql")
	model.SetCompiledSQL("SELECT 1 as id, 'test' as name")
	model.SetMaterializationConfig(materialization.MaterializationConfig{
		Type: materialization.MaterializationView,
	})

	ctx := context.Background()
	result, err := exec.ExecuteModel(ctx, model)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Status != StatusSuccess {
		t.Errorf("Status = %v, want %v", result.Status, StatusSuccess)
	}

	if result.ModelID != "test_view" {
		t.Errorf("ModelID = %v, want test_view", result.ModelID)
	}

	// Verify SQL was executed
	if len(adapter.executedSQL) == 0 {
		t.Error("expected SQL to be executed")
	}
}

func TestEngine_ExecuteModel_Table(t *testing.T) {
	adapter := newMockAdapter()
	engine := template.New()

	exec, _ := NewEngine(adapter, engine)

	model, _ := NewModel("test_table", "models/test_table.sql")
	model.SetCompiledSQL("SELECT 1 as id")
	model.SetMaterializationConfig(materialization.MaterializationConfig{
		Type: materialization.MaterializationTable,
	})

	ctx := context.Background()
	result, err := exec.ExecuteModel(ctx, model)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Status != StatusSuccess {
		t.Errorf("Status = %v, want %v", result.Status, StatusSuccess)
	}
}

func TestEngine_ExecuteModel_EmptySQL(t *testing.T) {
	adapter := newMockAdapter()
	engine := template.New()

	exec, _ := NewEngine(adapter, engine)

	model, _ := NewModel("test_model", "models/test.sql")
	// No compiled SQL set

	ctx := context.Background()
	result, err := exec.ExecuteModel(ctx, model)

	if err == nil {
		t.Error("expected error for empty SQL")
	}

	if result.Status != StatusFailed {
		t.Errorf("Status = %v, want %v", result.Status, StatusFailed)
	}
}

func TestEngine_ExecuteModel_DatabaseError(t *testing.T) {
	adapter := newMockAdapter()
	adapter.executeDDLErr = fmt.Errorf("database error")

	engine := template.New()

	exec, _ := NewEngine(adapter, engine)

	model, _ := NewModel("test_view", "models/test.sql")
	model.SetCompiledSQL("SELECT 1")
	model.SetMaterializationConfig(materialization.MaterializationConfig{
		Type: materialization.MaterializationView,
	})

	ctx := context.Background()
	result, err := exec.ExecuteModel(ctx, model)

	if err == nil {
		t.Error("expected error from database")
	}

	if result.Status != StatusFailed {
		t.Errorf("Status = %v, want %v", result.Status, StatusFailed)
	}

	if result.Error == "" {
		t.Error("expected error message to be set")
	}
}

func TestEngine_ExecuteModels_Sequential(t *testing.T) {
	adapter := newMockAdapter()
	engine := template.New()

	exec, _ := NewEngine(adapter, engine)

	// Create three models
	model1, _ := NewModel("model_a", "models/a.sql")
	model1.SetCompiledSQL("SELECT 1 as id")
	model1.SetMaterializationConfig(materialization.MaterializationConfig{
		Type: materialization.MaterializationView,
	})

	model2, _ := NewModel("model_b", "models/b.sql")
	model2.SetCompiledSQL("SELECT * FROM model_a")
	model2.SetMaterializationConfig(materialization.MaterializationConfig{
		Type: materialization.MaterializationView,
	})
	model2.AddDependency("model_a")

	model3, _ := NewModel("model_c", "models/c.sql")
	model3.SetCompiledSQL("SELECT * FROM model_b")
	model3.SetMaterializationConfig(materialization.MaterializationConfig{
		Type: materialization.MaterializationView,
	})
	model3.AddDependency("model_b")

	ctx := context.Background()
	models := []*Model{model1, model2, model3}

	result, err := exec.ExecuteModels(ctx, models, false)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Status != StatusSuccess {
		t.Errorf("Status = %v, want %v", result.Status, StatusSuccess)
	}

	if len(result.ModelResults) != 3 {
		t.Errorf("ModelResults length = %d, want 3", len(result.ModelResults))
	}

	// Verify execution order (should be a, b, c based on dependencies)
	if result.ModelResults[0].ModelID != "model_a" {
		t.Errorf("First model = %v, want model_a", result.ModelResults[0].ModelID)
	}
	if result.ModelResults[1].ModelID != "model_b" {
		t.Errorf("Second model = %v, want model_b", result.ModelResults[1].ModelID)
	}
	if result.ModelResults[2].ModelID != "model_c" {
		t.Errorf("Third model = %v, want model_c", result.ModelResults[2].ModelID)
	}
}

func TestEngine_ExecuteModels_FailFast(t *testing.T) {
	adapter := newMockAdapter()
	adapter.executeDDLErr = fmt.Errorf("database error")

	engine := template.New()

	exec, _ := NewEngine(adapter, engine)

	model1, _ := NewModel("model_a", "models/a.sql")
	model1.SetCompiledSQL("SELECT 1")
	model1.SetMaterializationConfig(materialization.MaterializationConfig{
		Type: materialization.MaterializationView,
	})

	model2, _ := NewModel("model_b", "models/b.sql")
	model2.SetCompiledSQL("SELECT 2")
	model2.SetMaterializationConfig(materialization.MaterializationConfig{
		Type: materialization.MaterializationView,
	})

	ctx := context.Background()
	models := []*Model{model1, model2}

	result, err := exec.ExecuteModels(ctx, models, true) // fail-fast enabled

	if err == nil {
		t.Error("expected error with fail-fast")
	}

	if result.Status != StatusFailed {
		t.Errorf("Status = %v, want %v", result.Status, StatusFailed)
	}

	// Should only have 1 result (stopped after first failure)
	if len(result.ModelResults) != 1 {
		t.Errorf("ModelResults length = %d, want 1 (failed fast)", len(result.ModelResults))
	}
}

func TestEngine_ExecuteModels_ContinueOnError(t *testing.T) {
	adapter := newMockAdapter()
	engine := template.New()

	exec, _ := NewEngine(adapter, engine)

	// First model will fail (empty SQL)
	model1, _ := NewModel("model_a", "models/a.sql")
	model1.SetMaterializationConfig(materialization.MaterializationConfig{
		Type: materialization.MaterializationView,
	})

	// Second model should succeed
	model2, _ := NewModel("model_b", "models/b.sql")
	model2.SetCompiledSQL("SELECT 2")
	model2.SetMaterializationConfig(materialization.MaterializationConfig{
		Type: materialization.MaterializationView,
	})

	ctx := context.Background()
	models := []*Model{model1, model2}

	result, err := exec.ExecuteModels(ctx, models, false) // fail-fast disabled

	if err != nil {
		t.Errorf("unexpected error with continue-on-error: %v", err)
	}

	if result.Status != StatusFailed {
		t.Errorf("Status = %v, want %v (has failures)", result.Status, StatusFailed)
	}

	// Should have 2 results (continued after first failure)
	if len(result.ModelResults) != 2 {
		t.Errorf("ModelResults length = %d, want 2 (continued on error)", len(result.ModelResults))
	}

	if result.SuccessCount() != 1 {
		t.Errorf("SuccessCount = %d, want 1", result.SuccessCount())
	}

	if result.FailureCount() != 1 {
		t.Errorf("FailureCount = %d, want 1", result.FailureCount())
	}
}
