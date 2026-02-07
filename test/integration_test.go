package test

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/domain/executor"
	"github.com/jpconstantineau/gorchata/internal/domain/materialization"
	"github.com/jpconstantineau/gorchata/internal/platform"
	"github.com/jpconstantineau/gorchata/internal/platform/sqlite"
	"github.com/jpconstantineau/gorchata/internal/template"
)

// simpleDependencyTracker implements template.DependencyTracker for integration testing
type simpleDependencyTracker struct {
	dependencies map[string][]string
}

func newSimpleDependencyTracker() *simpleDependencyTracker {
	return &simpleDependencyTracker{
		dependencies: make(map[string][]string),
	}
}

func (t *simpleDependencyTracker) AddDependency(from, to string) error {
	t.dependencies[from] = append(t.dependencies[from], to)
	return nil
}

func (t *simpleDependencyTracker) GetDependencies(modelID string) []string {
	return t.dependencies[modelID]
}

func TestIntegration_SampleProject(t *testing.T) {
	// Create temporary database
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create SQLite adapter
	config := &platform.ConnectionConfig{
		DatabasePath: dbPath,
	}
	adapter := sqlite.NewSQLiteAdapter(config)

	ctx := context.Background()
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer adapter.Close()

	// Create source tables with sample data
	if err := createSourceTables(ctx, adapter); err != nil {
		t.Fatalf("failed to create source tables: %v", err)
	}

	// Load models from sample project
	modelsDir := filepath.Join("fixtures", "sample_project", "models")
	models, err := loadModels(modelsDir)
	if err != nil {
		t.Fatalf("failed to load models: %v", err)
	}

	// Parse templates and compile SQL
	templateEngine := template.New()
	tracker := newSimpleDependencyTracker()
	templateEngineWithTracker := template.New(template.WithDependencyTracker(tracker))

	for _, model := range models {
		// Read model content
		content, err := os.ReadFile(model.Path)
		if err != nil {
			t.Fatalf("failed to read model %s: %v", model.ID, err)
		}

		contentStr := string(content)

		// Extract config from comments BEFORE parsing template
		config := extractConfig(contentStr)
		model.SetMaterializationConfig(config)

		// Remove config() calls from content so they don't interfere with parsing
		contentStr = removeConfigCalls(contentStr)

		// Parse and render template
		tmpl, err := templateEngineWithTracker.Parse(model.ID, contentStr)
		if err != nil {
			t.Fatalf("failed to parse template %s: %v", model.ID, err)
		}

		ctx := template.NewContext(template.WithCurrentModel(model.ID))
		rendered, err := template.Render(tmpl, ctx, nil)
		if err != nil {
			t.Fatalf("failed to render template %s: %v", model.ID, err)
		}

		model.SetCompiledSQL(rendered)

		// Extract dependencies
		deps := tracker.GetDependencies(model.ID)
		for _, dep := range deps {
			model.AddDependency(dep)
		}
	}

	// Create execution engine
	exec, err := executor.NewEngine(adapter, templateEngine)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	// Execute models
	result, err := exec.ExecuteModels(ctx, models, false)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	// Verify execution results
	if result.Status != executor.StatusSuccess {
		t.Errorf("execution status = %v, want success", result.Status)
	}

	if len(result.ModelResults) != 3 {
		t.Errorf("executed models = %d, want 3", len(result.ModelResults))
	}

	if result.FailureCount() > 0 {
		t.Errorf("failures = %d, want 0", result.FailureCount())
		for _, mr := range result.ModelResults {
			if mr.Status == executor.StatusFailed {
				t.Logf("Model %s failed: %s", mr.ModelID, mr.Error)
			}
		}
	}

	// Verify execution order (stg_users and stg_orders should come before fct_order_summary)
	fctIndex := -1
	stgUsersIndex := -1
	stgOrdersIndex := -1
	for i, mr := range result.ModelResults {
		switch mr.ModelID {
		case "fct_order_summary":
			fctIndex = i
		case "stg_users":
			stgUsersIndex = i
		case "stg_orders":
			stgOrdersIndex = i
		}
	}

	if fctIndex != -1 && stgUsersIndex != -1 && fctIndex < stgUsersIndex {
		t.Error("fct_order_summary executed before stg_users")
	}
	if fctIndex != -1 && stgOrdersIndex != -1 && fctIndex < stgOrdersIndex {
		t.Error("fct_order_summary executed before stg_orders")
	}

	// Verify tables/views were created
	// Note: SQLite stores views and tables in sqlite_master with different types
	// stg_users and stg_orders are views, fct_order_summary is a table

	// Check if views exist
	viewCheckQuery := "SELECT COUNT(*) as count FROM sqlite_master WHERE type='view' AND name=?"

	viewResult, err := adapter.ExecuteQuery(ctx, viewCheckQuery, "stg_users")
	if err != nil || len(viewResult.Rows) == 0 || viewResult.Rows[0][0].(int64) == 0 {
		t.Error("stg_users view was not created")
	}

	viewResult, err = adapter.ExecuteQuery(ctx, viewCheckQuery, "stg_orders")
	if err != nil || len(viewResult.Rows) == 0 || viewResult.Rows[0][0].(int64) == 0 {
		t.Error("stg_orders view was not created")
	}

	if exists, _ := adapter.TableExists(ctx, "fct_order_summary"); !exists {
		t.Error("fct_order_summary table was not created")
	}

	// Verify data in final table
	queryResult, err := adapter.ExecuteQuery(ctx, "SELECT COUNT(*) as count FROM fct_order_summary")
	if err != nil {
		t.Fatalf("failed to query fct_order_summary: %v", err)
	}

	if len(queryResult.Rows) == 0 {
		t.Error("fct_order_summary has no rows")
	}
}

// createSourceTables creates and populates source tables for testing
func createSourceTables(ctx context.Context, adapter *sqlite.SQLiteAdapter) error {
	// Create raw_users table
	if err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE raw_users (
			id INTEGER PRIMARY KEY,
			name TEXT,
			email TEXT,
			created_at TEXT,
			deleted_at TEXT
		)
	`); err != nil {
		return err
	}

	// Insert sample users
	if err := adapter.ExecuteDDL(ctx, `
		INSERT INTO raw_users (id, name, email, created_at, deleted_at) VALUES
		(1, 'Alice Smith', 'alice@example.com', '2024-01-01', NULL),
		(2, 'Bob Jones', 'bob@example.com', '2024-01-02', NULL),
		(3, 'Charlie Brown', 'charlie@example.com', '2024-01-03', '2024-02-01')
	`); err != nil {
		return err
	}

	// Create raw_orders table
	if err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE raw_orders (
			id INTEGER PRIMARY KEY,
			user_id INTEGER,
			amount REAL,
			order_date TEXT,
			status TEXT
		)
	`); err != nil {
		return err
	}

	// Insert sample orders
	if err := adapter.ExecuteDDL(ctx, `
		INSERT INTO raw_orders (id, user_id, amount, order_date, status) VALUES
		(1, 1, 100.0, '2024-01-15', 'completed'),
		(2, 1, 150.0, '2024-01-20', 'completed'),
		(3, 2, 200.0, '2024-01-18', 'completed'),
		(4, 2, 50.0, '2024-01-19', 'cancelled'),
		(5, 3, 75.0, '2024-01-25', 'completed')
	`); err != nil {
		return err
	}

	return nil
}

// loadModels loads model definitions from a directory
func loadModels(dir string) ([]*executor.Model, error) {
	var models []*executor.Model

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		modelID := strings.TrimSuffix(entry.Name(), ".sql")
		modelPath := filepath.Join(dir, entry.Name())

		model, err := executor.NewModel(modelID, modelPath)
		if err != nil {
			return nil, err
		}

		models = append(models, model)
	}

	return models, nil
}

// extractConfig extracts materialization config from SQL comments
// Looks for {{ config(materialized='view') }} or similar
func extractConfig(content string) materialization.MaterializationConfig {
	config := materialization.DefaultConfig()

	// Look for config() function in comments or template tags
	configRe := regexp.MustCompile(`{{\s*config\s*\(\s*materialized\s*=\s*['"](\w+)['"]\s*\)\s*}}`)
	matches := configRe.FindStringSubmatch(content)

	if len(matches) > 1 {
		switch matches[1] {
		case "view":
			config.Type = materialization.MaterializationView
		case "table":
			config.Type = materialization.MaterializationTable
		case "incremental":
			config.Type = materialization.MaterializationIncremental
		}
	}

	return config
}

// removeConfigCalls removes {{ config(...) }} calls from content
// so they don't interfere with template parsing
func removeConfigCalls(content string) string {
	configRe := regexp.MustCompile(`{{\s*config\s*\([^}]+\)\s*}}`)
	return configRe.ReplaceAllString(content, "")
}

// TestIntegration_IncrementalModel tests end-to-end incremental model execution
// with both is_incremental template function and {{ this }} reference
func TestIntegration_IncrementalModel(t *testing.T) {
	// Create temporary database
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create SQLite adapter
	config := &platform.ConnectionConfig{
		DatabasePath: dbPath,
	}
	adapter := sqlite.NewSQLiteAdapter(config)

	ctx := context.Background()
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer adapter.Close()

	// Create source table with initial data
	createSQL := `CREATE TABLE source_data (
		id INTEGER PRIMARY KEY,
		value TEXT,
		updated_at TEXT
	)`
	if err := adapter.ExecuteDDL(ctx, createSQL); err != nil {
		t.Fatalf("failed to create source table: %v", err)
	}

	insertSQL := `INSERT INTO source_data (id, value, updated_at) VALUES
		(1, 'first', '2024-01-01'),
		(2, 'second', '2024-01-02'),
		(3, 'third', '2024-01-03')`
	if err := adapter.ExecuteDDL(ctx, insertSQL); err != nil {
		t.Fatalf("failed to insert initial data: %v", err)
	}

	// Create incremental model
	model, err := executor.NewModel("incremental_test", "test_model.sql")
	if err != nil {
		t.Fatalf("failed to create model: %v", err)
	}

	// Template with incremental logic
	templateContent := `SELECT 
		id,
		value,
		updated_at
	FROM source_data
	{{ if is_incremental }}
	WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01') FROM {{ this }})
	{{ end }}`

	model.SetTemplateContent(templateContent)
	model.SetMaterializationConfig(materialization.MaterializationConfig{
		Type:      materialization.MaterializationIncremental,
		UniqueKey: []string{"id"},
	})

	// Create execution engine
	templateEngine := template.New()
	engine, err := executor.NewEngine(adapter, templateEngine)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	// First run - should create table with all 3 rows (table doesn't exist yet)
	result, err := engine.ExecuteModel(ctx, model)
	if err != nil {
		t.Fatalf("first execution failed: %v", err)
	}

	if result.Status != executor.StatusSuccess {
		t.Errorf("expected success, got %v: %s", result.Status, result.Error)
	}

	// Verify all 3 rows were inserted
	queryResult, err := adapter.ExecuteQuery(ctx, "SELECT COUNT(*) as count FROM incremental_test")
	if err != nil {
		t.Fatalf("failed to query table: %v", err)
	}
	rows, _ := queryResult.FetchAll()
	if len(rows) == 0 || rows[0]["count"] != int64(3) {
		t.Errorf("expected 3 rows after first run, got %v", rows)
	}

	// Add more data to source
	insertSQL2 := `INSERT INTO source_data (id, value, updated_at) VALUES
		(4, 'fourth', '2024-01-04'),
		(5, 'fifth', '2024-01-05')`
	if err := adapter.ExecuteDDL(ctx, insertSQL2); err != nil {
		t.Fatalf("failed to insert more data: %v", err)
	}

	// Second run - incremental, should only add 2 new rows
	result2, err := engine.ExecuteModel(ctx, model)
	if err != nil {
		t.Fatalf("second execution failed: %v", err)
	}

	if result2.Status != executor.StatusSuccess {
		t.Errorf("expected success on second run, got %v: %s", result2.Status, result2.Error)
	}

	// Verify we now have 5 rows total
	queryResult2, err := adapter.ExecuteQuery(ctx, "SELECT COUNT(*) as count FROM incremental_test")
	if err != nil {
		t.Fatalf("failed to query table after second run: %v", err)
	}
	rows2, _ := queryResult2.FetchAll()
	if len(rows2) == 0 || rows2[0]["count"] != int64(5) {
		t.Errorf("expected 5 rows after second run, got %v", rows2)
	}

	// Third run with full refresh - should rebuild from scratch
	model.SetMaterializationConfig(materialization.MaterializationConfig{
		Type:        materialization.MaterializationIncremental,
		UniqueKey:   []string{"id"},
		FullRefresh: true,
	})

	result3, err := engine.ExecuteModel(ctx, model)
	if err != nil {
		t.Fatalf("third execution (full refresh) failed: %v", err)
	}

	if result3.Status != executor.StatusSuccess {
		t.Errorf("expected success on full refresh, got %v: %s", result3.Status, result3.Error)
	}

	// Verify we still have 5 rows (all data from source)
	queryResult3, err := adapter.ExecuteQuery(ctx, "SELECT COUNT(*) as count FROM incremental_test")
	if err != nil {
		t.Fatalf("failed to query table after full refresh: %v", err)
	}
	rows3, _ := queryResult3.FetchAll()
	if len(rows3) == 0 || rows3[0]["count"] != int64(5) {
		t.Errorf("expected 5 rows after full refresh, got %v", rows3)
	}

	t.Logf("✓ Incremental model test completed successfully")
	t.Logf("  - First run: Created table with 3 rows")
	t.Logf("  - Second run: Added 2 new rows incrementally")
	t.Logf("  - Third run: Full refresh with 5 rows")
}
