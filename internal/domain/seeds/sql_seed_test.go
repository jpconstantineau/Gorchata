package seeds

import (
	"context"
	"strings"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/platform"
)

// mockAdapter is a simple mock adapter for testing SQL seed execution
type mockAdapter struct {
	ExecutedStatements []string
}

func newMockAdapter() *mockAdapter {
	return &mockAdapter{
		ExecutedStatements: make([]string, 0),
	}
}

func (m *mockAdapter) Connect(ctx context.Context) error { return nil }
func (m *mockAdapter) Close() error                      { return nil }
func (m *mockAdapter) ExecuteQuery(ctx context.Context, sql string, args ...interface{}) (*platform.QueryResult, error) {
	return nil, nil
}
func (m *mockAdapter) ExecuteDDL(ctx context.Context, sql string) error {
	m.ExecutedStatements = append(m.ExecutedStatements, sql)
	return nil
}
func (m *mockAdapter) TableExists(ctx context.Context, table string) (bool, error) {
	return false, nil
}
func (m *mockAdapter) GetTableSchema(ctx context.Context, table string) (*platform.Schema, error) {
	return nil, nil
}
func (m *mockAdapter) CreateTableAs(ctx context.Context, table, selectSQL string) error {
	return nil
}
func (m *mockAdapter) CreateView(ctx context.Context, view, selectSQL string) error {
	return nil
}
func (m *mockAdapter) BeginTransaction(ctx context.Context) (platform.Transaction, error) {
	return nil, nil
}

// TestExecuteSQLSeed_BasicSQL tests execution of plain SQL without templates
func TestExecuteSQLSeed_BasicSQL(t *testing.T) {
	// Create a mock adapter
	adapter := newMockAdapter()
	ctx := context.Background()

	// Simple SQL content
	sqlContent := `CREATE TABLE test_table (
		id INTEGER PRIMARY KEY,
		name TEXT
	);
	INSERT INTO test_table (id, name) VALUES (1, 'Alice');`

	// Execute SQL seed
	err := ExecuteSQLSeed(ctx, adapter, sqlContent, nil, nil)
	if err != nil {
		t.Fatalf("ExecuteSQLSeed failed: %v", err)
	}

	// Verify two statements were executed
	if len(adapter.ExecutedStatements) != 2 {
		t.Errorf("Expected 2 statements, got %d", len(adapter.ExecutedStatements))
	}

	// Verify CREATE TABLE
	if !strings.Contains(adapter.ExecutedStatements[0], "CREATE TABLE test_table") {
		t.Errorf("Expected CREATE TABLE statement, got: %s", adapter.ExecutedStatements[0])
	}

	// Verify INSERT
	if !strings.Contains(adapter.ExecutedStatements[1], "INSERT INTO test_table") {
		t.Errorf("Expected INSERT statement, got: %s", adapter.ExecutedStatements[1])
	}
}

// TestExecuteSQLSeed_WithVarSubstitution tests {{ var }} template substitution
func TestExecuteSQLSeed_WithVarSubstitution(t *testing.T) {
	adapter := newMockAdapter()
	ctx := context.Background()

	// SQL with {{ var }} template
	sqlContent := `CREATE TABLE {{ var "table_name" }} (
		id INTEGER,
		status TEXT DEFAULT '{{ var "default_status" }}'
	);`

	// Variables for substitution
	vars := map[string]interface{}{
		"table_name":     "dynamic_table",
		"default_status": "active",
	}

	// Execute with variables
	err := ExecuteSQLSeed(ctx, adapter, sqlContent, vars, nil)
	if err != nil {
		t.Fatalf("ExecuteSQLSeed failed: %v", err)
	}

	// Verify substitution occurred
	executed := adapter.ExecutedStatements[0]
	if !strings.Contains(executed, "CREATE TABLE dynamic_table") {
		t.Errorf("Variable substitution failed for table_name: %s", executed)
	}
	if !strings.Contains(executed, "DEFAULT 'active'") {
		t.Errorf("Variable substitution failed for default_status: %s", executed)
	}
}

// TestExecuteSQLSeed_MultipleStatements tests splitting by semicolons
func TestExecuteSQLSeed_MultipleStatements(t *testing.T) {
	adapter := newMockAdapter()
	ctx := context.Background()

	sqlContent := `CREATE TABLE table1 (id INTEGER);
	CREATE TABLE table2 (id INTEGER);
	INSERT INTO table1 VALUES (1);
	INSERT INTO table2 VALUES (2);`

	err := ExecuteSQLSeed(ctx, adapter, sqlContent, nil, nil)
	if err != nil {
		t.Fatalf("ExecuteSQLSeed failed: %v", err)
	}

	// Should have 4 statements
	if len(adapter.ExecutedStatements) != 4 {
		t.Errorf("Expected 4 statements, got %d", len(adapter.ExecutedStatements))
	}
}

// TestValidateNoForbiddenFunctions tests rejection of ref/source/seed functions
func TestValidateNoForbiddenFunctions(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid with var",
			sql:     `CREATE TABLE {{ var "name" }} (id INTEGER);`,
			wantErr: false,
		},
		{
			name:    "forbidden ref",
			sql:     `SELECT * FROM {{ ref "model" }};`,
			wantErr: true,
			errMsg:  "ref",
		},
		{
			name:    "forbidden source",
			sql:     `SELECT * FROM {{ source "src" "table" }};`,
			wantErr: true,
			errMsg:  "source",
		},
		{
			name:    "forbidden seed",
			sql:     `SELECT * FROM {{ seed "name" }};`,
			wantErr: true,
			errMsg:  "seed",
		},
		{
			name:    "multiple forbidden",
			sql:     `SELECT * FROM {{ ref "a" }} JOIN {{ seed "b" }};`,
			wantErr: true,
			errMsg:  "ref",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateNoForbiddenFunctions(tt.sql)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateNoForbiddenFunctions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("Error should contain '%s', got: %v", tt.errMsg, err)
			}
		})
	}
}

// TestRenderSQLSeedTemplate tests template rendering with var only
func TestRenderSQLSeedTemplate(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		vars     map[string]interface{}
		expected string
		wantErr  bool
	}{
		{
			name:     "no template",
			content:  "CREATE TABLE test (id INTEGER);",
			vars:     nil,
			expected: "CREATE TABLE test (id INTEGER);",
			wantErr:  false,
		},
		{
			name:     "single var",
			content:  "CREATE TABLE {{ var \"name\" }} (id INTEGER);",
			vars:     map[string]interface{}{"name": "my_table"},
			expected: "CREATE TABLE my_table (id INTEGER);",
			wantErr:  false,
		},
		{
			name:     "multiple vars",
			content:  "CREATE TABLE {{ var \"table\" }} ({{ var \"col\" }} {{ var \"type\" }});",
			vars:     map[string]interface{}{"table": "users", "col": "id", "type": "INTEGER"},
			expected: "CREATE TABLE users (id INTEGER);",
			wantErr:  false,
		},
		{
			name:     "missing var",
			content:  "CREATE TABLE {{ var \"missing\" }} (id INTEGER);",
			vars:     map[string]interface{}{"name": "test"},
			expected: "",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := renderSQLSeedTemplate(tt.content, tt.vars)
			if (err != nil) != tt.wantErr {
				t.Errorf("renderSQLSeedTemplate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && result != tt.expected {
				t.Errorf("renderSQLSeedTemplate() = %q, expected %q", result, tt.expected)
			}
		})
	}
}

// TestExecuteSQLSeed_EmptySQL tests handling of empty SQL
func TestExecuteSQLSeed_EmptySQL(t *testing.T) {
	adapter := newMockAdapter()
	ctx := context.Background()

	err := ExecuteSQLSeed(ctx, adapter, "", nil, nil)
	if err != nil {
		t.Fatalf("Empty SQL should not error: %v", err)
	}

	// Should execute no statements
	if len(adapter.ExecutedStatements) != 0 {
		t.Errorf("Expected 0 statements for empty SQL, got %d", len(adapter.ExecutedStatements))
	}
}

// TestExecuteSQLSeed_NilAdapter tests error handling for nil adapter
func TestExecuteSQLSeed_NilAdapter(t *testing.T) {
	ctx := context.Background()
	err := ExecuteSQLSeed(ctx, nil, "SELECT 1;", nil, nil)
	if err == nil {
		t.Error("Expected error for nil adapter")
	}
	if !strings.Contains(err.Error(), "adapter") {
		t.Errorf("Error should mention adapter, got: %v", err)
	}
}
