package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/pierre/gorchata/internal/platform"
)

func TestConnect(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	config := &platform.ConnectionConfig{
		DatabasePath: dbPath,
	}

	adapter := NewSQLiteAdapter(config)

	ctx := context.Background()
	err := adapter.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	// Verify connection is usable
	if adapter.db == nil {
		t.Error("expected db to be initialized")
	}

	// Clean up
	if err := adapter.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestConnectInvalidPath(t *testing.T) {
	config := &platform.ConnectionConfig{
		DatabasePath: "/invalid/nonexistent/path/test.db",
	}

	adapter := NewSQLiteAdapter(config)

	ctx := context.Background()
	err := adapter.Connect(ctx)
	if err == nil {
		t.Error("expected error for invalid path, got nil")
		adapter.Close()
	}
}

func TestClose(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	config := &platform.ConnectionConfig{
		DatabasePath: dbPath,
	}

	adapter := NewSQLiteAdapter(config)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	err := adapter.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestExecuteDDL(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	config := &platform.ConnectionConfig{
		DatabasePath: dbPath,
	}

	adapter := NewSQLiteAdapter(config)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer adapter.Close()

	// Create a test table
	createSQL := `CREATE TABLE test_users (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL
	)`

	err := adapter.ExecuteDDL(ctx, createSQL)
	if err != nil {
		t.Errorf("ExecuteDDL() error = %v", err)
	}

	// Verify table exists
	exists, err := adapter.TableExists(ctx, "test_users")
	if err != nil {
		t.Fatalf("TableExists() error = %v", err)
	}
	if !exists {
		t.Error("expected table to exist after CREATE TABLE")
	}
}

func TestTableExists(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	config := &platform.ConnectionConfig{
		DatabasePath: dbPath,
	}

	adapter := NewSQLiteAdapter(config)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer adapter.Close()

	// Check non-existent table
	exists, err := adapter.TableExists(ctx, "nonexistent_table")
	if err != nil {
		t.Fatalf("TableExists() error = %v", err)
	}
	if exists {
		t.Error("expected table to not exist")
	}

	// Create table
	createSQL := `CREATE TABLE existing_table (id INTEGER)`
	if err := adapter.ExecuteDDL(ctx, createSQL); err != nil {
		t.Fatalf("ExecuteDDL() error = %v", err)
	}

	// Check existing table
	exists, err = adapter.TableExists(ctx, "existing_table")
	if err != nil {
		t.Fatalf("TableExists() error = %v", err)
	}
	if !exists {
		t.Error("expected table to exist")
	}
}

func TestExecuteQuery(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	config := &platform.ConnectionConfig{
		DatabasePath: dbPath,
	}

	adapter := NewSQLiteAdapter(config)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer adapter.Close()

	// Create and populate table
	createSQL := `CREATE TABLE test_data (id INTEGER, value TEXT)`
	if err := adapter.ExecuteDDL(ctx, createSQL); err != nil {
		t.Fatalf("ExecuteDDL() error = %v", err)
	}

	insertSQL := `INSERT INTO test_data (id, value) VALUES (1, 'test')`
	if err := adapter.ExecuteDDL(ctx, insertSQL); err != nil {
		t.Fatalf("ExecuteDDL() error = %v", err)
	}

	// Execute query
	result, err := adapter.ExecuteQuery(ctx, "SELECT id, value FROM test_data")
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}

	if len(result.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(result.Columns))
	}

	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.Rows))
	}

	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected id = 1, got %v", result.Rows[0][0])
	}

	if result.Rows[0][1] != "test" {
		t.Errorf("expected value = 'test', got %v", result.Rows[0][1])
	}
}

func TestExecuteQueryWithArgs(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	config := &platform.ConnectionConfig{
		DatabasePath: dbPath,
	}

	adapter := NewSQLiteAdapter(config)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer adapter.Close()

	// Create and populate table
	createSQL := `CREATE TABLE test_params (id INTEGER, name TEXT)`
	if err := adapter.ExecuteDDL(ctx, createSQL); err != nil {
		t.Fatalf("ExecuteDDL() error = %v", err)
	}

	insertSQL := `INSERT INTO test_params (id, name) VALUES (1, 'Alice'), (2, 'Bob')`
	if err := adapter.ExecuteDDL(ctx, insertSQL); err != nil {
		t.Fatalf("ExecuteDDL() error = %v", err)
	}

	// Execute parameterized query
	result, err := adapter.ExecuteQuery(ctx, "SELECT id, name FROM test_params WHERE id = ?", 2)
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.Rows))
	}

	if result.Rows[0][1] != "Bob" {
		t.Errorf("expected name = 'Bob', got %v", result.Rows[0][1])
	}
}

func TestGetTableSchema(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	config := &platform.ConnectionConfig{
		DatabasePath: dbPath,
	}

	adapter := NewSQLiteAdapter(config)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer adapter.Close()

	// Create table with known schema
	createSQL := `CREATE TABLE schema_test (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT
	)`
	if err := adapter.ExecuteDDL(ctx, createSQL); err != nil {
		t.Fatalf("ExecuteDDL() error = %v", err)
	}

	// Get schema
	schema, err := adapter.GetTableSchema(ctx, "schema_test")
	if err != nil {
		t.Fatalf("GetTableSchema() error = %v", err)
	}

	if schema.TableName != "schema_test" {
		t.Errorf("expected TableName = 'schema_test', got %v", schema.TableName)
	}

	if len(schema.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(schema.Columns))
	}

	// Check first column (id)
	if schema.Columns[0].Name != "id" {
		t.Errorf("expected first column name = 'id', got %v", schema.Columns[0].Name)
	}
	if !schema.Columns[0].PrimaryKey {
		t.Error("expected first column to be primary key")
	}

	// Check second column (name)
	if schema.Columns[1].Name != "name" {
		t.Errorf("expected second column name = 'name', got %v", schema.Columns[1].Name)
	}
	if schema.Columns[1].Nullable {
		t.Error("expected 'name' column to be NOT NULL")
	}
}

func TestGetTableSchemaMissing(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	config := &platform.ConnectionConfig{
		DatabasePath: dbPath,
	}

	adapter := NewSQLiteAdapter(config)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer adapter.Close()

	// Try to get schema for non-existent table
	_, err := adapter.GetTableSchema(ctx, "nonexistent")
	if err == nil {
		t.Error("expected error for non-existent table, got nil")
	}
}

func TestCreateTableAs(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	config := &platform.ConnectionConfig{
		DatabasePath: dbPath,
	}

	adapter := NewSQLiteAdapter(config)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer adapter.Close()

	// Create source table with data
	createSQL := `CREATE TABLE source_table (id INTEGER, name TEXT)`
	if err := adapter.ExecuteDDL(ctx, createSQL); err != nil {
		t.Fatalf("ExecuteDDL() error = %v", err)
	}

	insertSQL := `INSERT INTO source_table VALUES (1, 'Alice'), (2, 'Bob')`
	if err := adapter.ExecuteDDL(ctx, insertSQL); err != nil {
		t.Fatalf("ExecuteDDL() error = %v", err)
	}

	// Create table from SELECT
	err := adapter.CreateTableAs(ctx, "derived_table", "SELECT * FROM source_table WHERE id = 1")
	if err != nil {
		t.Fatalf("CreateTableAs() error = %v", err)
	}

	// Verify new table exists
	exists, err := adapter.TableExists(ctx, "derived_table")
	if err != nil {
		t.Fatalf("TableExists() error = %v", err)
	}
	if !exists {
		t.Error("expected derived table to exist")
	}

	// Verify data
	result, err := adapter.ExecuteQuery(ctx, "SELECT id, name FROM derived_table")
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row in derived table, got %d", len(result.Rows))
	}

	if result.Rows[0][1] != "Alice" {
		t.Errorf("expected name = 'Alice', got %v", result.Rows[0][1])
	}
}

func TestCreateView(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	config := &platform.ConnectionConfig{
		DatabasePath: dbPath,
	}

	adapter := NewSQLiteAdapter(config)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer adapter.Close()

	// Create source table with data
	createSQL := `CREATE TABLE view_source (id INTEGER, status TEXT)`
	if err := adapter.ExecuteDDL(ctx, createSQL); err != nil {
		t.Fatalf("ExecuteDDL() error = %v", err)
	}

	insertSQL := `INSERT INTO view_source VALUES (1, 'active'), (2, 'inactive')`
	if err := adapter.ExecuteDDL(ctx, insertSQL); err != nil {
		t.Fatalf("ExecuteDDL() error = %v", err)
	}

	// Create view
	err := adapter.CreateView(ctx, "active_view", "SELECT * FROM view_source WHERE status = 'active'")
	if err != nil {
		t.Fatalf("CreateView() error = %v", err)
	}

	// Query view
	result, err := adapter.ExecuteQuery(ctx, "SELECT id, status FROM active_view")
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row in view, got %d", len(result.Rows))
	}

	if result.Rows[0][1] != "active" {
		t.Errorf("expected status = 'active', got %v", result.Rows[0][1])
	}
}

func TestBeginTransaction(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	config := &platform.ConnectionConfig{
		DatabasePath: dbPath,
	}

	adapter := NewSQLiteAdapter(config)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer adapter.Close()

	// Begin transaction
	tx, err := adapter.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("BeginTransaction() error = %v", err)
	}

	if tx == nil {
		t.Fatal("expected transaction to be non-nil")
	}

	// Rollback to clean up
	if err := tx.Rollback(); err != nil {
		t.Errorf("Rollback() error = %v", err)
	}
}
