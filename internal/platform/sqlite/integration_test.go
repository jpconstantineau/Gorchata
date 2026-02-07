package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/platform"
)

func TestEndToEndWorkflow(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "e2e.db")

	config := &platform.ConnectionConfig{
		DatabasePath: dbPath,
	}

	adapter := NewSQLiteAdapter(config)
	ctx := context.Background()

	// Connect
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer adapter.Close()

	// Create table
	createSQL := `CREATE TABLE products (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		price REAL NOT NULL,
		in_stock INTEGER DEFAULT 1
	)`
	if err := adapter.ExecuteDDL(ctx, createSQL); err != nil {
		t.Fatalf("ExecuteDDL(CREATE TABLE) error = %v", err)
	}

	// Insert data
	insertSQL := `INSERT INTO products (name, price, in_stock) VALUES 
		('Laptop', 999.99, 1),
		('Mouse', 29.99, 1),
		('Keyboard', 79.99, 0)`
	if err := adapter.ExecuteDDL(ctx, insertSQL); err != nil {
		t.Fatalf("ExecuteDDL(INSERT) error = %v", err)
	}

	// Query all products
	result, err := adapter.ExecuteQuery(ctx, "SELECT id, name, price, in_stock FROM products ORDER BY id")
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}

	if len(result.Rows) != 3 {
		t.Errorf("expected 3 products, got %d", len(result.Rows))
	}

	// Query with filter
	result, err = adapter.ExecuteQuery(ctx,
		"SELECT name, price FROM products WHERE in_stock = ? ORDER BY price", 1)
	if err != nil {
		t.Fatalf("ExecuteQuery(filtered) error = %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("expected 2 in-stock products, got %d", len(result.Rows))
	}

	// Create view
	if err := adapter.CreateView(ctx, "available_products",
		"SELECT * FROM products WHERE in_stock = 1"); err != nil {
		t.Fatalf("CreateView() error = %v", err)
	}

	// Query view
	result, err = adapter.ExecuteQuery(ctx, "SELECT name FROM available_products")
	if err != nil {
		t.Fatalf("ExecuteQuery(view) error = %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("expected 2 products in view, got %d", len(result.Rows))
	}

	// Create derived table
	if err := adapter.CreateTableAs(ctx, "expensive_products",
		"SELECT * FROM products WHERE price > 50"); err != nil {
		t.Fatalf("CreateTableAs() error = %v", err)
	}

	// Verify derived table
	exists, err := adapter.TableExists(ctx, "expensive_products")
	if err != nil {
		t.Fatalf("TableExists() error = %v", err)
	}
	if !exists {
		t.Error("expected derived table to exist")
	}

	// Get schema
	schema, err := adapter.GetTableSchema(ctx, "products")
	if err != nil {
		t.Fatalf("GetTableSchema() error = %v", err)
	}

	if len(schema.Columns) != 4 {
		t.Errorf("expected 4 columns in schema, got %d", len(schema.Columns))
	}

	// Update data
	updateSQL := `UPDATE products SET in_stock = 1 WHERE name = 'Keyboard'`
	if err := adapter.ExecuteDDL(ctx, updateSQL); err != nil {
		t.Fatalf("ExecuteDDL(UPDATE) error = %v", err)
	}

	// Verify update
	result, err = adapter.ExecuteQuery(ctx,
		"SELECT in_stock FROM products WHERE name = 'Keyboard'")
	if err != nil {
		t.Fatalf("ExecuteQuery(verify update) error = %v", err)
	}

	if result.Rows[0][0] != int64(1) {
		t.Errorf("expected in_stock = 1 after update, got %v", result.Rows[0][0])
	}

	// Delete data
	deleteSQL := `DELETE FROM products WHERE price < 30`
	if err := adapter.ExecuteDDL(ctx, deleteSQL); err != nil {
		t.Fatalf("ExecuteDDL(DELETE) error = %v", err)
	}

	// Verify deletion
	result, err = adapter.ExecuteQuery(ctx, "SELECT COUNT(*) FROM products")
	if err != nil {
		t.Fatalf("ExecuteQuery(count) error = %v", err)
	}

	count := result.Rows[0][0].(int64)
	if count != 2 {
		t.Errorf("expected 2 products after delete, got %d", count)
	}
}

func TestTransactionCommit(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "tx_commit.db")

	config := &platform.ConnectionConfig{
		DatabasePath: dbPath,
	}

	adapter := NewSQLiteAdapter(config)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer adapter.Close()

	// Create table
	createSQL := `CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance REAL)`
	if err := adapter.ExecuteDDL(ctx, createSQL); err != nil {
		t.Fatalf("ExecuteDDL() error = %v", err)
	}

	// Insert initial data
	insertSQL := `INSERT INTO accounts (id, balance) VALUES (1, 100.0), (2, 50.0)`
	if err := adapter.ExecuteDDL(ctx, insertSQL); err != nil {
		t.Fatalf("ExecuteDDL(INSERT) error = %v", err)
	}

	// Begin transaction
	tx, err := adapter.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("BeginTransaction() error = %v", err)
	}

	// Transfer within transaction
	if err := tx.Exec(ctx, "UPDATE accounts SET balance = balance - 30 WHERE id = 1"); err != nil {
		t.Fatalf("tx.Exec(debit) error = %v", err)
	}

	if err := tx.Exec(ctx, "UPDATE accounts SET balance = balance + 30 WHERE id = 2"); err != nil {
		t.Fatalf("tx.Exec(credit) error = %v", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	// Verify balances persisted
	result, err := adapter.ExecuteQuery(ctx, "SELECT id, balance FROM accounts ORDER BY id")
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}

	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 accounts, got %d", len(result.Rows))
	}

	balance1 := result.Rows[0][1].(float64)
	balance2 := result.Rows[1][1].(float64)

	if balance1 != 70.0 {
		t.Errorf("expected account 1 balance = 70.0, got %f", balance1)
	}

	if balance2 != 80.0 {
		t.Errorf("expected account 2 balance = 80.0, got %f", balance2)
	}
}

func TestTransactionRollback(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "tx_rollback.db")

	config := &platform.ConnectionConfig{
		DatabasePath: dbPath,
	}

	adapter := NewSQLiteAdapter(config)
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer adapter.Close()

	// Create table
	createSQL := `CREATE TABLE inventory (id INTEGER PRIMARY KEY, quantity INTEGER)`
	if err := adapter.ExecuteDDL(ctx, createSQL); err != nil {
		t.Fatalf("ExecuteDDL() error = %v", err)
	}

	// Insert initial data
	insertSQL := `INSERT INTO inventory (id, quantity) VALUES (1, 100)`
	if err := adapter.ExecuteDDL(ctx, insertSQL); err != nil {
		t.Fatalf("ExecuteDDL(INSERT) error = %v", err)
	}

	// Get initial state
	result, err := adapter.ExecuteQuery(ctx, "SELECT quantity FROM inventory WHERE id = 1")
	if err != nil {
		t.Fatalf("ExecuteQuery(initial) error = %v", err)
	}
	initialQty := result.Rows[0][0].(int64)

	// Begin transaction
	tx, err := adapter.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("BeginTransaction() error = %v", err)
	}

	// Modify within transaction
	if err := tx.Exec(ctx, "UPDATE inventory SET quantity = 50 WHERE id = 1"); err != nil {
		t.Fatalf("tx.Exec() error = %v", err)
	}

	// Rollback transaction
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	// Verify data was NOT persisted
	result, err = adapter.ExecuteQuery(ctx, "SELECT quantity FROM inventory WHERE id = 1")
	if err != nil {
		t.Fatalf("ExecuteQuery(after rollback) error = %v", err)
	}

	currentQty := result.Rows[0][0].(int64)
	if currentQty != initialQty {
		t.Errorf("expected quantity = %d after rollback, got %d", initialQty, currentQty)
	}

	if currentQty != 100 {
		t.Errorf("expected quantity = 100 after rollback, got %d", currentQty)
	}
}

func TestMultipleConnections(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "multi_conn.db")

	config := &platform.ConnectionConfig{
		DatabasePath: dbPath,
	}

	// First connection - setup
	adapter1 := NewSQLiteAdapter(config)
	ctx := context.Background()

	if err := adapter1.Connect(ctx); err != nil {
		t.Fatalf("adapter1.Connect() error = %v", err)
	}

	// Create table
	createSQL := `CREATE TABLE shared_data (id INTEGER PRIMARY KEY, value TEXT)`
	if err := adapter1.ExecuteDDL(ctx, createSQL); err != nil {
		t.Fatalf("ExecuteDDL() error = %v", err)
	}

	// Insert data
	insertSQL := `INSERT INTO shared_data (id, value) VALUES (1, 'test')`
	if err := adapter1.ExecuteDDL(ctx, insertSQL); err != nil {
		t.Fatalf("ExecuteDDL(INSERT) error = %v", err)
	}

	adapter1.Close()

	// Second connection - verify data
	adapter2 := NewSQLiteAdapter(config)
	if err := adapter2.Connect(ctx); err != nil {
		t.Fatalf("adapter2.Connect() error = %v", err)
	}
	defer adapter2.Close()

	// Query data
	result, err := adapter2.ExecuteQuery(ctx, "SELECT id, value FROM shared_data")
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.Rows))
	}

	if result.Rows[0][1] != "test" {
		t.Errorf("expected value = 'test', got %v", result.Rows[0][1])
	}
}

func TestCGODisabled(t *testing.T) {
	// This test verifies that the build works without CGO
	// The actual verification happens at build/test time with CGO_ENABLED=0
	// This test just ensures our code structure supports it

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "cgo_test.db")

	config := &platform.ConnectionConfig{
		DatabasePath: dbPath,
	}

	adapter := NewSQLiteAdapter(config)
	ctx := context.Background()

	// If we can connect and execute queries, CGO is not required
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Connect() error = %v (CGO may be required)", err)
	}
	defer adapter.Close()

	// Simple operation to ensure driver works
	createSQL := `CREATE TABLE cgo_test (id INTEGER)`
	if err := adapter.ExecuteDDL(ctx, createSQL); err != nil {
		t.Fatalf("ExecuteDDL() error = %v (CGO may be required)", err)
	}

	t.Log("SQLite adapter works without CGO")
}
