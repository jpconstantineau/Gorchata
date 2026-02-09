package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/config"
	"github.com/jpconstantineau/gorchata/internal/domain/test"
	"github.com/jpconstantineau/gorchata/internal/platform"
	"github.com/jpconstantineau/gorchata/internal/platform/sqlite"
)

// SetupTestProject creates a temporary test project with models and tests
func SetupTestProject(t *testing.T) string {
	t.Helper()

	// Create temp directory
	tmpDir := t.TempDir()

	// Copy fixture files to temp directory
	fixturesPath := filepath.Join("fixtures", "test_project")
	err := copyDir(fixturesPath, tmpDir)
	if err != nil {
		t.Fatalf("failed to copy fixtures: %v", err)
	}

	return tmpDir
}

// CreateTestDatabase creates a temp SQLite database with sample data
func CreateTestDatabase(t *testing.T) (*sqlite.SQLiteAdapter, string) {
	t.Helper()

	// Create temp database file
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create adapter
	cfg := &platform.ConnectionConfig{
		DatabasePath: dbPath,
	}
	adapter := sqlite.NewSQLiteAdapter(cfg)

	// Connect
	ctx := context.Background()
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	// Create raw tables with sample data
	CreateSampleData(t, adapter)

	// Register cleanup to properly close WAL before test ends
	t.Cleanup(func() {
		// Checkpoint and close WAL to release file locks on Windows
		adapter.ExecuteDDL(context.Background(), "PRAGMA wal_checkpoint(TRUNCATE)")
		adapter.ExecuteDDL(context.Background(), "PRAGMA journal_mode=DELETE")
		adapter.Close()
	})

	return adapter, dbPath
}

// CreateSampleData inserts sample data into raw_users and raw_orders tables
func CreateSampleData(t *testing.T, adapter *sqlite.SQLiteAdapter) {
	t.Helper()

	ctx := context.Background()

	// Create raw_users table
	// Note: email does not have NOT NULL to allow testing data quality issues
	err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE IF NOT EXISTS raw_users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT,
			status TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("failed to create raw_users table: %v", err)
	}

	// Insert sample users (mix of valid and invalid data)
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO raw_users (id, name, email, status) VALUES
		(1, 'Alice', 'alice@example.com', 'active'),
		(2, 'Bob', 'bob@example.com', 'active'),
		(3, 'Charlie', 'charlie@example.com', 'inactive'),
		(4, 'David', 'david@example.com', 'pending'),
		(5, 'Eve', 'eve@example.com', 'active')
	`)
	if err != nil {
		t.Fatalf("failed to insert users: %v", err)
	}

	// Create raw_orders table
	err = adapter.ExecuteDDL(ctx, `
		CREATE TABLE IF NOT EXISTS raw_orders (
			id INTEGER PRIMARY KEY,
			user_id INTEGER NOT NULL,
			total_amount REAL NOT NULL,
			status TEXT NOT NULL,
			order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("failed to create raw_orders table: %v", err)
	}

	// Insert sample orders
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO raw_orders (id, user_id, total_amount, status) VALUES
		(1, 1, 100.50, 'completed'),
		(2, 1, 200.00, 'completed'),
		(3, 2, 50.25, 'pending'),
		(4, 3, 300.00, 'completed'),
		(5, 4, 75.00, 'cancelled')
	`)
	if err != nil {
		t.Fatalf("failed to insert orders: %v", err)
	}
}

// CreateInvalidData inserts data that will fail tests
func CreateInvalidData(t *testing.T, adapter *sqlite.SQLiteAdapter) {
	t.Helper()

	ctx := context.Background()

	// Insert user with NULL email (violates not_null test)
	err := adapter.ExecuteDDL(ctx, `
		INSERT INTO raw_users (id, name, email, status)
		VALUES (100, 'Invalid User', NULL, 'active')
	`)
	if err != nil {
		t.Fatalf("failed to insert invalid user: %v", err)
	}

	// Insert duplicate email (violates unique test)
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO raw_users (id, name, email, status)
		VALUES (101, 'Duplicate', 'alice@example.com', 'active')
	`)
	if err != nil {
		t.Fatalf("failed to insert duplicate user: %v", err)
	}

	// Insert user with invalid status (violates accepted_values test)
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO raw_users (id, name, email, status)
		VALUES (102, 'Bad Status', 'bad@example.com', 'suspended')
	`)
	if err != nil {
		t.Fatalf("failed to insert user with bad status: %v", err)
	}

	// Insert order with invalid foreign key (violates relationships test)
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO raw_orders (id, user_id, total_amount, status)
		VALUES (100, 999, 50.00, 'completed')
	`)
	if err != nil {
		t.Fatalf("failed to insert order with invalid FK: %v", err)
	}

	// Insert order with negative amount (violates accepted_range test)
	err = adapter.ExecuteDDL(ctx, `
		INSERT INTO raw_orders (id, user_id, total_amount, status)
		VALUES (101, 1, -50.00, 'completed')
	`)
	if err != nil {
		t.Fatalf("failed to insert order with negative amount: %v", err)
	}
}

// CreateLargeTestTable creates a table with >1M rows for sampling tests
func CreateLargeTestTable(t *testing.T, adapter *sqlite.SQLiteAdapter, tableName string, rowCount int) {
	t.Helper()

	ctx := context.Background()

	// Create table
	err := adapter.ExecuteDDL(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INTEGER PRIMARY KEY,
			value TEXT NOT NULL
		)
	`, tableName))
	if err != nil {
		t.Fatalf("failed to create large test table: %v", err)
	}

	// Insert rows in batches
	const batchSize = 10000
	for i := 0; i < rowCount; i += batchSize {
		values := ""
		limit := batchSize
		if i+batchSize > rowCount {
			limit = rowCount - i
		}

		for j := 0; j < limit; j++ {
			if j > 0 {
				values += ","
			}
			values += fmt.Sprintf("(%d, 'value_%d')", i+j+1, i+j+1)
		}

		query := fmt.Sprintf("INSERT INTO %s (id, value) VALUES %s", tableName, values)
		err := adapter.ExecuteDDL(ctx, query)
		if err != nil {
			t.Fatalf("failed to insert batch at %d: %v", i, err)
		}
	}

	t.Logf("Created large test table %s with %d rows", tableName, rowCount)
}

// CleanupTestProject removes temporary files
func CleanupTestProject(t *testing.T, projectPath string) {
	t.Helper()
	// t.TempDir() handles cleanup automatically, but this is here for explicit cleanup if needed
	if err := os.RemoveAll(projectPath); err != nil {
		t.Logf("warning: failed to cleanup project: %v", err)
	}
}

// AssertTestsPassed verifies all tests in summary passed
func AssertTestsPassed(t *testing.T, summary *test.TestSummary) {
	t.Helper()

	if summary.FailedTests > 0 {
		t.Errorf("expected all tests to pass, but %d failed", summary.FailedTests)
	}

	if summary.PassedTests != summary.TotalTests {
		t.Errorf("expected %d tests to pass, but only %d passed", summary.TotalTests, summary.PassedTests)
	}
}

// AssertTestFailed verifies specific test failed
func AssertTestFailed(t *testing.T, summary *test.TestSummary, testName string) {
	t.Helper()

	found := false
	for _, result := range summary.TestResults {
		if result.TestID == testName {
			found = true
			if result.Status != test.StatusFailed {
				t.Errorf("expected test %s to fail, but status is %s", testName, result.Status)
			}
			break
		}
	}

	if !found {
		t.Errorf("test %s not found in results", testName)
	}
}

// AssertTestPassed verifies specific test passed
func AssertTestPassed(t *testing.T, summary *test.TestSummary, testName string) {
	t.Helper()

	found := false
	for _, result := range summary.TestResults {
		if result.TestID == testName {
			found = true
			if result.Status != test.StatusPassed {
				t.Errorf("expected test %s to pass, but status is %s (failures: %d, error: %s)",
					testName, result.Status, result.FailureCount, result.ErrorMessage)
			}
			break
		}
	}

	if !found {
		t.Errorf("test %s not found in results", testName)
	}
}

// LoadTestConfig loads configuration from test project directory
func LoadTestConfig(t *testing.T, projectDir string) *config.Config {
	t.Helper()

	// Change to project directory
	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get current directory: %v", err)
	}
	defer os.Chdir(oldDir)

	if err := os.Chdir(projectDir); err != nil {
		t.Fatalf("failed to change to project directory: %v", err)
	}

	// Load config (using default profile)
	cfg, err := config.Discover("default")
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	// Update database path to be absolute
	if !filepath.IsAbs(cfg.Output.Database) {
		cfg.Output.Database = filepath.Join(projectDir, cfg.Output.Database)
	}

	return cfg
}

// copyDir recursively copies a directory
func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Get relative path
		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		// Target path
		targetPath := filepath.Join(dst, relPath)

		if info.IsDir() {
			// Create directory
			return os.MkdirAll(targetPath, 0755)
		}

		// Copy file
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		return os.WriteFile(targetPath, data, 0644)
	})
}
