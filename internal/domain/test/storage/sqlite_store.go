package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jpconstantineau/gorchata/internal/domain/test"
	"github.com/jpconstantineau/gorchata/internal/platform"
)

// SQLiteFailureStore implements FailureStore for SQLite databases
type SQLiteFailureStore struct {
	adapter platform.DatabaseAdapter
}

// NewSQLiteFailureStore creates a new SQLite failure store
func NewSQLiteFailureStore(adapter platform.DatabaseAdapter) *SQLiteFailureStore {
	if adapter == nil {
		return nil
	}
	return &SQLiteFailureStore{
		adapter: adapter,
	}
}

// Initialize creates the dbt_test__audit schema and required structures
func (s *SQLiteFailureStore) Initialize(ctx context.Context) error {
	if s.adapter == nil {
		return fmt.Errorf("adapter is nil")
	}

	// For SQLite, initialization is minimal
	// Individual failure tables are created on-demand when failures are stored
	return CreateAuditSchema(ctx, s.adapter)
}

// StoreFailures persists failing rows for a test
func (s *SQLiteFailureStore) StoreFailures(ctx context.Context, t *test.Test, testRunID string, failures []FailureRow) error {
	if s.adapter == nil {
		return fmt.Errorf("adapter is nil")
	}

	if len(failures) == 0 {
		return nil // Nothing to store
	}

	// Determine table name
	tableName := GenerateTableName(t.ID, t.Config.StoreFailuresAs)

	// Check if table exists, if not create it
	exists, err := s.adapter.TableExists(ctx, tableName)
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}

	if !exists {
		// Create table based on first failure row structure
		createSQL := GenerateCreateTableSQL(tableName, failures[0].RowData)
		if err := s.adapter.ExecuteDDL(ctx, createSQL); err != nil {
			return fmt.Errorf("failed to create failure table: %w", err)
		}
	}

	// Insert failures
	for _, failure := range failures {
		if err := s.insertFailureRow(ctx, tableName, testRunID, failure); err != nil {
			return fmt.Errorf("failed to insert failure row: %w", err)
		}
	}

	return nil
}

// insertFailureRow inserts a single failure row into the table
func (s *SQLiteFailureStore) insertFailureRow(ctx context.Context, tableName, testRunID string, failure FailureRow) error {
	// Build column names and values
	columns := []string{"test_id", "test_run_id", "failed_at", "failure_reason"}
	placeholders := []string{"?", "?", "?", "?"}
	values := []interface{}{failure.TestID, testRunID, failure.FailedAt, failure.FailureReason}

	// Add dynamic columns from RowData (sorted for consistency)
	keys := make([]string, 0, len(failure.RowData))
	for k := range failure.RowData {
		keys = append(keys, k)
	}
	// Sort keys for deterministic SQL
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}

	for _, key := range keys {
		colName := SanitizeIdentifier(key)
		columns = append(columns, colName)
		placeholders = append(placeholders, "?")
		values = append(values, failure.RowData[key])
	}

	// Build INSERT statement
	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	// Execute insert
	// Since ExecuteDDL is for DDL statements, we need to use a different approach
	// We'll construct and execute the insert using ExecuteQuery but ignore results
	_, err := s.adapter.ExecuteQuery(ctx, sql, values...)
	if err != nil {
		return fmt.Errorf("failed to execute insert: %w", err)
	}

	return nil
}

// GetFailures retrieves stored failures for a test
func (s *SQLiteFailureStore) GetFailures(ctx context.Context, testID string, limit int) ([]FailureRow, error) {
	if s.adapter == nil {
		return nil, fmt.Errorf("adapter is nil")
	}

	// Get all audit tables and search for failures matching this test ID
	tables, err := s.listAuditTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list audit tables: %w", err)
	}

	allFailures := make([]FailureRow, 0)

	// Search each table for failures matching this test ID
	for _, tableName := range tables {
		sql := fmt.Sprintf("SELECT * FROM %s WHERE test_id = ? ORDER BY failed_at DESC", tableName)
		result, err := s.adapter.ExecuteQuery(ctx, sql, testID)
		if err != nil {
			// Skip tables that error (might have different schema)
			continue
		}

		// Convert results to FailureRow
		for _, row := range result.Rows {
			failure, err := s.rowToFailureRow(result.Columns, row, testID)
			if err != nil {
				continue
			}
			allFailures = append(allFailures, failure)
		}
	}

	// Sort by failed_at descending and apply limit
	// Simple sort using failed_at
	for i := 0; i < len(allFailures); i++ {
		for j := i + 1; j < len(allFailures); j++ {
			if allFailures[i].FailedAt.Before(allFailures[j].FailedAt) {
				allFailures[i], allFailures[j] = allFailures[j], allFailures[i]
			}
		}
	}

	// Apply limit
	if limit > 0 && len(allFailures) > limit {
		allFailures = allFailures[:limit]
	}

	return allFailures, nil
}

// rowToFailureRow converts a database row to a FailureRow
func (s *SQLiteFailureStore) rowToFailureRow(columns []string, row []interface{}, testID string) (FailureRow, error) {
	failure := FailureRow{
		TestID:  testID,
		RowData: make(map[string]interface{}),
	}

	for i, col := range columns {
		value := row[i]

		switch col {
		case "test_id":
			if v, ok := value.(string); ok {
				failure.TestID = v
			}
		case "test_run_id":
			if v, ok := value.(string); ok {
				failure.TestRunID = v
			}
		case "failed_at":
			// Handle different time formats
			switch v := value.(type) {
			case time.Time:
				failure.FailedAt = v
			case string:
				// Parse string timestamp
				t, err := time.Parse("2006-01-02 15:04:05", v)
				if err != nil {
					t, err = time.Parse(time.RFC3339, v)
				}
				if err == nil {
					failure.FailedAt = t
				}
			case int64:
				// Unix timestamp
				failure.FailedAt = time.Unix(v, 0)
			}
		case "failure_reason":
			if v, ok := value.(string); ok {
				failure.FailureReason = v
			}
		default:
			// Dynamic column - add to RowData
			failure.RowData[col] = value
		}
	}

	return failure, nil
}

// CleanupOldFailures removes failure records older than retention period
func (s *SQLiteFailureStore) CleanupOldFailures(ctx context.Context, retentionDays int) error {
	if s.adapter == nil {
		return fmt.Errorf("adapter is nil")
	}

	// Get list of all audit tables
	tables, err := s.listAuditTables(ctx)
	if err != nil {
		return fmt.Errorf("failed to list audit tables: %w", err)
	}

	// For each table, delete old records
	for _, table := range tables {
		deleteSQL := fmt.Sprintf(
			"DELETE FROM %s WHERE failed_at < datetime('now', '-%d days')",
			table,
			retentionDays,
		)

		_, err := s.adapter.ExecuteQuery(ctx, deleteSQL)
		if err != nil {
			// Log error but continue with other tables
			// In a real implementation, we'd use proper logging
			fmt.Printf("Warning: failed to cleanup table %s: %v\n", table, err)
		}
	}

	return nil
}

// listAuditTables retrieves all tables matching the audit table prefix
func (s *SQLiteFailureStore) listAuditTables(ctx context.Context) ([]string, error) {
	// Query SQLite's sqlite_master table to find all audit tables
	sql := `
		SELECT name 
		FROM sqlite_master 
		WHERE type='table' 
		AND name LIKE ?
	`

	result, err := s.adapter.ExecuteQuery(ctx, sql, GetAuditTableSearchPattern())
	if err != nil {
		// If we get an error querying sqlite_master, it might not exist or we don't have permissions
		// Return empty list rather than failing
		return []string{}, nil
	}

	tables := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		if len(row) > 0 {
			if tableName, ok := row[0].(string); ok {
				tables = append(tables, tableName)
			}
		}
	}

	return tables, nil
}

// Helper function to convert interface{} to sql.NullString for nullable columns
func toNullString(v interface{}) sql.NullString {
	if v == nil {
		return sql.NullString{Valid: false}
	}
	if s, ok := v.(string); ok {
		return sql.NullString{String: s, Valid: true}
	}
	return sql.NullString{String: fmt.Sprintf("%v", v), Valid: true}
}
