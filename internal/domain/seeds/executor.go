package seeds

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jpconstantineau/gorchata/internal/config"
	"github.com/jpconstantineau/gorchata/internal/platform"
)

// ExecuteSeed executes a seed by creating a table and loading data
func ExecuteSeed(ctx context.Context, adapter platform.DatabaseAdapter, seed *Seed, rows [][]string, cfg *config.SeedConfig) (*SeedResult, error) {
	// Input validation
	if adapter == nil {
		return nil, fmt.Errorf("adapter cannot be nil")
	}
	if seed == nil {
		return nil, fmt.Errorf("seed cannot be nil")
	}
	if rows == nil {
		return nil, fmt.Errorf("rows cannot be nil")
	}
	if cfg == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	// Config validation
	if cfg.Import.BatchSize <= 0 {
		return nil, fmt.Errorf("batch size must be greater than 0, got %d", cfg.Import.BatchSize)
	}

	result := &SeedResult{
		SeedID:    seed.ID,
		Status:    StatusRunning,
		StartTime: time.Now(),
	}

	// Start transaction
	tx, err := adapter.BeginTransaction(ctx)
	if err != nil {
		result.Status = StatusFailed
		result.Error = fmt.Sprintf("failed to begin transaction: %v", err)
		result.EndTime = time.Now()
		return result, err
	}

	// Track if transaction has been committed
	committed := false

	// Ensure transaction is handled properly
	defer func() {
		if !committed && result.Status == StatusFailed {
			tx.Rollback()
		}
	}()

	// Drop table if exists (full refresh)
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdentifier(seed.ID))
	if err := tx.Exec(ctx, dropSQL); err != nil {
		result.Status = StatusFailed
		result.Error = fmt.Sprintf("failed to drop table: %v", err)
		result.EndTime = time.Now()
		return result, err
	}

	// Create table
	createSQL := buildCreateTableSQL(seed.ID, seed.Schema)
	if err := tx.Exec(ctx, createSQL); err != nil {
		result.Status = StatusFailed
		result.Error = fmt.Sprintf("failed to create table: %v", err)
		result.EndTime = time.Now()
		return result, err
	}

	// Process rows in batches
	batchProcessor := NewBatchProcessor(cfg.Import.BatchSize)
	rowsLoaded := 0

	err = batchProcessor.Process(rows, func(batch [][]string) error {
		insertSQL := buildInsertSQL(seed.ID, seed.Schema, batch)
		if err := tx.Exec(ctx, insertSQL); err != nil {
			return fmt.Errorf("failed to insert batch: %w", err)
		}
		rowsLoaded += len(batch)
		return nil
	})

	if err != nil {
		result.Status = StatusFailed
		result.Error = fmt.Sprintf("batch processing failed: %v", err)
		result.EndTime = time.Now()
		return result, err
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		result.Status = StatusFailed
		result.Error = fmt.Sprintf("failed to commit transaction: %v", err)
		result.EndTime = time.Now()
		return result, err
	}
	committed = true

	result.Status = StatusSuccess
	result.RowsLoaded = rowsLoaded
	result.EndTime = time.Now()

	return result, nil
}

// quoteIdentifier quotes a SQL identifier (table or column name) with double quotes
// and escapes any embedded double quotes by doubling them (SQLite standard)
func quoteIdentifier(name string) string {
	// Escape double quotes by doubling them
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return fmt.Sprintf(`"%s"`, escaped)
}

// formatValue formats a value for SQL insertion based on its column type
func formatValue(value string, columnType string) string {
	if value == "" {
		return "''" // Empty string, not NULL
	}

	switch columnType {
	case "INTEGER", "REAL":
		return value // No quotes for numbers
	default: // TEXT, TIMESTAMP, etc.
		// Escape single quotes by doubling them
		escaped := strings.ReplaceAll(value, "'", "''")
		return fmt.Sprintf("'%s'", escaped)
	}
}

// buildCreateTableSQL generates a CREATE TABLE statement from a schema
func buildCreateTableSQL(tableName string, schema *SeedSchema) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", quoteIdentifier(tableName)))

	for i, col := range schema.Columns {
		sb.WriteString(fmt.Sprintf("    %s %s", quoteIdentifier(col.Name), col.Type))
		if i < len(schema.Columns)-1 {
			sb.WriteString(",\n")
		} else {
			sb.WriteString("\n")
		}
	}

	sb.WriteString(")")

	return sb.String()
}

// buildInsertSQL generates an INSERT statement for a batch of rows
func buildInsertSQL(tableName string, schema *SeedSchema, rows [][]string) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("INSERT INTO %s VALUES\n", quoteIdentifier(tableName)))

	for i, row := range rows {
		sb.WriteString("    (")
		for j, value := range row {
			// Format value based on column type
			var columnType string
			if j < len(schema.Columns) {
				columnType = schema.Columns[j].Type
			} else {
				columnType = "TEXT" // Default to TEXT if column index is out of range
			}
			sb.WriteString(formatValue(value, columnType))
			if j < len(row)-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString(")")
		if i < len(rows)-1 {
			sb.WriteString(",\n")
		}
	}

	return sb.String()
}
