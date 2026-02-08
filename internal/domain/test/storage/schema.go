package storage

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/jpconstantineau/gorchata/internal/platform"
)

const (
	// AuditTablePrefix is the prefix for all test failure tables
	AuditTablePrefix = "dbt_test__audit_"
)

// GetAuditTablePrefix returns the prefix for audit tables
func GetAuditTablePrefix() string {
	return AuditTablePrefix
}

// GetAuditTableSearchPattern returns the pattern for finding audit tables
func GetAuditTableSearchPattern() string {
	return AuditTablePrefix + "%"
}

// GenerateTableName generates a table name from test ID and optional custom name
func GenerateTableName(testID, customName string) string {
	if customName != "" {
		return AuditTablePrefix + SanitizeIdentifier(customName)
	}
	return AuditTablePrefix + SanitizeIdentifier(testID)
}

// SanitizeIdentifier sanitizes a string for use as a SQL identifier
// Converts to lowercase, replaces non-alphanumeric chars with underscore
func SanitizeIdentifier(s string) string {
	// Convert to lowercase
	s = strings.ToLower(s)

	// Replace non-alphanumeric characters with underscore
	reg := regexp.MustCompile(`[^a-z0-9_]`)
	s = reg.ReplaceAllString(s, "_")

	return s
}

// GenerateCreateTableSQL generates CREATE TABLE SQL for a failure table
func GenerateCreateTableSQL(tableName string, rowData map[string]interface{}) string {
	var columns []string

	// Standard columns
	columns = append(columns, "test_id TEXT NOT NULL")
	columns = append(columns, "test_run_id TEXT NOT NULL")
	columns = append(columns, "failed_at TIMESTAMP NOT NULL")
	columns = append(columns, "failure_reason TEXT")

	// Dynamic columns from row data
	// Sort keys for deterministic output
	keys := make([]string, 0, len(rowData))
	for k := range rowData {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		value := rowData[key]
		sqlType := InferSQLiteType(value)
		colName := SanitizeIdentifier(key)
		columns = append(columns, fmt.Sprintf("%s %s", colName, sqlType))
	}

	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n    %s\n)",
		tableName,
		strings.Join(columns, ",\n    "))

	return sql
}

// InferSQLiteType infers the SQLite type from a Go value
func InferSQLiteType(value interface{}) string {
	if value == nil {
		return "TEXT"
	}

	switch value.(type) {
	case string:
		return "TEXT"
	case int, int64:
		return "INTEGER"
	case float64, float32:
		return "REAL"
	case bool:
		return "INTEGER" // SQLite stores bools as 0/1
	case time.Time:
		return "TIMESTAMP"
	default:
		return "TEXT" // Default to TEXT for unknown types
	}
}

// CreateAuditSchema ensures the dbt_test__audit namespace is ready
// For SQLite, this is primarily a metadata operation
func CreateAuditSchema(ctx context.Context, adapter platform.DatabaseAdapter) error {
	// For SQLite, we don't need a separate schema
	// Tables are created with the dbt_test__audit_ prefix
	// This is primarily a no-op but could create a metadata table in the future
	return nil
}
