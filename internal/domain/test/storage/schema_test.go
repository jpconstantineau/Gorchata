package storage

import (
	"strings"
	"testing"
)

// TestGenerateTableName tests table name generation from test ID
func TestGenerateTableName(t *testing.T) {
	tests := []struct {
		name     string
		testID   string
		custom   string
		expected string
	}{
		{
			name:     "basic test ID",
			testID:   "not_null_users_email",
			custom:   "",
			expected: "dbt_test__audit_not_null_users_email",
		},
		{
			name:     "test ID with special characters",
			testID:   "test-with-dashes_and_underscores",
			custom:   "",
			expected: "dbt_test__audit_test_with_dashes_and_underscores",
		},
		{
			name:     "custom table name",
			testID:   "not_null_users_email",
			custom:   "user_email_nulls",
			expected: "dbt_test__audit_user_email_nulls",
		},
		{
			name:     "test ID with uppercase",
			testID:   "NotNull_Users_Email",
			custom:   "",
			expected: "dbt_test__audit_notnull_users_email",
		},
		{
			name:     "test ID with numbers",
			testID:   "test_123_abc",
			custom:   "",
			expected: "dbt_test__audit_test_123_abc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GenerateTableName(tt.testID, tt.custom)
			if result != tt.expected {
				t.Errorf("GenerateTableName(%q, %q) = %q, want %q", tt.testID, tt.custom, result, tt.expected)
			}
		})
	}
}

// TestSanitizeIdentifier tests identifier sanitization
func TestSanitizeIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "already clean",
			input:    "test_123_abc",
			expected: "test_123_abc",
		},
		{
			name:     "with dashes",
			input:    "test-with-dashes",
			expected: "test_with_dashes",
		},
		{
			name:     "with spaces",
			input:    "test with spaces",
			expected: "test_with_spaces",
		},
		{
			name:     "with special chars",
			input:    "test!@#$%^&*()",
			expected: "test__________",
		},
		{
			name:     "with uppercase",
			input:    "TestWithCamelCase",
			expected: "testwithcamelcase",
		},
		{
			name:     "mixed",
			input:    "Test-123_ABC def",
			expected: "test_123_abc_def",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SanitizeIdentifier(tt.input)
			if result != tt.expected {
				t.Errorf("SanitizeIdentifier(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// TestGenerateCreateTableSQL tests SQL generation for dynamic tables
func TestGenerateCreateTableSQL(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		rowData   map[string]interface{}
		wantParts []string
	}{
		{
			name:      "basic columns",
			tableName: "dbt_test__audit_test",
			rowData: map[string]interface{}{
				"id":    123,
				"email": "test@example.com",
				"age":   30,
			},
			wantParts: []string{
				"CREATE TABLE IF NOT EXISTS dbt_test__audit_test",
				"test_id TEXT NOT NULL",
				"test_run_id TEXT NOT NULL",
				"failed_at TIMESTAMP NOT NULL",
				"failure_reason TEXT",
				"id",
				"email",
				"age",
			},
		},
		{
			name:      "various types",
			tableName: "dbt_test__audit_test2",
			rowData: map[string]interface{}{
				"str_col":   "text",
				"int_col":   42,
				"float_col": 3.14,
				"bool_col":  true,
			},
			wantParts: []string{
				"CREATE TABLE IF NOT EXISTS dbt_test__audit_test2",
				"str_col",
				"int_col",
				"float_col",
				"bool_col",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql := GenerateCreateTableSQL(tt.tableName, tt.rowData)

			// Check that SQL contains all expected parts
			for _, part := range tt.wantParts {
				if !strings.Contains(sql, part) {
					t.Errorf("GenerateCreateTableSQL() SQL missing %q\nGot: %s", part, sql)
				}
			}

			// Check basic structure
			if !strings.HasPrefix(sql, "CREATE TABLE IF NOT EXISTS") {
				t.Error("SQL should start with CREATE TABLE IF NOT EXISTS")
			}
			if !strings.Contains(sql, "(") || !strings.Contains(sql, ")") {
				t.Error("SQL should contain parentheses")
			}
		})
	}
}

// TestInferSQLiteType tests type inference from Go values
func TestInferSQLiteType(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{
			name:     "string",
			value:    "text",
			expected: "TEXT",
		},
		{
			name:     "int",
			value:    42,
			expected: "INTEGER",
		},
		{
			name:     "int64",
			value:    int64(42),
			expected: "INTEGER",
		},
		{
			name:     "float64",
			value:    3.14,
			expected: "REAL",
		},
		{
			name:     "bool",
			value:    true,
			expected: "INTEGER",
		},
		{
			name:     "nil",
			value:    nil,
			expected: "TEXT",
		},
		{
			name:     "unknown type",
			value:    []byte{1, 2, 3},
			expected: "TEXT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := InferSQLiteType(tt.value)
			if result != tt.expected {
				t.Errorf("InferSQLiteType(%v) = %q, want %q", tt.value, result, tt.expected)
			}
		})
	}
}

// TestGetAuditTablePrefix tests getting the table prefix
func TestGetAuditTablePrefix(t *testing.T) {
	prefix := GetAuditTablePrefix()
	if prefix != "dbt_test__audit_" {
		t.Errorf("GetAuditTablePrefix() = %q, want dbt_test__audit_", prefix)
	}
}

// TestListAuditTables tests the pattern for finding audit tables
func TestListAuditTables(t *testing.T) {
	// This test verifies the SQL pattern used to find audit tables
	pattern := GetAuditTableSearchPattern()
	expected := "dbt_test__audit_%"

	if pattern != expected {
		t.Errorf("GetAuditTableSearchPattern() = %q, want %q", pattern, expected)
	}
}
