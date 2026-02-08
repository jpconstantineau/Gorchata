package singular

import (
	"testing"

	"github.com/jpconstantineau/gorchata/internal/domain/test"
)

func TestParseTestMetadata_WithoutConfig(t *testing.T) {
	sqlContent := `-- Simple test without config
SELECT * FROM my_table WHERE value < 0
`

	config, err := ParseTestMetadata(sqlContent)
	if err != nil {
		t.Fatalf("ParseTestMetadata failed: %v", err)
	}

	// Should return default config
	if config.Severity != test.SeverityError {
		t.Errorf("Expected severity 'error', got '%s'", config.Severity)
	}
	if config.StoreFailures != false {
		t.Errorf("Expected store_failures false, got %v", config.StoreFailures)
	}
}

func TestParseTestMetadata_WithConfig(t *testing.T) {
	sqlContent := `-- config(severity='warn', store_failures=true)
SELECT * FROM my_table WHERE value < 0
`

	config, err := ParseTestMetadata(sqlContent)
	if err != nil {
		t.Fatalf("ParseTestMetadata failed: %v", err)
	}

	if config.Severity != test.SeverityWarn {
		t.Errorf("Expected severity 'warn', got '%s'", config.Severity)
	}
	if config.StoreFailures != true {
		t.Errorf("Expected store_failures true, got %v", config.StoreFailures)
	}
}

func TestParseTestMetadata_MultipleSeverities(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected test.Severity
	}{
		{
			name:     "error severity",
			sql:      "-- config(severity='error')\nSELECT 1",
			expected: test.SeverityError,
		},
		{
			name:     "warn severity",
			sql:      "-- config(severity='warn')\nSELECT 1",
			expected: test.SeverityWarn,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := ParseTestMetadata(tt.sql)
			if err != nil {
				t.Fatalf("ParseTestMetadata failed: %v", err)
			}

			if config.Severity != tt.expected {
				t.Errorf("Expected severity '%s', got '%s'", tt.expected, config.Severity)
			}
		})
	}
}

func TestParseTestMetadata_StoreFailures(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected bool
	}{
		{
			name:     "store_failures true",
			sql:      "-- config(store_failures=true)\nSELECT 1",
			expected: true,
		},
		{
			name:     "store_failures false",
			sql:      "-- config(store_failures=false)\nSELECT 1",
			expected: false,
		},
		{
			name:     "no store_failures",
			sql:      "-- config(severity='warn')\nSELECT 1",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := ParseTestMetadata(tt.sql)
			if err != nil {
				t.Fatalf("ParseTestMetadata failed: %v", err)
			}

			if config.StoreFailures != tt.expected {
				t.Errorf("Expected store_failures %v, got %v", tt.expected, config.StoreFailures)
			}
		})
	}
}

func TestParseTestMetadata_WithWhere(t *testing.T) {
	sqlContent := `-- config(where='date > 2024-01-01')
SELECT * FROM my_table WHERE value < 0
`

	config, err := ParseTestMetadata(sqlContent)
	if err != nil {
		t.Fatalf("ParseTestMetadata failed: %v", err)
	}

	if config.Where != "date > 2024-01-01" {
		t.Errorf("Expected where 'date > 2024-01-01', got '%s'", config.Where)
	}
}

func TestParseTestMetadata_MultipleFields(t *testing.T) {
	sqlContent := `-- config(severity='warn', store_failures=true, where='active=1')
SELECT * FROM my_table WHERE value < 0
`

	config, err := ParseTestMetadata(sqlContent)
	if err != nil {
		t.Fatalf("ParseTestMetadata failed: %v", err)
	}

	if config.Severity != test.SeverityWarn {
		t.Errorf("Expected severity 'warn', got '%s'", config.Severity)
	}
	if config.StoreFailures != true {
		t.Errorf("Expected store_failures true, got %v", config.StoreFailures)
	}
	if config.Where != "active=1" {
		t.Errorf("Expected where 'active=1', got '%s'", config.Where)
	}
}

func TestParseTestMetadata_InvalidSyntax(t *testing.T) {
	sqlContent := `-- config(severity=invalid)
SELECT * FROM my_table WHERE value < 0
`

	// Should not error, just use defaults
	config, err := ParseTestMetadata(sqlContent)
	if err != nil {
		t.Fatalf("ParseTestMetadata failed: %v", err)
	}

	// Should have default config
	if config.Severity != test.SeverityError {
		t.Errorf("Expected default severity 'error', got '%s'", config.Severity)
	}
}
