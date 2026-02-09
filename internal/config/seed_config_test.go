package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseSeedConfig_NamingStrategy(t *testing.T) {
	tests := []struct {
		name             string
		yamlContent      string
		expectedStrategy string
		expectedStatic   string
		expectedPrefix   string
	}{
		{
			name: "filename strategy",
			yamlContent: `version: 1
naming:
  strategy: filename
`,
			expectedStrategy: "filename",
			expectedStatic:   "",
			expectedPrefix:   "",
		},
		{
			name: "folder strategy",
			yamlContent: `version: 1
naming:
  strategy: folder
`,
			expectedStrategy: "folder",
			expectedStatic:   "",
			expectedPrefix:   "",
		},
		{
			name: "static strategy with name",
			yamlContent: `version: 1
naming:
  strategy: static
  static_name: my_custom_table
`,
			expectedStrategy: "static",
			expectedStatic:   "my_custom_table",
			expectedPrefix:   "",
		},
		{
			name: "with prefix",
			yamlContent: `version: 1
naming:
  strategy: filename
  prefix: seed_
`,
			expectedStrategy: "filename",
			expectedStatic:   "",
			expectedPrefix:   "seed_",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temp file
			tmpDir := t.TempDir()
			tmpFile := filepath.Join(tmpDir, "seed.yml")
			if err := os.WriteFile(tmpFile, []byte(tt.yamlContent), 0644); err != nil {
				t.Fatalf("failed to write temp file: %v", err)
			}

			// Parse config
			cfg, err := ParseSeedConfig(tmpFile)
			if err != nil {
				t.Fatalf("ParseSeedConfig failed: %v", err)
			}

			// Verify naming config
			if cfg.Naming.Strategy != tt.expectedStrategy {
				t.Errorf("expected strategy %q, got %q", tt.expectedStrategy, cfg.Naming.Strategy)
			}
			if cfg.Naming.StaticName != tt.expectedStatic {
				t.Errorf("expected static_name %q, got %q", tt.expectedStatic, cfg.Naming.StaticName)
			}
			if cfg.Naming.Prefix != tt.expectedPrefix {
				t.Errorf("expected prefix %q, got %q", tt.expectedPrefix, cfg.Naming.Prefix)
			}
		})
	}
}

func TestParseSeedConfig_ImportConfig(t *testing.T) {
	tests := []struct {
		name              string
		yamlContent       string
		expectedBatchSize int
		expectedScope     string
	}{
		{
			name: "custom batch size and scope",
			yamlContent: `version: 1
import:
  batch_size: 500
  scope: file
`,
			expectedBatchSize: 500,
			expectedScope:     "file",
		},
		{
			name: "folder scope",
			yamlContent: `version: 1
import:
  batch_size: 2000
  scope: folder
`,
			expectedBatchSize: 2000,
			expectedScope:     "folder",
		},
		{
			name: "tree scope",
			yamlContent: `version: 1
import:
  batch_size: 1500
  scope: tree
`,
			expectedBatchSize: 1500,
			expectedScope:     "tree",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			tmpFile := filepath.Join(tmpDir, "seed.yml")
			if err := os.WriteFile(tmpFile, []byte(tt.yamlContent), 0644); err != nil {
				t.Fatalf("failed to write temp file: %v", err)
			}

			cfg, err := ParseSeedConfig(tmpFile)
			if err != nil {
				t.Fatalf("ParseSeedConfig failed: %v", err)
			}

			if cfg.Import.BatchSize != tt.expectedBatchSize {
				t.Errorf("expected batch_size %d, got %d", tt.expectedBatchSize, cfg.Import.BatchSize)
			}
			if cfg.Import.Scope != tt.expectedScope {
				t.Errorf("expected scope %q, got %q", tt.expectedScope, cfg.Import.Scope)
			}
		})
	}
}

func TestParseSeedConfig_ColumnTypes(t *testing.T) {
	yamlContent := `version: 1
column_types:
  customer_id: INTEGER
  name: TEXT
  email: TEXT
  age: INTEGER
  balance: REAL
`

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "seed.yml")
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}

	cfg, err := ParseSeedConfig(tmpFile)
	if err != nil {
		t.Fatalf("ParseSeedConfig failed: %v", err)
	}

	expectedTypes := map[string]string{
		"customer_id": "INTEGER",
		"name":        "TEXT",
		"email":       "TEXT",
		"age":         "INTEGER",
		"balance":     "REAL",
	}

	if len(cfg.ColumnTypes) != len(expectedTypes) {
		t.Errorf("expected %d column types, got %d", len(expectedTypes), len(cfg.ColumnTypes))
	}

	for col, expectedType := range expectedTypes {
		if gotType, ok := cfg.ColumnTypes[col]; !ok {
			t.Errorf("column %q not found in column_types", col)
		} else if gotType != expectedType {
			t.Errorf("column %q: expected type %q, got %q", col, expectedType, gotType)
		}
	}
}

func TestParseSeedConfig_FullExample(t *testing.T) {
	yamlContent := `version: 1

naming:
  strategy: filename
  prefix: seed_

import:
  batch_size: 1000
  scope: tree

column_types:
  customer_id: INTEGER
  name: TEXT

config:
  quote_columns: true
  delimiter: ","
`

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "seed.yml")
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}

	cfg, err := ParseSeedConfig(tmpFile)
	if err != nil {
		t.Fatalf("ParseSeedConfig failed: %v", err)
	}

	// Verify all fields
	if cfg.Version != 1 {
		t.Errorf("expected version 1, got %d", cfg.Version)
	}
	if cfg.Naming.Strategy != "filename" {
		t.Errorf("expected strategy 'filename', got %q", cfg.Naming.Strategy)
	}
	if cfg.Naming.Prefix != "seed_" {
		t.Errorf("expected prefix 'seed_', got %q", cfg.Naming.Prefix)
	}
	if cfg.Import.BatchSize != 1000 {
		t.Errorf("expected batch_size 1000, got %d", cfg.Import.BatchSize)
	}
	if cfg.Import.Scope != "tree" {
		t.Errorf("expected scope 'tree', got %q", cfg.Import.Scope)
	}
	if len(cfg.ColumnTypes) != 2 {
		t.Errorf("expected 2 column types, got %d", len(cfg.ColumnTypes))
	}
	if len(cfg.Config) != 2 {
		t.Errorf("expected 2 config entries, got %d", len(cfg.Config))
	}
	if quote, ok := cfg.Config["quote_columns"].(bool); !ok || !quote {
		t.Errorf("expected config.quote_columns to be true")
	}
}

func TestParseSeedConfig_Defaults(t *testing.T) {
	yamlContent := `version: 1
`

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "seed.yml")
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}

	cfg, err := ParseSeedConfig(tmpFile)
	if err != nil {
		t.Fatalf("ParseSeedConfig failed: %v", err)
	}

	// Verify defaults
	if cfg.Naming.Strategy != "filename" {
		t.Errorf("expected default strategy 'filename', got %q", cfg.Naming.Strategy)
	}
	if cfg.Import.BatchSize != 1000 {
		t.Errorf("expected default batch_size 1000, got %d", cfg.Import.BatchSize)
	}
	if cfg.Import.Scope != "tree" {
		t.Errorf("expected default scope 'tree', got %q", cfg.Import.Scope)
	}
}

func TestParseSeedConfig_InvalidFile(t *testing.T) {
	tests := []struct {
		name        string
		yamlContent string
		expectError bool
	}{
		{
			name:        "missing file",
			yamlContent: "",
			expectError: true,
		},
		{
			name: "malformed YAML",
			yamlContent: `version: 1
naming:
  strategy: filename
    invalid_indent: value
`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "missing file" {
				// Test with non-existent file
				_, err := ParseSeedConfig("/nonexistent/path/seed.yml")
				if err == nil {
					t.Error("expected error for missing file, got nil")
				}
			} else {
				tmpDir := t.TempDir()
				tmpFile := filepath.Join(tmpDir, "seed.yml")
				if err := os.WriteFile(tmpFile, []byte(tt.yamlContent), 0644); err != nil {
					t.Fatalf("failed to write temp file: %v", err)
				}

				_, err := ParseSeedConfig(tmpFile)
				if tt.expectError && err == nil {
					t.Error("expected error for malformed YAML, got nil")
				}
			}
		})
	}
}
