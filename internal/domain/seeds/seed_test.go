package seeds

import (
	"testing"
)

func TestSeedCreation(t *testing.T) {
	tests := []struct {
		name    string
		seed    Seed
		wantErr bool
	}{
		{
			name: "valid seed with all fields",
			seed: Seed{
				ID:   "test-seed-1",
				Path: "/path/to/seed.csv",
				Type: SeedTypeCSV,
				Schema: &SeedSchema{
					Columns: []SeedColumn{
						{Name: "id", Type: "INTEGER"},
						{Name: "name", Type: "TEXT"},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid seed with minimal fields",
			seed: Seed{
				ID:   "test-seed-2",
				Path: "/path/to/seed.csv",
				Type: SeedTypeCSV,
			},
			wantErr: false,
		},
		{
			name: "seed with SQL type",
			seed: Seed{
				ID:   "test-seed-3",
				Path: "/path/to/seed.sql",
				Type: SeedTypeSQL,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test that seed can be created
			if tt.seed.ID == "" && tt.wantErr {
				t.Error("expected error for empty ID")
			}

			// Verify basic field access
			if tt.seed.ID == "" && !tt.wantErr {
				t.Error("seed ID should not be empty")
			}
			if tt.seed.Path == "" && !tt.wantErr {
				t.Error("seed Path should not be empty")
			}
			if tt.seed.Type == "" && !tt.wantErr {
				t.Error("seed Type should not be empty")
			}
		})
	}
}

func TestSeedType(t *testing.T) {
	tests := []struct {
		name     string
		seedType SeedType
		want     string
	}{
		{
			name:     "CSV type",
			seedType: SeedTypeCSV,
			want:     "csv",
		},
		{
			name:     "SQL type",
			seedType: SeedTypeSQL,
			want:     "sql",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.seedType) != tt.want {
				t.Errorf("SeedType = %v, want %v", tt.seedType, tt.want)
			}
		})
	}
}

func TestSeedSchema(t *testing.T) {
	schema := SeedSchema{
		Columns: []SeedColumn{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "TEXT"},
			{Name: "created_at", Type: "TIMESTAMP"},
		},
	}

	if len(schema.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(schema.Columns))
	}

	if schema.Columns[0].Name != "id" {
		t.Errorf("expected first column name 'id', got '%s'", schema.Columns[0].Name)
	}

	if schema.Columns[0].Type != "INTEGER" {
		t.Errorf("expected first column type 'INTEGER', got '%s'", schema.Columns[0].Type)
	}
}
