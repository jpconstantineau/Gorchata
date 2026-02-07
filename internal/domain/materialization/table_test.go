package materialization

import (
	"strings"
	"testing"
)

func TestTableMaterialize(t *testing.T) {
	tests := []struct {
		name        string
		modelName   string
		compiledSQL string
		config      MaterializationConfig
		wantErr     bool
		checkSQL    func(t *testing.T, sql []string)
	}{
		{
			name:        "creates table with full refresh",
			modelName:   "my_table",
			compiledSQL: "SELECT id, name FROM source_table",
			config:      MaterializationConfig{Type: MaterializationTable},
			wantErr:     false,
			checkSQL: func(t *testing.T, sql []string) {
				if len(sql) != 2 {
					t.Errorf("expected 2 SQL statements, got %d", len(sql))
				}
				// Should drop existing table first
				if !strings.Contains(sql[0], "DROP TABLE IF EXISTS") {
					t.Errorf("expected DROP TABLE IF EXISTS, got: %s", sql[0])
				}
				if !strings.Contains(sql[0], "my_table") {
					t.Errorf("expected table name in DROP statement, got: %s", sql[0])
				}
				// Should create table from SELECT
				if !strings.Contains(sql[1], "CREATE TABLE") {
					t.Errorf("expected CREATE TABLE, got: %s", sql[1])
				}
				if !strings.Contains(sql[1], "my_table") {
					t.Errorf("expected table name in CREATE statement, got: %s", sql[1])
				}
				if !strings.Contains(sql[1], "AS") {
					t.Errorf("expected AS in CREATE TABLE statement, got: %s", sql[1])
				}
				if !strings.Contains(sql[1], "SELECT id, name FROM source_table") {
					t.Errorf("expected SQL in CREATE TABLE, got: %s", sql[1])
				}
			},
		},
		{
			name:        "handles empty model name",
			modelName:   "",
			compiledSQL: "SELECT 1",
			config:      MaterializationConfig{Type: MaterializationTable},
			wantErr:     true,
		},
		{
			name:        "handles empty SQL",
			modelName:   "test_table",
			compiledSQL: "",
			config:      MaterializationConfig{Type: MaterializationTable},
			wantErr:     true,
		},
		{
			name:        "handles SQL with whitespace only",
			modelName:   "test_table",
			compiledSQL: "  \n  ",
			config:      MaterializationConfig{Type: MaterializationTable},
			wantErr:     true,
		},
		{
			name:        "replaces existing table",
			modelName:   "existing_table",
			compiledSQL: "SELECT * FROM new_source",
			config:      MaterializationConfig{Type: MaterializationTable, FullRefresh: true},
			wantErr:     false,
			checkSQL: func(t *testing.T, sql []string) {
				// Should have DROP + CREATE pattern
				if len(sql) != 2 {
					t.Errorf("expected 2 SQL statements, got %d", len(sql))
				}
				if !strings.Contains(sql[0], "DROP TABLE IF EXISTS existing_table") {
					t.Errorf("expected DROP for existing_table, got: %s", sql[0])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := &TableStrategy{}
			sql, err := strategy.Materialize(tt.modelName, tt.compiledSQL, tt.config)

			if (err != nil) != tt.wantErr {
				t.Errorf("Materialize() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && tt.checkSQL != nil {
				tt.checkSQL(t, sql)
			}
		})
	}
}

func TestTableName(t *testing.T) {
	strategy := &TableStrategy{}
	if got := strategy.Name(); got != "table" {
		t.Errorf("Name() = %v, want %v", got, "table")
	}
}
