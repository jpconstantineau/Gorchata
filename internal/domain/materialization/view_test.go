package materialization

import (
	"strings"
	"testing"
)

func TestViewMaterialize(t *testing.T) {
	tests := []struct {
		name        string
		modelName   string
		compiledSQL string
		config      MaterializationConfig
		wantErr     bool
		checkSQL    func(t *testing.T, sql []string)
	}{
		{
			name:        "creates view with valid SQL",
			modelName:   "my_view",
			compiledSQL: "SELECT * FROM source_table",
			config:      MaterializationConfig{Type: MaterializationView},
			wantErr:     false,
			checkSQL: func(t *testing.T, sql []string) {
				if len(sql) != 2 {
					t.Errorf("expected 2 SQL statements, got %d", len(sql))
				}
				// Should drop existing view first
				if !strings.Contains(sql[0], "DROP VIEW IF EXISTS") {
					t.Errorf("expected DROP VIEW IF EXISTS, got: %s", sql[0])
				}
				if !strings.Contains(sql[0], "my_view") {
					t.Errorf("expected view name in DROP statement, got: %s", sql[0])
				}
				// Should create view
				if !strings.Contains(sql[1], "CREATE VIEW") {
					t.Errorf("expected CREATE VIEW, got: %s", sql[1])
				}
				if !strings.Contains(sql[1], "my_view") {
					t.Errorf("expected view name in CREATE statement, got: %s", sql[1])
				}
				if !strings.Contains(sql[1], "SELECT * FROM source_table") {
					t.Errorf("expected SQL in CREATE VIEW, got: %s", sql[1])
				}
			},
		},
		{
			name:        "handles empty model name",
			modelName:   "",
			compiledSQL: "SELECT 1",
			config:      MaterializationConfig{Type: MaterializationView},
			wantErr:     true,
		},
		{
			name:        "handles empty SQL",
			modelName:   "test_view",
			compiledSQL: "",
			config:      MaterializationConfig{Type: MaterializationView},
			wantErr:     true,
		},
		{
			name:        "handles SQL with whitespace",
			modelName:   "test_view",
			compiledSQL: "  \n\t  ",
			config:      MaterializationConfig{Type: MaterializationView},
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := &ViewStrategy{}
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

func TestViewName(t *testing.T) {
	strategy := &ViewStrategy{}
	if got := strategy.Name(); got != "view" {
		t.Errorf("Name() = %v, want %v", got, "view")
	}
}
