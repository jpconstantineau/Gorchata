package materialization

import (
	"strings"
	"testing"
)

func TestIncrementalMaterialize(t *testing.T) {
	tests := []struct {
		name        string
		modelName   string
		compiledSQL string
		config      MaterializationConfig
		wantErr     bool
		checkSQL    func(t *testing.T, sql []string)
	}{
		{
			name:        "creates incremental table with merge logic",
			modelName:   "my_incremental_table",
			compiledSQL: "SELECT id, name, updated_at FROM source_table",
			config: MaterializationConfig{
				Type:      MaterializationIncremental,
				UniqueKey: []string{"id"},
			},
			wantErr: false,
			checkSQL: func(t *testing.T, sql []string) {
				if len(sql) < 3 {
					t.Errorf("expected at least 3 SQL statements, got %d", len(sql))
				}
				// Should create temp table
				if !strings.Contains(sql[0], "CREATE TEMP TABLE") {
					t.Errorf("expected CREATE TEMP TABLE, got: %s", sql[0])
				}
				if !strings.Contains(sql[0], "__tmp") {
					t.Errorf("expected temp table suffix, got: %s", sql[0])
				}
				// Should create target table if not exists
				if !strings.Contains(sql[1], "CREATE TABLE IF NOT EXISTS") {
					t.Errorf("expected CREATE TABLE IF NOT EXISTS, got: %s", sql[1])
				}
				if !strings.Contains(sql[1], "my_incremental_table") {
					t.Errorf("expected target table name, got: %s", sql[1])
				}
				// Should have merge/upsert logic
				hasDelete := false
				hasInsert := false
				for _, stmt := range sql {
					if strings.Contains(stmt, "DELETE FROM") && strings.Contains(stmt, "my_incremental_table") {
						hasDelete = true
					}
					if strings.Contains(stmt, "INSERT INTO") && strings.Contains(stmt, "my_incremental_table") {
						hasInsert = true
					}
				}
				if !hasDelete || !hasInsert {
					t.Errorf("expected DELETE and INSERT statements for merge logic")
				}
			},
		},
		{
			name:        "requires unique key for incremental",
			modelName:   "my_table",
			compiledSQL: "SELECT * FROM source",
			config: MaterializationConfig{
				Type:      MaterializationIncremental,
				UniqueKey: []string{},
			},
			wantErr: true,
		},
		{
			name:        "handles empty model name",
			modelName:   "",
			compiledSQL: "SELECT * FROM source",
			config: MaterializationConfig{
				Type:      MaterializationIncremental,
				UniqueKey: []string{"id"},
			},
			wantErr: true,
		},
		{
			name:        "handles empty SQL",
			modelName:   "test_table",
			compiledSQL: "",
			config: MaterializationConfig{
				Type:      MaterializationIncremental,
				UniqueKey: []string{"id"},
			},
			wantErr: true,
		},
		{
			name:        "performs full refresh when requested",
			modelName:   "my_table",
			compiledSQL: "SELECT id, value FROM source",
			config: MaterializationConfig{
				Type:        MaterializationIncremental,
				UniqueKey:   []string{"id"},
				FullRefresh: true,
			},
			wantErr: false,
			checkSQL: func(t *testing.T, sql []string) {
				// Should drop and recreate on full refresh
				hasDropTable := false
				for _, stmt := range sql {
					if strings.Contains(stmt, "DROP TABLE IF EXISTS") && strings.Contains(stmt, "my_table") {
						hasDropTable = true
					}
				}
				if !hasDropTable {
					t.Errorf("expected DROP TABLE on full refresh")
				}
			},
		},
		{
			name:        "handles composite unique key",
			modelName:   "composite_key_table",
			compiledSQL: "SELECT tenant_id, user_id, data FROM source",
			config: MaterializationConfig{
				Type:      MaterializationIncremental,
				UniqueKey: []string{"tenant_id", "user_id"},
			},
			wantErr: false,
			checkSQL: func(t *testing.T, sql []string) {
				// Check that both keys are used in the WHERE clause
				hasCompositeWhere := false
				for _, stmt := range sql {
					if strings.Contains(stmt, "tenant_id") && strings.Contains(stmt, "user_id") && strings.Contains(stmt, "WHERE") {
						hasCompositeWhere = true
					}
				}
				if !hasCompositeWhere {
					t.Errorf("expected composite key in WHERE clause")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := &IncrementalStrategy{}
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

func TestIncrementalName(t *testing.T) {
	strategy := &IncrementalStrategy{}
	if got := strategy.Name(); got != "incremental" {
		t.Errorf("Name() = %v, want %v", got, "incremental")
	}
}
