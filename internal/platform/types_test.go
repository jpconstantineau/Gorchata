package platform

import (
	"testing"
)

func TestConnectionConfig(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		options  map[string]string
		wantPath string
	}{
		{
			name:     "simple path",
			path:     "/tmp/test.db",
			options:  nil,
			wantPath: "/tmp/test.db",
		},
		{
			name:     "path with options",
			path:     "/tmp/test.db",
			options:  map[string]string{"mode": "rwc"},
			wantPath: "/tmp/test.db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &ConnectionConfig{
				DatabasePath: tt.path,
				Options:      tt.options,
			}

			if config.DatabasePath != tt.wantPath {
				t.Errorf("DatabasePath = %v, want %v", config.DatabasePath, tt.wantPath)
			}
		})
	}
}

func TestQueryResult(t *testing.T) {
	result := &QueryResult{
		Columns:      []string{"id", "name"},
		Rows:         [][]interface{}{{1, "test"}},
		RowsAffected: 1,
	}

	if len(result.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(result.Columns))
	}

	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.Rows))
	}

	if result.RowsAffected != 1 {
		t.Errorf("expected RowsAffected = 1, got %d", result.RowsAffected)
	}
}

func TestSchema(t *testing.T) {
	schema := &Schema{
		TableName: "users",
		Columns: []Column{
			{Name: "id", Type: "INTEGER", Nullable: false, PrimaryKey: true},
			{Name: "name", Type: "TEXT", Nullable: false, PrimaryKey: false},
		},
	}

	if schema.TableName != "users" {
		t.Errorf("TableName = %v, want users", schema.TableName)
	}

	if len(schema.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(schema.Columns))
	}

	if schema.Columns[0].Name != "id" {
		t.Errorf("first column name = %v, want id", schema.Columns[0].Name)
	}

	if !schema.Columns[0].PrimaryKey {
		t.Errorf("expected first column to be primary key")
	}
}

func TestColumn(t *testing.T) {
	col := Column{
		Name:       "email",
		Type:       "TEXT",
		Nullable:   true,
		PrimaryKey: false,
	}

	if col.Name != "email" {
		t.Errorf("Name = %v, want email", col.Name)
	}

	if col.Type != "TEXT" {
		t.Errorf("Type = %v, want TEXT", col.Type)
	}

	if !col.Nullable {
		t.Errorf("expected Nullable = true")
	}

	if col.PrimaryKey {
		t.Errorf("expected PrimaryKey = false")
	}
}
