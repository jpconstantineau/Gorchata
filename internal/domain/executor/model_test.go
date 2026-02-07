package executor

import (
	"testing"

	"github.com/pierre/gorchata/internal/domain/materialization"
)

func TestNewModel(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		path    string
		wantErr bool
	}{
		{
			name:    "valid model",
			id:      "stg_users",
			path:    "models/stg_users.sql",
			wantErr: false,
		},
		{
			name:    "empty id",
			id:      "",
			path:    "models/stg_users.sql",
			wantErr: true,
		},
		{
			name:    "empty path",
			id:      "stg_users",
			path:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model, err := NewModel(tt.id, tt.path)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if model.ID != tt.id {
				t.Errorf("ID = %v, want %v", model.ID, tt.id)
			}
			if model.Path != tt.path {
				t.Errorf("Path = %v, want %v", model.Path, tt.path)
			}
		})
	}
}

func TestModel_AddDependency(t *testing.T) {
	model, err := NewModel("model_a", "models/a.sql")
	if err != nil {
		t.Fatalf("failed to create model: %v", err)
	}

	// Add dependency
	model.AddDependency("model_b")

	if len(model.Dependencies) != 1 {
		t.Errorf("Dependencies length = %d, want 1", len(model.Dependencies))
	}

	if model.Dependencies[0] != "model_b" {
		t.Errorf("Dependency = %s, want model_b", model.Dependencies[0])
	}

	// Add duplicate dependency (should not add)
	model.AddDependency("model_b")

	if len(model.Dependencies) != 1 {
		t.Errorf("Dependencies length = %d, want 1 (duplicate not added)", len(model.Dependencies))
	}
}

func TestModel_SetCompiledSQL(t *testing.T) {
	model, _ := NewModel("test_model", "models/test.sql")

	sql := "SELECT * FROM users"
	model.SetCompiledSQL(sql)

	if model.CompiledSQL != sql {
		t.Errorf("CompiledSQL = %v, want %v", model.CompiledSQL, sql)
	}
}

func TestModel_SetMaterializationConfig(t *testing.T) {
	model, _ := NewModel("test_model", "models/test.sql")

	config := materialization.MaterializationConfig{
		Type:      materialization.MaterializationTable,
		UniqueKey: []string{"id"},
	}

	model.SetMaterializationConfig(config)

	if model.MaterializationConfig.Type != materialization.MaterializationTable {
		t.Errorf("Type = %v, want table", model.MaterializationConfig.Type)
	}

	if len(model.MaterializationConfig.UniqueKey) != 1 || model.MaterializationConfig.UniqueKey[0] != "id" {
		t.Errorf("UniqueKey = %v, want [id]", model.MaterializationConfig.UniqueKey)
	}
}

func TestModel_SetMetadata(t *testing.T) {
	model, _ := NewModel("test_model", "models/test.sql")

	model.SetMetadata("tags", []string{"daily", "fact"})
	model.SetMetadata("owner", "data-team")

	if tags, ok := model.Metadata["tags"].([]string); !ok || len(tags) != 2 {
		t.Errorf("Metadata tags = %v, want [daily, fact]", model.Metadata["tags"])
	}

	if owner := model.Metadata["owner"]; owner != "data-team" {
		t.Errorf("Metadata owner = %v, want data-team", owner)
	}
}
