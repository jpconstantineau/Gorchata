package config

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLoadProject tests loading a valid project YAML file and verifying its structure
func TestLoadProject(t *testing.T) {
	path := filepath.Join("..", "..", "test", "fixtures", "configs", "valid_project.yml")

	cfg, err := LoadProject(path)
	if err != nil {
		t.Fatalf("LoadProject() error = %v, want nil", err)
	}

	if cfg.Name != "my_project" {
		t.Errorf("Name = %q, want %q", cfg.Name, "my_project")
	}

	if cfg.Version != "1.0.0" {
		t.Errorf("Version = %q, want %q", cfg.Version, "1.0.0")
	}

	if cfg.Profile != "default" {
		t.Errorf("Profile = %q, want %q", cfg.Profile, "default")
	}

	// Check model paths
	expectedPaths := []string{"models", "custom_models"}
	if len(cfg.ModelPaths) != len(expectedPaths) {
		t.Errorf("len(ModelPaths) = %d, want %d", len(cfg.ModelPaths), len(expectedPaths))
	}
	for i, path := range expectedPaths {
		if i < len(cfg.ModelPaths) && cfg.ModelPaths[i] != path {
			t.Errorf("ModelPaths[%d] = %q, want %q", i, cfg.ModelPaths[i], path)
		}
	}

	// Check vars
	if cfg.Vars["start_date"] != "2024-01-01" {
		t.Errorf("Vars[start_date] = %q, want %q", cfg.Vars["start_date"], "2024-01-01")
	}

	if cfg.Vars["environment"] != "dev" {
		t.Errorf("Vars[environment] = %q, want %q", cfg.Vars["environment"], "dev")
	}

	// Check models config
	if cfg.Models == nil {
		t.Fatal("Models is nil, want non-nil map")
	}

	myProject, ok := cfg.Models["my_project"]
	if !ok {
		t.Fatal("Models[my_project] not found")
	}

	materialized, ok := myProject["materialized"].(string)
	if !ok || materialized != "table" {
		t.Errorf("Models[my_project][materialized] = %v, want %q", myProject["materialized"], "table")
	}

	schema, ok := myProject["schema"].(string)
	if !ok || schema != "analytics" {
		t.Errorf("Models[my_project][schema] = %v, want %q", myProject["schema"], "analytics")
	}
}

// TestLoadProjectDefaults verifies that default values are applied when fields are omitted
func TestLoadProjectDefaults(t *testing.T) {
	path := filepath.Join("..", "..", "test", "fixtures", "configs", "minimal_project.yml")

	cfg, err := LoadProject(path)
	if err != nil {
		t.Fatalf("LoadProject() error = %v, want nil", err)
	}

	if cfg.Name != "minimal_project" {
		t.Errorf("Name = %q, want %q", cfg.Name, "minimal_project")
	}

	if cfg.Version != "2.0.0" {
		t.Errorf("Version = %q, want %q", cfg.Version, "2.0.0")
	}

	// Check default paths are applied
	expectedModelPaths := []string{"models"}
	if len(cfg.ModelPaths) != len(expectedModelPaths) {
		t.Errorf("len(ModelPaths) = %d, want %d", len(cfg.ModelPaths), len(expectedModelPaths))
	} else if cfg.ModelPaths[0] != expectedModelPaths[0] {
		t.Errorf("ModelPaths[0] = %q, want %q", cfg.ModelPaths[0], expectedModelPaths[0])
	}

	expectedSeedPaths := []string{"seeds"}
	if len(cfg.SeedPaths) != len(expectedSeedPaths) {
		t.Errorf("len(SeedPaths) = %d, want %d", len(cfg.SeedPaths), len(expectedSeedPaths))
	} else if cfg.SeedPaths[0] != expectedSeedPaths[0] {
		t.Errorf("SeedPaths[0] = %q, want %q", cfg.SeedPaths[0], expectedSeedPaths[0])
	}

	expectedTestPaths := []string{"tests"}
	if len(cfg.TestPaths) != len(expectedTestPaths) {
		t.Errorf("len(TestPaths) = %d, want %d", len(cfg.TestPaths), len(expectedTestPaths))
	} else if cfg.TestPaths[0] != expectedTestPaths[0] {
		t.Errorf("TestPaths[0] = %q, want %q", cfg.TestPaths[0], expectedTestPaths[0])
	}

	expectedMacroPaths := []string{"macros"}
	if len(cfg.MacroPaths) != len(expectedMacroPaths) {
		t.Errorf("len(MacroPaths) = %d, want %d", len(cfg.MacroPaths), len(expectedMacroPaths))
	} else if cfg.MacroPaths[0] != expectedMacroPaths[0] {
		t.Errorf("MacroPaths[0] = %q, want %q", cfg.MacroPaths[0], expectedMacroPaths[0])
	}

	// Profile should default to empty string or be handled gracefully
	// Vars should be initialized as empty map
	if cfg.Vars == nil {
		t.Error("Vars is nil, want empty map")
	}

	// Models should be initialized as empty map
	if cfg.Models == nil {
		t.Error("Models is nil, want empty map")
	}
}

// TestLoadProjectInvalid tests error handling for invalid YAML
func TestLoadProjectInvalid(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		wantErr  bool
	}{
		{
			name:     "missing required field (version)",
			filename: "invalid_project.yml",
			wantErr:  true,
		},
		{
			name:     "malformed YAML",
			filename: "malformed_project.yml",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join("..", "..", "test", "fixtures", "configs", tt.filename)

			cfg, err := LoadProject(path)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadProject() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && cfg == nil {
				t.Error("LoadProject() returned nil config when error is nil")
			}
		})
	}
}

// TestLoadProjectMissing tests error handling when file doesn't exist
func TestLoadProjectMissing(t *testing.T) {
	path := filepath.Join("..", "..", "test", "fixtures", "configs", "nonexistent.yml")

	cfg, err := LoadProject(path)
	if err == nil {
		t.Error("LoadProject() error = nil, want error for missing file")
	}

	if cfg != nil {
		t.Error("LoadProject() returned non-nil config when file is missing")
	}

	// Verify the error is related to file not found
	if !os.IsNotExist(err) {
		t.Errorf("LoadProject() error = %v, want os.IsNotExist error", err)
	}
}

// TestProjectValidation tests validation of required fields
func TestProjectValidation(t *testing.T) {
	tests := []struct {
		name    string
		project *ProjectConfig
		wantErr bool
	}{
		{
			name: "valid project",
			project: &ProjectConfig{
				Name:    "test_project",
				Version: "1.0.0",
			},
			wantErr: false,
		},
		{
			name: "missing name",
			project: &ProjectConfig{
				Version: "1.0.0",
			},
			wantErr: true,
		},
		{
			name: "missing version",
			project: &ProjectConfig{
				Name: "test_project",
			},
			wantErr: true,
		},
		{
			name: "empty name",
			project: &ProjectConfig{
				Name:    "",
				Version: "1.0.0",
			},
			wantErr: true,
		},
		{
			name: "empty version",
			project: &ProjectConfig{
				Name:    "test_project",
				Version: "",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.project.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
