package config

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLoadConfig tests loading complete configuration (project + profiles)
func TestLoadConfig(t *testing.T) {
	projectPath := filepath.Join("..", "..", "test", "fixtures", "configs", "valid_project.yml")
	profilesPath := filepath.Join("..", "..", "test", "fixtures", "configs", "valid_profiles.yml")

	cfg, err := Load(projectPath, profilesPath, "dev")
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}

	// Verify project config loaded
	if cfg.Project == nil {
		t.Fatal("Project is nil")
	}

	if cfg.Project.Name != "my_project" {
		t.Errorf("Project.Name = %q, want %q", cfg.Project.Name, "my_project")
	}

	// Verify profiles config loaded
	if cfg.Profiles == nil {
		t.Fatal("Profiles is nil")
	}

	// Verify target output selected
	if cfg.Output == nil {
		t.Fatal("Output is nil")
	}

	if cfg.Output.Database != "./gorchata_dev.db" {
		t.Errorf("Output.Database = %q, want %q", cfg.Output.Database, "./gorchata_dev.db")
	}
}

// TestLoadConfigWithTarget tests loading with specific target
func TestLoadConfigWithTarget(t *testing.T) {
	projectPath := filepath.Join("..", "..", "test", "fixtures", "configs", "valid_project.yml")
	profilesPath := filepath.Join("..", "..", "test", "fixtures", "configs", "valid_profiles.yml")

	tests := []struct {
		name    string
		target  string
		wantDB  string
		wantErr bool
	}{
		{
			name:    "dev target",
			target:  "dev",
			wantDB:  "./gorchata_dev.db",
			wantErr: false,
		},
		{
			name:    "prod target",
			target:  "prod",
			wantDB:  "/data/prod.db",
			wantErr: false,
		},
		{
			name:    "nonexistent target",
			target:  "nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := Load(projectPath, profilesPath, tt.target)
			if (err != nil) != tt.wantErr {
				t.Errorf("Load() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if cfg.Output == nil {
					t.Fatal("Output is nil")
				}
				if cfg.Output.Database != tt.wantDB {
					t.Errorf("Output.Database = %q, want %q", cfg.Output.Database, tt.wantDB)
				}
			}
		})
	}
}

// TestLoadConfigMissingProject tests error when project.yml is missing
func TestLoadConfigMissingProject(t *testing.T) {
	projectPath := filepath.Join("..", "..", "test", "fixtures", "configs", "nonexistent.yml")
	profilesPath := filepath.Join("..", "..", "test", "fixtures", "configs", "valid_profiles.yml")

	cfg, err := Load(projectPath, profilesPath, "dev")
	if err == nil {
		t.Error("Load() error = nil, want error for missing project file")
	}

	if cfg != nil {
		t.Error("Load() returned non-nil config when project file is missing")
	}

	if !os.IsNotExist(err) {
		t.Errorf("Load() error = %v, want os.IsNotExist error", err)
	}
}

// TestLoadConfigMissingProfiles tests error when profiles.yml is missing
func TestLoadConfigMissingProfiles(t *testing.T) {
	projectPath := filepath.Join("..", "..", "test", "fixtures", "configs", "valid_project.yml")
	profilesPath := filepath.Join("..", "..", "test", "fixtures", "configs", "nonexistent.yml")

	cfg, err := Load(projectPath, profilesPath, "dev")
	if err == nil {
		t.Error("Load() error = nil, want error for missing profiles file")
	}

	if cfg != nil {
		t.Error("Load() returned non-nil config when profiles file is missing")
	}

	if !os.IsNotExist(err) {
		t.Errorf("Load() error = %v, want os.IsNotExist error", err)
	}
}

// TestLoadConfigFileDiscovery tests finding config files in current/parent directories
func TestLoadConfigFileDiscovery(t *testing.T) {
	// Create a temporary directory structure for testing
	tmpDir := t.TempDir()

	// Create project root with config files
	projectYml := filepath.Join(tmpDir, "gorchata_project.yml")
	profilesYml := filepath.Join(tmpDir, "profiles.yml")

	// Write minimal valid configs
	projectContent := "name: test_project\nversion: 1.0.0\n"
	if err := os.WriteFile(projectYml, []byte(projectContent), 0644); err != nil {
		t.Fatalf("Failed to write test project.yml: %v", err)
	}

	profilesContent := `default:
  target: dev
  outputs:
    dev:
      type: sqlite
      database: ./test.db
`
	if err := os.WriteFile(profilesYml, []byte(profilesContent), 0644); err != nil {
		t.Fatalf("Failed to write test profiles.yml: %v", err)
	}

	// Create a subdirectory
	subDir := filepath.Join(tmpDir, "subdir")
	if err := os.Mkdir(subDir, 0755); err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}

	// Test discovery from subdirectory
	oldWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working dir: %v", err)
	}
	defer os.Chdir(oldWd)

	if err := os.Chdir(subDir); err != nil {
		t.Fatalf("Failed to change to subdir: %v", err)
	}

	cfg, err := Discover("dev")
	if err != nil {
		t.Fatalf("Discover() error = %v, want nil", err)
	}

	if cfg == nil {
		t.Fatal("Discover() returned nil config")
	}

	if cfg.Project.Name != "test_project" {
		t.Errorf("Project.Name = %q, want %q", cfg.Project.Name, "test_project")
	}
}

// TestLoadConfigDefaultTarget tests using default target from profile
func TestLoadConfigDefaultTarget(t *testing.T) {
	projectPath := filepath.Join("..", "..", "test", "fixtures", "configs", "valid_project.yml")
	profilesPath := filepath.Join("..", "..", "test", "fixtures", "configs", "valid_profiles.yml")

	// Load with empty target to use default
	cfg, err := Load(projectPath, profilesPath, "")
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}

	// Should use the default target from profiles (which is "dev")
	if cfg.Output == nil {
		t.Fatal("Output is nil")
	}

	if cfg.Output.Database != "./gorchata_dev.db" {
		t.Errorf("Output.Database = %q, want %q (default target)", cfg.Output.Database, "./gorchata_dev.db")
	}
}
