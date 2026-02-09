package bottleneck_analysis_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/config"
	_ "modernc.org/sqlite"
)

// TestProjectConfigExists verifies gorchata_project.yml exists and loads correctly
func TestProjectConfigExists(t *testing.T) {
	projectPath := filepath.Join("gorchata_project.yml")

	// Verify file exists
	if _, err := os.Stat(projectPath); os.IsNotExist(err) {
		t.Fatalf("gorchata_project.yml does not exist at %s", projectPath)
	}

	// Load and validate project config
	cfg, err := config.LoadProject(projectPath)
	if err != nil {
		t.Fatalf("LoadProject() error = %v, want nil", err)
	}

	// Verify project name
	if cfg.Name != "bottleneck_analysis" {
		t.Errorf("Name = %q, want %q", cfg.Name, "bottleneck_analysis")
	}

	// Verify version
	if cfg.Version != "1.0" {
		t.Errorf("Version = %q, want %q", cfg.Version, "1.0")
	}

	// Verify profile
	if cfg.Profile != "dev" {
		t.Errorf("Profile = %q, want %q", cfg.Profile, "dev")
	}

	// Verify model paths
	if len(cfg.ModelPaths) == 0 {
		t.Error("ModelPaths is empty, want at least 1 path")
	}
	if len(cfg.ModelPaths) > 0 && cfg.ModelPaths[0] != "models" {
		t.Errorf("ModelPaths[0] = %q, want %q", cfg.ModelPaths[0], "models")
	}

	// Verify vars exist
	if cfg.Vars == nil {
		t.Fatal("Vars is nil, want non-nil")
	}

	// Verify analysis_start_date var
	if _, ok := cfg.Vars["analysis_start_date"]; !ok {
		t.Error("Vars['analysis_start_date'] not found")
	}

	// Verify analysis_end_date var
	if _, ok := cfg.Vars["analysis_end_date"]; !ok {
		t.Error("Vars['analysis_end_date'] not found")
	}

	// Verify shift_hours var
	if _, ok := cfg.Vars["shift_hours"]; !ok {
		t.Error("Vars['shift_hours'] not found")
	}
}

// TestProfilesConfigExists verifies profiles.yml exists and has valid structure
func TestProfilesConfigExists(t *testing.T) {
	profilesPath := filepath.Join("profiles.yml")

	// Verify file exists
	if _, err := os.Stat(profilesPath); os.IsNotExist(err) {
		t.Fatalf("profiles.yml does not exist at %s", profilesPath)
	}

	// Load profiles config
	profilesCfg, err := config.LoadProfiles(profilesPath)
	if err != nil {
		t.Fatalf("LoadProfiles() error = %v, want nil", err)
	}

	// Verify dev output exists
	output, err := profilesCfg.GetOutput("dev")
	if err != nil {
		t.Fatalf("GetOutput('dev') error = %v, want nil", err)
	}

	// Verify database type is sqlite
	if output.Type != "sqlite" {
		t.Errorf("Type = %q, want %q", output.Type, "sqlite")
	}

	// Verify database path is set
	if output.Database == "" {
		t.Error("Database path is empty, want non-empty")
	}
}

// TestDirectoryStructure verifies all required directories exist
func TestDirectoryStructure(t *testing.T) {
	requiredDirs := []string{
		"seeds",
		"models",
		"models/sources",
		"models/dimensions",
		"models/facts",
		"models/rollups",
		"tests",
		"tests/generic",
		"docs",
	}

	for _, dir := range requiredDirs {
		dirPath := filepath.Join(dir)
		info, err := os.Stat(dirPath)
		if os.IsNotExist(err) {
			t.Errorf("Directory %s does not exist", dirPath)
			continue
		}
		if err != nil {
			t.Errorf("Error checking directory %s: %v", dirPath, err)
			continue
		}
		if !info.IsDir() {
			t.Errorf("%s exists but is not a directory", dirPath)
		}
	}
}

// TestREADMEExists verifies README.md exists
func TestREADMEExists(t *testing.T) {
	readmePath := filepath.Join("README.md")

	// Verify file exists
	if _, err := os.Stat(readmePath); os.IsNotExist(err) {
		t.Fatalf("README.md does not exist at %s", readmePath)
	}

	// Read file to verify it has content
	content, err := os.ReadFile(readmePath)
	if err != nil {
		t.Fatalf("Failed to read README.md: %v", err)
	}

	if len(content) == 0 {
		t.Error("README.md is empty, want non-empty")
	}
}
