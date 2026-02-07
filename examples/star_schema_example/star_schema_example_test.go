package star_schema_example_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/config"
)

// TestStarSchemaProjectConfig tests that the star_schema_example project config can be loaded
func TestStarSchemaProjectConfig(t *testing.T) {
	projectPath := filepath.Join("gorchata_project.yml")

	cfg, err := config.LoadProject(projectPath)
	if err != nil {
		t.Fatalf("LoadProject() error = %v, want nil", err)
	}

	// Verify project name
	if cfg.Name != "star_schema_example" {
		t.Errorf("Name = %q, want %q", cfg.Name, "star_schema_example")
	}

	// Verify version
	if cfg.Version != "1.0.0" {
		t.Errorf("Version = %q, want %q", cfg.Version, "1.0.0")
	}

	// Verify profile
	if cfg.Profile != "dev" {
		t.Errorf("Profile = %q, want %q", cfg.Profile, "dev")
	}

	// Verify model paths
	if len(cfg.ModelPaths) != 1 {
		t.Errorf("ModelPaths length = %d, want 1", len(cfg.ModelPaths))
	}
	if len(cfg.ModelPaths) > 0 && cfg.ModelPaths[0] != "models" {
		t.Errorf("ModelPaths[0] = %q, want %q", cfg.ModelPaths[0], "models")
	}

	// Verify vars exist
	if cfg.Vars == nil {
		t.Fatal("Vars is nil, want non-nil")
	}

	// Verify start_date var
	if startDate, ok := cfg.Vars["start_date"]; !ok {
		t.Error("Vars['start_date'] not found")
	} else if startDate != "2024-01-01" {
		t.Errorf("Vars['start_date'] = %v, want %q", startDate, "2024-01-01")
	}

	// Verify end_date var
	if endDate, ok := cfg.Vars["end_date"]; !ok {
		t.Error("Vars['end_date'] not found")
	} else if endDate != "2024-12-31" {
		t.Errorf("Vars['end_date'] = %v, want %q", endDate, "2024-12-31")
	}
}

// TestStarSchemaProfilesConfig tests that the star_schema_example profiles config can be loaded
func TestStarSchemaProfilesConfig(t *testing.T) {
	profilesPath := filepath.Join("profiles.yml")

	cfg, err := config.LoadProfiles(profilesPath)
	if err != nil {
		t.Fatalf("LoadProfiles() error = %v, want nil", err)
	}

	// Verify default profile exists
	if cfg.Default == nil {
		t.Fatal("Default profile is nil")
	}

	// Verify default target
	if cfg.Default.Target != "dev" {
		t.Errorf("Default.Target = %q, want %q", cfg.Default.Target, "dev")
	}

	// Verify dev output exists
	devOutput, err := cfg.GetOutput("dev")
	if err != nil {
		t.Fatalf("GetOutput('dev') error = %v, want nil", err)
	}

	// Verify output type
	if devOutput.Type != "sqlite" {
		t.Errorf("devOutput.Type = %q, want %q", devOutput.Type, "sqlite")
	}

	// Database path should contain the example path
	// Note: env var expansion happens in LoadProfiles, so we'll get the actual value
	if devOutput.Database == "" {
		t.Error("devOutput.Database is empty")
	}
}

// TestStarSchemaProfilesConfigWithEnvVar tests env var expansion
func TestStarSchemaProfilesConfigWithEnvVar(t *testing.T) {
	// Set a custom env var
	customPath := "./custom/path/test.db"
	os.Setenv("STAR_SCHEMA_DB", customPath)
	defer os.Unsetenv("STAR_SCHEMA_DB")

	profilesPath := filepath.Join("profiles.yml")

	cfg, err := config.LoadProfiles(profilesPath)
	if err != nil {
		t.Fatalf("LoadProfiles() error = %v, want nil", err)
	}

	devOutput, err := cfg.GetOutput("dev")
	if err != nil {
		t.Fatalf("GetOutput('dev') error = %v, want nil", err)
	}

	// Should use the env var value
	if devOutput.Database != customPath {
		t.Errorf("devOutput.Database = %q, want %q", devOutput.Database, customPath)
	}
}

// TestStarSchemaProfilesConfigDefaultEnvVar tests default env var value
func TestStarSchemaProfilesConfigDefaultEnvVar(t *testing.T) {
	// Ensure env var is NOT set
	os.Unsetenv("STAR_SCHEMA_DB")

	profilesPath := filepath.Join("profiles.yml")

	cfg, err := config.LoadProfiles(profilesPath)
	if err != nil {
		t.Fatalf("LoadProfiles() error = %v, want nil", err)
	}

	devOutput, err := cfg.GetOutput("dev")
	if err != nil {
		t.Fatalf("GetOutput('dev') error = %v, want nil", err)
	}

	// Should use the default value
	expectedDefault := "./examples/star_schema_example/star_schema.db"
	if devOutput.Database != expectedDefault {
		t.Errorf("devOutput.Database = %q, want %q", devOutput.Database, expectedDefault)
	}
}

// TestStarSchemaDirectoryStructure verifies the required directory structure exists
func TestStarSchemaDirectoryStructure(t *testing.T) {
	requiredDirs := []string{
		"models",
		"models/sources",
		"models/dimensions",
		"models/facts",
		"models/rollups",
	}

	for _, dir := range requiredDirs {
		path := filepath.Join(dir)
		info, err := os.Stat(path)
		if err != nil {
			t.Errorf("Directory %q does not exist: %v", dir, err)
			continue
		}
		if !info.IsDir() {
			t.Errorf("Path %q exists but is not a directory", dir)
		}
	}
}

// TestStarSchemaREADMEExists verifies README.md exists
func TestStarSchemaREADMEExists(t *testing.T) {
	readmePath := filepath.Join("README.md")
	info, err := os.Stat(readmePath)
	if err != nil {
		t.Fatalf("README.md does not exist: %v", err)
	}
	if info.IsDir() {
		t.Fatal("README.md is a directory, expected a file")
	}
	if info.Size() == 0 {
		t.Error("README.md is empty")
	}
}
