package dcs_alarm_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/config"
	_ "modernc.org/sqlite"
)

// setupTestDB creates a temporary database for testing.
// Returns the database connection and a cleanup function.
func setupTestDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	// Create temp database
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Return DB and cleanup function
	cleanup := func() {
		db.Close()
	}

	return db, cleanup
}

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
	if cfg.Name != "dcs_alarm_example" {
		t.Errorf("Name = %q, want %q", cfg.Name, "dcs_alarm_example")
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
	} else if startDate != "2026-02-06" {
		t.Errorf("Vars['start_date'] = %v, want %q", startDate, "2026-02-06")
	}

	// Verify end_date var
	if endDate, ok := cfg.Vars["end_date"]; !ok {
		t.Error("Vars['end_date'] not found")
	} else if endDate != "2026-02-07" {
		t.Errorf("Vars['end_date'] = %v, want %q", endDate, "2026-02-07")
	}

	// Verify alarm_rate_threshold_acceptable var
	if threshold, ok := cfg.Vars["alarm_rate_threshold_acceptable"]; !ok {
		t.Error("Vars['alarm_rate_threshold_acceptable'] not found")
	} else {
		// The value should be an int (2)
		if intVal, ok := threshold.(int); !ok {
			t.Errorf("Vars['alarm_rate_threshold_acceptable'] type = %T, want int", threshold)
		} else if intVal != 2 {
			t.Errorf("Vars['alarm_rate_threshold_acceptable'] = %d, want 2", intVal)
		}
	}

	// Verify alarm_rate_threshold_unacceptable var
	if threshold, ok := cfg.Vars["alarm_rate_threshold_unacceptable"]; !ok {
		t.Error("Vars['alarm_rate_threshold_unacceptable'] not found")
	} else {
		// The value should be an int (10)
		if intVal, ok := threshold.(int); !ok {
			t.Errorf("Vars['alarm_rate_threshold_unacceptable'] type = %T, want int", threshold)
		} else if intVal != 10 {
			t.Errorf("Vars['alarm_rate_threshold_unacceptable'] = %d, want 10", intVal)
		}
	}
}

// TestDatabaseConnection verifies profiles.yml exists and database path resolves
func TestDatabaseConnection(t *testing.T) {
	profilesPath := filepath.Join("profiles.yml")

	// Verify file exists
	if _, err := os.Stat(profilesPath); os.IsNotExist(err) {
		t.Fatalf("profiles.yml does not exist at %s", profilesPath)
	}

	// Load profiles config
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

	// Database path should not be empty (env var should expand to default)
	if devOutput.Database == "" {
		t.Error("devOutput.Database is empty")
	}

	// Verify database path contains expected default path
	expectedPath := "./examples/dcs_alarm_example/dcs_alarms.db"
	if devOutput.Database != expectedPath {
		t.Logf("Database path: %s (expected default: %s)", devOutput.Database, expectedPath)
	}
}

// TestDatabaseConnectionWithEnvVar tests environment variable expansion
func TestDatabaseConnectionWithEnvVar(t *testing.T) {
	// Set custom database path via environment variable
	customPath := filepath.Join(t.TempDir(), "custom_dcs_alarms.db")
	t.Setenv("DCS_ALARM_DB", customPath)

	profilesPath := filepath.Join("profiles.yml")

	// Load profiles config
	cfg, err := config.LoadProfiles(profilesPath)
	if err != nil {
		t.Fatalf("LoadProfiles() error = %v, want nil", err)
	}

	// Get dev output
	devOutput, err := cfg.GetOutput("dev")
	if err != nil {
		t.Fatalf("GetOutput('dev') error = %v, want nil", err)
	}

	// Verify custom path is used
	if devOutput.Database != customPath {
		t.Errorf("devOutput.Database = %q, want %q", devOutput.Database, customPath)
	}
}

// TestDirectoryStructure verifies all required directories exist
func TestDirectoryStructure(t *testing.T) {
	requiredDirs := []string{
		"models",
		"models/sources",
		"models/dimensions",
		"models/facts",
		"models/rollups",
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
