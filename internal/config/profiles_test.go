package config

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLoadProfiles tests loading a valid profiles YAML file
func TestLoadProfiles(t *testing.T) {
	path := filepath.Join("..", "..", "test", "fixtures", "configs", "valid_profiles.yml")

	cfg, err := LoadProfiles(path)
	if err != nil {
		t.Fatalf("LoadProfiles() error = %v, want nil", err)
	}

	// Check default profile exists
	if cfg.Default == nil {
		t.Fatal("Default profile is nil")
	}

	if cfg.Default.Target != "dev" {
		t.Errorf("Default.Target = %q, want %q", cfg.Default.Target, "dev")
	}

	// Check outputs
	if cfg.Default.Outputs == nil {
		t.Fatal("Default.Outputs is nil")
	}

	if len(cfg.Default.Outputs) != 2 {
		t.Errorf("len(Default.Outputs) = %d, want 2", len(cfg.Default.Outputs))
	}

	// Check dev output
	devOutput, ok := cfg.Default.Outputs["dev"]
	if !ok {
		t.Fatal("dev output not found")
	}

	if devOutput.Type != "sqlite" {
		t.Errorf("dev output Type = %q, want %q", devOutput.Type, "sqlite")
	}

	if devOutput.Database != "./gorchata_dev.db" {
		t.Errorf("dev output Database = %q, want %q", devOutput.Database, "./gorchata_dev.db")
	}

	// Check prod output
	prodOutput, ok := cfg.Default.Outputs["prod"]
	if !ok {
		t.Fatal("prod output not found")
	}

	if prodOutput.Database != "/data/prod.db" {
		t.Errorf("prod output Database = %q, want %q", prodOutput.Database, "/data/prod.db")
	}
}

// TestLoadProfilesEnvVar tests environment variable expansion
func TestLoadProfilesEnvVar(t *testing.T) {
	// Set environment variable for test
	testDBPath := "./test_from_env.db"
	os.Setenv("GORCHATA_DB_PATH", testDBPath)
	defer os.Unsetenv("GORCHATA_DB_PATH")

	path := filepath.Join("..", "..", "test", "fixtures", "configs", "profiles_with_envvars.yml")

	cfg, err := LoadProfiles(path)
	if err != nil {
		t.Fatalf("LoadProfiles() error = %v, want nil", err)
	}

	// Check dev output has expanded env var
	devOutput, ok := cfg.Default.Outputs["dev"]
	if !ok {
		t.Fatal("dev output not found")
	}

	if devOutput.Database != testDBPath {
		t.Errorf("dev output Database = %q, want %q (from env var)", devOutput.Database, testDBPath)
	}
}

// TestLoadProfilesEnvVarWithDefault tests ${VAR:default} syntax
func TestLoadProfilesEnvVarWithDefault(t *testing.T) {
	// Set required env var but not the optional ones with defaults
	os.Setenv("GORCHATA_DB_PATH", "./dev.db")
	defer os.Unsetenv("GORCHATA_DB_PATH")

	// Ensure optional env vars are NOT set for default testing
	os.Unsetenv("GORCHATA_STAGING_DB")
	os.Unsetenv("GORCHATA_PROD_DB")

	path := filepath.Join("..", "..", "test", "fixtures", "configs", "profiles_with_envvars.yml")

	cfg, err := LoadProfiles(path)
	if err != nil {
		t.Fatalf("LoadProfiles() error = %v, want nil", err)
	}

	// Check staging output uses default value
	stagingOutput, ok := cfg.Default.Outputs["staging"]
	if !ok {
		t.Fatal("staging output not found")
	}

	expectedStaging := "./staging.db"
	if stagingOutput.Database != expectedStaging {
		t.Errorf("staging output Database = %q, want %q (default)", stagingOutput.Database, expectedStaging)
	}

	// Check prod output uses default value
	prodOutput, ok := cfg.Default.Outputs["prod"]
	if !ok {
		t.Fatal("prod output not found")
	}

	expectedProd := "/data/prod.db"
	if prodOutput.Database != expectedProd {
		t.Errorf("prod output Database = %q, want %q (default)", prodOutput.Database, expectedProd)
	}

	// Now test with env var set
	os.Setenv("GORCHATA_STAGING_DB", "/custom/staging.db")
	defer os.Unsetenv("GORCHATA_STAGING_DB")

	cfg2, err := LoadProfiles(path)
	if err != nil {
		t.Fatalf("LoadProfiles() error = %v, want nil", err)
	}

	stagingOutput2, ok := cfg2.Default.Outputs["staging"]
	if !ok {
		t.Fatal("staging output not found in second load")
	}

	if stagingOutput2.Database != "/custom/staging.db" {
		t.Errorf("staging output Database = %q, want %q (from env)", stagingOutput2.Database, "/custom/staging.db")
	}
}

// TestLoadProfilesEnvVarRequired tests that missing required env vars cause errors
func TestLoadProfilesEnvVarRequired(t *testing.T) {
	// Ensure variable is NOT set
	os.Unsetenv("GORCHATA_DB_PATH")

	path := filepath.Join("..", "..", "test", "fixtures", "configs", "profiles_with_envvars.yml")

	cfg, err := LoadProfiles(path)
	if err == nil {
		t.Error("LoadProfiles() error = nil, want error for missing required env var")
	}

	if cfg != nil {
		t.Error("LoadProfiles() returned non-nil config when required env var is missing")
	}
}

// TestLoadProfilesInvalid tests error handling for invalid YAML
func TestLoadProfilesInvalid(t *testing.T) {
	path := filepath.Join("..", "..", "test", "fixtures", "configs", "invalid_profiles.yml")

	cfg, err := LoadProfiles(path)
	if err == nil {
		t.Error("LoadProfiles() error = nil, want error for invalid profiles")
	}

	if cfg != nil {
		t.Error("LoadProfiles() returned non-nil config for invalid profiles")
	}
}

// TestLoadProfilesMissing tests error handling when file doesn't exist
func TestLoadProfilesMissing(t *testing.T) {
	path := filepath.Join("..", "..", "test", "fixtures", "configs", "nonexistent_profiles.yml")

	cfg, err := LoadProfiles(path)
	if err == nil {
		t.Error("LoadProfiles() error = nil, want error for missing file")
	}

	if cfg != nil {
		t.Error("LoadProfiles() returned non-nil config when file is missing")
	}

	if !os.IsNotExist(err) {
		t.Errorf("LoadProfiles() error = %v, want os.IsNotExist error", err)
	}
}

// TestProfileTargetSelection tests selecting the correct target output
func TestProfileTargetSelection(t *testing.T) {
	path := filepath.Join("..", "..", "test", "fixtures", "configs", "valid_profiles.yml")

	cfg, err := LoadProfiles(path)
	if err != nil {
		t.Fatalf("LoadProfiles() error = %v, want nil", err)
	}

	tests := []struct {
		name       string
		target     string
		wantErr    bool
		wantDBPath string
	}{
		{
			name:       "select dev target",
			target:     "dev",
			wantErr:    false,
			wantDBPath: "./gorchata_dev.db",
		},
		{
			name:       "select prod target",
			target:     "prod",
			wantErr:    false,
			wantDBPath: "/data/prod.db",
		},
		{
			name:    "select nonexistent target",
			target:  "nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output, err := cfg.GetOutput(tt.target)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetOutput(%q) error = %v, wantErr %v", tt.target, err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if output == nil {
					t.Fatalf("GetOutput(%q) returned nil output", tt.target)
				}
				if output.Database != tt.wantDBPath {
					t.Errorf("GetOutput(%q) Database = %q, want %q", tt.target, output.Database, tt.wantDBPath)
				}
			}
		})
	}
}

// TestOutputValidation tests validation of output configuration
func TestOutputValidation(t *testing.T) {
	tests := []struct {
		name    string
		output  *OutputConfig
		wantErr bool
	}{
		{
			name: "valid sqlite output",
			output: &OutputConfig{
				Type:     "sqlite",
				Database: "./test.db",
			},
			wantErr: false,
		},
		{
			name: "missing type",
			output: &OutputConfig{
				Database: "./test.db",
			},
			wantErr: true,
		},
		{
			name: "missing database for sqlite",
			output: &OutputConfig{
				Type: "sqlite",
			},
			wantErr: true,
		},
		{
			name: "empty type",
			output: &OutputConfig{
				Type:     "",
				Database: "./test.db",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.output.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
