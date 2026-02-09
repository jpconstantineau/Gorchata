package cli

import (
	"os"
	"path/filepath"
	"testing"
)

// TestSeedCommand_BasicExecution tests basic seed command execution
func TestSeedCommand_BasicExecution(t *testing.T) {
	// Setup: Create test project structure
	tmpDir := t.TempDir()

	// Create project config
	projectConfig := `name: test_project
version: 1.0.0
profile: dev
seed-paths:
  - seeds
`
	projectPath := filepath.Join(tmpDir, "gorchata_project.yml")
	err := os.WriteFile(projectPath, []byte(projectConfig), 0644)
	if err != nil {
		t.Fatalf("Failed to create project config: %v", err)
	}

	// Create profiles config
	profilesConfig := `default:
  target: dev
  outputs:
    dev:
      type: sqlite
      database: ":memory:"
`
	profilesPath := filepath.Join(tmpDir, "profiles.yml")
	err = os.WriteFile(profilesPath, []byte(profilesConfig), 0644)
	if err != nil {
		t.Fatalf("Failed to create profiles config: %v", err)
	}

	// Create seeds directory with CSV file
	seedsDir := filepath.Join(tmpDir, "seeds")
	err = os.Mkdir(seedsDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create seeds directory: %v", err)
	}

	csvFile := filepath.Join(seedsDir, "customers.csv")
	err = os.WriteFile(csvFile, []byte("id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com"), 0644)
	if err != nil {
		t.Fatalf("Failed to create CSV file: %v", err)
	}

	// Change to project directory
	originalDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current directory: %v", err)
	}
	defer os.Chdir(originalDir)

	err = os.Chdir(tmpDir)
	if err != nil {
		t.Fatalf("Failed to change directory: %v", err)
	}

	// Act
	err = SeedCommand([]string{})

	// Assert
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
}

// TestSeedCommand_SelectFlag tests --select flag
func TestSeedCommand_SelectFlag(t *testing.T) {
	// Setup: Create test project structure
	tmpDir := t.TempDir()

	// Create project config
	projectConfig := `name: test_project
version: 1.0.0
profile: dev
seed-paths:
  - seeds
`
	projectPath := filepath.Join(tmpDir, "gorchata_project.yml")
	err := os.WriteFile(projectPath, []byte(projectConfig), 0644)
	if err != nil {
		t.Fatalf("Failed to create project config: %v", err)
	}

	// Create profiles config
	profilesConfig := `default:
  target: dev
  outputs:
    dev:
      type: sqlite
      database: ":memory:"
`
	profilesPath := filepath.Join(tmpDir, "profiles.yml")
	err = os.WriteFile(profilesPath, []byte(profilesConfig), 0644)
	if err != nil {
		t.Fatalf("Failed to create profiles config: %v", err)
	}

	// Create seeds directory with multiple CSV files
	seedsDir := filepath.Join(tmpDir, "seeds")
	err = os.Mkdir(seedsDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create seeds directory: %v", err)
	}

	// Create customers.csv
	customersFile := filepath.Join(seedsDir, "customers.csv")
	err = os.WriteFile(customersFile, []byte("id,name\n1,Alice\n2,Bob"), 0644)
	if err != nil {
		t.Fatalf("Failed to create CSV file: %v", err)
	}

	// Create orders.csv
	ordersFile := filepath.Join(seedsDir, "orders.csv")
	err = os.WriteFile(ordersFile, []byte("id,total\n1,100\n2,200"), 0644)
	if err != nil {
		t.Fatalf("Failed to create CSV file: %v", err)
	}

	// Change to project directory
	originalDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current directory: %v", err)
	}
	defer os.Chdir(originalDir)

	err = os.Chdir(tmpDir)
	if err != nil {
		t.Fatalf("Failed to change directory: %v", err)
	}

	// Act
	err = SeedCommand([]string{"--select", "customers"})

	// Assert
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	// Note: More detailed verification would check that only customers was executed
}

// TestSeedCommand_NoSeedsFound tests handling of empty seed directories
func TestSeedCommand_NoSeedsFound(t *testing.T) {
	// Setup: Create test project structure with no seeds
	tmpDir := t.TempDir()

	// Create project config
	projectConfig := `name: test_project
version: 1.0.0
profile: dev
seed-paths:
  - seeds
`
	projectPath := filepath.Join(tmpDir, "gorchata_project.yml")
	err := os.WriteFile(projectPath, []byte(projectConfig), 0644)
	if err != nil {
		t.Fatalf("Failed to create project config: %v", err)
	}

	// Create profiles config
	profilesConfig := `default:
  target: dev
  outputs:
    dev:
      type: sqlite
      database: ":memory:"
`
	profilesPath := filepath.Join(tmpDir, "profiles.yml")
	err = os.WriteFile(profilesPath, []byte(profilesConfig), 0644)
	if err != nil {
		t.Fatalf("Failed to create profiles config: %v", err)
	}

	// Create empty seeds directory
	seedsDir := filepath.Join(tmpDir, "seeds")
	err = os.Mkdir(seedsDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create seeds directory: %v", err)
	}

	// Change to project directory
	originalDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current directory: %v", err)
	}
	defer os.Chdir(originalDir)

	err = os.Chdir(tmpDir)
	if err != nil {
		t.Fatalf("Failed to change directory: %v", err)
	}

	// Act
	err = SeedCommand([]string{})

	// Assert - should not error, but should report no seeds found
	if err != nil {
		t.Fatalf("Expected no error for empty seeds directory, got: %v", err)
	}
}

// TestSeedCommand_InvalidFlags tests error handling for invalid flags
func TestSeedCommand_InvalidFlags(t *testing.T) {
	// Act
	err := SeedCommand([]string{"--invalid-flag"})

	// Assert
	if err == nil {
		t.Fatal("Expected error for invalid flag, got nil")
	}
}

// TestSeedCommand_MissingConfig tests error handling for missing project config
func TestSeedCommand_MissingConfig(t *testing.T) {
	// Setup: Create temp directory with no config files
	tmpDir := t.TempDir()

	// Change to directory without config
	originalDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current directory: %v", err)
	}
	defer os.Chdir(originalDir)

	err = os.Chdir(tmpDir)
	if err != nil {
		t.Fatalf("Failed to change directory: %v", err)
	}

	// Act
	err = SeedCommand([]string{})

	// Assert
	if err == nil {
		t.Fatal("Expected error for missing config, got nil")
	}
}
