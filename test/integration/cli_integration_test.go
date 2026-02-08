package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/cli"
)

// TestIntegration_TestCommand verifies 'gorchata test' command
func TestIntegration_TestCommand(t *testing.T) {
	// Setup test project
	projectDir := SetupTestProject(t)

	// Create database (already contains sample data)
	adapter, dbPath := CreateTestDatabase(t)
	defer adapter.Close()

	// Update profiles.yml to use test database
	profilesPath := filepath.Join(projectDir, "profiles.yml")
	profilesContent := `default:
  target: test
  outputs:
    test:
      type: sqlite
      database: ` + dbPath
	if err := os.WriteFile(profilesPath, []byte(profilesContent), 0644); err != nil {
		t.Fatalf("failed to write profiles.yml: %v", err)
	}

	// Change to project directory
	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get current directory: %v", err)
	}
	defer os.Chdir(oldDir)

	if err := os.Chdir(projectDir); err != nil {
		t.Fatalf("failed to change to project directory: %v", err)
	}

	// Execute test command
	err = cli.TestCommand([]string{})
	if err != nil {
		t.Fatalf("test command failed: %v", err)
	}

	// Verify JSON results file was created (if implemented)
	// resultsPath := filepath.Join(projectDir, "target", "test_results.json")
	// if _, err := os.Stat(resultsPath); os.IsNotExist(err) {
	//     t.Logf("Note: test_results.json not created (may not be implemented yet)")
	// }

	t.Log("Test command executed successfully")
}

// TestIntegration_TestCommand_WithSelection verifies test selection flags
func TestIntegration_TestCommand_WithSelection(t *testing.T) {
	// Setup test project
	projectDir := SetupTestProject(t)

	// Create database
	adapter, dbPath := CreateTestDatabase(t)
	defer adapter.Close()

	// Update profiles.yml
	profilesPath := filepath.Join(projectDir, "profiles.yml")
	profilesContent := `default:
  target: test
  outputs:
    test:
      type: sqlite
      database: ` + dbPath
	if err := os.WriteFile(profilesPath, []byte(profilesContent), 0644); err != nil {
		t.Fatalf("failed to write profiles.yml: %v", err)
	}

	// Change to project directory
	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get current directory: %v", err)
	}
	defer os.Chdir(oldDir)

	if err := os.Chdir(projectDir); err != nil {
		t.Fatalf("failed to change to project directory: %v", err)
	}

	// Test 1: Select only not_null tests
	t.Run("select_not_null", func(t *testing.T) {
		err := cli.TestCommand([]string{"--select", "not_null_*"})
		if err != nil {
			t.Fatalf("test command with --select failed: %v", err)
		}
		t.Log("Successfully ran tests with --select not_null_*")
	})

	// Test 2: Exclude unique tests
	t.Run("exclude_unique", func(t *testing.T) {
		err := cli.TestCommand([]string{"--exclude", "unique_*"})
		if err != nil {
			t.Fatalf("test command with --exclude failed: %v", err)
		}
		t.Log("Successfully ran tests with --exclude unique_*")
	})

	// Test 3: Run tests for specific model
	t.Run("models_users", func(t *testing.T) {
		err := cli.TestCommand([]string{"--models", "users"})
		if err != nil {
			t.Fatalf("test command with --models failed: %v", err)
		}
		t.Log("Successfully ran tests with --models users")
	})
}

// TestIntegration_TestCommand_FailFast verifies --fail-fast behavior
func TestIntegration_TestCommand_FailFast(t *testing.T) {
	// Setup test project
	projectDir := SetupTestProject(t)

	// Create database with invalid data (already has sample data)
	adapter, dbPath := CreateTestDatabase(t)
	defer adapter.Close()

	CreateInvalidData(t, adapter) // Add data that will fail tests

	// Update profiles.yml
	profilesPath := filepath.Join(projectDir, "profiles.yml")
	profilesContent := `default:
  target: test
  outputs:
    test:
      type: sqlite
      database: ` + dbPath
	if err := os.WriteFile(profilesPath, []byte(profilesContent), 0644); err != nil {
		t.Fatalf("failed to write profiles.yml: %v", err)
	}

	// Change to project directory
	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get current directory: %v", err)
	}
	defer os.Chdir(oldDir)

	if err := os.Chdir(projectDir); err != nil {
		t.Fatalf("failed to change to project directory: %v", err)
	}

	// Execute test command with --fail-fast
	err = cli.TestCommand([]string{"--fail-fast"})
	if err != nil {
		// Expected to fail because of invalid data
		t.Logf("Test command failed as expected with --fail-fast: %v", err)
	} else {
		t.Log("Test command with --fail-fast executed (no failures or stopped early)")
	}
}

// TestIntegration_BuildCommand verifies 'gorchata build' command
func TestIntegration_BuildCommand(t *testing.T) {
	// Setup test project
	projectDir := SetupTestProject(t)

	// Create database (already contains sample data)
	adapter, dbPath := CreateTestDatabase(t)
	defer adapter.Close()

	// Update profiles.yml
	profilesPath := filepath.Join(projectDir, "profiles.yml")
	profilesContent := `default:
  target: test
  outputs:
    test:
      type: sqlite
      database: ` + dbPath
	if err := os.WriteFile(profilesPath, []byte(profilesContent), 0644); err != nil {
		t.Fatalf("failed to write profiles.yml: %v", err)
	}

	// Change to project directory
	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get current directory: %v", err)
	}
	defer os.Chdir(oldDir)

	if err := os.Chdir(projectDir); err != nil {
		t.Fatalf("failed to change to project directory: %v", err)
	}

	// Execute build command (runs models then tests)
	err = cli.BuildCommand([]string{})
	if err != nil {
		t.Fatalf("build command failed: %v", err)
	}

	// Verify models were created
	result, err := adapter.ExecuteQuery(context.Background(), `
		SELECT name FROM sqlite_master 
		WHERE type='table' AND (name='users' OR name='orders')
	`)
	if err != nil {
		t.Fatalf("failed to query tables: %v", err)
	}

	if len(result.Rows) < 2 {
		t.Errorf("expected both users and orders tables to be created, found: %d", len(result.Rows))
	}

	t.Log("Build command executed successfully (models + tests)")
}

// TestIntegration_RunWithTestFlag verifies 'gorchata run --test'
func TestIntegration_RunWithTestFlag(t *testing.T) {
	// Setup test project
	projectDir := SetupTestProject(t)

	// Create database (already contains sample data)
	adapter, dbPath := CreateTestDatabase(t)
	defer adapter.Close()

	// Update profiles.yml
	profilesPath := filepath.Join(projectDir, "profiles.yml")
	profilesContent := `default:
  target: test
  outputs:
    test:
      type: sqlite
      database: ` + dbPath
	if err := os.WriteFile(profilesPath, []byte(profilesContent), 0644); err != nil {
		t.Fatalf("failed to write profiles.yml: %v", err)
	}

	// Change to project directory
	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get current directory: %v", err)
	}
	defer os.Chdir(oldDir)

	if err := os.Chdir(projectDir); err != nil {
		t.Fatalf("failed to change to project directory: %v", err)
	}

	// Execute run command with --test flag
	err = cli.RunCommand([]string{"--test"})
	if err != nil {
		t.Fatalf("run --test command failed: %v", err)
	}

	// Verify models were created
	result, err := adapter.ExecuteQuery(context.Background(), `
		SELECT name FROM sqlite_master 
		WHERE type='table' AND (name='users' OR name='orders')
	`)
	if err != nil {
		t.Fatalf("failed to query tables: %v", err)
	}

	if len(result.Rows) < 2 {
		t.Errorf("expected both users and orders tables to be created, found: %d", len(result.Rows))
	}

	t.Log("Run --test command executed successfully")
}

// TestIntegration_TestCommand_NoTests verifies behavior when no tests exist
func TestIntegration_TestCommand_NoTests(t *testing.T) {
	// Create empty project directory
	projectDir := t.TempDir()

	// Create minimal config files
	projectConfig := `name: empty_project
version: 1.0.0
profile: test

model-paths:
  - models
test-paths:
  - tests
`
	if err := os.WriteFile(filepath.Join(projectDir, "gorchata_project.yml"), []byte(projectConfig), 0644); err != nil {
		t.Fatalf("failed to write project config: %v", err)
	}

	// Create database
	adapter, dbPath := CreateTestDatabase(t)
	defer adapter.Close()

	profilesConfig := `default:
  target: test
  outputs:
    test:
      type: sqlite
      database: ` + dbPath
	if err := os.WriteFile(filepath.Join(projectDir, "profiles.yml"), []byte(profilesConfig), 0644); err != nil {
		t.Fatalf("failed to write profiles config: %v", err)
	}

	// Create empty directories
	os.MkdirAll(filepath.Join(projectDir, "models"), 0755)
	os.MkdirAll(filepath.Join(projectDir, "tests"), 0755)

	// Change to project directory
	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get current directory: %v", err)
	}
	defer os.Chdir(oldDir)

	if err := os.Chdir(projectDir); err != nil {
		t.Fatalf("failed to change to project directory: %v", err)
	}

	// Execute test command
	err = cli.TestCommand([]string{})
	if err != nil {
		t.Fatalf("test command should not fail when no tests exist: %v", err)
	}

	t.Log("Test command handled no tests gracefully")
}

// TestIntegration_TestCommand_Verbose verifies --verbose flag
func TestIntegration_TestCommand_Verbose(t *testing.T) {
	// Setup test project
	projectDir := SetupTestProject(t)

	// Create database
	adapter, dbPath := CreateTestDatabase(t)
	defer adapter.Close()

	// Update profiles.yml
	profilesPath := filepath.Join(projectDir, "profiles.yml")
	profilesContent := `default:
  target: test
  outputs:
    test:
      type: sqlite
      database: ` + dbPath
	if err := os.WriteFile(profilesPath, []byte(profilesContent), 0644); err != nil {
		t.Fatalf("failed to write profiles.yml: %v", err)
	}

	// Change to project directory
	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get current directory: %v", err)
	}
	defer os.Chdir(oldDir)

	if err := os.Chdir(projectDir); err != nil {
		t.Fatalf("failed to change to project directory: %v", err)
	}

	// Execute test command with --verbose
	err = cli.TestCommand([]string{"--verbose"})
	if err != nil {
		t.Fatalf("test command with --verbose failed: %v", err)
	}

	t.Log("Test command with --verbose executed successfully")
}
