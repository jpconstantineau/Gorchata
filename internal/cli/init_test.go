package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestInitCommand_RequiresProjectName verifies that the init command returns an error when no project name is provided
func TestInitCommand_RequiresProjectName(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "no arguments",
			args: []string{},
		},
		{
			name: "only flags",
			args: []string{"--force"},
		},
		{
			name: "only empty flag",
			args: []string{"--empty"},
		},
		{
			name: "multiple flags no project name",
			args: []string{"--force", "--empty"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := InitCommand(tt.args)
			if err == nil {
				t.Error("expected error when no project name provided, got nil")
			}
			if err != nil && !strings.Contains(err.Error(), "project name") {
				t.Errorf("expected error message to mention 'project name', got: %v", err)
			}
		})
	}
}

// TestInitCommand_ValidatesProjectName verifies that project names are validated correctly
func TestInitCommand_ValidatesProjectName(t *testing.T) {
	tests := []struct {
		name        string
		projectName string
		shouldError bool
	}{
		{
			name:        "valid alphanumeric",
			projectName: "myproject",
			shouldError: false,
		},
		{
			name:        "valid with underscores",
			projectName: "my_project",
			shouldError: false,
		},
		{
			name:        "valid with hyphens",
			projectName: "my-project",
			shouldError: false,
		},
		{
			name:        "valid with numbers",
			projectName: "project123",
			shouldError: false,
		},
		{
			name:        "valid mixed",
			projectName: "my_project-123",
			shouldError: false,
		},
		{
			name:        "invalid with spaces",
			projectName: "my project",
			shouldError: true,
		},
		{
			name:        "invalid with special characters",
			projectName: "my@project",
			shouldError: true,
		},
		{
			name:        "invalid with dots",
			projectName: "my.project",
			shouldError: true,
		},
		{
			name:        "invalid with slashes",
			projectName: "my/project",
			shouldError: true,
		},
		{
			name:        "empty string",
			projectName: "",
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := []string{tt.projectName}
			err := InitCommand(args)

			if tt.shouldError {
				if err == nil {
					t.Errorf("expected error for project name %q, got nil", tt.projectName)
				}
			} else {
				// For now, we expect a "not yet implemented" error or similar
				// since we're just testing validation logic at this stage
				// The actual implementation will create directories, which we'll test later
				if err != nil && !strings.Contains(err.Error(), "not yet implemented") {
					// If it's not a "not yet implemented" error, it should not be a validation error
					if strings.Contains(err.Error(), "invalid") || strings.Contains(err.Error(), "project name") {
						t.Errorf("expected valid project name %q to pass validation, got error: %v", tt.projectName, err)
					}
				}
			}
		})
	}
}

// TestInitCommand_HelpFlag verifies that --help flag shows help output
func TestInitCommand_HelpFlag(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "help flag long form",
			args: []string{"--help"},
		},
		{
			name: "help flag short form",
			args: []string{"-h"},
		},
		{
			name: "help flag with project name",
			args: []string{"--help", "myproject"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// When help is requested, the command should return nil (success)
			// and print help information to stdout
			err := InitCommand(tt.args)
			if err != nil {
				t.Errorf("expected no error for help flag, got: %v", err)
			}
		})
	}
}

// TestCreateProjectDirectories_Success verifies that directories are created correctly
func TestCreateProjectDirectories_Success(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	projectPath := filepath.Join(tempDir, "test_project")

	// Call the function to create directories
	err := createProjectDirectories(projectPath, false)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Verify project root directory exists
	if _, err := os.Stat(projectPath); os.IsNotExist(err) {
		t.Errorf("expected project directory to exist at %s", projectPath)
	}

	// Verify subdirectories exist
	expectedDirs := []string{"models", "seeds", "tests", "macros"}
	for _, dir := range expectedDirs {
		dirPath := filepath.Join(projectPath, dir)
		if _, err := os.Stat(dirPath); os.IsNotExist(err) {
			t.Errorf("expected subdirectory %s to exist at %s", dir, dirPath)
		}
	}
}

// TestCreateProjectDirectories_AlreadyExists verifies error when directory exists without --force
func TestCreateProjectDirectories_AlreadyExists(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	projectPath := filepath.Join(tempDir, "existing_project")

	// Create the directory first
	err := os.MkdirAll(projectPath, 0755)
	if err != nil {
		t.Fatalf("failed to create test directory: %v", err)
	}

	// Try to create directories without force flag
	err = createProjectDirectories(projectPath, false)
	if err == nil {
		t.Error("expected error when directory exists without --force, got nil")
	}

	// Verify error message mentions the directory exists
	if !strings.Contains(err.Error(), "already exists") && !strings.Contains(err.Error(), "exists") {
		t.Errorf("expected error message to mention directory exists, got: %v", err)
	}
}

// TestCreateProjectDirectories_ForceOverwrite verifies --force removes and recreates directory
func TestCreateProjectDirectories_ForceOverwrite(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	projectPath := filepath.Join(tempDir, "force_project")

	// Create the directory first with a marker file
	err := os.MkdirAll(projectPath, 0755)
	if err != nil {
		t.Fatalf("failed to create test directory: %v", err)
	}

	markerFile := filepath.Join(projectPath, "marker.txt")
	err = os.WriteFile(markerFile, []byte("old content"), 0644)
	if err != nil {
		t.Fatalf("failed to create marker file: %v", err)
	}

	// Try to create directories with force flag
	err = createProjectDirectories(projectPath, true)
	if err != nil {
		t.Fatalf("expected no error with --force, got: %v", err)
	}

	// Verify project root directory still exists
	if _, err := os.Stat(projectPath); os.IsNotExist(err) {
		t.Errorf("expected project directory to exist at %s", projectPath)
	}

	// Verify marker file is gone (directory was removed and recreated)
	if _, err := os.Stat(markerFile); !os.IsNotExist(err) {
		t.Error("expected marker file to be removed when using --force")
	}

	// Verify subdirectories exist
	expectedDirs := []string{"models", "seeds", "tests", "macros"}
	for _, dir := range expectedDirs {
		dirPath := filepath.Join(projectPath, dir)
		if _, err := os.Stat(dirPath); os.IsNotExist(err) {
			t.Errorf("expected subdirectory %s to exist at %s", dir, dirPath)
		}
	}
}

// TestGenerateProjectConfig_CorrectName verifies that project name is inserted correctly
func TestGenerateProjectConfig_CorrectName(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	projectName := "my_test_project"

	// Generate the config file
	err := generateProjectConfig(tempDir, projectName)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Read the generated file
	configPath := filepath.Join(tempDir, "gorchata_project.yml")
	content, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("failed to read generated config file: %v", err)
	}

	// Verify project name appears in the content
	contentStr := string(content)
	if !strings.Contains(contentStr, "name: "+projectName) {
		t.Errorf("expected config to contain 'name: %s', got:\n%s", projectName, contentStr)
	}
}

// TestGenerateProjectConfig_DateVar verifies that start_date uses current year
func TestGenerateProjectConfig_DateVar(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	projectName := "date_test_project"

	// Generate the config file
	err := generateProjectConfig(tempDir, projectName)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Read the generated file
	configPath := filepath.Join(tempDir, "gorchata_project.yml")
	content, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("failed to read generated config file: %v", err)
	}

	// Verify start_date contains current year
	contentStr := string(content)
	// We expect format like: start_date: '2026-01-01'
	expectedYear := fmt.Sprintf("%d", time.Now().Year())
	expectedStartDate := fmt.Sprintf("start_date: '%s-01-01'", expectedYear)
	if !strings.Contains(contentStr, expectedStartDate) {
		t.Errorf("expected config to contain start_date '%s', got content:\n%s", expectedStartDate, contentStr)
	}
}

// TestGenerateProjectConfig_FileCreation verifies file is written to correct location
func TestGenerateProjectConfig_FileCreation(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	projectName := "file_test_project"

	// Generate the config file
	err := generateProjectConfig(tempDir, projectName)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Verify file exists at the correct location
	configPath := filepath.Join(tempDir, "gorchata_project.yml")
	info, err := os.Stat(configPath)
	if os.IsNotExist(err) {
		t.Fatalf("expected config file to exist at %s", configPath)
	}
	if err != nil {
		t.Fatalf("failed to stat config file: %v", err)
	}

	// Verify it's a file, not a directory
	if info.IsDir() {
		t.Errorf("expected %s to be a file, not a directory", configPath)
	}

	// Verify permissions (should be readable)
	content, err := os.ReadFile(configPath)
	if err != nil {
		t.Errorf("failed to read config file: %v", err)
	}

	// Verify content is not empty
	if len(content) == 0 {
		t.Error("expected config file to have content, got empty file")
	}
}

// TestGenerateProfiles_DatabasePath verifies that database path matches project name
func TestGenerateProfiles_DatabasePath(t *testing.T) {
	tests := []struct {
		name        string
		projectName string
		wantDevDB   string
		wantProdDB  string
	}{
		{
			name:        "simple project name",
			projectName: "myproject",
			wantDevDB:   "myproject.db",
			wantProdDB:  "myproject_prod.db",
		},
		{
			name:        "project with underscores",
			projectName: "my_project",
			wantDevDB:   "my_project.db",
			wantProdDB:  "my_project_prod.db",
		},
		{
			name:        "project with hyphens",
			projectName: "my-project",
			wantDevDB:   "my-project.db",
			wantProdDB:  "my-project_prod.db",
		},
		{
			name:        "project with numbers",
			projectName: "project123",
			wantDevDB:   "project123.db",
			wantProdDB:  "project123_prod.db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a temporary directory for testing
			tempDir := t.TempDir()

			// Generate the profiles file
			err := generateProfiles(tempDir, tt.projectName)
			if err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}

			// Read the generated file
			profilesPath := filepath.Join(tempDir, "profiles.yml")
			content, err := os.ReadFile(profilesPath)
			if err != nil {
				t.Fatalf("failed to read generated profiles file: %v", err)
			}

			contentStr := string(content)

			// Verify dev database path
			expectedDevDB := fmt.Sprintf(`database: "%s"`, tt.wantDevDB)
			if !strings.Contains(contentStr, expectedDevDB) {
				t.Errorf("expected profiles to contain dev database '%s', got:\n%s", expectedDevDB, contentStr)
			}

			// Verify prod database path
			expectedProdDB := fmt.Sprintf(`database: "%s"`, tt.wantProdDB)
			if !strings.Contains(contentStr, expectedProdDB) {
				t.Errorf("expected profiles to contain prod database '%s', got:\n%s", expectedProdDB, contentStr)
			}
		})
	}
}

// TestGenerateProfiles_MultipleEnvs verifies that dev and prod environments are configured
func TestGenerateProfiles_MultipleEnvs(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	projectName := "test_project"

	// Generate the profiles file
	err := generateProfiles(tempDir, projectName)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Read the generated file
	profilesPath := filepath.Join(tempDir, "profiles.yml")
	content, err := os.ReadFile(profilesPath)
	if err != nil {
		t.Fatalf("failed to read generated profiles file: %v", err)
	}

	contentStr := string(content)

	// Verify default section exists
	if !strings.Contains(contentStr, "default:") {
		t.Error("expected profiles to contain 'default:' section")
	}

	// Verify default target is dev
	if !strings.Contains(contentStr, "target: dev") {
		t.Error("expected profiles to contain 'target: dev'")
	}

	// Verify dev output exists
	if !strings.Contains(contentStr, "dev:") {
		t.Error("expected profiles to contain 'dev:' output")
	}

	// Verify dev uses sqlite
	devSection := strings.Index(contentStr, "dev:")
	if devSection != -1 {
		devContent := contentStr[devSection:]
		if !strings.Contains(devContent, "type: sqlite") {
			t.Error("expected dev output to have 'type: sqlite'")
		}
	}

	// Verify prod section exists
	if !strings.Contains(contentStr, "prod:") {
		t.Error("expected profiles to contain 'prod:' section")
	}

	// Verify prod target is prod
	prodSection := strings.Index(contentStr, "prod:")
	if prodSection != -1 {
		// Find the second occurrence for the target line
		remainingContent := contentStr[prodSection:]
		if !strings.Contains(remainingContent, "target: prod") {
			t.Error("expected profiles to contain 'target: prod'")
		}
	}

	// Verify prod uses sqlite
	if prodSection != -1 {
		prodContent := contentStr[prodSection:]
		if !strings.Contains(prodContent, "type: sqlite") {
			t.Error("expected prod output to have 'type: sqlite'")
		}
	}
}

// TestGenerateProfiles_FileCreation verifies file is written correctly to projectPath/profiles.yml
func TestGenerateProfiles_FileCreation(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	projectName := "file_test_project"

	// Generate the profiles file
	err := generateProfiles(tempDir, projectName)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Verify file exists at the correct location
	profilesPath := filepath.Join(tempDir, "profiles.yml")
	info, err := os.Stat(profilesPath)
	if os.IsNotExist(err) {
		t.Fatalf("expected profiles file to exist at %s", profilesPath)
	}
	if err != nil {
		t.Fatalf("failed to stat profiles file: %v", err)
	}

	// Verify it's a file, not a directory
	if info.IsDir() {
		t.Errorf("expected %s to be a file, not a directory", profilesPath)
	}

	// Verify permissions (should be readable)
	content, err := os.ReadFile(profilesPath)
	if err != nil {
		t.Errorf("failed to read profiles file: %v", err)
	}

	// Verify content is not empty
	if len(content) == 0 {
		t.Error("expected profiles file to have content, got empty file")
	}

	// Verify content is valid YAML format (contains expected structure)
	contentStr := string(content)
	if !strings.Contains(contentStr, "outputs:") {
		t.Error("expected profiles to contain 'outputs:' section")
	}
}

// TestGenerateModels_AllFiles verifies that all three model files are created in models/ subdirectory
func TestGenerateModels_AllFiles(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	projectPath := filepath.Join(tempDir, "test_project")

	// Create project structure first
	err := createProjectDirectories(projectPath, false)
	if err != nil {
		t.Fatalf("failed to create project directories: %v", err)
	}

	// Generate models
	err = generateModels(projectPath)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Verify all three model files exist
	expectedFiles := []string{
		"stg_users.sql",
		"stg_orders.sql",
		"fct_order_summary.sql",
	}

	for _, filename := range expectedFiles {
		filePath := filepath.Join(projectPath, "models", filename)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			t.Errorf("expected model file to exist at %s", filePath)
		}
	}
}

// TestGenerateModels_ContentCorrect verifies that SQL content and config blocks are present
func TestGenerateModels_ContentCorrect(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	projectPath := filepath.Join(tempDir, "test_project")

	// Create project structure first
	err := createProjectDirectories(projectPath, false)
	if err != nil {
		t.Fatalf("failed to create project directories: %v", err)
	}

	// Generate models
	err = generateModels(projectPath)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	tests := []struct {
		name            string
		filename        string
		expectedStrings []string
	}{
		{
			name:     "stg_users.sql",
			filename: "stg_users.sql",
			expectedStrings: []string{
				"{{ config(materialized='view') }}",
				"SELECT",
				"FROM raw_users",
				"WHERE deleted_at IS NULL",
			},
		},
		{
			name:     "stg_orders.sql",
			filename: "stg_orders.sql",
			expectedStrings: []string{
				"{{ config(materialized='view') }}",
				"SELECT",
				"FROM raw_orders",
				"WHERE status = 'completed'",
			},
		},
		{
			name:     "fct_order_summary.sql",
			filename: "fct_order_summary.sql",
			expectedStrings: []string{
				"{{ config(materialized='table') }}",
				"SELECT",
				"FROM",
				"LEFT JOIN",
				"GROUP BY",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filePath := filepath.Join(projectPath, "models", tt.filename)
			content, err := os.ReadFile(filePath)
			if err != nil {
				t.Fatalf("failed to read model file %s: %v", tt.filename, err)
			}

			contentStr := string(content)
			for _, expected := range tt.expectedStrings {
				if !strings.Contains(contentStr, expected) {
					t.Errorf("expected model file %s to contain %q, got:\n%s", tt.filename, expected, contentStr)
				}
			}
		})
	}
}

// TestGenerateModels_RefSyntax verifies that {{ ref }} syntax is correct in fct_order_summary.sql
func TestGenerateModels_RefSyntax(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	projectPath := filepath.Join(tempDir, "test_project")

	// Create project structure first
	err := createProjectDirectories(projectPath, false)
	if err != nil {
		t.Fatalf("failed to create project directories: %v", err)
	}

	// Generate models
	err = generateModels(projectPath)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Read fct_order_summary.sql
	filePath := filepath.Join(projectPath, "models", "fct_order_summary.sql")
	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read fct_order_summary.sql: %v", err)
	}

	contentStr := string(content)

	// Verify {{ ref "stg_users" }} syntax
	if !strings.Contains(contentStr, `{{ ref "stg_users" }}`) {
		t.Errorf("expected fct_order_summary.sql to contain {{ ref \"stg_users\" }}, got:\n%s", contentStr)
	}

	// Verify {{ ref "stg_orders" }} syntax
	if !strings.Contains(contentStr, `{{ ref "stg_orders" }}`) {
		t.Errorf("expected fct_order_summary.sql to contain {{ ref \"stg_orders\" }}, got:\n%s", contentStr)
	}
}

// TestInitCommand_Integration_CompleteProject verifies entire init process creates valid project structure with all files
func TestInitCommand_Integration_CompleteProject(t *testing.T) {
	// Use t.TempDir() for test project
	tempDir := t.TempDir()
	projectName := "integration_test_project"

	// Change to temp directory first so the project is created there
	originalDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get current directory: %v", err)
	}
	defer os.Chdir(originalDir)

	err = os.Chdir(tempDir)
	if err != nil {
		t.Fatalf("failed to change to temp directory: %v", err)
	}

	// Call InitCommand with project name
	err = InitCommand([]string{projectName})
	if err != nil {
		t.Fatalf("expected no error from InitCommand, got: %v", err)
	}

	projectPath := filepath.Join(tempDir, projectName)

	// Verify no error returned
	// (already checked above)

	// Check all 4 folders exist: models/, seeds/, tests/, macros/
	expectedFolders := []string{"models", "seeds", "tests", "macros"}
	for _, folder := range expectedFolders {
		folderPath := filepath.Join(projectPath, folder)
		info, err := os.Stat(folderPath)
		if os.IsNotExist(err) {
			t.Errorf("expected folder %s to exist at %s", folder, folderPath)
		} else if err != nil {
			t.Errorf("failed to stat folder %s: %v", folder, err)
		} else if !info.IsDir() {
			t.Errorf("expected %s to be a directory", folderPath)
		}
	}

	// Check gorchata_project.yml exists and contains project name
	configPath := filepath.Join(projectPath, "gorchata_project.yml")
	configInfo, err := os.Stat(configPath)
	if os.IsNotExist(err) {
		t.Errorf("expected gorchata_project.yml to exist at %s", configPath)
	} else if err != nil {
		t.Errorf("failed to stat gorchata_project.yml: %v", err)
	} else if configInfo.IsDir() {
		t.Errorf("expected gorchata_project.yml to be a file, not a directory")
	} else {
		// Read and verify content
		content, err := os.ReadFile(configPath)
		if err != nil {
			t.Errorf("failed to read gorchata_project.yml: %v", err)
		} else {
			contentStr := string(content)
			if !strings.Contains(contentStr, "name: "+projectName) {
				t.Errorf("expected gorchata_project.yml to contain 'name: %s', got:\n%s", projectName, contentStr)
			}
		}
	}

	// Check profiles.yml exists and contains database config
	profilesPath := filepath.Join(projectPath, "profiles.yml")
	profilesInfo, err := os.Stat(profilesPath)
	if os.IsNotExist(err) {
		t.Errorf("expected profiles.yml to exist at %s", profilesPath)
	} else if err != nil {
		t.Errorf("failed to stat profiles.yml: %v", err)
	} else if profilesInfo.IsDir() {
		t.Errorf("expected profiles.yml to be a file, not a directory")
	} else {
		// Read and verify content
		content, err := os.ReadFile(profilesPath)
		if err != nil {
			t.Errorf("failed to read profiles.yml: %v", err)
		} else {
			contentStr := string(content)
			if !strings.Contains(contentStr, "type: sqlite") {
				t.Errorf("expected profiles.yml to contain 'type: sqlite', got:\n%s", contentStr)
			}
			if !strings.Contains(contentStr, projectName) {
				t.Errorf("expected profiles.yml to contain project name '%s', got:\n%s", projectName, contentStr)
			}
		}
	}

	// Check all 3 SQL model files exist: stg_users.sql, stg_orders.sql, fct_order_summary.sql
	expectedModelFiles := []string{
		"stg_users.sql",
		"stg_orders.sql",
		"fct_order_summary.sql",
	}
	for _, filename := range expectedModelFiles {
		modelPath := filepath.Join(projectPath, "models", filename)
		modelInfo, err := os.Stat(modelPath)
		if os.IsNotExist(err) {
			t.Errorf("expected model file %s to exist at %s", filename, modelPath)
		} else if err != nil {
			t.Errorf("failed to stat model file %s: %v", filename, err)
		} else if modelInfo.IsDir() {
			t.Errorf("expected %s to be a file, not a directory", modelPath)
		}
	}

	// Verify at least one model file contains {{ config }} and {{ ref }} syntax
	fctPath := filepath.Join(projectPath, "models", "fct_order_summary.sql")
	fctContent, err := os.ReadFile(fctPath)
	if err != nil {
		t.Errorf("failed to read fct_order_summary.sql: %v", err)
	} else {
		contentStr := string(fctContent)
		if !strings.Contains(contentStr, "{{ config") {
			t.Errorf("expected fct_order_summary.sql to contain {{ config }} syntax")
		}
		if !strings.Contains(contentStr, "{{ ref") {
			t.Errorf("expected fct_order_summary.sql to contain {{ ref }} syntax")
		}
	}
}

// TestInitCommand_Integration_AllFolders verifies all subdirectories exist and are actually directories
func TestInitCommand_Integration_AllFolders(t *testing.T) {
	// Use t.TempDir() for test project
	tempDir := t.TempDir()
	projectName := "folder_test_project"

	// Change to temp directory first so the project is created there
	originalDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get current directory: %v", err)
	}
	defer os.Chdir(originalDir)

	err = os.Chdir(tempDir)
	if err != nil {
		t.Fatalf("failed to change to temp directory: %v", err)
	}

	// Call InitCommand with project name
	err = InitCommand([]string{projectName})
	if err != nil {
		t.Fatalf("expected no error from InitCommand, got: %v", err)
	}

	projectPath := filepath.Join(tempDir, projectName)

	// Verify all subdirectories exist and are actually directories
	expectedDirs := []string{"models", "seeds", "tests", "macros"}
	for _, dirName := range expectedDirs {
		dirPath := filepath.Join(projectPath, dirName)

		// Check existence
		info, err := os.Stat(dirPath)
		if os.IsNotExist(err) {
			t.Errorf("expected directory %s to exist at %s", dirName, dirPath)
			continue
		}
		if err != nil {
			t.Errorf("failed to stat directory %s: %v", dirName, err)
			continue
		}

		// Verify it's actually a directory
		if !info.IsDir() {
			t.Errorf("expected %s to be a directory, but it is not", dirPath)
		}
	}
}
