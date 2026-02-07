package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
