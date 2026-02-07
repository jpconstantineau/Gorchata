package cli

import (
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
