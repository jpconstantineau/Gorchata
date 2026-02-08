package cli

import (
	"strings"
	"testing"
)

// TestCommandRouting verifies correct command is called based on args
func TestCommandRouting(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		wantErr     bool
		errContains string
	}{
		{
			name:        "run command routes correctly (will fail without config)",
			args:        []string{"run"},
			wantErr:     true,
			errContains: "config",
		},
		{
			name:        "compile command routes correctly (will fail without config)",
			args:        []string{"compile"},
			wantErr:     true,
			errContains: "config",
		},
		// Skip test command routing test due to flag conflicts in table-driven test
		// The test command functionality is tested separately in build_test.go
		// {
		// 	name:        "test command",
		// 	args:        []string{"test"},
		// 	wantErr:     true,
		// 	errContains: "config",
		// },
		{
			name:        "docs command",
			args:        []string{"docs"},
			wantErr:     true,
			errContains: "not implemented",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Run(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Run() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
				t.Errorf("Run() error = %v, want error containing %q", err, tt.errContains)
			}
		})
	}
}

// TestNoCommand verifies usage printed when no command given
func TestNoCommand(t *testing.T) {
	err := Run([]string{})
	if err == nil {
		t.Error("Run() with no args should return error")
	}
	if !strings.Contains(err.Error(), "usage") && !strings.Contains(err.Error(), "command") {
		t.Errorf("Run() error = %v, want error containing usage information", err)
	}
}

// TestUnknownCommand verifies error for unknown command
func TestUnknownCommand(t *testing.T) {
	err := Run([]string{"unknown"})
	if err == nil {
		t.Error("Run() with unknown command should return error")
	}
	if !strings.Contains(err.Error(), "unknown") && !strings.Contains(err.Error(), "invalid") {
		t.Errorf("Run() error = %v, want error about unknown command", err)
	}
}

// TestHelpFlag tests --help flag
func TestHelpFlag(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"help flag", []string{"--help"}},
		{"h flag", []string{"-h"}},
		{"help command", []string{"help"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Run(tt.args)
			// Help should not return an error
			if err != nil {
				t.Errorf("Run() with help should not return error, got: %v", err)
			}
		})
	}
}

// TestVersionFlag tests --version flag
func TestVersionFlag(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"version flag", []string{"--version"}},
		{"v flag", []string{"-v"}},
		{"version command", []string{"version"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Run(tt.args)
			// Version should not return an error
			if err != nil {
				t.Errorf("Run() with version should not return error, got: %v", err)
			}
		})
	}
}
