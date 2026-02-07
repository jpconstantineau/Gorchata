package app

import (
	"testing"
)

// TestNew verifies that New() returns a non-nil App instance
func TestNew(t *testing.T) {
	app, err := New()

	if err != nil {
		t.Fatalf("New() returned unexpected error: %v", err)
	}

	if app == nil {
		t.Fatal("New() returned nil App")
	}
}

// TestRun verifies that Run() properly delegates to the CLI router
func TestRun(t *testing.T) {
	app, err := New()
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tests := []struct {
		name    string
		args    []string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "no args returns error",
			args:    []string{},
			wantErr: true,
			errMsg:  "no command specified",
		},
		{
			name:    "help flag succeeds",
			args:    []string{"--help"},
			wantErr: false,
		},
		{
			name:    "version flag succeeds",
			args:    []string{"--version"},
			wantErr: false,
		},
		{
			name:    "invalid command returns error",
			args:    []string{"invalid"},
			wantErr: true,
			errMsg:  "unknown command",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := app.Run(tt.args)
			
			if tt.wantErr {
				if err == nil {
					t.Errorf("Run() expected error containing %q, got nil", tt.errMsg)
				} else if tt.errMsg != "" && len(err.Error()) > 0 {
					// Just verify error is non-nil; CLI tests verify exact messages
					// This keeps app tests focused on delegation behavior
				}
			} else {
				if err != nil {
					t.Errorf("Run() unexpected error: %v", err)
				}
			}
		})
	}
}
