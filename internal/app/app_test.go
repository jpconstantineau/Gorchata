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

// TestRun verifies that Run() executes without error (basic version)
func TestRun(t *testing.T) {
	app, err := New()
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Test with empty args
	err = app.Run([]string{})
	if err != nil {
		t.Errorf("Run() returned unexpected error: %v", err)
	}

	// Test with some args
	err = app.Run([]string{"test", "arg"})
	if err != nil {
		t.Errorf("Run() with args returned unexpected error: %v", err)
	}
}
