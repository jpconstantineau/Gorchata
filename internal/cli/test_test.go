package cli

import (
	"strings"
	"testing"
)

// TestTestCommandPlaceholder verifies "not implemented" message
func TestTestCommandPlaceholder(t *testing.T) {
	err := TestCommand([]string{})
	if err == nil {
		t.Fatal("TestCommand() should return error with not implemented message")
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "not") || !strings.Contains(errMsg, "implemented") {
		t.Errorf("TestCommand() error = %v, want error containing 'not implemented'", err)
	}
}
