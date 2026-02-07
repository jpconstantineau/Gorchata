package cli

import (
	"strings"
	"testing"
)

// TestDocsCommandPlaceholder verifies "not implemented" message
func TestDocsCommandPlaceholder(t *testing.T) {
	err := DocsCommand([]string{})
	if err == nil {
		t.Fatal("DocsCommand() should return error with not implemented message")
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "not") || !strings.Contains(errMsg, "implemented") {
		t.Errorf("DocsCommand() error = %v, want error containing 'not implemented'", err)
	}
}
