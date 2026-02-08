package executor

import (
	"strings"

	"github.com/jpconstantineau/gorchata/internal/domain/test"
)

// TestSelector handles test filtering based on patterns, tags, and models
type TestSelector struct {
	includes []string // Test name patterns to include
	excludes []string // Test name patterns to exclude
	tags     []string // Tag filters
	models   []string // Model name filters
}

// NewTestSelector creates a new test selector with the given filters
func NewTestSelector(includes, excludes, tags, models []string) *TestSelector {
	return &TestSelector{
		includes: includes,
		excludes: excludes,
		tags:     tags,
		models:   models,
	}
}

// Matches returns true if the test matches the selector's criteria
func (s *TestSelector) Matches(t *test.Test) bool {
	// Check excludes first (highest priority)
	if len(s.excludes) > 0 {
		for _, pattern := range s.excludes {
			if matchPattern(pattern, t.ID) {
				return false
			}
		}
	}

	// Check includes
	if len(s.includes) > 0 {
		matchesInclude := false
		for _, pattern := range s.includes {
			if matchPattern(pattern, t.ID) {
				matchesInclude = true
				break
			}
		}
		if !matchesInclude {
			return false
		}
	}

	// Check tags
	if len(s.tags) > 0 {
		matchesTag := false
		for _, filterTag := range s.tags {
			for _, testTag := range t.Config.Tags {
				if testTag == filterTag {
					matchesTag = true
					break
				}
			}
			if matchesTag {
				break
			}
		}
		if !matchesTag {
			return false
		}
	}

	// Check models
	if len(s.models) > 0 {
		matchesModel := false
		for _, pattern := range s.models {
			if matchPattern(pattern, t.ModelName) {
				matchesModel = true
				break
			}
		}
		if !matchesModel {
			return false
		}
	}

	return true
}

// Filter returns a subset of tests that match the selector's criteria
func (s *TestSelector) Filter(tests []*test.Test) []*test.Test {
	var filtered []*test.Test

	for _, t := range tests {
		if s.Matches(t) {
			filtered = append(filtered, t)
		}
	}

	return filtered
}

// matchPattern performs simple wildcard pattern matching
// Supports * as a wildcard for zero or more characters
func matchPattern(pattern, text string) bool {
	// Simple case: exact match or wildcard *
	if pattern == "*" {
		return true
	}
	if pattern == text {
		return true
	}

	// Handle patterns with wildcards
	if strings.Contains(pattern, "*") {
		// Split pattern by *
		parts := strings.Split(pattern, "*")

		// Check if text starts with first part
		if len(parts[0]) > 0 {
			if !strings.HasPrefix(text, parts[0]) {
				return false
			}
			text = text[len(parts[0]):]
		}

		// Check middle parts
		for i := 1; i < len(parts)-1; i++ {
			if parts[i] == "" {
				continue
			}
			idx := strings.Index(text, parts[i])
			if idx == -1 {
				return false
			}
			text = text[idx+len(parts[i]):]
		}

		// Check if text ends with last part
		if len(parts) > 1 && len(parts[len(parts)-1]) > 0 {
			if !strings.HasSuffix(text, parts[len(parts)-1]) {
				return false
			}
		}

		return true
	}

	return false
}
