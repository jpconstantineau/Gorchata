package integration

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/config"
	"github.com/jpconstantineau/gorchata/internal/domain/test/executor"
	"github.com/jpconstantineau/gorchata/internal/domain/test/generic"
)

// TestDCSAlarmExample_TestDiscovery verifies test discovery for DCS alarm example
func TestDCSAlarmExample_TestDiscovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Get to repo root
	repoRoot := getRepoRoot(t)
	exampleDir := filepath.Join(repoRoot, "examples", "dcs_alarm_example")

	// Change to example directory
	originalDir, _ := os.Getwd()
	defer os.Chdir(originalDir)
	if err := os.Chdir(exampleDir); err != nil {
		t.Fatalf("Failed to change to example directory: %v", err)
	}

	// Load config using Discover (looks in current directory)
	cfg, err := config.Discover("dev")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Initialize registry
	registry := generic.NewDefaultRegistry()

	// Discover all tests
	tests, err := executor.DiscoverAllTests(cfg, registry)
	if err != nil {
		t.Fatalf("Failed to discover tests: %v", err)
	}

	// Verify minimum test count
	if len(tests) < 20 {
		t.Errorf("Expected at least 20 tests (15 generic + 3 singular + 1 custom), got %d", len(tests))
	}

	// Verify schema.yml exists
	schemaPath := filepath.Join(exampleDir, "models", "schema.yml")
	if _, err := os.Stat(schemaPath); os.IsNotExist(err) {
		t.Error("models/schema.yml does not exist")
	}

	// Verify singular test files exist
	expectedSingularTests := []string{
		"tests/test_alarm_lifecycle.sql",
		"tests/test_standing_alarm_duration.sql",
		"tests/test_chattering_detection.sql",
	}

	for _, testFile := range expectedSingularTests {
		testPath := filepath.Join(exampleDir, testFile)
		if _, err := os.Stat(testPath); os.IsNotExist(err) {
			t.Errorf("Singular test file %s does not exist", testFile)
		}
	}

	// Verify custom generic test template exists
	customTestPath := filepath.Join(exampleDir, "tests", "generic", "test_valid_timestamp.sql")
	if _, err := os.Stat(customTestPath); os.IsNotExist(err) {
		t.Error("Custom generic test tests/generic/test_valid_timestamp.sql does not exist")
	}

	// Verify we have both generic and singular tests
	hasGeneric := false
	hasSingular := false
	for _, test := range tests {
		if test.Type == "generic" {
			hasGeneric = true
		}
		if test.Type == "singular" {
			hasSingular = true
		}
	}

	if !hasGeneric {
		t.Error("No generic tests discovered")
	}
	if !hasSingular {
		t.Error("No singular tests discovered")
	}
}

// TestStarSchemaExample_TestDiscovery verifies test discovery for star schema example
func TestStarSchemaExample_TestDiscovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Get to repo root
	repoRoot := getRepoRoot(t)
	exampleDir := filepath.Join(repoRoot, "examples", "star_schema_example")

	// Change to example directory
	originalDir, _ := os.Getwd()
	defer os.Chdir(originalDir)
	if err := os.Chdir(exampleDir); err != nil {
		t.Fatalf("Failed to change to example directory: %v", err)
	}

	// Load config using Discover (looks in current directory)
	cfg, err := config.Discover("dev")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Initialize registry
	registry := generic.NewDefaultRegistry()

	// Discover all tests
	tests, err := executor.DiscoverAllTests(cfg, registry)
	if err != nil {
		t.Fatalf("Failed to discover tests: %v", err)
	}

	// Verify minimum test count
	if len(tests) < 10 {
		t.Errorf("Expected at least 10 tests (10-15 generic + 1 singular), got %d", len(tests))
	}

	// Verify schema.yml exists
	schemaPath := filepath.Join(exampleDir, "models", "schema.yml")
	if _, err := os.Stat(schemaPath); os.IsNotExist(err) {
		t.Error("models/schema.yml does not exist")
	}

	// Verify singular test file exists
	testPath := filepath.Join(exampleDir, "tests", "test_fact_integrity.sql")
	if _, err := os.Stat(testPath); os.IsNotExist(err) {
		t.Error("Singular test file tests/test_fact_integrity.sql does not exist")
	}

	// Verify we have both generic and singular tests
	hasGeneric := false
	hasSingular := false
	for _, test := range tests {
		if test.Type == "generic" {
			hasGeneric = true
		}
		if test.Type == "singular" {
			hasSingular = true
		}
	}

	if !hasGeneric {
		t.Error("No generic tests discovered")
	}
	if !hasSingular {
		t.Error("No singular tests discovered")
	}
}

func getRepoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working directory: %v", err)
	}

	// Walk up until we find go.mod
	for {
		if _, err := os.Stat(filepath.Join(wd, "go.mod")); err == nil {
			return wd
		}
		parent := filepath.Dir(wd)
		if parent == wd {
			t.Fatal("Could not find repository root (no go.mod found)")
		}
		wd = parent
	}
}
