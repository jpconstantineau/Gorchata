package singular

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jpconstantineau/gorchata/internal/domain/test"
)

// LoadSingularTests scans a directory recursively for .sql test files
func LoadSingularTests(testDir string) ([]*test.Test, error) {
	var tests []*test.Test

	// Check if directory exists
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("test directory does not exist: %s", testDir)
	}

	// Walk directory recursively
	err := filepath.Walk(testDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Only process .sql files
		if !strings.HasSuffix(path, ".sql") {
			return nil
		}

		// Load test from file
		t, err := loadTestFromFile(path)
		if err != nil {
			return fmt.Errorf("failed to load test from %s: %w", path, err)
		}

		tests = append(tests, t)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return tests, nil
}

// loadTestFromFile loads a single test from a .sql file
func loadTestFromFile(filePath string) (*test.Test, error) {
	// Read file content
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	sqlContent := string(content)

	// Extract test name from filename (without extension)
	filename := filepath.Base(filePath)
	testName := strings.TrimSuffix(filename, ".sql")

	// Parse metadata from comments
	config, err := ParseTestMetadata(sqlContent)
	if err != nil {
		return nil, fmt.Errorf("failed to parse metadata: %w", err)
	}

	// Create test instance
	// Note: We use testName as both ID and Name for simplicity
	// ModelName will be set later when tests are executed
	t := &test.Test{
		ID:          testName,
		Name:        testName,
		ModelName:   "", // Will be determined during execution
		ColumnName:  "", // Singular tests don't have columns
		Type:        test.SingularTest,
		SQLTemplate: sqlContent,
		Config:      config,
	}

	return t, nil
}
