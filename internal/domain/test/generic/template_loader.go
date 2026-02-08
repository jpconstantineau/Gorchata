package generic

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// LoadCustomGenericTests loads custom generic test templates from a directory
// and registers them in the provided registry
func LoadCustomGenericTests(testDir string, registry *Registry) error {
	// Check if directory exists
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		return fmt.Errorf("test directory does not exist: %s", testDir)
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

		// Load and parse template
		if err := loadAndRegisterTemplate(path, registry); err != nil {
			// Return error immediately to stop Walk
			return err
		}

		return nil
	})

	return err
}

// loadAndRegisterTemplate loads a template file and registers it
func loadAndRegisterTemplate(filePath string, registry *Registry) error {
	// Read file content
	content, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	sqlContent := string(content)

	// Parse the template
	testName, params, sqlTemplate, err := ParseTestTemplate(sqlContent)
	if err != nil {
		return fmt.Errorf("failed to parse template: %w", err)
	}

	// Create TemplateTest instance
	test := NewTemplateTest(testName, params, sqlTemplate)

	// Register in registry
	registry.Register(testName, test)

	return nil
}
