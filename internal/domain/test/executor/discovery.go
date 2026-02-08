package executor

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jpconstantineau/gorchata/internal/config"
	"github.com/jpconstantineau/gorchata/internal/domain/test"
	"github.com/jpconstantineau/gorchata/internal/domain/test/generic"
	"github.com/jpconstantineau/gorchata/internal/domain/test/schema"
	"github.com/jpconstantineau/gorchata/internal/domain/test/singular"
)

// DiscoverAllTests discovers all tests from the configured paths
func DiscoverAllTests(cfg *config.Config, registry *generic.Registry) ([]*test.Test, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if registry == nil {
		return nil, fmt.Errorf("registry cannot be nil")
	}

	allTests := make([]*test.Test, 0)

	// 1. Load singular tests from test paths
	if cfg.Project.TestPaths != nil {
		for _, testPath := range cfg.Project.TestPaths {
			// Check if directory exists
			if _, err := os.Stat(testPath); os.IsNotExist(err) {
				continue // Skip non-existent paths
			}

			tests, err := singular.LoadSingularTests(testPath)
			if err != nil {
				return nil, fmt.Errorf("failed to load singular tests from %s: %w", testPath, err)
			}
			allTests = append(allTests, tests...)
		}
	}

	// 2. Load schema files and build tests from model paths
	if cfg.Project.ModelPaths != nil {
		var schemaFiles []*schema.SchemaFile

		for _, modelPath := range cfg.Project.ModelPaths {
			// Check if directory exists
			if _, err := os.Stat(modelPath); os.IsNotExist(err) {
				continue // Skip non-existent paths
			}

			// Find all schema files
			schemas, err := findSchemaFiles(modelPath)
			if err != nil {
				return nil, fmt.Errorf("failed to find schema files in %s: %w", modelPath, err)
			}
			schemaFiles = append(schemaFiles, schemas...)
		}

		// Build tests from schema files
		if len(schemaFiles) > 0 {
			tests, err := schema.BuildTestsFromSchema(schemaFiles, registry)
			if err != nil {
				return nil, fmt.Errorf("failed to build tests from schema: %w", err)
			}
			allTests = append(allTests, tests...)
		}
	}

	return allTests, nil
}

// findSchemaFiles recursively finds all .yml and .yaml files that are schema files
func findSchemaFiles(dir string) ([]*schema.SchemaFile, error) {
	var schemaFiles []*schema.SchemaFile

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Only process .yml and .yaml files
		ext := strings.ToLower(filepath.Ext(path))
		if ext != ".yml" && ext != ".yaml" {
			return nil
		}

		// Try to parse as schema file
		schemaFile, err := schema.ParseSchemaFile(path)
		if err != nil {
			// Not a valid schema file, skip
			return nil
		}

		schemaFiles = append(schemaFiles, schemaFile)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return schemaFiles, nil
}
