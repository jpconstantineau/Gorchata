package main

import (
	"fmt"
	"log"

	"github.com/jpconstantineau/gorchata/internal/domain/test/generic"
	"github.com/jpconstantineau/gorchata/internal/domain/test/schema"
)

// Example usage of schema.yml parsing and test building
func main() {
	// Step 1: Load schema files from a directory
	// This will recursively find all *schema.yml or *_schema.yml files
	schemaFiles, err := schema.LoadSchemaFiles("./models")
	if err != nil {
		log.Fatalf("Failed to load schema files: %v", err)
	}

	fmt.Printf("Loaded %d schema file(s)\n", len(schemaFiles))

	// Step 2: Get registry with all available tests
	// This includes core tests (not_null, unique, etc.) and extended tests
	registry := generic.NewDefaultRegistry()

	// Optional: Load custom generic tests
	// customTests, _ := generic.LoadCustomGenericTests("./tests/generic", registry)
	// fmt.Printf("Loaded %d custom test(s)\n", customTests)

	// Step 3: Build test instances from schema
	// This parses all test definitions and creates executable Test objects
	tests, err := schema.BuildTestsFromSchema(schemaFiles, registry)
	if err != nil {
		log.Fatalf("Failed to build tests from schema: %v", err)
	}

	fmt.Printf("Built %d test(s)\n\n", len(tests))

	// Step 4: Inspect the tests
	for _, test := range tests {
		fmt.Printf("Test ID: %s\n", test.ID)
		fmt.Printf("  Name: %s\n", test.Name)
		fmt.Printf("  Model: %s\n", test.ModelName)
		if test.ColumnName != "" {
			fmt.Printf("  Column: %s\n", test.ColumnName)
		} else {
			fmt.Printf("  Type: Table-level test\n")
		}
		fmt.Printf("  Severity: %s\n", test.Config.Severity)
		if test.Config.Where != "" {
			fmt.Printf("  Where: %s\n", test.Config.Where)
		}
		fmt.Printf("  SQL Preview: %s...\n\n", truncate(test.SQLTemplate, 60))
	}

	// Step 5: Tests are now ready for execution
	// In Phase 5, these Test instances would be passed to the execution engine
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}
