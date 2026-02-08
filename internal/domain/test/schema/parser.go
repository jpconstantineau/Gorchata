package schema

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// ParseSchemaFile reads and parses a single schema YAML file
func ParseSchemaFile(filePath string) (*SchemaFile, error) {
	// Read the file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file %s: %w", filePath, err)
	}

	// Parse YAML
	var schema SchemaFile
	err = yaml.Unmarshal(data, &schema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse YAML in %s: %w", filePath, err)
	}

	// Basic validation
	if schema.Version == 0 {
		return nil, fmt.Errorf("schema file %s missing or invalid version", filePath)
	}

	return &schema, nil
}

// LoadSchemaFiles recursively scans a directory for schema YAML files and parses them
func LoadSchemaFiles(directory string) ([]*SchemaFile, error) {
	// Check if directory exists
	info, err := os.Stat(directory)
	if err != nil {
		return nil, fmt.Errorf("failed to access directory %s: %w", directory, err)
	}

	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", directory)
	}

	var schemas []*SchemaFile
	var parseErrors []error

	// Walk the directory tree
	err = filepath.WalkDir(directory, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if d.IsDir() {
			return nil
		}

		// Check if file matches schema naming pattern
		basename := filepath.Base(path)
		if !isSchemaFile(basename) {
			return nil
		}

		// Parse the schema file
		schema, err := ParseSchemaFile(path)
		if err != nil {
			parseErrors = append(parseErrors, err)
			// Continue processing other files
			return nil
		}

		schemas = append(schemas, schema)
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to walk directory %s: %w", directory, err)
	}

	// If we had parse errors but found no schemas, return the errors
	if len(schemas) == 0 && len(parseErrors) > 0 {
		return nil, fmt.Errorf("failed to parse schema files: %v", parseErrors)
	}

	return schemas, nil
}

// isSchemaFile checks if a filename matches the schema file pattern
func isSchemaFile(filename string) bool {
	// Match files ending in schema.yml or _schema.yml
	return strings.HasSuffix(filename, "schema.yml") ||
		strings.HasSuffix(filename, "schema.yaml") ||
		strings.HasSuffix(filename, "_schema.yml") ||
		strings.HasSuffix(filename, "_schema.yaml")
}
