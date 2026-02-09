package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// SchemaYAML represents the schema.yml configuration file
type SchemaYAML struct {
	Version int          `yaml:"version"`
	Seeds   []SeedSchema `yaml:"seeds"`
}

// SeedSchema represents a seed definition in schema.yml
type SeedSchema struct {
	Name   string           `yaml:"name"`
	Config SeedSchemaConfig `yaml:"config"`
}

// SeedSchemaConfig holds configuration for a seed
type SeedSchemaConfig struct {
	ColumnTypes map[string]string `yaml:"column_types"`
}

// ParseSchemaYAML loads and parses a schema.yml file
func ParseSchemaYAML(filePath string) (*SchemaYAML, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file: %w", err)
	}

	var schema SchemaYAML
	if err := yaml.Unmarshal(data, &schema); err != nil {
		return nil, fmt.Errorf("failed to parse schema file: %w", err)
	}

	// Initialize empty maps for seeds without column_types
	for i := range schema.Seeds {
		if schema.Seeds[i].Config.ColumnTypes == nil {
			schema.Seeds[i].Config.ColumnTypes = make(map[string]string)
		}
	}

	return &schema, nil
}

// GetSeedConfig retrieves the configuration for a specific seed by name
func (s *SchemaYAML) GetSeedConfig(seedName string) (SeedSchemaConfig, bool) {
	for _, seed := range s.Seeds {
		if seed.Name == seedName {
			return seed.Config, true
		}
	}
	return SeedSchemaConfig{}, false
}
