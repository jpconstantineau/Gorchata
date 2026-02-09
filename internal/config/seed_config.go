package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Naming strategy constants
const (
	NamingStrategyFilename = "filename"
	NamingStrategyFolder   = "folder"
	NamingStrategyStatic   = "static"
)

// Scope constants
const (
	ScopeFile   = "file"
	ScopeFolder = "folder"
	ScopeTree   = "tree"
)

// SeedConfig represents the seed.yml configuration
type SeedConfig struct {
	Version     int                    `yaml:"version"`
	Naming      NamingConfig           `yaml:"naming"`
	Import      ImportConfig           `yaml:"import"`
	ColumnTypes map[string]string      `yaml:"column_types"`
	Config      map[string]interface{} `yaml:"config"`
}

// NamingConfig defines how table names are resolved from seed files
type NamingConfig struct {
	Strategy   string `yaml:"strategy"`
	StaticName string `yaml:"static_name"`
	Prefix     string `yaml:"prefix"`
}

// ImportConfig defines import behavior
type ImportConfig struct {
	BatchSize int    `yaml:"batch_size"`
	Scope     string `yaml:"scope"`
}

// ParseSeedConfig loads and parses a seed.yml file
func ParseSeedConfig(filePath string) (*SeedConfig, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read seed config: %w", err)
	}

	var cfg SeedConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse seed config: %w", err)
	}

	// Apply defaults
	cfg.applyDefaults()

	return &cfg, nil
}

// applyDefaults sets default values for optional fields
func (c *SeedConfig) applyDefaults() {
	// Default naming strategy
	if c.Naming.Strategy == "" {
		c.Naming.Strategy = NamingStrategyFilename
	}

	// Default import settings
	if c.Import.BatchSize == 0 {
		c.Import.BatchSize = 1000
	}
	if c.Import.Scope == "" {
		c.Import.Scope = ScopeTree
	}
}
