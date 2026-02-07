package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// ProjectConfig represents the gorchata_project.yml configuration
type ProjectConfig struct {
	Name       string                            `yaml:"name"`
	Version    string                            `yaml:"version"`
	Profile    string                            `yaml:"profile"`
	ModelPaths []string                          `yaml:"model-paths"`
	SeedPaths  []string                          `yaml:"seed-paths"`
	TestPaths  []string                          `yaml:"test-paths"`
	MacroPaths []string                          `yaml:"macro-paths"`
	Vars       map[string]interface{}            `yaml:"vars"`
	Models     map[string]map[string]interface{} `yaml:"models"`
}

// LoadProject loads and parses a gorchata_project.yml file
func LoadProject(path string) (*ProjectConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg ProjectConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse project config: %w", err)
	}

	// Apply defaults
	cfg.applyDefaults()

	// Validate required fields
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid project config: %w", err)
	}

	return &cfg, nil
}

// applyDefaults sets default values for optional fields
func (c *ProjectConfig) applyDefaults() {
	if len(c.ModelPaths) == 0 {
		c.ModelPaths = []string{"models"}
	}
	if len(c.SeedPaths) == 0 {
		c.SeedPaths = []string{"seeds"}
	}
	if len(c.TestPaths) == 0 {
		c.TestPaths = []string{"tests"}
	}
	if len(c.MacroPaths) == 0 {
		c.MacroPaths = []string{"macros"}
	}
	if c.Vars == nil {
		c.Vars = make(map[string]interface{})
	}
	if c.Models == nil {
		c.Models = make(map[string]map[string]interface{})
	}
}

// Validate checks that all required fields are present and valid
func (c *ProjectConfig) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("name is required")
	}
	if c.Version == "" {
		return fmt.Errorf("version is required")
	}
	return nil
}
