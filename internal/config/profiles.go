package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// ProfilesConfig represents the profiles.yml configuration
type ProfilesConfig struct {
	Default  *Profile            `yaml:"default"`
	Profiles map[string]*Profile `yaml:",inline"`
}

// Profile represents a single profile with target and outputs
type Profile struct {
	Target  string                   `yaml:"target"`
	Outputs map[string]*OutputConfig `yaml:"outputs"`
}

// OutputConfig represents output configuration (e.g., database connection)
type OutputConfig struct {
	Type     string `yaml:"type"`
	Database string `yaml:"database"`
	// Additional fields can be added as needed for other database types
}

// envVarPattern matches ${VAR} or ${VAR:default}
var envVarPattern = regexp.MustCompile(`\$\{([^}:]+)(?::([^}]*))?\}`)

// LoadProfiles loads and parses a profiles.yml file
func LoadProfiles(path string) (*ProfilesConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg ProfilesConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse profiles config: %w", err)
	}

	// Apply environment variable expansion
	if err := cfg.expandEnvVars(); err != nil {
		return nil, fmt.Errorf("failed to expand environment variables: %w", err)
	}

	// Validate the configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid profiles config: %w", err)
	}

	return &cfg, nil
}

// expandEnvVars expands environment variables in all string fields
func (c *ProfilesConfig) expandEnvVars() error {
	if c.Default != nil {
		if err := c.Default.expandEnvVars(); err != nil {
			return err
		}
	}

	for _, profile := range c.Profiles {
		if profile != nil {
			if err := profile.expandEnvVars(); err != nil {
				return err
			}
		}
	}

	return nil
}

// expandEnvVars expands environment variables in profile
func (p *Profile) expandEnvVars() error {
	for _, output := range p.Outputs {
		if err := output.expandEnvVars(); err != nil {
			return err
		}
	}
	return nil
}

// expandEnvVars expands environment variables in output config
func (o *OutputConfig) expandEnvVars() error {
	var err error
	o.Database, err = expandEnvVar(o.Database)
	if err != nil {
		return err
	}
	return nil
}

// expandEnvVar expands a single string with ${VAR} or ${VAR:default} syntax
func expandEnvVar(s string) (string, error) {
	var expandErr error
	result := envVarPattern.ReplaceAllStringFunc(s, func(match string) string {
		// Extract variable name and default value
		matches := envVarPattern.FindStringSubmatch(match)
		if len(matches) < 2 {
			expandErr = fmt.Errorf("invalid environment variable syntax: %s", match)
			return match
		}

		varName := matches[1]
		defaultValue := ""
		hasDefault := len(matches) > 2 && matches[2] != ""
		if hasDefault {
			defaultValue = matches[2]
		}

		// Get environment variable value
		value := os.Getenv(varName)

		// If not set and no default, return error
		if value == "" && !hasDefault {
			expandErr = fmt.Errorf("required environment variable %q is not set", varName)
			return match
		}

		// Use value or default
		if value != "" {
			return value
		}
		return defaultValue
	})

	if expandErr != nil {
		return "", expandErr
	}

	return result, nil
}

// Validate checks that the profiles configuration is valid
func (c *ProfilesConfig) Validate() error {
	if c.Default == nil {
		return fmt.Errorf("default profile is required")
	}

	if err := c.Default.Validate(); err != nil {
		return fmt.Errorf("default profile: %w", err)
	}

	return nil
}

// Validate checks that the profile is valid
func (p *Profile) Validate() error {
	if p.Target == "" {
		return fmt.Errorf("target is required")
	}

	if len(p.Outputs) == 0 {
		return fmt.Errorf("at least one output is required")
	}

	// Validate that the target exists in outputs
	if _, ok := p.Outputs[p.Target]; !ok {
		return fmt.Errorf("target %q not found in outputs", p.Target)
	}

	// Validate each output
	for name, output := range p.Outputs {
		if err := output.Validate(); err != nil {
			return fmt.Errorf("output %q: %w", name, err)
		}
	}

	return nil
}

// Validate checks that the output configuration is valid
func (o *OutputConfig) Validate() error {
	if o.Type == "" {
		return fmt.Errorf("type is required")
	}

	// Type-specific validation
	switch strings.ToLower(o.Type) {
	case "sqlite":
		if o.Database == "" {
			return fmt.Errorf("database path is required for sqlite")
		}
	default:
		// Other database types can be added here
		return fmt.Errorf("unsupported database type: %s", o.Type)
	}

	return nil
}

// GetOutput returns the output configuration for the given target
func (c *ProfilesConfig) GetOutput(target string) (*OutputConfig, error) {
	if c.Default == nil {
		return nil, fmt.Errorf("no default profile configured")
	}

	output, ok := c.Default.Outputs[target]
	if !ok {
		return nil, fmt.Errorf("target %q not found in outputs", target)
	}

	return output, nil
}
