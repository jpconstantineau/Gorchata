package config

import (
	"fmt"
	"os"
	"path/filepath"
)

// Config represents the complete configuration combining project and profiles
type Config struct {
	Project  *ProjectConfig
	Profiles *ProfilesConfig
	Output   *OutputConfig
}

// Load loads both project and profiles configuration and selects the target output
func Load(projectPath, profilesPath, target string) (*Config, error) {
	// Load project configuration
	project, err := LoadProject(projectPath)
	if err != nil {
		return nil, err
	}

	// Load profiles configuration
	profiles, err := LoadProfiles(profilesPath)
	if err != nil {
		return nil, err
	}

	// If target is empty, use default from profiles
	if target == "" {
		if profiles.Default != nil {
			target = profiles.Default.Target
		} else {
			return nil, fmt.Errorf("no default target specified")
		}
	}

	// Get the output for the target
	output, err := profiles.GetOutput(target)
	if err != nil {
		return nil, fmt.Errorf("failed to get output for target %q: %w", target, err)
	}

	return &Config{
		Project:  project,
		Profiles: profiles,
		Output:   output,
	}, nil
}

// Discover searches for config files in the current directory and parent directories
func Discover(target string) (*Config, error) {
	projectPath, err := findConfigFile("gorchata_project.yml")
	if err != nil {
		return nil, fmt.Errorf("failed to find gorchata_project.yml: %w", err)
	}

	profilesPath, err := findConfigFile("profiles.yml")
	if err != nil {
		return nil, fmt.Errorf("failed to find profiles.yml: %w", err)
	}

	return Load(projectPath, profilesPath, target)
}

// findConfigFile searches for a config file in current directory and parent directories
func findConfigFile(filename string) (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	// Search up to root directory
	for {
		configPath := filepath.Join(dir, filename)
		if _, err := os.Stat(configPath); err == nil {
			return configPath, nil
		}

		// Check if we've reached the root
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	return "", fmt.Errorf("%s not found in current directory or any parent directory", filename)
}
