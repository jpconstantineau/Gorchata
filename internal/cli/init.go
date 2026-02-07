package cli

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
)

var projectNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// createProjectDirectories creates the project root directory and its subdirectories
func createProjectDirectories(projectPath string, force bool) error {
	// Check if directory exists
	if _, err := os.Stat(projectPath); err == nil {
		// Directory exists
		if !force {
			return fmt.Errorf("directory %s already exists (use --force to overwrite)", projectPath)
		}
		// Remove existing directory if force is true
		if err := os.RemoveAll(projectPath); err != nil {
			return fmt.Errorf("failed to remove existing directory: %w", err)
		}
	}

	// Create project root directory
	if err := os.MkdirAll(projectPath, 0755); err != nil {
		return fmt.Errorf("failed to create project directory: %w", err)
	}

	// Create subdirectories
	subdirs := []string{"models", "seeds", "tests", "macros"}
	for _, subdir := range subdirs {
		subdirPath := filepath.Join(projectPath, subdir)
		if err := os.MkdirAll(subdirPath, 0755); err != nil {
			return fmt.Errorf("failed to create subdirectory %s: %w", subdir, err)
		}
	}

	return nil
}

// InitCommand initializes a new Gorchata project
func InitCommand(args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)

	// Define flags
	help := fs.Bool("help", false, "Show help information")
	fs.BoolVar(help, "h", false, "Show help information (shorthand)")
	force := fs.Bool("force", false, "Force initialization even if directory exists")
	empty := fs.Bool("empty", false, "Create an empty project without example models")

	// Parse flags
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	// Handle help flag
	if *help {
		printInitHelp()
		return nil
	}

	// Get positional arguments (project name)
	positionalArgs := fs.Args()

	// Require project name
	if len(positionalArgs) == 0 {
		return fmt.Errorf("project name is required")
	}

	projectName := positionalArgs[0]

	// Validate project name
	if projectName == "" {
		return fmt.Errorf("project name cannot be empty")
	}

	if !projectNameRegex.MatchString(projectName) {
		return fmt.Errorf("invalid project name: must contain only alphanumeric characters, underscores, and hyphens")
	}

	// Create project directories
	if err := createProjectDirectories(projectName, *force); err != nil {
		return err
	}

	// Placeholder for remaining implementation
	_ = empty

	return fmt.Errorf("init command not yet implemented")
}

// printInitHelp prints help information for the init command
func printInitHelp() {
	fmt.Println("Initialize a new Gorchata project")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  gorchata init [project-name] [flags]")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  -h, --help      Show this help message")
	fmt.Println("  --force         Force initialization even if directory exists")
	fmt.Println("  --empty         Create an empty project without example models")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  gorchata init my_project")
	fmt.Println("  gorchata init my-project --empty")
	fmt.Println("  gorchata init my_project --force")
}
