package cli

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var projectNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// projectConfigTemplate is the template for gorchata_project.yml
const projectConfigTemplate = `name: {{PROJECT_NAME}}
version: 1.0.0
profile: dev

model-paths:
  - models

vars:
  start_date: '{{CURRENT_YEAR}}-01-01'
`

// profilesTemplate is the template for profiles.yml
const profilesTemplate = `default:
  target: dev
  outputs:
    dev:
      type: sqlite
      database: "{{PROJECT_NAME}}.db"

prod:
  target: prod
  outputs:
    prod:
      type: sqlite
      database: "{{PROJECT_NAME}}_prod.db"
`

// stgUsersTemplate is the template for stg_users.sql
const stgUsersTemplate = `-- Stage clean users data
-- {{ config(materialized='view') }}

SELECT 
    id,
    name,
    email,
    created_at
FROM raw_users
WHERE deleted_at IS NULL
`

// stgOrdersTemplate is the template for stg_orders.sql
const stgOrdersTemplate = `-- Stage clean orders data
-- {{ config(materialized='view') }}

SELECT 
    id,
    user_id,
    amount,
    order_date
FROM raw_orders
WHERE status = 'completed'
`

// fctOrderSummaryTemplate is the template for fct_order_summary.sql
const fctOrderSummaryTemplate = `-- Create order summary fact table
-- {{ config(materialized='table') }}

SELECT 
    u.id as user_id,
    u.name as user_name,
    u.email,
    COUNT(o.id) as order_count,
    SUM(o.amount) as total_amount,
    MAX(o.order_date) as last_order_date
FROM {{ ref "stg_users" }} u
LEFT JOIN {{ ref "stg_orders" }} o ON u.id = o.user_id
GROUP BY u.id, u.name, u.email
`

// generateModels creates the sample SQL model files in the models/ subdirectory
func generateModels(projectPath string) error {
	modelsDir := filepath.Join(projectPath, "models")

	// Define model files to create
	models := map[string]string{
		"stg_users.sql":         stgUsersTemplate,
		"stg_orders.sql":        stgOrdersTemplate,
		"fct_order_summary.sql": fctOrderSummaryTemplate,
	}

	// Write each model file
	for filename, content := range models {
		filePath := filepath.Join(modelsDir, filename)
		if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
			return fmt.Errorf("failed to write model file %s: %w", filename, err)
		}
	}

	return nil
}

// generateProfiles creates the profiles.yml file with project-specific values
func generateProfiles(projectPath, projectName string) error {
	// Replace placeholder in template
	content := strings.ReplaceAll(profilesTemplate, "{{PROJECT_NAME}}", projectName)

	// Write file
	profilesPath := filepath.Join(projectPath, "profiles.yml")
	if err := os.WriteFile(profilesPath, []byte(content), 0644); err != nil {
		return fmt.Errorf("failed to write profiles file: %w", err)
	}

	return nil
}

// generateProjectConfig creates the gorchata_project.yml file with project-specific values
func generateProjectConfig(projectPath, projectName string) error {
	// Get current year
	currentYear := time.Now().Year()

	// Replace placeholders in template
	content := strings.ReplaceAll(projectConfigTemplate, "{{PROJECT_NAME}}", projectName)
	content = strings.ReplaceAll(content, "{{CURRENT_YEAR}}", fmt.Sprintf("%d", currentYear))

	// Write file
	configPath := filepath.Join(projectPath, "gorchata_project.yml")
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		return fmt.Errorf("failed to write project config file: %w", err)
	}

	return nil
}

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

	// Generate project config file
	if err := generateProjectConfig(projectName, projectName); err != nil {
		return err
	}

	// Generate profiles file
	if err := generateProfiles(projectName, projectName); err != nil {
		return err
	}

	// Generate model files (only if not empty)
	if !*empty {
		if err := generateModels(projectName); err != nil {
			return fmt.Errorf("failed to generate models: %w", err)
		}
	}

	// Print success message
	fmt.Printf("✓ Successfully initialized Gorchata project: %s\n\n", projectName)
	fmt.Println("Created:")
	fmt.Println("  - Project configuration: gorchata_project.yml")
	fmt.Println("  - Database profiles: profiles.yml")
	fmt.Println("  - Folders: models/, seeds/, tests/, macros/")
	if *empty {
		fmt.Println("  - Sample models: 0 SQL files (empty project)")
	} else {
		fmt.Println("  - Sample models: 3 SQL files")
	}
	fmt.Println()
	fmt.Println("Next steps:")
	fmt.Printf("  1. cd %s\n", projectName)
	fmt.Println("  2. gorchata run")
	fmt.Println()
	fmt.Println("Run 'gorchata --help' for more commands.")

	return nil
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
