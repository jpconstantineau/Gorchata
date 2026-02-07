package cli

import (
	"context"
	"flag"
	"fmt"
	"strings"

	"github.com/pierre/gorchata/internal/config"
	"github.com/pierre/gorchata/internal/domain/dag"
	"github.com/pierre/gorchata/internal/platform"
	"github.com/pierre/gorchata/internal/platform/sqlite"
	"github.com/pierre/gorchata/internal/template"
)

// RunCommand executes SQL transformations against the database
func RunCommand(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)

	var common CommonFlags
	AddCommonFlags(fs, &common)

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	// Load configuration
	cfg, err := config.Discover(common.Target)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Validate we have at least one model path
	if len(cfg.Project.ModelPaths) == 0 {
		return fmt.Errorf("no model paths configured in project")
	}

	// Create database adapter based on output type
	adapter, err := createAdapter(cfg.Output)
	if err != nil {
		return fmt.Errorf("failed to create database adapter: %w", err)
	}

	// Connect to database
	ctx := context.Background()
	if err := adapter.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer adapter.Close()

	if common.Verbose {
		fmt.Printf("Connected to %s database: %s\n", cfg.Output.Type, cfg.Output.Database)
	}

	// Build DAG from first model path
	builder := dag.NewBuilder()
	graph, err := builder.BuildFromDirectory(cfg.Project.ModelPaths[0])
	if err != nil {
		return fmt.Errorf("failed to build DAG: %w", err)
	}

	// Validate DAG
	if err := dag.Validate(graph); err != nil {
		return fmt.Errorf("DAG validation failed: %w", err)
	}

	// Get topologically sorted nodes
	sorted, err := dag.TopologicalSort(graph)
	if err != nil {
		return fmt.Errorf("failed to sort DAG: %w", err)
	}

	// Filter models if specified
	if common.Models != "" {
		sorted = filterModels(sorted, strings.Split(common.Models, ","))
	}

	if common.Verbose {
		fmt.Printf("Executing %d model(s)\n", len(sorted))
	}

	// Create template engine (tracker not needed for basic execution)
	engine := template.New()

	// Execute each model
	successCount := 0
	for _, node := range sorted {
		if common.Verbose {
			fmt.Printf("Running model: %s\n", node.Name)
		}

		// Get file content from node metadata
		content, ok := node.Metadata["content"].(string)
		if !ok {
			err := fmt.Errorf("model %s has no content", node.Name)
			if common.FailFast {
				return err
			}
			fmt.Printf("  Error: %v\n", err)
			continue
		}

		// Parse and render template
		tmplCtx := template.NewContext()
		tmpl, err := engine.Parse(node.Name, content)
		if err != nil {
			err = fmt.Errorf("failed to parse template for model %s: %w", node.Name, err)
			if common.FailFast {
				return err
			}
			fmt.Printf("  Error: %v\n", err)
			continue
		}

		sql, err := template.Render(tmpl, tmplCtx, nil)
		if err != nil {
			err = fmt.Errorf("failed to render template for model %s: %w", node.Name, err)
			if common.FailFast {
				return err
			}
			fmt.Printf("  Error: %v\n", err)
			continue
		}

		// Execute SQL against database
		if err := adapter.ExecuteDDL(ctx, sql); err != nil {
			err = fmt.Errorf("model %s execution failed: %w", node.Name, err)
			if common.FailFast {
				return err
			}
			fmt.Printf("  Error: %v\n", err)
			continue
		}

		successCount++

		if common.Verbose {
			fmt.Printf("  -> Success\n")
		}
	}

	if common.Verbose {
		fmt.Printf("\nExecuted %d/%d model(s) successfully\n", successCount, len(sorted))
	}

	return nil
}

// createAdapter creates a database adapter based on output configuration
func createAdapter(output *config.OutputConfig) (platform.DatabaseAdapter, error) {
	switch output.Type {
	case "sqlite":
		connConfig := &platform.ConnectionConfig{
			DatabasePath: output.Database,
		}
		return sqlite.NewSQLiteAdapter(connConfig), nil
	default:
		return nil, fmt.Errorf("unsupported database type: %s", output.Type)
	}
}
