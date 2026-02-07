package cli

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jpconstantineau/gorchata/internal/config"
	"github.com/jpconstantineau/gorchata/internal/domain/dag"
	"github.com/jpconstantineau/gorchata/internal/template"
)

// CompileCommand compiles SQL templates without executing them
func CompileCommand(args []string) error {
	fs := flag.NewFlagSet("compile", flag.ContinueOnError)

	var outputDir string
	var common CommonFlags

	fs.StringVar(&outputDir, "output-dir", "", "Directory to write compiled SQL files (default: stdout)")
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

	// Create template engine
	engine := template.New()

	// Compile each model
	for _, node := range sorted {
		if common.Verbose {
			fmt.Printf("Compiling model: %s\n", node.Name)
		}

		// Get file content from node metadata
		content, ok := node.Metadata["content"].(string)
		if !ok {
			return fmt.Errorf("model %s has no content", node.Name)
		}

		// Parse and render template
		ctx := template.NewContext()
		tmpl, err := engine.Parse(node.Name, content)
		if err != nil {
			return fmt.Errorf("failed to parse template for model %s: %w", node.Name, err)
		}

		sql, err := template.Render(tmpl, ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to render template for model %s: %w", node.Name, err)
		}

		// Output result
		if outputDir != "" {
			// Write to file
			outputPath := filepath.Join(outputDir, node.Name+".sql")
			if err := os.WriteFile(outputPath, []byte(sql), 0644); err != nil {
				return fmt.Errorf("failed to write output file %s: %w", outputPath, err)
			}
			if common.Verbose {
				fmt.Printf("  -> %s\n", outputPath)
			}
		} else {
			// Write to stdout
			fmt.Printf("-- Model: %s\n", node.Name)
			fmt.Println(sql)
			fmt.Println()
		}
	}

	if !common.Verbose && outputDir == "" {
		fmt.Fprintf(os.Stderr, "Compiled %d model(s)\n", len(sorted))
	} else if common.Verbose {
		fmt.Printf("\nCompiled %d model(s) successfully\n", len(sorted))
	}

	return nil
}

// filterModels filters a slice of nodes to only include specified models
func filterModels(nodes []*dag.Node, modelNames []string) []*dag.Node {
	// Create a set of model names for fast lookup
	nameSet := make(map[string]bool)
	for _, name := range modelNames {
		nameSet[strings.TrimSpace(name)] = true
	}

	// Filter nodes
	filtered := make([]*dag.Node, 0)
	for _, node := range nodes {
		if nameSet[node.Name] {
			filtered = append(filtered, node)
		}
	}

	return filtered
}
