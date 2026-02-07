package dag

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// Builder constructs a DAG from template files.
type Builder struct {
	// Additional fields can be added as needed
}

// NewBuilder creates a new DAG builder.
func NewBuilder() *Builder {
	return &Builder{}
}

// extractDependencies extracts model dependencies from SQL template content.
// It looks for {{ ref "model_name" }} or {{ ref 'model_name' }} patterns.
// Returns a sorted slice of unique model names.
func extractDependencies(content string) []string {
	// Pattern matches: {{ ref "model" }} or {{ref "model"}} or {{ ref 'model' }}
	// \{\{     - matches {{
	// \s*      - matches zero or more whitespace
	// ref      - matches literal "ref"
	// \s+      - matches one or more whitespace
	// ["']     - matches either " or '
	// ([^"']+) - captures one or more non-quote characters (the model name)
	// ["']     - matches either " or '
	// \s*      - matches zero or more whitespace
	// \}\}     - matches }}
	pattern := `\{\{\s*ref\s+["']([^"']+)["']\s*\}\}`
	re := regexp.MustCompile(pattern)

	matches := re.FindAllStringSubmatch(content, -1)

	// Extract unique model names
	depsMap := make(map[string]bool)
	for _, match := range matches {
		if len(match) > 1 {
			modelName := strings.TrimSpace(match[1])
			depsMap[modelName] = true
		}
	}

	// Convert to sorted slice for deterministic output
	deps := make([]string, 0, len(depsMap))
	for dep := range depsMap {
		deps = append(deps, dep)
	}
	sort.Strings(deps)

	return deps
}

// createNode creates a Node from a model file.
// Returns the node with metadata about the file and its dependencies.
func (b *Builder) createNode(path, name, content string) (*Node, error) {
	// Extract dependencies from the content
	deps := extractDependencies(content)

	// Create node with metadata
	node := &Node{
		ID:           "model_" + name,
		Name:         name,
		Type:         "model",
		Dependencies: []string{},
		Metadata: map[string]interface{}{
			"file_path": path,
			"content":   content,
		},
	}

	// Add "model_" prefix to each dependency for consistency
	for _, dep := range deps {
		node.Dependencies = append(node.Dependencies, "model_"+dep)
	}

	return node, nil
}

// BuildFromDirectory scans a directory for SQL model files and builds a dependency graph.
// It recursively searches for .sql files, extracts dependencies, and constructs the DAG.
// Returns the constructed graph or an error if the directory can't be read.
func (b *Builder) BuildFromDirectory(modelsDir string) (*Graph, error) {
	// Check if directory exists
	info, err := os.Stat(modelsDir)
	if err != nil {
		return nil, fmt.Errorf("cannot access directory '%s': %w", modelsDir, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("'%s' is not a directory", modelsDir)
	}

	g := NewGraph()

	// Walk the directory tree
	err = filepath.Walk(modelsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Process only .sql files
		if !strings.HasSuffix(strings.ToLower(info.Name()), ".sql") {
			return nil
		}

		// Read file content
		content, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("failed to read file '%s': %w", path, err)
		}

		// Extract model name from filename (without extension)
		modelName := strings.TrimSuffix(info.Name(), filepath.Ext(info.Name()))

		// Create node
		node, err := b.createNode(path, modelName, string(content))
		if err != nil {
			return fmt.Errorf("failed to create node for '%s': %w", path, err)
		}

		// Add node to graph
		if err := g.AddNode(node); err != nil {
			return fmt.Errorf("failed to add node '%s': %w", node.ID, err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Now add edges based on dependencies
	for _, node := range g.GetNodes() {
		for _, depID := range node.Dependencies {
			// Check if dependency exists in graph
			if _, exists := g.GetNode(depID); exists {
				// Add edge: this node depends on depID
				if err := g.AddEdge(node.ID, depID); err != nil {
					return nil, fmt.Errorf("failed to add edge from '%s' to '%s': %w", node.ID, depID, err)
				}
			}
			// Note: We don't error on missing dependencies here
			// because Validate() will catch them later
		}
	}

	return g, nil
}
