package cli

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/jpconstantineau/gorchata/internal/config"
	"github.com/jpconstantineau/gorchata/internal/domain/executor"
	"github.com/jpconstantineau/gorchata/internal/domain/materialization"
	testExecutor "github.com/jpconstantineau/gorchata/internal/domain/test/executor"
	"github.com/jpconstantineau/gorchata/internal/domain/test/generic"
	"github.com/jpconstantineau/gorchata/internal/domain/test/storage"
	"github.com/jpconstantineau/gorchata/internal/platform"
	"github.com/jpconstantineau/gorchata/internal/platform/sqlite"
	"github.com/jpconstantineau/gorchata/internal/template"
)

// RunCommand executes SQL transformations against the database
func RunCommand(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)

	var common CommonFlags
	AddCommonFlags(fs, &common)

	// Add --test flag for run command
	runTests := fs.Bool("test", false, "Run tests after executing models")

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

	// Load models from model paths
	var allModels []*executor.Model
	for _, modelPath := range cfg.Project.ModelPaths {
		models, err := loadModelsFromDirectory(modelPath)
		if err != nil {
			return fmt.Errorf("failed to load models from %s: %w", modelPath, err)
		}
		allModels = append(allModels, models...)
	}

	if len(allModels) == 0 {
		return fmt.Errorf("no models found in model paths")
	}

	if common.Verbose {
		fmt.Printf("Found %d model(s)\n", len(allModels))
	}

	// Load seeds for template context
	seedsMap, err := LoadSeedsForTemplateContext(cfg)
	if err != nil {
		return fmt.Errorf("failed to load seeds: %w", err)
	}

	// Parse templates and extract config/dependencies
	templateEngine := template.New()
	tracker := newSimpleDependencyTracker()
	templateEngineWithTracker := template.New(template.WithDependencyTracker(tracker))

	for _, model := range allModels {
		// Read model content
		content, err := os.ReadFile(model.Path)
		if err != nil {
			return fmt.Errorf("failed to read model %s: %w", model.ID, err)
		}

		contentStr := string(content)

		// Extract config from template
		cfg := extractModelConfig(contentStr)

		// Apply --full-refresh flag if set
		if common.FullRefresh && cfg.Type == materialization.MaterializationIncremental {
			cfg.FullRefresh = true
		}

		model.SetMaterializationConfig(cfg)

		// Remove config() calls before parsing
		contentStr = removeConfigCalls(contentStr)

		// Store template content for engine to re-render with incremental context
		model.SetTemplateContent(contentStr)

		// Parse template
		tmpl, err := templateEngineWithTracker.Parse(model.ID, contentStr)
		if err != nil {
			return fmt.Errorf("failed to parse template %s: %w", model.ID, err)
		}

		// Render template
		ctx := template.NewContext(
			template.WithCurrentModel(model.ID),
		)
		ctx.Seeds = seedsMap

		rendered, err := template.Render(tmpl, ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to render template %s: %w", model.ID, err)
		}

		model.SetCompiledSQL(rendered)

		// Extract dependencies
		deps := tracker.GetDependencies(model.ID)
		for _, dep := range deps {
			model.AddDependency(dep)
		}
	}

	// Filter models if specified
	if common.Models != "" {
		modelNames := strings.Split(common.Models, ",")
		allModels = filterModelsByName(allModels, modelNames)
	}

	if common.Verbose {
		fmt.Printf("Executing %d model(s)\n", len(allModels))
	}

	// Create execution engine
	engine, err := executor.NewEngine(adapter, templateEngine)
	if err != nil {
		return fmt.Errorf("failed to create execution engine: %w", err)
	}

	// Execute models
	result, err := engine.ExecuteModels(ctx, allModels, common.FailFast)
	if err != nil {
		return fmt.Errorf("execution failed: %w", err)
	}

	// Print results
	if common.Verbose {
		fmt.Printf("\n")
		for _, mr := range result.ModelResults {
			status := "✓"
			if mr.Status == executor.StatusFailed {
				status = "✗"
			}
			fmt.Printf("  %s %s (%.2fs)\n", status, mr.ModelID, mr.Duration().Seconds())
			if mr.Error != "" {
				fmt.Printf("    Error: %s\n", mr.Error)
			}
		}
		fmt.Printf("\n")
	}

	fmt.Printf("Executed %d/%d model(s) successfully in %.2fs\n",
		result.SuccessCount(),
		len(result.ModelResults),
		result.Duration().Seconds())

	if result.FailureCount() > 0 {
		return fmt.Errorf("%d model(s) failed", result.FailureCount())
	}

	// Run tests if --test flag is set
	if *runTests {
		fmt.Println("\n========================================")
		fmt.Println("Running tests...")
		fmt.Println("========================================")

		// Run tests using TestCommand logic
		if err := runTestsAfterModels(ctx, cfg, adapter, common.Verbose); err != nil {
			return fmt.Errorf("tests failed: %w", err)
		}
	}

	return nil
}

// runTestsAfterModels executes tests after models have been run
func runTestsAfterModels(ctx context.Context, cfg *config.Config, adapter platform.DatabaseAdapter, verbose bool) error {
	// Create test registry
	registry := generic.NewDefaultRegistry()

	// Discover all tests
	if verbose {
		fmt.Println("Discovering tests...")
	}

	allTests, err := testExecutor.DiscoverAllTests(cfg, registry)
	if err != nil {
		return fmt.Errorf("failed to discover tests: %w", err)
	}

	if len(allTests) == 0 {
		fmt.Println("No tests found")
		return nil
	}

	if verbose {
		fmt.Printf("Found %d test(s)\n", len(allTests))
	}

	fmt.Printf("Running %d test(s)...\n\n", len(allTests))

	// Create and initialize failure store
	failureStore := storage.NewSQLiteFailureStore(adapter)
	if failureStore != nil {
		if err := failureStore.Initialize(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to initialize failure store: %v\n", err)
			failureStore = nil
		}
	}

	// Create test engine
	engine, err := testExecutor.NewTestEngine(adapter, nil, failureStore)
	if err != nil {
		return fmt.Errorf("failed to create test engine: %w", err)
	}

	// Create result writers
	consoleWriter := testExecutor.NewConsoleResultWriter(os.Stdout, true)
	jsonWriter := testExecutor.NewJSONResultWriter("target/test_results.json")

	// Execute tests
	summary, err := engine.ExecuteTests(ctx, allTests)
	if err != nil {
		return fmt.Errorf("failed to execute tests: %w", err)
	}

	// Write results
	for _, result := range summary.TestResults {
		consoleWriter.Write(result)
		jsonWriter.Write(result)
	}

	// Write summary
	consoleWriter.WriteSummary(summary)
	jsonWriter.WriteSummary(summary)

	// Run cleanup if failure store was initialized
	if failureStore != nil {
		cleanupConfig := storage.DefaultCleanupConfig()
		if err := storage.CleanupOldFailures(ctx, failureStore, cleanupConfig); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: cleanup failed: %v\n", err)
		}
	}

	// Return error if failures
	if summary.FailedTests > 0 {
		return fmt.Errorf("%d test(s) failed", summary.FailedTests)
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

// loadModelsFromDirectory loads all SQL models from a directory
func loadModelsFromDirectory(dir string) ([]*executor.Model, error) {
	var models []*executor.Model

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		modelID := strings.TrimSuffix(entry.Name(), ".sql")
		modelPath := filepath.Join(dir, entry.Name())

		model, err := executor.NewModel(modelID, modelPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create model %s: %w", modelID, err)
		}

		models = append(models, model)
	}

	return models, nil
}

// extractModelConfig extracts materialization config from SQL template
func extractModelConfig(content string) materialization.MaterializationConfig {
	config := materialization.DefaultConfig()

	// Look for {{ config "materialized" "view" }} pattern (Go template syntax)
	goTemplateRe := regexp.MustCompile(`{{\s*config\s+"materialized"\s+"(\w+)"\s*}}`)
	matches := goTemplateRe.FindStringSubmatch(content)

	if len(matches) > 1 {
		switch matches[1] {
		case "view":
			config.Type = materialization.MaterializationView
		case "table":
			config.Type = materialization.MaterializationTable
		case "incremental":
			config.Type = materialization.MaterializationIncremental
		}
		return config
	}

	// Fall back to {{ config(materialized='view') }} pattern (legacy Jinja-style syntax)
	legacyRe := regexp.MustCompile(`{{\s*config\s*\(\s*materialized\s*=\s*['"](\w+)['"]\s*\)\s*}}`)
	matches = legacyRe.FindStringSubmatch(content)

	if len(matches) > 1 {
		switch matches[1] {
		case "view":
			config.Type = materialization.MaterializationView
		case "table":
			config.Type = materialization.MaterializationTable
		case "incremental":
			config.Type = materialization.MaterializationIncremental
		}
		return config
	}

	// Fall back to -- Materialization: table comment (old format)
	oldFormatRe := regexp.MustCompile(`--\s*Materialization:\s*(\w+)`)
	matches = oldFormatRe.FindStringSubmatch(content)

	if len(matches) > 1 {
		switch matches[1] {
		case "view":
			config.Type = materialization.MaterializationView
		case "table":
			config.Type = materialization.MaterializationTable
		case "incremental":
			config.Type = materialization.MaterializationIncremental
		}
	}

	return config
}

// removeConfigCalls removes {{ config ... }} from content (both Go template and legacy syntax)
func removeConfigCalls(content string) string {
	// Remove Go template syntax: {{ config "key" "value" }}
	goTemplateRe := regexp.MustCompile(`{{\s*config\s+"[^"]+"\s+"[^"]+"\s*}}`)
	content = goTemplateRe.ReplaceAllString(content, "")

	// Remove legacy Jinja-style syntax: {{ config(key='value') }}
	legacyRe := regexp.MustCompile(`{{\s*config\s*\([^}]+\)\s*}}`)
	return legacyRe.ReplaceAllString(content, "")
}

// simpleDependencyTracker tracks template dependencies
type simpleDependencyTracker struct {
	dependencies map[string][]string
}

func newSimpleDependencyTracker() *simpleDependencyTracker {
	return &simpleDependencyTracker{
		dependencies: make(map[string][]string),
	}
}

func (t *simpleDependencyTracker) AddDependency(from, to string) error {
	t.dependencies[from] = append(t.dependencies[from], to)
	return nil
}

func (t *simpleDependencyTracker) GetDependencies(modelID string) []string {
	return t.dependencies[modelID]
}

// filterModelsByName filters models by name
func filterModelsByName(models []*executor.Model, names []string) []*executor.Model {
	nameSet := make(map[string]bool)
	for _, name := range names {
		nameSet[strings.TrimSpace(name)] = true
	}

	var filtered []*executor.Model
	for _, model := range models {
		if nameSet[model.ID] {
			filtered = append(filtered, model)
		}
	}

	return filtered
}
