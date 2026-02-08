package cli

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/jpconstantineau/gorchata/internal/config"
	"github.com/jpconstantineau/gorchata/internal/domain/test/executor"
	"github.com/jpconstantineau/gorchata/internal/domain/test/generic"
	"github.com/jpconstantineau/gorchata/internal/domain/test/storage"
)

// TestCommand executes data quality tests
func TestCommand(args []string) error {
	fs := flag.NewFlagSet("gorchata-test", flag.ContinueOnError)

	var common CommonFlags
	AddCommonFlags(fs, &common)

	// Test-specific flags
	selectFlag := fs.String("select", "", "Run tests matching pattern")
	excludeFlag := fs.String("exclude", "", "Exclude tests matching pattern")
	models := fs.String("models", "", "Test models matching pattern")
	tags := fs.String("tags", "", "Test with tags (comma-separated)")
	failFast := fs.Bool("fail-fast", false, "Stop on first failure")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	// Load configuration
	cfg, err := config.Discover(common.Target)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Create database adapter
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

	// Create test registry
	registry := generic.NewDefaultRegistry()

	// Discover all tests
	if common.Verbose {
		fmt.Println("Discovering tests...")
	}

	allTests, err := executor.DiscoverAllTests(cfg, registry)
	if err != nil {
		return fmt.Errorf("failed to discover tests: %w", err)
	}

	if common.Verbose {
		fmt.Printf("Found %d test(s)\n", len(allTests))
	}

	if len(allTests) == 0 {
		fmt.Println("No tests found")
		return nil
	}

	// Build selector from flags
	var includes []string
	if *selectFlag != "" {
		includes = []string{*selectFlag}
	}

	var excludes []string
	if *excludeFlag != "" {
		excludes = []string{*excludeFlag}
	}

	var modelFilters []string
	if *models != "" {
		modelFilters = []string{*models}
	}

	var tagFilters []string
	if *tags != "" {
		// Split comma-separated tags
		tagFilters = splitCommaSeparated(*tags)
	}

	selector := executor.NewTestSelector(includes, excludes, tagFilters, modelFilters)
	selectedTests := selector.Filter(allTests)

	if len(selectedTests) == 0 {
		fmt.Println("No tests matched the selection criteria")
		return nil
	}

	fmt.Printf("Running %d test(s)...\n\n", len(selectedTests))

	// Create and initialize failure store
	failureStore := storage.NewSQLiteFailureStore(adapter)
	if failureStore != nil {
		if err := failureStore.Initialize(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to initialize failure store: %v\n", err)
			failureStore = nil
		}
	}

	// Create test engine
	engine, err := executor.NewTestEngine(adapter, nil, failureStore)
	if err != nil {
		return fmt.Errorf("failed to create test engine: %w", err)
	}

	// Create result writers
	consoleWriter := executor.NewConsoleResultWriter(os.Stdout, true)
	jsonWriter := executor.NewJSONResultWriter("target/test_results.json")

	// Execute tests
	summary, err := engine.ExecuteTests(ctx, selectedTests)
	if err != nil {
		return fmt.Errorf("failed to execute tests: %w", err)
	}

	// Write results
	for _, result := range summary.TestResults {
		consoleWriter.Write(result)
		jsonWriter.Write(result)

		// Check fail-fast
		if *failFast && result.Status == "failed" {
			break
		}
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

	// Exit with non-zero if failures
	if summary.FailedTests > 0 {
		return fmt.Errorf("tests failed: %d failures", summary.FailedTests)
	}

	return nil
}

// splitCommaSeparated splits a comma-separated string into a slice
func splitCommaSeparated(s string) []string {
	if s == "" {
		return []string{}
	}

	var result []string
	for _, part := range splitString(s, ",") {
		trimmed := trimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// splitString splits a string by separator
func splitString(s, sep string) []string {
	if s == "" {
		return []string{}
	}

	var result []string
	start := 0
	for i := 0; i < len(s); i++ {
		if i+len(sep) <= len(s) && s[i:i+len(sep)] == sep {
			result = append(result, s[start:i])
			start = i + len(sep)
			i += len(sep) - 1
		}
	}
	result = append(result, s[start:])
	return result
}

// trimSpace removes leading and trailing whitespace
func trimSpace(s string) string {
	start := 0
	end := len(s)

	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n' || s[start] == '\r') {
		start++
	}

	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n' || s[end-1] == '\r') {
		end--
	}

	return s[start:end]
}
