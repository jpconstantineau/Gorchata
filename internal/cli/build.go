package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/jpconstantineau/gorchata/internal/config"
	testExecutor "github.com/jpconstantineau/gorchata/internal/domain/test/executor"
	"github.com/jpconstantineau/gorchata/internal/domain/test/generic"
	"github.com/jpconstantineau/gorchata/internal/domain/test/storage"
	"github.com/jpconstantineau/gorchata/internal/platform"
)

// BuildCommand runs models and then tests (full build workflow)
func BuildCommand(args []string) error {
	fmt.Println("Running models...")

	// Run models using the run command logic
	if err := RunCommand(args); err != nil {
		return fmt.Errorf("model run failed: %w", err)
	}

	fmt.Println("\nModels completed successfully. Running tests...")

	// For tests, we need to load config and run tests directly
	// to avoid flag parsing conflicts
	cfg, err := config.Discover("")
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

	// Run tests
	if err := runTestsForBuild(ctx, cfg, adapter); err != nil {
		return fmt.Errorf("tests failed: %w", err)
	}

	fmt.Println("\nBuild completed successfully!")
	return nil
}

// runTestsForBuild executes tests as part of the build command
func runTestsForBuild(ctx context.Context, cfg *config.Config, adapter platform.DatabaseAdapter) error {
	// Create test registry
	registry := generic.NewDefaultRegistry()

	// Discover all tests
	allTests, err := testExecutor.DiscoverAllTests(cfg, registry)
	if err != nil {
		return fmt.Errorf("failed to discover tests: %w", err)
	}

	if len(allTests) == 0 {
		fmt.Println("No tests found")
		return nil
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
