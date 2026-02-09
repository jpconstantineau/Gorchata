package cli

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jpconstantineau/gorchata/internal/config"
	"github.com/jpconstantineau/gorchata/internal/domain/seeds"
	"github.com/jpconstantineau/gorchata/internal/platform"
)

// SeedCommand executes seed data loading against the database
func SeedCommand(args []string) error {
	fs := flag.NewFlagSet("seed", flag.ContinueOnError)

	var common CommonFlags
	AddCommonFlags(fs, &common)

	// Add seed-specific flags
	selectFlag := fs.String("select", "", "Specific seed(s) to run (comma-separated)")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("failed to parse flags: %w", err)
	}

	// Load configuration
	cfg, err := config.Discover(common.Target)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Validate we have at least one seed path
	if len(cfg.Project.SeedPaths) == 0 {
		if common.Verbose {
			fmt.Println("No seed paths configured in project")
		}
		return nil
	}

	// Load seed config (or use defaults)
	seedConfig := loadOrDefaultSeedConfig()

	if common.Verbose {
		fmt.Printf("Discovering seeds from %d path(s)...\n", len(cfg.Project.SeedPaths))
	}

	// Load seeds from configured paths
	seedsList, err := loadSeedsFromPaths(cfg.Project.SeedPaths, seedConfig)
	if err != nil {
		return fmt.Errorf("failed to load seeds: %w", err)
	}

	if len(seedsList) == 0 {
		if common.Verbose {
			fmt.Println("No seeds found")
		}
		return nil
	}

	// Filter by --select if specified
	if *selectFlag != "" {
		selectNames := strings.Split(*selectFlag, ",")
		seedsList = filterSeeds(seedsList, selectNames)
		if len(seedsList) == 0 {
			return fmt.Errorf("no seeds matched selection: %s", *selectFlag)
		}
	}

	if common.Verbose {
		fmt.Printf("Found %d seed(s) to execute\n", len(seedsList))
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

	// Execute seeds
	if err := executeSeeds(ctx, adapter, seedsList, seedConfig, common.Verbose, common.FullRefresh); err != nil {
		return fmt.Errorf("seed execution failed: %w", err)
	}

	return nil
}

// loadOrDefaultSeedConfig loads seed config from seed.yml or returns defaults
func loadOrDefaultSeedConfig() *config.SeedConfig {
	// Try to load seed.yml from current directory
	seedConfigPath := "seed.yml"
	if _, err := os.Stat(seedConfigPath); err == nil {
		cfg, err := config.ParseSeedConfig(seedConfigPath)
		if err == nil {
			return cfg
		}
	}

	// Return defaults
	return &config.SeedConfig{
		Version: 1,
		Naming: config.NamingConfig{
			Strategy: config.NamingStrategyFilename,
		},
		Import: config.ImportConfig{
			BatchSize: 1000,
			Scope:     config.ScopeTree,
		},
	}
}

// loadSeedsFromPaths discovers and loads seeds from the configured paths
func loadSeedsFromPaths(seedPaths []string, seedConfig *config.SeedConfig) ([]*seedInfo, error) {
	var allSeeds []*seedInfo

	for _, seedPath := range seedPaths {
		// Check if path exists
		if _, err := os.Stat(seedPath); os.IsNotExist(err) {
			// Skip non-existent paths (they may not have been created yet)
			continue
		}

		// Discover seed files using configured scope
		seedFiles, err := seeds.DiscoverSeeds(seedPath, seedConfig.Import.Scope)
		if err != nil {
			return nil, fmt.Errorf("failed to discover seeds in %s: %w", seedPath, err)
		}

		// Load each seed
		for _, seedFile := range seedFiles {
			// Detect file type
			ext := strings.ToLower(filepath.Ext(seedFile))

			if ext == ".csv" {
				// Handle CSV seed
				// Parse CSV
				rows, err := seeds.ParseCSV(seedFile)
				if err != nil {
					return nil, fmt.Errorf("failed to parse CSV %s: %w", seedFile, err)
				}

				// Infer schema (use default sample size of 100, no overrides)
				schema, err := seeds.InferSchema(rows, 100, nil)
				if err != nil {
					return nil, fmt.Errorf("failed to infer schema for %s: %w", seedFile, err)
				}

				// Resolve table name
				tableName := seeds.ResolveTableName(seedFile, &seedConfig.Naming)

				// Create seed info
				// Note: rows[0] is headers (used by InferSchema), rows[1:] is data
				dataRows := rows[1:]
				info := &seedInfo{
					Seed: &seeds.Seed{
						ID:     tableName,
						Path:   seedFile,
						Type:   seeds.SeedTypeCSV,
						Schema: schema,
					},
					Rows: dataRows,
				}

				allSeeds = append(allSeeds, info)

			} else if ext == ".sql" {
				// Handle SQL seed
				// Read SQL content
				sqlContent, err := os.ReadFile(seedFile)
				if err != nil {
					return nil, fmt.Errorf("failed to read SQL file %s: %w", seedFile, err)
				}

				// Resolve table name (for identification purposes)
				tableName := seeds.ResolveTableName(seedFile, &seedConfig.Naming)

				// Create seed info (no schema needed for SQL seeds)
				info := &seedInfo{
					Seed: &seeds.Seed{
						ID:     tableName,
						Path:   seedFile,
						Type:   seeds.SeedTypeSQL,
						Schema: nil, // SQL seeds don't need schema
					},
					SQLContent: string(sqlContent),
				}

				allSeeds = append(allSeeds, info)
			}
		}
	}

	return allSeeds, nil
}

// seedInfo holds a seed and its parsed data (rows for CSV, content for SQL)
type seedInfo struct {
	Seed       *seeds.Seed
	Rows       [][]string // For CSV seeds
	SQLContent string     // For SQL seeds
}

// filterSeeds filters seeds by name
func filterSeeds(seedsList []*seedInfo, selectNames []string) []*seedInfo {
	// Create lookup map for selected names
	selected := make(map[string]bool)
	for _, name := range selectNames {
		selected[strings.TrimSpace(name)] = true
	}

	// Filter seeds
	var filtered []*seedInfo
	for _, info := range seedsList {
		if selected[info.Seed.ID] {
			filtered = append(filtered, info)
		}
	}

	return filtered
}

// executeSeeds executes all seeds in sequence
func executeSeeds(ctx context.Context, adapter platform.DatabaseAdapter, seedsList []*seedInfo, seedConfig *config.SeedConfig, verbose bool, fullRefresh bool) error {
	successCount := 0
	failureCount := 0

	for _, info := range seedsList {
		if verbose {
			fmt.Printf("Executing seed: %s (from %s)...\n", info.Seed.ID, filepath.Base(info.Seed.Path))
		}

		var err error

		// Branch based on seed type
		if info.Seed.Type == seeds.SeedTypeSQL {
			// Execute SQL seed
			// TODO: Support vars from config or flags
			vars := make(map[string]interface{})
			err = seeds.ExecuteSQLSeed(ctx, adapter, info.SQLContent, vars, nil)
			if err != nil {
				failureCount++
				if verbose {
					fmt.Printf("  ✗ Failed: %s\n", err)
				}
				return fmt.Errorf("SQL seed %s failed: %w", info.Seed.ID, err)
			}

			successCount++
			if verbose {
				fmt.Printf("  ✓ Success: SQL seed executed\n")
			}

		} else {
			// Execute CSV seed
			result, err := seeds.ExecuteSeed(ctx, adapter, info.Seed, info.Rows, seedConfig)
			if err != nil {
				failureCount++
				if verbose {
					fmt.Printf("  ✗ Failed: %s\n", err)
				}
				return fmt.Errorf("seed %s failed: %w", info.Seed.ID, err)
			}

			if result.Status == seeds.StatusSuccess {
				successCount++
				if verbose {
					fmt.Printf("  ✓ Success: loaded %d rows\n", result.RowsLoaded)
				}
			} else {
				failureCount++
				if verbose {
					fmt.Printf("  ✗ Failed: %s\n", result.Error)
				}
				return fmt.Errorf("seed %s failed: %s", info.Seed.ID, result.Error)
			}
		}
	}

	fmt.Printf("\nExecuted %d/%d seed(s) successfully\n", successCount, successCount+failureCount)

	if failureCount > 0 {
		return fmt.Errorf("%d seed(s) failed", failureCount)
	}

	return nil
}

// buildSeedsMapForTemplate builds a Seeds map for template context from loaded seeds
// Maps seedName -> qualified table name (with schema prefix if configured)
func buildSeedsMapForTemplate(seedsList []*seedInfo, schema string) map[string]string {
	seedsMap := make(map[string]string)
	for _, info := range seedsList {
		// Build qualified table name
		tableName := info.Seed.ID
		if schema != "" {
			tableName = schema + "." + info.Seed.ID
		}

		// Store in ResolvedTableName for future use
		info.Seed.ResolvedTableName = tableName

		// Add to map
		seedsMap[info.Seed.ID] = tableName
	}
	return seedsMap
}
