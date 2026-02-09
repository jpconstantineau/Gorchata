package cli

import (
	"fmt"
	"os"

	"github.com/jpconstantineau/gorchata/internal/config"
	"github.com/jpconstantineau/gorchata/internal/domain/seeds"
)

// LoadSeedsForTemplateContext loads seeds from configured paths and builds a Seeds map
// for use in template contexts. Returns the Seeds map (seedName -> qualified table name).
// If no seed paths are configured or no seeds are found, returns an empty map (not an error).
func LoadSeedsForTemplateContext(cfg *config.Config) (map[string]string, error) {
	// Return empty map if no seed paths configured
	if len(cfg.Project.SeedPaths) == 0 {
		return make(map[string]string), nil
	}

	// Load seed config (or use defaults)
	seedConfig := loadOrDefaultSeedConfig()

	// Load seeds from configured paths
	var allSeeds []*seedInfo
	for _, seedPath := range cfg.Project.SeedPaths {
		// Check if path exists
		if _, err := os.Stat(seedPath); os.IsNotExist(err) {
			// Skip non-existent paths
			continue
		}

		// Discover seed files using configured scope
		seedFiles, err := seeds.DiscoverSeeds(seedPath, seedConfig.Import.Scope)
		if err != nil {
			return nil, fmt.Errorf("failed to discover seeds in %s: %w", seedPath, err)
		}

		// Load each seed
		for _, seedFile := range seedFiles {
			// Parse CSV
			rows, err := seeds.ParseCSV(seedFile)
			if err != nil {
				return nil, fmt.Errorf("failed to parse CSV %s: %w", seedFile, err)
			}

			// Infer schema (use default sample size of 100)
			schema, err := seeds.InferSchema(rows, 100)
			if err != nil {
				return nil, fmt.Errorf("failed to infer schema for %s: %w", seedFile, err)
			}

			// Resolve table name
			tableName := seeds.ResolveTableName(seedFile, &seedConfig.Naming)

			// Create seed info
			info := &seedInfo{
				Seed: &seeds.Seed{
					ID:     tableName,
					Path:   seedFile,
					Type:   seeds.SeedTypeCSV,
					Schema: schema,
				},
			}

			allSeeds = append(allSeeds, info)
		}
	}

	// Build Seeds map (no schema qualification by default)
	seedsMap := buildSeedsMapForTemplate(allSeeds, "")

	return seedsMap, nil
}
