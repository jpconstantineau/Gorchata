package config

// Default configuration values for gorchata projects

// Default paths
const (
	DefaultModelPath  = "models"
	DefaultSeedPath   = "seeds"
	DefaultTestPath   = "tests"
	DefaultMacroPath  = "macros"
	DefaultTargetPath = "target"
)

// Default materialization strategies
const (
	DefaultMaterializationStrategy = "view"
	TableMaterialization           = "table"
	ViewMaterialization            = "view"
	EphemeralMaterialization       = "ephemeral"
)

// Default schema names
const (
	DefaultSchema = "main"
)

// Default database configuration
const (
	DefaultDatabaseType = "sqlite"
	DefaultDatabasePath = "./gorchata.db"
)

// GetDefaultModelPaths returns the default model paths
func GetDefaultModelPaths() []string {
	return []string{DefaultModelPath}
}

// GetDefaultSeedPaths returns the default seed paths
func GetDefaultSeedPaths() []string {
	return []string{DefaultSeedPath}
}

// GetDefaultTestPaths returns the default test paths
func GetDefaultTestPaths() []string {
	return []string{DefaultTestPath}
}

// GetDefaultMacroPaths returns the default macro paths
func GetDefaultMacroPaths() []string {
	return []string{DefaultMacroPath}
}
