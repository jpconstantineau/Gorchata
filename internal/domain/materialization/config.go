package materialization

// MaterializationType defines the type of materialization
type MaterializationType string

const (
	// MaterializationView creates a SQL view
	MaterializationView MaterializationType = "view"
	// MaterializationTable creates a full-refresh table
	MaterializationTable MaterializationType = "table"
	// MaterializationIncremental creates an incrementally updated table
	MaterializationIncremental MaterializationType = "incremental"
)

// MaterializationConfig holds configuration for how a model should be materialized
type MaterializationConfig struct {
	// Type specifies the materialization strategy (view, table, incremental)
	Type MaterializationType

	// UniqueKey is required for incremental materialization
	// It specifies the column(s) used to identify unique rows
	UniqueKey []string

	// FullRefresh forces a full refresh even for incremental models
	FullRefresh bool

	// PreHooks are SQL statements to execute before materialization
	PreHooks []string

	// PostHooks are SQL statements to execute after materialization
	PostHooks []string
}

// DefaultConfig returns a MaterializationConfig with sensible defaults
func DefaultConfig() MaterializationConfig {
	return MaterializationConfig{
		Type:        MaterializationTable,
		FullRefresh: false,
		UniqueKey:   []string{},
		PreHooks:    []string{},
		PostHooks:   []string{},
	}
}
