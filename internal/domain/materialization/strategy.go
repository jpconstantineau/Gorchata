package materialization

// Strategy defines the interface for materialization strategies
type Strategy interface {
	// Materialize generates and returns SQL statements to materialize the model
	// Parameters:
	//   - modelName: the name of the model/table/view to create
	//   - compiledSQL: the compiled SELECT query that defines the model
	//   - config: configuration for materialization behavior
	// Returns:
	//   - []string: ordered list of SQL statements to execute
	//   - error: any error encountered during SQL generation
	Materialize(modelName string, compiledSQL string, config MaterializationConfig) ([]string, error)

	// Name returns the strategy name (view, table, incremental)
	Name() string
}
