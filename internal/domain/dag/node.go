package dag

// Node represents a node in the dependency graph.
// Each node corresponds to a model or data transformation with its dependencies.
type Node struct {
	// ID is the unique identifier for the node (e.g., "model_users")
	ID string

	// Name is the human-readable name (e.g., "users")
	Name string

	// Type indicates the node type (e.g., "model", "seed", "snapshot")
	Type string

	// Dependencies lists the IDs of nodes this node depends on
	Dependencies []string

	// Metadata stores additional information about the node
	// (e.g., file path, SQL content, configuration)
	Metadata map[string]interface{}
}
