package executor

import (
	"fmt"

	"github.com/pierre/gorchata/internal/domain/materialization"
)

// Model represents a single data transformation (SQL model)
type Model struct {
	// ID is the unique identifier for the model (e.g., "stg_users")
	ID string

	// Path is the file path to the model template (e.g., "models/staging/stg_users.sql")
	Path string

	// CompiledSQL is the rendered SQL after template processing
	CompiledSQL string

	// MaterializationConfig defines how the model should be materialized
	MaterializationConfig materialization.MaterializationConfig

	// Dependencies is a list of model IDs that this model depends on
	Dependencies []string

	// Metadata stores arbitrary key-value pairs for the model
	Metadata map[string]interface{}
}

// NewModel creates a new Model instance with validation
func NewModel(id, path string) (*Model, error) {
	if id == "" {
		return nil, fmt.Errorf("model ID cannot be empty")
	}
	if path == "" {
		return nil, fmt.Errorf("model path cannot be empty")
	}

	return &Model{
		ID:                    id,
		Path:                  path,
		MaterializationConfig: materialization.DefaultConfig(),
		Dependencies:          []string{},
		Metadata:              make(map[string]interface{}),
	}, nil
}

// AddDependency adds a dependency to the model (if not already present)
func (m *Model) AddDependency(modelID string) {
	// Check if dependency already exists
	for _, dep := range m.Dependencies {
		if dep == modelID {
			return
		}
	}
	m.Dependencies = append(m.Dependencies, modelID)
}

// SetCompiledSQL sets the compiled SQL for the model
func (m *Model) SetCompiledSQL(sql string) {
	m.CompiledSQL = sql
}

// SetMaterializationConfig sets the materialization configuration
func (m *Model) SetMaterializationConfig(config materialization.MaterializationConfig) {
	m.MaterializationConfig = config
}

// SetMetadata sets a metadata value
func (m *Model) SetMetadata(key string, value interface{}) {
	m.Metadata[key] = value
}
