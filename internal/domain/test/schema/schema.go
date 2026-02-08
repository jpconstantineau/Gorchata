package schema

// SchemaFile represents a DBT-compatible schema.yml file
type SchemaFile struct {
	Version int           `yaml:"version"`
	Models  []ModelSchema `yaml:"models"`
}

// ModelSchema represents a model configuration in a schema file
type ModelSchema struct {
	Name        string         `yaml:"name"`
	Description string         `yaml:"description,omitempty"`
	Columns     []ColumnSchema `yaml:"columns,omitempty"`
	DataTests   []interface{}  `yaml:"data_tests,omitempty"` // Table-level tests
}

// ColumnSchema represents a column configuration
type ColumnSchema struct {
	Name        string        `yaml:"name"`
	Description string        `yaml:"description,omitempty"`
	DataTests   []interface{} `yaml:"data_tests,omitempty"` // Column-level tests
}
