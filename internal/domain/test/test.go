package test

import (
	"fmt"
)

// TestType represents the type of test
type TestType string

const (
	// GenericTest represents a reusable test applied to columns
	GenericTest TestType = "generic"
	// SingularTest represents a custom SQL test for a specific model
	SingularTest TestType = "singular"
)

// String returns the string representation of TestType
func (tt TestType) String() string {
	return string(tt)
}

// TestStatus represents the execution status of a test
type TestStatus string

const (
	// StatusPending indicates the test has not started
	StatusPending TestStatus = "pending"
	// StatusRunning indicates the test is currently executing
	StatusRunning TestStatus = "running"
	// StatusPassed indicates the test passed
	StatusPassed TestStatus = "passed"
	// StatusFailed indicates the test failed
	StatusFailed TestStatus = "failed"
	// StatusWarning indicates the test passed with warnings
	StatusWarning TestStatus = "warning"
	// StatusSkipped indicates the test was skipped
	StatusSkipped TestStatus = "skipped"
)

// String returns the string representation of TestStatus
func (ts TestStatus) String() string {
	return string(ts)
}

// Test represents a data quality test
type Test struct {
	// ID is the unique identifier for the test
	ID string

	// Name is the descriptive name of the test
	Name string

	// ModelName is the name of the model this test applies to
	ModelName string

	// ColumnName is the optional column name (for generic tests)
	ColumnName string

	// Type is the type of test (generic or singular)
	Type TestType

	// SQLTemplate is the SQL template for the test query
	SQLTemplate string

	// Config holds the test configuration
	Config *TestConfig
}

// NewTest creates a new Test instance with validation
func NewTest(id, name, modelName, columnName string, testType TestType, sqlTemplate string) (*Test, error) {
	if id == "" {
		return nil, fmt.Errorf("test ID cannot be empty")
	}
	if name == "" {
		return nil, fmt.Errorf("test name cannot be empty")
	}
	if modelName == "" {
		return nil, fmt.Errorf("model name cannot be empty")
	}
	if sqlTemplate == "" {
		return nil, fmt.Errorf("SQL template cannot be empty")
	}

	return &Test{
		ID:          id,
		Name:        name,
		ModelName:   modelName,
		ColumnName:  columnName,
		Type:        testType,
		SQLTemplate: sqlTemplate,
		Config:      DefaultTestConfig(),
	}, nil
}

// Validate checks if the Test is valid
func (t *Test) Validate() error {
	if t.ID == "" {
		return fmt.Errorf("test ID cannot be empty")
	}
	if t.Name == "" {
		return fmt.Errorf("test name cannot be empty")
	}
	if t.ModelName == "" {
		return fmt.Errorf("model name cannot be empty")
	}
	if t.SQLTemplate == "" {
		return fmt.Errorf("SQL template cannot be empty")
	}
	if t.Config == nil {
		return fmt.Errorf("test config cannot be nil")
	}
	return t.Config.Validate()
}

// SetConfig sets the test configuration
func (t *Test) SetConfig(config *TestConfig) {
	t.Config = config
}
