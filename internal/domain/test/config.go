package test

import (
	"fmt"
)

// Severity represents the severity level of a test failure
type Severity string

const (
	// SeverityError indicates test failure should cause an error
	SeverityError Severity = "error"
	// SeverityWarn indicates test failure should cause a warning
	SeverityWarn Severity = "warn"
)

// String returns the string representation of Severity
func (s Severity) String() string {
	return string(s)
}

// ComparisonOperator represents a comparison operator for conditional thresholds
type ComparisonOperator string

const (
	// OperatorGreaterThan represents the > operator
	OperatorGreaterThan ComparisonOperator = ">"
	// OperatorGreaterThanOrEqual represents the >= operator
	OperatorGreaterThanOrEqual ComparisonOperator = ">="
	// OperatorEquals represents the = operator
	OperatorEquals ComparisonOperator = "="
	// OperatorNotEquals represents the != operator
	OperatorNotEquals ComparisonOperator = "!="
)

// String returns the string representation of ComparisonOperator
func (op ComparisonOperator) String() string {
	return string(op)
}

// ConditionalThreshold defines a condition for when a test should fail or warn
type ConditionalThreshold struct {
	// Operator is the comparison operator
	Operator ComparisonOperator

	// Value is the threshold value to compare against
	Value int64
}

// Evaluate evaluates the threshold against the given row count
func (ct ConditionalThreshold) Evaluate(rowCount int64) bool {
	switch ct.Operator {
	case OperatorGreaterThan:
		return rowCount > ct.Value
	case OperatorGreaterThanOrEqual:
		return rowCount >= ct.Value
	case OperatorEquals:
		return rowCount == ct.Value
	case OperatorNotEquals:
		return rowCount != ct.Value
	default:
		return false
	}
}

// TestConfig holds configuration options for a test
type TestConfig struct {
	// Severity defines whether failures are errors or warnings
	Severity Severity

	// ErrorIf defines a conditional threshold for when test results should be errors
	ErrorIf *ConditionalThreshold

	// WarnIf defines a conditional threshold for when test results should be warnings
	WarnIf *ConditionalThreshold

	// StoreFailures indicates whether to store failed rows for debugging
	StoreFailures bool

	// Where is an optional SQL WHERE clause to filter test execution
	Where string

	// SampleSize is an optional limit on the number of rows to test (for performance)
	SampleSize int

	// Tags are optional labels for organizing tests
	Tags []string

	// CustomName is an optional override for the test name
	CustomName string
}

// DefaultTestConfig returns a TestConfig with default values
func DefaultTestConfig() *TestConfig {
	return &TestConfig{
		Severity:      SeverityError,
		StoreFailures: false,
		Where:         "",
		SampleSize:    0,
		Tags:          []string{},
		CustomName:    "",
	}
}

// Validate checks if the TestConfig is valid
func (tc *TestConfig) Validate() error {
	if tc.Severity == "" {
		return fmt.Errorf("severity cannot be empty")
	}
	if tc.Severity != SeverityError && tc.Severity != SeverityWarn {
		return fmt.Errorf("invalid severity: %s", tc.Severity)
	}
	if tc.SampleSize < 0 {
		return fmt.Errorf("sample size cannot be negative")
	}
	return nil
}

// SetSeverity sets the severity level
func (tc *TestConfig) SetSeverity(severity Severity) {
	tc.Severity = severity
}

// SetStoreFailures sets whether to store failed rows
func (tc *TestConfig) SetStoreFailures(store bool) {
	tc.StoreFailures = store
}

// SetWhere sets the WHERE clause
func (tc *TestConfig) SetWhere(where string) {
	tc.Where = where
}

// SetSampleSize sets the sample size
func (tc *TestConfig) SetSampleSize(size int) {
	tc.SampleSize = size
}

// AddTag adds a tag to the test config (if not already present)
func (tc *TestConfig) AddTag(tag string) {
	// Check if tag already exists
	for _, t := range tc.Tags {
		if t == tag {
			return
		}
	}
	tc.Tags = append(tc.Tags, tag)
}

// SetCustomName sets the custom name
func (tc *TestConfig) SetCustomName(name string) {
	tc.CustomName = name
}

// SetErrorIf sets the error threshold
func (tc *TestConfig) SetErrorIf(threshold ConditionalThreshold) {
	tc.ErrorIf = &threshold
}

// SetWarnIf sets the warning threshold
func (tc *TestConfig) SetWarnIf(threshold ConditionalThreshold) {
	tc.WarnIf = &threshold
}
