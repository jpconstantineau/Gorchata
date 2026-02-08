package generic

import (
	"testing"
)

func TestGenericTest_Interface(t *testing.T) {
	// This test validates that the GenericTest interface is properly defined
	// We'll create a mock implementation to verify the interface contract

	var _ GenericTest = (*mockGenericTest)(nil)
}

// mockGenericTest is a test implementation of GenericTest
type mockGenericTest struct {
	name string
	sql  string
	err  error
}

func (m *mockGenericTest) Name() string {
	return m.name
}

func (m *mockGenericTest) GenerateSQL(model, column string, args map[string]interface{}) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	return m.sql, nil
}

func (m *mockGenericTest) Validate(model, column string, args map[string]interface{}) error {
	return m.err
}

func TestBuildWhereClause(t *testing.T) {
	tests := []struct {
		name     string
		args     map[string]interface{}
		expected string
	}{
		{
			name:     "no where clause",
			args:     map[string]interface{}{},
			expected: "",
		},
		{
			name:     "with where clause",
			args:     map[string]interface{}{"where": "status = 'active'"},
			expected: " AND (status = 'active')",
		},
		{
			name:     "nil args",
			args:     nil,
			expected: "",
		},
		{
			name:     "where clause with empty string",
			args:     map[string]interface{}{"where": ""},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildWhereClause(tt.args)
			if result != tt.expected {
				t.Errorf("BuildWhereClause() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestValidateRequired(t *testing.T) {
	tests := []struct {
		name      string
		args      map[string]interface{}
		required  []string
		wantError bool
	}{
		{
			name:      "all required present",
			args:      map[string]interface{}{"field1": "value1", "field2": "value2"},
			required:  []string{"field1", "field2"},
			wantError: false,
		},
		{
			name:      "missing required field",
			args:      map[string]interface{}{"field1": "value1"},
			required:  []string{"field1", "field2"},
			wantError: true,
		},
		{
			name:      "no required fields",
			args:      map[string]interface{}{"field1": "value1"},
			required:  []string{},
			wantError: false,
		},
		{
			name:      "nil args with required fields",
			args:      nil,
			required:  []string{"field1"},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRequired(tt.args, tt.required)
			if (err != nil) != tt.wantError {
				t.Errorf("ValidateRequired() error = %v, wantError %v", err, tt.wantError)
			}
		})
	}
}

func TestValidateModelColumn(t *testing.T) {
	tests := []struct {
		name      string
		model     string
		column    string
		wantError bool
	}{
		{
			name:      "valid model and column",
			model:     "users",
			column:    "email",
			wantError: false,
		},
		{
			name:      "empty model",
			model:     "",
			column:    "email",
			wantError: true,
		},
		{
			name:      "empty column",
			model:     "users",
			column:    "",
			wantError: true,
		},
		{
			name:      "both empty",
			model:     "",
			column:    "",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateModelColumn(tt.model, tt.column)
			if (err != nil) != tt.wantError {
				t.Errorf("ValidateModelColumn() error = %v, wantError %v", err, tt.wantError)
			}
		})
	}
}
