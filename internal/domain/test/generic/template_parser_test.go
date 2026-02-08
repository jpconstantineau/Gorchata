package generic

import (
	"testing"
)

func TestParseTestTemplate_ValidTemplate(t *testing.T) {
	sqlContent := `{% test positive_values(model, column_name) %}
SELECT * FROM {{ model }} WHERE {{ column_name }} <= 0
{% endtest %}`

	name, params, sqlTemplate, err := ParseTestTemplate(sqlContent)
	if err != nil {
		t.Fatalf("ParseTestTemplate failed: %v", err)
	}

	if name != "positive_values" {
		t.Errorf("Expected name 'positive_values', got '%s'", name)
	}

	if len(params) != 2 {
		t.Fatalf("Expected 2 params, got %d", len(params))
	}

	if params[0] != "model" || params[1] != "column_name" {
		t.Errorf("Expected params [model, column_name], got %v", params)
	}

	expectedSQL := "SELECT * FROM {{ model }} WHERE {{ column_name }} <= 0"
	if sqlTemplate != expectedSQL {
		t.Errorf("SQL template mismatch.\nExpected: %s\nGot: %s", expectedSQL, sqlTemplate)
	}
}

func TestParseTestTemplate_WithArguments(t *testing.T) {
	sqlContent := `{% test range_check(model, column_name, min_value, max_value) %}
SELECT * FROM {{ model }} 
WHERE {{ column_name }} < {{ min_value }} 
   OR {{ column_name }} > {{ max_value }}
{% endtest %}`

	name, params, sqlTemplate, err := ParseTestTemplate(sqlContent)
	if err != nil {
		t.Fatalf("ParseTestTemplate failed: %v", err)
	}

	if name != "range_check" {
		t.Errorf("Expected name 'range_check', got '%s'", name)
	}

	if len(params) != 4 {
		t.Fatalf("Expected 4 params, got %d", len(params))
	}

	expectedParams := []string{"model", "column_name", "min_value", "max_value"}
	for i, expected := range expectedParams {
		if params[i] != expected {
			t.Errorf("Param %d: expected '%s', got '%s'", i, expected, params[i])
		}
	}

	// Verify SQL contains placeholders
	if sqlTemplate == "" {
		t.Errorf("SQL template is empty")
	}
}

func TestParseTestTemplate_InvalidSyntax(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "missing endtest",
			sql:  `{% test mytest(model, column_name) %} SELECT 1`,
		},
		{
			name: "missing start tag",
			sql:  `SELECT 1 {% endtest %}`,
		},
		{
			name: "malformed params",
			sql:  `{% test mytest model column_name %} SELECT 1 {% endtest %}`,
		},
		{
			name: "empty content",
			sql:  ``,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, err := ParseTestTemplate(tt.sql)
			if err == nil {
				t.Errorf("Expected error for %s, got nil", tt.name)
			}
		})
	}
}

func TestParseTestTemplate_NoParameters(t *testing.T) {
	sqlContent := `{% test simple_test() %}
SELECT 1 WHERE FALSE
{% endtest %}`

	name, params, _, err := ParseTestTemplate(sqlContent)
	if err != nil {
		t.Fatalf("ParseTestTemplate failed: %v", err)
	}

	if name != "simple_test" {
		t.Errorf("Expected name 'simple_test', got '%s'", name)
	}

	if len(params) != 0 {
		t.Errorf("Expected 0 params, got %d", len(params))
	}
}

func TestParseTestTemplate_WhitespaceHandling(t *testing.T) {
	sqlContent := `{% test spaces_test( model , column_name ) %}
SELECT * FROM {{ model }} WHERE {{ column_name }} IS NULL
{% endtest %}`

	name, params, _, err := ParseTestTemplate(sqlContent)
	if err != nil {
		t.Fatalf("ParseTestTemplate failed: %v", err)
	}

	if name != "spaces_test" {
		t.Errorf("Expected name 'spaces_test', got '%s'", name)
	}

	// Params should be trimmed
	if len(params) != 2 {
		t.Fatalf("Expected 2 params, got %d", len(params))
	}

	if params[0] != "model" || params[1] != "column_name" {
		t.Errorf("Expected trimmed params [model, column_name], got %v", params)
	}
}
