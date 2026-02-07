package template

import (
	"os"
	"strings"
	"testing"
)

func TestRenderSimpleTemplate(t *testing.T) {
	t.Run("renders simple template with data", func(t *testing.T) {
		engine := New()
		tmpl, err := engine.Parse("test", "Hello {{ .Name }}")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}

		ctx := NewContext()
		data := map[string]string{"Name": "World"}

		result, err := Render(tmpl, ctx, data)
		if err != nil {
			t.Fatalf("render error: %v", err)
		}

		expected := "Hello World"
		if result != expected {
			t.Errorf("expected %q, got %q", expected, result)
		}
	})

	t.Run("renders template with no data", func(t *testing.T) {
		engine := New()
		tmpl, err := engine.Parse("test", "Static content")
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}

		ctx := NewContext()
		result, err := Render(tmpl, ctx, nil)
		if err != nil {
			t.Fatalf("render error: %v", err)
		}

		expected := "Static content"
		if result != expected {
			t.Errorf("expected %q, got %q", expected, result)
		}
	})
}

func TestRenderWithRef(t *testing.T) {
	t.Run("renders template with ref function", func(t *testing.T) {
		engine := New()
		tmpl, err := engine.Parse("test", `SELECT * FROM {{ ref "customers" }}`)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}

		ctx := NewContext(
			WithSchema("analytics"),
			WithCurrentModel("orders"),
		)

		result, err := Render(tmpl, ctx, nil)
		if err != nil {
			t.Fatalf("render error: %v", err)
		}

		expected := "SELECT * FROM analytics.customers"
		if result != expected {
			t.Errorf("expected %q, got %q", expected, result)
		}
	})

	t.Run("renders template with dependency tracking", func(t *testing.T) {
		tracker := newMockDependencyTracker()
		engine := New(WithDependencyTracker(tracker))

		tmpl, err := engine.Parse("test", `{{ ref "customers" }}`)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}

		ctx := NewContext(
			WithSchema("analytics"),
			WithCurrentModel("orders"),
		)

		_, err = Render(tmpl, ctx, nil)
		if err != nil {
			t.Fatalf("render error: %v", err)
		}

		if !tracker.hasDependency("orders", "customers") {
			t.Error("expected dependency to be tracked")
		}
	})
}

func TestRenderWithMultipleRefs(t *testing.T) {
	t.Run("renders template with multiple ref calls", func(t *testing.T) {
		tracker := newMockDependencyTracker()
		engine := New(WithDependencyTracker(tracker))

		content := `
SELECT c.*, o.total
FROM {{ ref "customers" }} c
LEFT JOIN {{ ref "orders" }} o ON c.id = o.customer_id
WHERE c.created_at >= '{{ var "start_date" }}'
`
		tmpl, err := engine.Parse("test", content)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}

		ctx := NewContext(
			WithSchema("analytics"),
			WithCurrentModel("customer_orders"),
			WithVars(map[string]interface{}{
				"start_date": "2024-01-01",
			}),
		)

		result, err := Render(tmpl, ctx, nil)
		if err != nil {
			t.Fatalf("render error: %v", err)
		}

		// Check that all substitutions were made
		if !strings.Contains(result, "analytics.customers") {
			t.Error("expected result to contain 'analytics.customers'")
		}
		if !strings.Contains(result, "analytics.orders") {
			t.Error("expected result to contain 'analytics.orders'")
		}
		if !strings.Contains(result, "2024-01-01") {
			t.Error("expected result to contain '2024-01-01'")
		}

		// Check dependencies
		if !tracker.hasDependency("customer_orders", "customers") {
			t.Error("expected dependency customer_orders -> customers")
		}
		if !tracker.hasDependency("customer_orders", "orders") {
			t.Error("expected dependency customer_orders -> orders")
		}
	})
}

func TestRenderWithVar(t *testing.T) {
	t.Run("renders template with var function", func(t *testing.T) {
		engine := New()
		tmpl, err := engine.Parse("test", `Value: {{ var "key" }}`)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}

		ctx := NewContext(
			WithVars(map[string]interface{}{
				"key": "test_value",
			}),
		)

		result, err := Render(tmpl, ctx, nil)
		if err != nil {
			t.Fatalf("render error: %v", err)
		}

		expected := "Value: test_value"
		if result != expected {
			t.Errorf("expected %q, got %q", expected, result)
		}
	})

	t.Run("renders template with multiple vars", func(t *testing.T) {
		engine := New()
		tmpl, err := engine.Parse("test", `{{ var "start" }} to {{ var "end" }}`)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}

		ctx := NewContext(
			WithVars(map[string]interface{}{
				"start": "2024-01-01",
				"end":   "2024-12-31",
			}),
		)

		result, err := Render(tmpl, ctx, nil)
		if err != nil {
			t.Fatalf("render error: %v", err)
		}

		expected := "2024-01-01 to 2024-12-31"
		if result != expected {
			t.Errorf("expected %q, got %q", expected, result)
		}
	})
}

func TestRenderWithConfig(t *testing.T) {
	t.Run("renders template with config function", func(t *testing.T) {
		engine := New()
		tmpl, err := engine.Parse("test", `Schema: {{ config "schema" }}`)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}

		ctx := NewContext(
			WithConfig(map[string]interface{}{
				"schema": "analytics",
			}),
		)

		result, err := Render(tmpl, ctx, nil)
		if err != nil {
			t.Fatalf("render error: %v", err)
		}

		expected := "Schema: analytics"
		if result != expected {
			t.Errorf("expected %q, got %q", expected, result)
		}
	})
}

func TestRenderWithSource(t *testing.T) {
	t.Run("renders template with source function", func(t *testing.T) {
		engine := New()
		tmpl, err := engine.Parse("test", `FROM {{ source "raw" "customers" }}`)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}

		ctx := NewContext(
			WithSources(map[string]map[string]string{
				"raw": {
					"customers": "raw_data.customers",
				},
			}),
		)

		result, err := Render(tmpl, ctx, nil)
		if err != nil {
			t.Fatalf("render error: %v", err)
		}

		expected := "FROM raw_data.customers"
		if result != expected {
			t.Errorf("expected %q, got %q", expected, result)
		}
	})
}

func TestRenderWithEnvVar(t *testing.T) {
	t.Run("renders template with env_var function", func(t *testing.T) {
		key := "TEST_RENDER_VAR"
		value := "test_value"
		os.Setenv(key, value)
		defer os.Unsetenv(key)

		engine := New()
		tmpl, err := engine.Parse("test", `Env: {{ env_var "TEST_RENDER_VAR" }}`)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}

		ctx := NewContext()
		result, err := Render(tmpl, ctx, nil)
		if err != nil {
			t.Fatalf("render error: %v", err)
		}

		expected := "Env: test_value"
		if result != expected {
			t.Errorf("expected %q, got %q", expected, result)
		}
	})

	t.Run("renders template with env_var default", func(t *testing.T) {
		engine := New()
		tmpl, err := engine.Parse("test", `Env: {{ env_var "NONEXISTENT_VAR" "default" }}`)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}

		ctx := NewContext()
		result, err := Render(tmpl, ctx, nil)
		if err != nil {
			t.Fatalf("render error: %v", err)
		}

		expected := "Env: default"
		if result != expected {
			t.Errorf("expected %q, got %q", expected, result)
		}
	})
}

func TestRenderErrors(t *testing.T) {
	t.Run("returns error for missing variable", func(t *testing.T) {
		engine := New()
		tmpl, err := engine.Parse("test", `{{ var "missing" }}`)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}

		ctx := NewContext()
		_, err = Render(tmpl, ctx, nil)
		if err == nil {
			t.Error("expected error for missing variable")
		}
		if !strings.Contains(err.Error(), "missing") {
			t.Errorf("expected error message to mention 'missing', got: %v", err)
		}
	})

	t.Run("returns error for missing config key", func(t *testing.T) {
		engine := New()
		tmpl, err := engine.Parse("test", `{{ config "missing" }}`)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}

		ctx := NewContext()
		_, err = Render(tmpl, ctx, nil)
		if err == nil {
			t.Error("expected error for missing config key")
		}
	})
}

func TestRenderUndefinedVariable(t *testing.T) {
	t.Run("returns error on undefined template variable", func(t *testing.T) {
		engine := New()
		// Use option "missingkey=error" behavior
		tmpl, err := engine.Parse("test", `{{ .UndefinedField }}`)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}

		ctx := NewContext()
		_, err = Render(tmpl, ctx, map[string]string{})
		if err == nil {
			t.Error("expected error for undefined field")
		}
	})
}

func TestRenderComplexSQLTemplate(t *testing.T) {
	t.Run("renders complete SQL template", func(t *testing.T) {
		tracker := newMockDependencyTracker()
		engine := New(WithDependencyTracker(tracker))

		content := `-- Customer orders summary
SELECT
    c.customer_id,
    c.name,
    COUNT(o.order_id) as order_count,
    SUM(o.total) as total_spent
FROM {{ source "raw" "customers" }} c
LEFT JOIN {{ ref "orders" }} o ON c.customer_id = o.customer_id
WHERE c.created_at >= '{{ var "start_date" }}'
  AND c.region = '{{ config "default_region" }}'
GROUP BY c.customer_id, c.name
HAVING total_spent > {{ var "min_spend" }}`

		tmpl, err := engine.Parse("customer_summary.sql", content)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}

		ctx := NewContext(
			WithSchema("analytics"),
			WithCurrentModel("customer_summary"),
			WithVars(map[string]interface{}{
				"start_date": "2024-01-01",
				"min_spend":  1000,
			}),
			WithConfig(map[string]interface{}{
				"default_region": "US",
			}),
			WithSources(map[string]map[string]string{
				"raw": {
					"customers": "raw_data.customers",
				},
			}),
		)

		result, err := Render(tmpl, ctx, nil)
		if err != nil {
			t.Fatalf("render error: %v", err)
		}

		// Verify all substitutions
		checks := map[string]string{
			"raw_data.customers": "source table",
			"analytics.orders":   "ref table",
			"2024-01-01":         "var start_date",
			"US":                 "config default_region",
			"1000":               "var min_spend",
		}

		for expected, desc := range checks {
			if !strings.Contains(result, expected) {
				t.Errorf("expected %s (%q) in result", desc, expected)
			}
		}

		// Verify dependency tracking
		if !tracker.hasDependency("customer_summary", "orders") {
			t.Error("expected dependency customer_summary -> orders")
		}
	})
}
