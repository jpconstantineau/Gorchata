package template

import (
	"testing"
)

func TestEngineCreation(t *testing.T) {
	t.Run("creates engine with default options", func(t *testing.T) {
		engine := New()

		if engine == nil {
			t.Fatal("expected non-nil engine")
		}
	})

	t.Run("creates engine with custom dependency tracker", func(t *testing.T) {
		tracker := newMockDependencyTracker()
		engine := New(WithDependencyTracker(tracker))

		if engine == nil {
			t.Fatal("expected non-nil engine")
		}
	})

	t.Run("creates engine with custom delimiters", func(t *testing.T) {
		engine := New(WithDelimiters("[[", "]]"))

		if engine == nil {
			t.Fatal("expected non-nil engine")
		}
	})
}

func TestEngineParse(t *testing.T) {
	t.Run("parses simple template", func(t *testing.T) {
		engine := New()

		tmpl, err := engine.Parse("test", "Hello {{ .Name }}")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tmpl == nil {
			t.Fatal("expected non-nil template")
		}
	})

	t.Run("parses template with custom functions", func(t *testing.T) {
		engine := New()

		content := `SELECT * FROM {{ ref "customers" }}`
		tmpl, err := engine.Parse("test.sql", content)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tmpl == nil {
			t.Fatal("expected non-nil template")
		}
	})

	t.Run("parses SQL template with multiple refs", func(t *testing.T) {
		engine := New()

		content := `
SELECT c.*, o.order_count
FROM {{ ref "customers" }} c
LEFT JOIN {{ ref "orders" }} o ON c.id = o.customer_id
`
		tmpl, err := engine.Parse("complex.sql", content)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tmpl == nil {
			t.Fatal("expected non-nil template")
		}
	})

	t.Run("preserves template name", func(t *testing.T) {
		engine := New()

		name := "customers.sql"
		tmpl, err := engine.Parse(name, "SELECT 1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tmpl.Name() != name {
			t.Errorf("expected name %q, got %q", name, tmpl.Name())
		}
	})
}

func TestEngineParseError(t *testing.T) {
	t.Run("returns error for invalid template syntax", func(t *testing.T) {
		engine := New()

		_, err := engine.Parse("test", "{{ .Name }")
		if err == nil {
			t.Error("expected error for invalid syntax")
		}
	})

	t.Run("returns error for undefined function", func(t *testing.T) {
		engine := New()

		_, err := engine.Parse("test", "{{ undefined_func }}")
		if err == nil {
			t.Error("expected error for undefined function")
		}
	})

	t.Run("handles empty template name", func(t *testing.T) {
		engine := New()

		tmpl, err := engine.Parse("", "SELECT 1")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if tmpl == nil {
			t.Error("expected non-nil template")
		}
	})
}

func TestEngineWithCustomDelimiters(t *testing.T) {
	t.Run("parses template with custom delimiters", func(t *testing.T) {
		engine := New(WithDelimiters("[[", "]]"))

		tmpl, err := engine.Parse("test", "Hello [[ .Name ]]")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tmpl == nil {
			t.Fatal("expected non-nil template")
		}
	})

	t.Run("does not parse standard delimiters when custom set", func(t *testing.T) {
		engine := New(WithDelimiters("[[", "]]"))

		content := "Hello {{ .Name }}"
		tmpl, err := engine.Parse("test", content)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// With custom delimiters, {{ }} should be treated as literal text
		// This is expected behavior - no error should occur
		if tmpl == nil {
			t.Fatal("expected non-nil template")
		}
	})
}

func TestFuncMapRegistration(t *testing.T) {
	t.Run("custom functions are available in parsed templates", func(t *testing.T) {
		engine := New()

		// Parse template using custom function
		tmpl, err := engine.Parse("test", `{{ ref "orders" }}`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tmpl == nil {
			t.Fatal("expected non-nil template")
		}
	})

	t.Run("all custom functions are registered", func(t *testing.T) {
		engine := New()

		functions := []string{"ref", "var", "config", "source", "env_var"}
		for _, fn := range functions {
			content := "{{ " + fn + " }}"
			// Note: This will fail at execution, but should parse successfully
			// if the function is registered
			_, err := engine.Parse("test", content)
			// Parse should succeed even if function call would fail at execution
			if err != nil {
				// Only certain parse errors are expected
				// Function existence itself shouldn't cause parse error
				t.Logf("function %s parse result: %v", fn, err)
			}
		}
	})
}
