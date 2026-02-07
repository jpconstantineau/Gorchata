package template

import (
	"testing"
	"text/template"
)

func TestBuildFuncMap(t *testing.T) {
	t.Run("creates FuncMap with all custom functions", func(t *testing.T) {
		ctx := NewContext(
			WithSchema("analytics"),
			WithVars(map[string]interface{}{"test": "value"}),
		)

		funcMap := BuildFuncMap(ctx, nil)

		// Verify all expected functions are present
		expectedFuncs := []string{"ref", "var", "config", "source", "env_var"}
		for _, funcName := range expectedFuncs {
			if _, ok := funcMap[funcName]; !ok {
				t.Errorf("expected function %q to be in FuncMap", funcName)
			}
		}
	})

	t.Run("creates FuncMap with dependency tracker", func(t *testing.T) {
		tracker := newMockDependencyTracker()
		ctx := NewContext(WithSchema("analytics"))

		funcMap := BuildFuncMap(ctx, tracker)

		if funcMap == nil {
			t.Fatal("expected non-nil FuncMap")
		}
		if len(funcMap) == 0 {
			t.Error("expected FuncMap to have functions")
		}
	})

	t.Run("FuncMap is compatible with text/template", func(t *testing.T) {
		ctx := NewContext(
			WithSchema("analytics"),
			WithVars(map[string]interface{}{"start_date": "2024-01-01"}),
		)

		funcMap := BuildFuncMap(ctx, nil)

		// Create a template and verify FuncMap can be used
		tmpl := template.New("test")
		tmpl = tmpl.Funcs(funcMap)

		_, err := tmpl.Parse("{{ ref \"customers\" }}")
		if err != nil {
			t.Fatalf("failed to parse template with FuncMap: %v", err)
		}
	})

	t.Run("handles nil context gracefully", func(t *testing.T) {
		// Should not panic with nil context
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("BuildFuncMap panicked with nil context: %v", r)
			}
		}()

		funcMap := BuildFuncMap(nil, nil)
		if funcMap == nil {
			t.Error("expected non-nil FuncMap even with nil context")
		}
	})
}

func TestBuildFuncMapExecution(t *testing.T) {
	t.Run("ref function works in FuncMap", func(t *testing.T) {
		tracker := newMockDependencyTracker()
		ctx := NewContext(
			WithSchema("analytics"),
			WithCurrentModel("customers"),
		)

		funcMap := BuildFuncMap(ctx, tracker)

		tmpl := template.Must(template.New("test").Funcs(funcMap).Parse("{{ ref \"orders\" }}"))

		var result string
		err := tmpl.Execute(&mockWriter{write: func(s string) { result = s }}, nil)
		if err != nil {
			t.Fatalf("template execution failed: %v", err)
		}

		expected := "analytics.orders"
		if result != expected {
			t.Errorf("expected %q, got %q", expected, result)
		}

		// Verify dependency was tracked
		if !tracker.hasDependency("customers", "orders") {
			t.Error("expected dependency to be tracked")
		}
	})

	t.Run("var function works in FuncMap", func(t *testing.T) {
		ctx := NewContext(
			WithVars(map[string]interface{}{"start_date": "2024-01-01"}),
		)

		funcMap := BuildFuncMap(ctx, nil)

		tmpl := template.Must(template.New("test").Funcs(funcMap).Parse("{{ var \"start_date\" }}"))

		var result string
		err := tmpl.Execute(&mockWriter{write: func(s string) { result = s }}, nil)
		if err != nil {
			t.Fatalf("template execution failed: %v", err)
		}

		expected := "2024-01-01"
		if result != expected {
			t.Errorf("expected %q, got %q", expected, result)
		}
	})
}

// mockWriter is a simple writer for capturing template output.
type mockWriter struct {
	write func(string)
}

func (m *mockWriter) Write(p []byte) (n int, err error) {
	m.write(string(p))
	return len(p), nil
}
