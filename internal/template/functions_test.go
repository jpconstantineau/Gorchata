package template

import (
	"fmt"
	"os"
	"testing"
)

// mockDependencyTracker implements DependencyTracker for testing.
type mockDependencyTracker struct {
	dependencies map[string][]string
}

func newMockDependencyTracker() *mockDependencyTracker {
	return &mockDependencyTracker{
		dependencies: make(map[string][]string),
	}
}

func (m *mockDependencyTracker) AddDependency(from, to string) error {
	m.dependencies[from] = append(m.dependencies[from], to)
	return nil
}

func (m *mockDependencyTracker) hasDependency(from, to string) bool {
	deps, ok := m.dependencies[from]
	if !ok {
		return false
	}
	for _, dep := range deps {
		if dep == to {
			return true
		}
	}
	return false
}

func TestRefFunction(t *testing.T) {
	t.Run("returns qualified table name", func(t *testing.T) {
		ctx := NewContext(
			WithSchema("analytics"),
			WithCurrentModel("customers"),
		)

		refFunc := makeRefFunc(ctx, nil)
		result := refFunc("orders")

		expected := "analytics.orders"
		if result != expected {
			t.Errorf("expected %q, got %q", expected, result)
		}
	})

	t.Run("uses default schema when not set", func(t *testing.T) {
		ctx := NewContext()

		refFunc := makeRefFunc(ctx, nil)
		result := refFunc("orders")

		expected := "orders"
		if result != expected {
			t.Errorf("expected %q, got %q", expected, result)
		}
	})
}

func TestRefFunctionWithDependencyTracker(t *testing.T) {
	t.Run("registers dependency when tracker provided", func(t *testing.T) {
		tracker := newMockDependencyTracker()
		ctx := NewContext(
			WithSchema("analytics"),
			WithCurrentModel("customers"),
		)

		refFunc := makeRefFunc(ctx, tracker)
		refFunc("orders")

		if !tracker.hasDependency("customers", "orders") {
			t.Error("expected dependency from customers to orders")
		}
	})

	t.Run("does not register dependency when no current model", func(t *testing.T) {
		tracker := newMockDependencyTracker()
		ctx := NewContext(WithSchema("analytics"))

		refFunc := makeRefFunc(ctx, tracker)
		refFunc("orders")

		if len(tracker.dependencies) != 0 {
			t.Error("expected no dependencies when current model not set")
		}
	})

	t.Run("does not error when tracker is nil", func(t *testing.T) {
		ctx := NewContext(
			WithSchema("analytics"),
			WithCurrentModel("customers"),
		)

		refFunc := makeRefFunc(ctx, nil)
		result := refFunc("orders")

		if result != "analytics.orders" {
			t.Errorf("unexpected result: %q", result)
		}
	})
}

func TestVarFunction(t *testing.T) {
	t.Run("retrieves variable from context", func(t *testing.T) {
		vars := map[string]interface{}{
			"start_date": "2024-01-01",
			"limit":      100,
		}
		ctx := NewContext(WithVars(vars))

		varFunc := makeVarFunc(ctx)

		result, err := varFunc("start_date")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != "2024-01-01" {
			t.Errorf("expected '2024-01-01', got %v", result)
		}
	})

	t.Run("retrieves integer variable", func(t *testing.T) {
		vars := map[string]interface{}{
			"limit": 100,
		}
		ctx := NewContext(WithVars(vars))

		varFunc := makeVarFunc(ctx)

		result, err := varFunc("limit")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != 100 {
			t.Errorf("expected 100, got %v", result)
		}
	})
}

func TestVarFunctionMissing(t *testing.T) {
	t.Run("returns error when variable not found", func(t *testing.T) {
		ctx := NewContext()

		varFunc := makeVarFunc(ctx)

		_, err := varFunc("missing_var")
		if err == nil {
			t.Error("expected error for missing variable")
		}

		expectedMsg := "variable not found: missing_var"
		if err.Error() != expectedMsg {
			t.Errorf("expected error %q, got %q", expectedMsg, err.Error())
		}
	})
}

func TestConfigFunction(t *testing.T) {
	t.Run("retrieves simple config value", func(t *testing.T) {
		config := map[string]interface{}{
			"schema":          "analytics",
			"materialization": "table",
		}
		ctx := NewContext(WithConfig(config))

		configFunc := makeConfigFunc(ctx)

		result, err := configFunc("schema")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != "analytics" {
			t.Errorf("expected 'analytics', got %v", result)
		}
	})

	t.Run("returns error when config key not found", func(t *testing.T) {
		ctx := NewContext()

		configFunc := makeConfigFunc(ctx)

		_, err := configFunc("missing_key")
		if err == nil {
			t.Error("expected error for missing config key")
		}
	})
}

func TestConfigFunctionNested(t *testing.T) {
	t.Run("retrieves nested config with dot notation", func(t *testing.T) {
		config := map[string]interface{}{
			"models": map[string]interface{}{
				"materialization": "view",
				"schema":          "staging",
			},
		}
		ctx := NewContext(WithConfig(config))

		configFunc := makeConfigFunc(ctx)

		result, err := configFunc("models.materialization")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != "view" {
			t.Errorf("expected 'view', got %v", result)
		}
	})

	t.Run("returns error for invalid nested path", func(t *testing.T) {
		config := map[string]interface{}{
			"models": map[string]interface{}{
				"materialization": "view",
			},
		}
		ctx := NewContext(WithConfig(config))

		configFunc := makeConfigFunc(ctx)

		_, err := configFunc("models.invalid.path")
		if err == nil {
			t.Error("expected error for invalid nested path")
		}
	})
}

func TestSourceFunction(t *testing.T) {
	t.Run("retrieves source table reference", func(t *testing.T) {
		sources := map[string]map[string]string{
			"raw": {
				"customers": "raw_data.customers",
				"orders":    "raw_data.orders",
			},
		}
		ctx := NewContext(WithSources(sources))

		sourceFunc := makeSourceFunc(ctx)

		result, err := sourceFunc("raw", "customers")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != "raw_data.customers" {
			t.Errorf("expected 'raw_data.customers', got %q", result)
		}
	})

	t.Run("falls back to simple format when source not configured", func(t *testing.T) {
		ctx := NewContext()

		sourceFunc := makeSourceFunc(ctx)

		result, err := sourceFunc("raw", "customers")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expected := "raw.customers"
		if result != expected {
			t.Errorf("expected %q, got %q", expected, result)
		}
	})

	t.Run("returns error when table not found in source", func(t *testing.T) {
		sources := map[string]map[string]string{
			"raw": {
				"customers": "raw_data.customers",
			},
		}
		ctx := NewContext(WithSources(sources))

		sourceFunc := makeSourceFunc(ctx)

		_, err := sourceFunc("raw", "orders")
		if err == nil {
			t.Error("expected error for missing table in source")
		}
	})

	t.Run("falls back to source.table format when not configured", func(t *testing.T) {
		sources := map[string]map[string]string{
			"raw": {
				"customers": "raw_data.customers",
			},
		}
		ctx := NewContext(WithSources(sources))

		sourceFunc := makeSourceFunc(ctx)

		result, err := sourceFunc("staging", "products")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		expected := "staging.products"
		if result != expected {
			t.Errorf("expected %q, got %q", expected, result)
		}
	})
}

func TestEnvVarFunction(t *testing.T) {
	t.Run("retrieves environment variable", func(t *testing.T) {
		key := "TEST_ENV_VAR"
		value := "test_value"
		os.Setenv(key, value)
		defer os.Unsetenv(key)

		envVarFunc := makeEnvVarFunc()

		result, err := envVarFunc(key)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != value {
			t.Errorf("expected %q, got %q", value, result)
		}
	})

	t.Run("uses default value when variable not set", func(t *testing.T) {
		key := "NONEXISTENT_VAR"
		defaultVal := "default_value"

		envVarFunc := makeEnvVarFunc()

		result, err := envVarFunc(key, defaultVal)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != defaultVal {
			t.Errorf("expected %q, got %q", defaultVal, result)
		}
	})

	t.Run("returns error when variable not set and no default", func(t *testing.T) {
		key := "NONEXISTENT_VAR"

		envVarFunc := makeEnvVarFunc()

		_, err := envVarFunc(key)
		if err == nil {
			t.Error("expected error when variable not set and no default")
		}

		expectedMsg := fmt.Sprintf("environment variable not set: %s", key)
		if err.Error() != expectedMsg {
			t.Errorf("expected error %q, got %q", expectedMsg, err.Error())
		}
	})

	t.Run("prefers actual value over default", func(t *testing.T) {
		key := "TEST_ENV_VAR_2"
		value := "actual_value"
		defaultVal := "default_value"
		os.Setenv(key, value)
		defer os.Unsetenv(key)

		envVarFunc := makeEnvVarFunc()

		result, err := envVarFunc(key, defaultVal)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != value {
			t.Errorf("expected actual value %q, got %q", value, result)
		}
	})
}
