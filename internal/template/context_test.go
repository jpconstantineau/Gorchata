package template

import (
	"testing"
)

func TestContextCreation(t *testing.T) {
	t.Run("creates context with vars", func(t *testing.T) {
		vars := map[string]interface{}{
			"start_date": "2024-01-01",
			"end_date":   "2024-12-31",
		}

		ctx := NewContext(WithVars(vars))

		if ctx == nil {
			t.Fatal("expected non-nil context")
		}
		if len(ctx.Vars) != 2 {
			t.Errorf("expected 2 vars, got %d", len(ctx.Vars))
		}
		if ctx.Vars["start_date"] != "2024-01-01" {
			t.Errorf("expected start_date to be '2024-01-01', got %v", ctx.Vars["start_date"])
		}
	})

	t.Run("creates context with current model", func(t *testing.T) {
		ctx := NewContext(WithCurrentModel("customers"))

		if ctx.CurrentModel != "customers" {
			t.Errorf("expected CurrentModel to be 'customers', got %s", ctx.CurrentModel)
		}
	})

	t.Run("creates context with schema", func(t *testing.T) {
		ctx := NewContext(WithSchema("analytics"))

		if ctx.Schema != "analytics" {
			t.Errorf("expected Schema to be 'analytics', got %s", ctx.Schema)
		}
	})

	t.Run("creates empty context with no options", func(t *testing.T) {
		ctx := NewContext()

		if ctx == nil {
			t.Fatal("expected non-nil context")
		}
		if ctx.Vars == nil {
			t.Error("expected Vars map to be initialized")
		}
		if ctx.Config == nil {
			t.Error("expected Config map to be initialized")
		}
		if ctx.Sources == nil {
			t.Error("expected Sources map to be initialized")
		}
	})
}

func TestContextWithConfig(t *testing.T) {
	t.Run("creates context with config", func(t *testing.T) {
		config := map[string]interface{}{
			"schema":          "analytics",
			"materialization": "table",
		}

		ctx := NewContext(WithConfig(config))

		if len(ctx.Config) != 2 {
			t.Errorf("expected 2 config items, got %d", len(ctx.Config))
		}
		if ctx.Config["schema"] != "analytics" {
			t.Errorf("expected schema to be 'analytics', got %v", ctx.Config["schema"])
		}
	})

	t.Run("creates context with nested config", func(t *testing.T) {
		config := map[string]interface{}{
			"models": map[string]interface{}{
				"materialization": "view",
				"schema":          "staging",
			},
		}

		ctx := NewContext(WithConfig(config))

		models, ok := ctx.Config["models"].(map[string]interface{})
		if !ok {
			t.Fatal("expected models to be a map")
		}
		if models["materialization"] != "view" {
			t.Errorf("expected materialization to be 'view', got %v", models["materialization"])
		}
	})
}

func TestContextWithSources(t *testing.T) {
	t.Run("creates context with sources", func(t *testing.T) {
		sources := map[string]map[string]string{
			"raw": {
				"customers": "raw_data.customers",
				"orders":    "raw_data.orders",
			},
		}

		ctx := NewContext(WithSources(sources))

		if len(ctx.Sources) != 1 {
			t.Errorf("expected 1 source, got %d", len(ctx.Sources))
		}
		if ctx.Sources["raw"]["customers"] != "raw_data.customers" {
			t.Errorf("expected raw.customers to be 'raw_data.customers', got %s", ctx.Sources["raw"]["customers"])
		}
	})
}

func TestContextMultipleOptions(t *testing.T) {
	t.Run("combines multiple options", func(t *testing.T) {
		vars := map[string]interface{}{"key": "value"}
		config := map[string]interface{}{"setting": "enabled"}

		ctx := NewContext(
			WithVars(vars),
			WithConfig(config),
			WithSchema("analytics"),
			WithCurrentModel("customers"),
		)

		if ctx.Vars["key"] != "value" {
			t.Error("vars not set correctly")
		}
		if ctx.Config["setting"] != "enabled" {
			t.Error("config not set correctly")
		}
		if ctx.Schema != "analytics" {
			t.Error("schema not set correctly")
		}
		if ctx.CurrentModel != "customers" {
			t.Error("current model not set correctly")
		}
	})
}

func TestContextWithIncrementalState(t *testing.T) {
	t.Run("defaults to non-incremental", func(t *testing.T) {
		ctx := NewContext()

		if ctx.IsIncremental {
			t.Error("expected IsIncremental to default to false")
		}
	})

	t.Run("sets incremental flag", func(t *testing.T) {
		ctx := NewContext(WithIsIncremental(true))

		if !ctx.IsIncremental {
			t.Error("expected IsIncremental to be true")
		}
	})

	t.Run("sets incremental flag to false explicitly", func(t *testing.T) {
		ctx := NewContext(WithIsIncremental(false))

		if ctx.IsIncremental {
			t.Error("expected IsIncremental to be false")
		}
	})
}

func TestContextWithCurrentModelTable(t *testing.T) {
	t.Run("defaults to empty string", func(t *testing.T) {
		ctx := NewContext()

		if ctx.CurrentModelTable != "" {
			t.Errorf("expected CurrentModelTable to default to empty string, got %s", ctx.CurrentModelTable)
		}
	})

	t.Run("sets current model table name", func(t *testing.T) {
		ctx := NewContext(WithCurrentModelTable("analytics.dim_customers"))

		if ctx.CurrentModelTable != "analytics.dim_customers" {
			t.Errorf("expected CurrentModelTable to be 'analytics.dim_customers', got %s", ctx.CurrentModelTable)
		}
	})

	t.Run("sets current model table with schema", func(t *testing.T) {
		ctx := NewContext(WithCurrentModelTable("staging.stg_orders"))

		if ctx.CurrentModelTable != "staging.stg_orders" {
			t.Errorf("expected CurrentModelTable to be 'staging.stg_orders', got %s", ctx.CurrentModelTable)
		}
	})

	t.Run("combines with other options", func(t *testing.T) {
		ctx := NewContext(
			WithCurrentModel("customers"),
			WithCurrentModelTable("analytics.dim_customers"),
			WithIsIncremental(true),
		)

		if ctx.CurrentModel != "customers" {
			t.Errorf("expected CurrentModel to be 'customers', got %s", ctx.CurrentModel)
		}
		if ctx.CurrentModelTable != "analytics.dim_customers" {
			t.Errorf("expected CurrentModelTable to be 'analytics.dim_customers', got %s", ctx.CurrentModelTable)
		}
		if !ctx.IsIncremental {
			t.Error("expected IsIncremental to be true")
		}
	})
}

