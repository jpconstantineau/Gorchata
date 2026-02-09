package template

import (
	"strings"
	"testing"
)

func TestIntegrationEndToEnd(t *testing.T) {
	t.Run("complete workflow from engine creation to rendering", func(t *testing.T) {
		// Create dependency tracker
		tracker := newMockDependencyTracker()

		// Create engine with tracker
		engine := New(WithDependencyTracker(tracker))

		// Define SQL template with all custom functions
		sqlTemplate := `-- Customer analysis
SELECT
    c.customer_id,
    c.name,
    c.email,
    COUNT(o.order_id) as order_count,
    SUM(o.total) as lifetime_value
FROM {{ source "raw" "customers" }} c
LEFT JOIN {{ ref "orders" }} o ON c.customer_id = o.customer_id
WHERE c.created_at >= '{{ var "start_date" }}'
  AND c.status = 'active'
  AND c.region = '{{ config "target_region" }}'
GROUP BY c.customer_id, c.name, c.email
HAVING lifetime_value > {{ var "min_lifetime_value" }}
ORDER BY lifetime_value DESC
LIMIT {{ var "limit" }}`

		// Parse the template
		tmpl, err := engine.Parse("customer_analysis.sql", sqlTemplate)
		if err != nil {
			t.Fatalf("failed to parse template: %v", err)
		}

		// Create context with all necessary data
		ctx := NewContext(
			WithSchema("analytics"),
			WithCurrentModel("customer_analysis"),
			WithVars(map[string]interface{}{
				"start_date":         "2024-01-01",
				"min_lifetime_value": 5000,
				"limit":              100,
			}),
			WithConfig(map[string]interface{}{
				"target_region": "US",
			}),
			WithSources(map[string]map[string]string{
				"raw": {
					"customers": "raw_data.customers",
				},
			}),
		)

		// Render the template
		result, err := Render(tmpl, ctx, nil)
		if err != nil {
			t.Fatalf("failed to render template: %v", err)
		}

		// Verify all substitutions were made correctly
		expectedSubstitutions := map[string]string{
			"raw_data.customers": "source customers table",
			"analytics.orders":   "ref orders table",
			"2024-01-01":         "start_date variable",
			"US":                 "target_region config",
			"5000":               "min_lifetime_value variable",
			"100":                "limit variable",
		}

		for expected, description := range expectedSubstitutions {
			if !strings.Contains(result, expected) {
				t.Errorf("missing %s: expected %q in result", description, expected)
			}
		}

		// Verify dependency was tracked
		if !tracker.hasDependency("customer_analysis", "orders") {
			t.Error("expected dependency customer_analysis -> orders")
		}

		// Verify the result is valid SQL (basic check)
		if !strings.Contains(result, "SELECT") {
			t.Error("result should contain SELECT")
		}
		if !strings.Contains(result, "FROM") {
			t.Error("result should contain FROM")
		}
		if !strings.Contains(result, "WHERE") {
			t.Error("result should contain WHERE")
		}
	})
}

func TestIntegrationMultipleModels(t *testing.T) {
	t.Run("renders multiple models with shared context", func(t *testing.T) {
		tracker := newMockDependencyTracker()
		engine := New(WithDependencyTracker(tracker))

		// Shared configuration
		baseCtx := NewContext(
			WithSchema("analytics"),
			WithVars(map[string]interface{}{
				"start_date": "2024-01-01",
			}),
			WithSources(map[string]map[string]string{
				"raw": {
					"customers": "raw_data.customers",
					"orders":    "raw_data.orders",
				},
			}),
		)

		// Model 1: Customers
		customersSQL := `SELECT * FROM {{ source "raw" "customers" }}
WHERE created_at >= '{{ var "start_date" }}'`

		customersTmpl, err := engine.Parse("customers.sql", customersSQL)
		if err != nil {
			t.Fatalf("failed to parse customers template: %v", err)
		}

		customersCtx := NewContext(
			WithSchema(baseCtx.Schema),
			WithVars(baseCtx.Vars),
			WithSources(baseCtx.Sources),
			WithCurrentModel("customers"),
		)

		customersResult, err := Render(customersTmpl, customersCtx, nil)
		if err != nil {
			t.Fatalf("failed to render customers: %v", err)
		}

		if !strings.Contains(customersResult, "raw_data.customers") {
			t.Error("customers result missing source table")
		}

		// Model 2: Orders (depends on customers)
		ordersSQL := `SELECT 
    o.*,
    c.name as customer_name
FROM {{ source "raw" "orders" }} o
LEFT JOIN {{ ref "customers" }} c ON o.customer_id = c.customer_id`

		ordersTmpl, err := engine.Parse("orders.sql", ordersSQL)
		if err != nil {
			t.Fatalf("failed to parse orders template: %v", err)
		}

		ordersCtx := NewContext(
			WithSchema(baseCtx.Schema),
			WithVars(baseCtx.Vars),
			WithSources(baseCtx.Sources),
			WithCurrentModel("orders"),
		)

		ordersResult, err := Render(ordersTmpl, ordersCtx, nil)
		if err != nil {
			t.Fatalf("failed to render orders: %v", err)
		}

		if !strings.Contains(ordersResult, "raw_data.orders") {
			t.Error("orders result missing source table")
		}
		if !strings.Contains(ordersResult, "analytics.customers") {
			t.Error("orders result missing ref to customers")
		}

		// Verify dependency
		if !tracker.hasDependency("orders", "customers") {
			t.Error("expected dependency orders -> customers")
		}
	})
}

func TestIntegrationCustomDelimiters(t *testing.T) {
	t.Run("works with custom delimiters", func(t *testing.T) {
		// Create engine with custom delimiters (useful for SQL with JSON)
		engine := New(WithDelimiters("[[", "]]"))

		template := `SELECT [[ ref "customers" ]], data::json->>'value' FROM table`

		tmpl, err := engine.Parse("test.sql", template)
		if err != nil {
			t.Fatalf("failed to parse: %v", err)
		}

		ctx := NewContext(WithSchema("analytics"))
		result, err := Render(tmpl, ctx, nil)
		if err != nil {
			t.Fatalf("failed to render: %v", err)
		}

		// Should have substituted [[ ]] but left {{ }} as-is
		if !strings.Contains(result, "analytics.customers") {
			t.Error("custom delimiter ref not substituted")
		}
		if !strings.Contains(result, "data::json->>'value'") {
			t.Error("JSON syntax should be preserved")
		}
	})
}

func TestIntegrationErrorHandling(t *testing.T) {
	t.Run("provides clear errors for template issues", func(t *testing.T) {
		engine := New()

		testCases := []struct {
			name        string
			template    string
			ctx         *Context
			expectError string
		}{
			{
				name:        "missing variable",
				template:    `{{ var "missing" }}`,
				ctx:         NewContext(),
				expectError: "missing",
			},
			{
				name:        "missing config",
				template:    `{{ config "missing" }}`,
				ctx:         NewContext(),
				expectError: "missing",
			},
			{
				name:     "missing source table",
				template: `{{ source "raw" "missing_table" }}`,
				ctx: NewContext(
					WithSources(map[string]map[string]string{
						"raw": {"other": "raw.other"},
					}),
				),
				expectError: "missing_table",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				tmpl, err := engine.Parse("test", tc.template)
				if err != nil {
					t.Fatalf("parse error: %v", err)
				}

				_, err = Render(tmpl, tc.ctx, nil)
				if err == nil {
					t.Errorf("expected error containing %q", tc.expectError)
					return
				}

				if !strings.Contains(err.Error(), tc.expectError) {
					t.Errorf("expected error to contain %q, got: %v", tc.expectError, err)
				}
			})
		}
	})
}

func TestIntegrationRealWorldScenario(t *testing.T) {
	t.Run("simulates complete dbt-like workflow", func(t *testing.T) {
		tracker := newMockDependencyTracker()
		engine := New(WithDependencyTracker(tracker))

		// Stage 1: Staging models (clean source data)
		stagingCustomers := `
-- Staging: Clean raw customer data
SELECT
    id as customer_id,
    TRIM(name) as name,
    LOWER(email) as email,
    created_at,
    region
FROM {{ source "raw" "customers" }}
WHERE deleted_at IS NULL
`
		stgCustomersTmpl, _ := engine.Parse("stg_customers.sql", stagingCustomers)
		stgCustomersCtx := NewContext(
			WithSchema("staging"),
			WithCurrentModel("stg_customers"),
			WithSources(map[string]map[string]string{
				"raw": {"customers": "raw_data.customers"},
			}),
		)
		stgResult, _ := Render(stgCustomersTmpl, stgCustomersCtx, nil)

		if !strings.Contains(stgResult, "raw_data.customers") {
			t.Error("staging should reference raw source")
		}

		// Stage 2: Intermediate models (join staging models)
		intCustomerOrders := `
-- Intermediate: Aggregate customer orders
SELECT
    c.customer_id,
    c.name,
    c.email,
    COUNT(o.order_id) as order_count,
    SUM(o.amount) as total_spent
FROM {{ ref "stg_customers" }} c
LEFT JOIN {{ ref "stg_orders" }} o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name, c.email
`
		intTmpl, _ := engine.Parse("int_customer_orders.sql", intCustomerOrders)
		intCtx := NewContext(
			WithSchema("analytics"),
			WithCurrentModel("int_customer_orders"),
		)
		intResult, _ := Render(intTmpl, intCtx, nil)

		if !strings.Contains(intResult, "analytics.stg_customers") {
			t.Error("intermediate should reference staging models")
		}

		// Stage 3: Marts (business-facing models)
		martCustomerSegments := `
-- Mart: Customer segments for business use
SELECT
    co.*,
    CASE
        WHEN co.total_spent > {{ var "high_value_threshold" }} THEN 'high_value'
        WHEN co.total_spent > {{ var "medium_value_threshold" }} THEN 'medium_value'
        ELSE 'low_value'
    END as customer_segment
FROM {{ ref "int_customer_orders" }} co
WHERE co.order_count > 0
`
		martTmpl, _ := engine.Parse("mart_customer_segments.sql", martCustomerSegments)
		martCtx := NewContext(
			WithSchema("analytics"),
			WithCurrentModel("mart_customer_segments"),
			WithVars(map[string]interface{}{
				"high_value_threshold":   10000,
				"medium_value_threshold": 1000,
			}),
		)
		martResult, _ := Render(martTmpl, martCtx, nil)

		if !strings.Contains(martResult, "analytics.int_customer_orders") {
			t.Error("mart should reference intermediate models")
		}
		if !strings.Contains(martResult, "10000") {
			t.Error("mart should use variables")
		}

		// Verify dependency chain
		expectedDeps := map[string][]string{
			"int_customer_orders":    {"stg_customers", "stg_orders"},
			"mart_customer_segments": {"int_customer_orders"},
		}

		for from, tos := range expectedDeps {
			for _, to := range tos {
				if !tracker.hasDependency(from, to) {
					t.Errorf("expected dependency %s -> %s", from, to)
				}
			}
		}
	})
}

func TestIntegration_ModelReferencesSeed(t *testing.T) {
	t.Run("model template uses seed() function", func(t *testing.T) {
		// Create engine
		engine := New()

		// Define SQL template that references seeds
		sqlTemplate := `-- Customer analysis using seed data
SELECT 
    c.customer_id,
    c.name,
    COUNT(*) as order_count
FROM {{ seed "customers" }} c
LEFT JOIN {{ ref "orders" }} o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name`

		// Parse the template
		tmpl, err := engine.Parse("customer_analysis.sql", sqlTemplate)
		if err != nil {
			t.Fatalf("failed to parse template: %v", err)
		}

		// Create context with Seeds map
		ctx := NewContext(
			WithSchema("analytics"),
			WithCurrentModel("customer_analysis"),
		)
		ctx.Seeds = map[string]string{
			"customers": "customers",
			"products":  "staging.products",
		}

		// Render the template
		result, err := Render(tmpl, ctx, nil)
		if err != nil {
			t.Fatalf("failed to render template: %v", err)
		}

		// Verify seed reference was replaced
		if !strings.Contains(result, "customers c") {
			t.Error("expected seed reference to be replaced with table name")
		}

		// Verify ref() still works
		if !strings.Contains(result, "analytics.orders") {
			t.Error("expected ref to be qualified with schema")
		}

		// Ensure template markers are gone
		if strings.Contains(result, "{{") || strings.Contains(result, "}}") {
			t.Error("template should be fully rendered")
		}
	})

	t.Run("model uses seed with schema prefix", func(t *testing.T) {
		engine := New()

		sqlTemplate := `SELECT * FROM {{ seed "products" }}`

		tmpl, err := engine.Parse("product_analysis.sql", sqlTemplate)
		if err != nil {
			t.Fatalf("failed to parse template: %v", err)
		}

		ctx := NewContext()
		ctx.Seeds = map[string]string{
			"products": "staging.products",
		}

		result, err := Render(tmpl, ctx, nil)
		if err != nil {
			t.Fatalf("failed to render template: %v", err)
		}

		if !strings.Contains(result, "staging.products") {
			t.Errorf("expected 'staging.products', got: %s", result)
		}
	})

	t.Run("seed() error for nonexistent seed", func(t *testing.T) {
		engine := New()

		sqlTemplate := `SELECT * FROM {{ seed "nonexistent" }}`

		tmpl, err := engine.Parse("test.sql", sqlTemplate)
		if err != nil {
			t.Fatalf("failed to parse template: %v", err)
		}

		ctx := NewContext()
		ctx.Seeds = map[string]string{
			"customers": "customers",
		}

		_, err = Render(tmpl, ctx, nil)
		if err == nil {
			t.Fatal("expected error for nonexistent seed")
		}

		if !strings.Contains(err.Error(), "seed \"nonexistent\" not found") {
			t.Errorf("expected error about nonexistent seed, got: %v", err)
		}
	})
}
