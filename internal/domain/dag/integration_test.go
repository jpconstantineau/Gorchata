package dag

import (
	"os"
	"path/filepath"
	"testing"
)

// TestIntegrationDAGConstruction tests the complete workflow:
// 1. Create realistic model directory structure
// 2. Build graph from directory
// 3. Validate graph (no cycles, no missing dependencies)
// 4. Perform topological sort
// 5. Verify execution order makes sense
func TestIntegrationDAGConstruction(t *testing.T) {
	// Create temporary directory structure for models
	tmpDir := t.TempDir()

	// Create a realistic set of interdependent models:
	// Raw layer (no dependencies)
	rawUsersContent := `
-- Raw users data from source system
SELECT 
	id,
	email,
	first_name,
	last_name,
	created_at
FROM source.users
WHERE deleted_at IS NULL
`
	err := os.WriteFile(filepath.Join(tmpDir, "raw_users.sql"), []byte(rawUsersContent), 0644)
	if err != nil {
		t.Fatalf("failed to create raw_users.sql: %v", err)
	}

	rawProductsContent := `
-- Raw products data
SELECT 
	id,
	name,
	price,
	category_id
FROM source.products
WHERE active = true
`
	err = os.WriteFile(filepath.Join(tmpDir, "raw_products.sql"), []byte(rawProductsContent), 0644)
	if err != nil {
		t.Fatalf("failed to create raw_products.sql: %v", err)
	}

	rawOrdersContent := `
-- Raw orders data
SELECT 
	id,
	user_id,
	product_id,
	quantity,
	order_date
FROM source.orders
`
	err = os.WriteFile(filepath.Join(tmpDir, "raw_orders.sql"), []byte(rawOrdersContent), 0644)
	if err != nil {
		t.Fatalf("failed to create raw_orders.sql: %v", err)
	}

	// Staging layer (depends on raw)
	stagingUsersContent := `
-- Cleaned and standardized users
SELECT 
	id,
	LOWER(email) as email,
	INITCAP(first_name) as first_name,
	INITCAP(last_name) as last_name,
	created_at
FROM {{ ref "raw_users" }}
`
	err = os.WriteFile(filepath.Join(tmpDir, "staging_users.sql"), []byte(stagingUsersContent), 0644)
	if err != nil {
		t.Fatalf("failed to create staging_users.sql: %v", err)
	}

	stagingProductsContent := `
-- Cleaned products with category names
SELECT 
	p.id,
	p.name,
	p.price,
	p.category_id
FROM {{ ref "raw_products" }} p
`
	err = os.WriteFile(filepath.Join(tmpDir, "staging_products.sql"), []byte(stagingProductsContent), 0644)
	if err != nil {
		t.Fatalf("failed to create staging_products.sql: %v", err)
	}

	// Mart layer (depends on staging)
	ordersEnrichedContent := `
-- Orders enriched with user and product information
SELECT 
	o.id as order_id,
	o.order_date,
	u.email as user_email,
	u.first_name || ' ' || u.last_name as user_name,
	p.name as product_name,
	p.price,
	o.quantity,
	p.price * o.quantity as total_amount
FROM {{ ref "raw_orders" }} o
JOIN {{ ref "staging_users" }} u ON o.user_id = u.id
JOIN {{ ref "staging_products" }} p ON o.product_id = p.id
`
	err = os.WriteFile(filepath.Join(tmpDir, "orders_enriched.sql"), []byte(ordersEnrichedContent), 0644)
	if err != nil {
		t.Fatalf("failed to create orders_enriched.sql: %v", err)
	}

	userSummaryContent := `
-- User order summary
SELECT 
	user_email,
	user_name,
	COUNT(*) as total_orders,
	SUM(total_amount) as lifetime_value,
	MAX(order_date) as last_order_date
FROM {{ ref "orders_enriched" }}
GROUP BY user_email, user_name
`
	err = os.WriteFile(filepath.Join(tmpDir, "user_summary.sql"), []byte(userSummaryContent), 0644)
	if err != nil {
		t.Fatalf("failed to create user_summary.sql: %v", err)
	}

	// Step 1: Build graph from directory
	builder := NewBuilder()
	graph, err := builder.BuildFromDirectory(tmpDir)
	if err != nil {
		t.Fatalf("failed to build graph: %v", err)
	}

	// Verify correct number of nodes
	nodes := graph.GetNodes()
	expectedNodeCount := 7 // 3 raw + 2 staging + 1 enriched + 1 summary
	if len(nodes) != expectedNodeCount {
		t.Fatalf("expected %d nodes, got %d", expectedNodeCount, len(nodes))
	}

	// Step 2: Validate graph
	err = Validate(graph)
	if err != nil {
		t.Fatalf("graph validation failed: %v", err)
	}

	// Step 3: Perform topological sort
	sorted, err := TopologicalSort(graph)
	if err != nil {
		t.Fatalf("topological sort failed: %v", err)
	}

	if len(sorted) != expectedNodeCount {
		t.Fatalf("expected %d sorted nodes, got %d", expectedNodeCount, len(sorted))
	}

	// Step 4: Verify execution order makes sense
	// Build position map
	positions := make(map[string]int)
	for i, node := range sorted {
		positions[node.ID] = i
	}

	// Verify raw layer comes before staging
	if positions["model_raw_users"] >= positions["model_staging_users"] {
		t.Error("raw_users should come before staging_users")
	}
	if positions["model_raw_products"] >= positions["model_staging_products"] {
		t.Error("raw_products should come before staging_products")
	}

	// Verify staging comes before mart
	if positions["model_staging_users"] >= positions["model_orders_enriched"] {
		t.Error("staging_users should come before orders_enriched")
	}
	if positions["model_staging_products"] >= positions["model_orders_enriched"] {
		t.Error("staging_products should come before orders_enriched")
	}
	if positions["model_raw_orders"] >= positions["model_orders_enriched"] {
		t.Error("raw_orders should come before orders_enriched")
	}

	// Verify mart comes before summary
	if positions["model_orders_enriched"] >= positions["model_user_summary"] {
		t.Error("orders_enriched should come before user_summary")
	}

	// Step 5: Verify no cycles were detected
	cycle, err := DetectCycles(graph)
	if err != nil {
		t.Errorf("unexpected cycle detection error: %v", err)
	}
	if cycle != nil {
		t.Errorf("unexpected cycle found: %v", cycle)
	}

	// Step 6: Verify all dependencies are present
	for _, node := range nodes {
		deps := graph.GetDependencies(node.ID)
		for _, dep := range deps {
			if _, exists := graph.GetNode(dep); !exists {
				t.Errorf("node '%s' depends on '%s' which doesn't exist", node.ID, dep)
			}
		}
	}

	t.Logf("Successfully built and validated DAG with %d models", len(nodes))
	t.Logf("Execution order:")
	for i, node := range sorted {
		deps := graph.GetDependencies(node.ID)
		t.Logf("  %d. %s (depends on: %v)", i+1, node.Name, deps)
	}
}

// TestIntegrationCycleDetection tests that cycles are properly detected
func TestIntegrationCycleDetection(t *testing.T) {
	tmpDir := t.TempDir()

	// Create models with a cycle: A → B → C → A
	modelAContent := `SELECT * FROM {{ ref "b" }}`
	err := os.WriteFile(filepath.Join(tmpDir, "a.sql"), []byte(modelAContent), 0644)
	if err != nil {
		t.Fatalf("failed to create a.sql: %v", err)
	}

	modelBContent := `SELECT * FROM {{ ref "c" }}`
	err = os.WriteFile(filepath.Join(tmpDir, "b.sql"), []byte(modelBContent), 0644)
	if err != nil {
		t.Fatalf("failed to create b.sql: %v", err)
	}

	modelCContent := `SELECT * FROM {{ ref "a" }}`
	err = os.WriteFile(filepath.Join(tmpDir, "c.sql"), []byte(modelCContent), 0644)
	if err != nil {
		t.Fatalf("failed to create c.sql: %v", err)
	}

	// Build graph
	builder := NewBuilder()
	graph, err := builder.BuildFromDirectory(tmpDir)
	if err != nil {
		t.Fatalf("failed to build graph: %v", err)
	}

	// Validate should fail due to cycle
	err = Validate(graph)
	if err == nil {
		t.Fatal("expected validation to fail due to cycle, but it passed")
	}

	// Topological sort should also fail
	_, err = TopologicalSort(graph)
	if err == nil {
		t.Fatal("expected topological sort to fail due to cycle, but it passed")
	}

	// DetectCycles should find the cycle
	cycle, err := DetectCycles(graph)
	if err == nil {
		t.Fatal("expected cycle detection to report error, but it didn't")
	}
	if cycle == nil {
		t.Fatal("expected cycle path, got nil")
	}

	t.Logf("Cycle correctly detected: %v", cycle)
}

// TestIntegrationComplexDependencies tests a more complex dependency graph
func TestIntegrationComplexDependencies(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a diamond dependency pattern
	//      base
	//     /    \
	//   left  right
	//     \    /
	//      top

	baseContent := `SELECT id, value FROM source_table`
	err := os.WriteFile(filepath.Join(tmpDir, "base.sql"), []byte(baseContent), 0644)
	if err != nil {
		t.Fatalf("failed to create base.sql: %v", err)
	}

	leftContent := `SELECT * FROM {{ ref "base" }} WHERE category = 'left'`
	err = os.WriteFile(filepath.Join(tmpDir, "left.sql"), []byte(leftContent), 0644)
	if err != nil {
		t.Fatalf("failed to create left.sql: %v", err)
	}

	rightContent := `SELECT * FROM {{ ref "base" }} WHERE category = 'right'`
	err = os.WriteFile(filepath.Join(tmpDir, "right.sql"), []byte(rightContent), 0644)
	if err != nil {
		t.Fatalf("failed to create right.sql: %v", err)
	}

	topContent := `
SELECT l.*, r.*
FROM {{ ref "left" }} l
FULL OUTER JOIN {{ ref "right" }} r ON l.id = r.id
`
	err = os.WriteFile(filepath.Join(tmpDir, "top.sql"), []byte(topContent), 0644)
	if err != nil {
		t.Fatalf("failed to create top.sql: %v", err)
	}

	// Build and validate
	builder := NewBuilder()
	graph, err := builder.BuildFromDirectory(tmpDir)
	if err != nil {
		t.Fatalf("failed to build graph: %v", err)
	}

	err = Validate(graph)
	if err != nil {
		t.Fatalf("validation failed: %v", err)
	}

	sorted, err := TopologicalSort(graph)
	if err != nil {
		t.Fatalf("topological sort failed: %v", err)
	}

	// Verify order
	positions := make(map[string]int)
	for i, node := range sorted {
		positions[node.ID] = i
	}

	// Base must come first
	if positions["model_base"] >= positions["model_left"] {
		t.Error("base should come before left")
	}
	if positions["model_base"] >= positions["model_right"] {
		t.Error("base should come before right")
	}

	// Left and right must come before top
	if positions["model_left"] >= positions["model_top"] {
		t.Error("left should come before top")
	}
	if positions["model_right"] >= positions["model_top"] {
		t.Error("right should come before top")
	}

	t.Logf("Diamond dependency correctly resolved")
}
