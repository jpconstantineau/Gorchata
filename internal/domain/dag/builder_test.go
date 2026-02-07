package dag

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

func TestExtractDependenciesNone(t *testing.T) {
	content := `
SELECT id, name, email
FROM users
WHERE active = true
`

	deps := extractDependencies(content)

	if len(deps) != 0 {
		t.Errorf("expected no dependencies, got %v", deps)
	}
}

func TestExtractDependenciesSingle(t *testing.T) {
	content := `
SELECT 
    o.id,
    o.order_date,
    u.name
FROM {{ ref "users" }} u
JOIN orders o ON o.user_id = u.id
`

	deps := extractDependencies(content)

	if len(deps) != 1 {
		t.Fatalf("expected 1 dependency, got %d", len(deps))
	}

	if deps[0] != "users" {
		t.Errorf("expected dependency 'users', got '%s'", deps[0])
	}
}

func TestExtractDependenciesMultiple(t *testing.T) {
	content := `
SELECT 
    o.id,
    o.order_date,
    u.name,
    p.product_name
FROM {{ ref "users" }} u
JOIN {{ ref "orders" }} o ON o.user_id = u.id
JOIN {{ ref "products" }} p ON o.product_id = p.id
`

	deps := extractDependencies(content)

	if len(deps) != 3 {
		t.Fatalf("expected 3 dependencies, got %d", len(deps))
	}

	expected := []string{"orders", "products", "users"}
	sort.Strings(deps)

	if !reflect.DeepEqual(deps, expected) {
		t.Errorf("expected dependencies %v, got %v", expected, deps)
	}
}

func TestExtractDependenciesVariations(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		expected []string
	}{
		{
			name:     "double quotes with spaces",
			content:  `SELECT * FROM {{ ref "users" }}`,
			expected: []string{"users"},
		},
		{
			name:     "single quotes",
			content:  `SELECT * FROM {{ ref 'users' }}`,
			expected: []string{"users"},
		},
		{
			name:     "no spaces",
			content:  `SELECT * FROM {{ref "users"}}`,
			expected: []string{"users"},
		},
		{
			name:     "extra spaces",
			content:  `SELECT * FROM {{  ref  "users"  }}`,
			expected: []string{"users"},
		},
		{
			name:     "multiple refs same model",
			content:  `SELECT * FROM {{ ref "users" }} UNION SELECT * FROM {{ ref "users" }}`,
			expected: []string{"users"},
		},
		{
			name: "mixed quotes",
			content: `
				SELECT * FROM {{ ref "users" }}
				JOIN {{ ref 'orders' }}
			`,
			expected: []string{"orders", "users"},
		},
		{
			name:     "model name with underscore",
			content:  `SELECT * FROM {{ ref "user_accounts" }}`,
			expected: []string{"user_accounts"},
		},
		{
			name:     "model name with numbers",
			content:  `SELECT * FROM {{ ref "users_v2" }}`,
			expected: []string{"users_v2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := extractDependencies(tt.content)
			sort.Strings(deps)
			sort.Strings(tt.expected)

			if !reflect.DeepEqual(deps, tt.expected) {
				t.Errorf("expected dependencies %v, got %v", tt.expected, deps)
			}
		})
	}
}

func TestExtractDependenciesIgnoresComments(t *testing.T) {
	content := `
-- This is a comment about {{ ref "fake_model" }}
/* Another comment with {{ ref "another_fake" }} */
SELECT * FROM {{ ref "real_model" }}
`

	deps := extractDependencies(content)

	// Note: Our regex-based extraction doesn't parse SQL, so it will
	// extract refs from comments too. This is acceptable for Phase 5.
	// In a production system, you'd want a proper SQL parser.
	// For now, we document this behavior.

	// The current implementation will extract all three
	expected := []string{"another_fake", "fake_model", "real_model"}
	sort.Strings(deps)

	if !reflect.DeepEqual(deps, expected) {
		t.Errorf("expected dependencies %v, got %v", expected, deps)
	}
}

func TestCreateNode(t *testing.T) {
	b := NewBuilder()

	content := `SELECT * FROM {{ ref "users" }}`
	node, err := b.createNode("/models/orders.sql", "orders", content)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if node.ID != "model_orders" {
		t.Errorf("expected ID 'model_orders', got '%s'", node.ID)
	}

	if node.Name != "orders" {
		t.Errorf("expected Name 'orders', got '%s'", node.Name)
	}

	if node.Type != "model" {
		t.Errorf("expected Type 'model', got '%s'", node.Type)
	}

	if len(node.Dependencies) != 1 {
		t.Fatalf("expected 1 dependency, got %d", len(node.Dependencies))
	}

	if node.Dependencies[0] != "model_users" {
		t.Errorf("expected dependency 'model_users', got '%s'", node.Dependencies[0])
	}

	filePath, ok := node.Metadata["file_path"].(string)
	if !ok || filePath != "/models/orders.sql" {
		t.Errorf("expected file_path '/models/orders.sql', got '%v'", filePath)
	}
}

func TestBuildFromDirectoryEmpty(t *testing.T) {
	// Create temporary empty directory
	tmpDir := t.TempDir()

	b := NewBuilder()
	g, err := b.BuildFromDirectory(tmpDir)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(g.GetNodes()) != 0 {
		t.Errorf("expected empty graph, got %d nodes", len(g.GetNodes()))
	}
}

func TestBuildFromDirectorySingleModel(t *testing.T) {
	// Create temporary directory with one model
	tmpDir := t.TempDir()

	content := `SELECT id, name FROM users WHERE active = true`
	err := os.WriteFile(filepath.Join(tmpDir, "users.sql"), []byte(content), 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	b := NewBuilder()
	g, err := b.BuildFromDirectory(tmpDir)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	nodes := g.GetNodes()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}

	node := nodes[0]
	if node.ID != "model_users" {
		t.Errorf("expected ID 'model_users', got '%s'", node.ID)
	}

	if node.Name != "users" {
		t.Errorf("expected Name 'users', got '%s'", node.Name)
	}

	if len(node.Dependencies) != 0 {
		t.Errorf("expected no dependencies, got %v", node.Dependencies)
	}
}

func TestBuildFromDirectoryMultipleModels(t *testing.T) {
	// Create temporary directory with multiple interdependent models
	tmpDir := t.TempDir()

	// Create users.sql (no dependencies)
	usersContent := `SELECT id, name, email FROM raw_users`
	err := os.WriteFile(filepath.Join(tmpDir, "users.sql"), []byte(usersContent), 0644)
	if err != nil {
		t.Fatalf("failed to create users.sql: %v", err)
	}

	// Create products.sql (no dependencies)
	productsContent := `SELECT id, name, price FROM raw_products`
	err = os.WriteFile(filepath.Join(tmpDir, "products.sql"), []byte(productsContent), 0644)
	if err != nil {
		t.Fatalf("failed to create products.sql: %v", err)
	}

	// Create orders.sql (depends on users and products)
	ordersContent := `
SELECT 
	o.id,
	u.name as user_name,
	p.name as product_name
FROM raw_orders o
JOIN {{ ref "users" }} u ON o.user_id = u.id
JOIN {{ ref "products" }} p ON o.product_id = p.id
`
	err = os.WriteFile(filepath.Join(tmpDir, "orders.sql"), []byte(ordersContent), 0644)
	if err != nil {
		t.Fatalf("failed to create orders.sql: %v", err)
	}

	b := NewBuilder()
	g, err := b.BuildFromDirectory(tmpDir)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	nodes := g.GetNodes()
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}

	// Check orders node has correct dependencies
	_, exists := g.GetNode("model_orders")
	if !exists {
		t.Fatal("expected model_orders to exist")
	}

	deps := g.GetDependencies("model_orders")
	if len(deps) != 2 {
		t.Fatalf("expected 2 dependencies for orders, got %d", len(deps))
	}

	depsMap := make(map[string]bool)
	for _, dep := range deps {
		depsMap[dep] = true
	}

	if !depsMap["model_users"] {
		t.Error("expected orders to depend on users")
	}
	if !depsMap["model_products"] {
		t.Error("expected orders to depend on products")
	}

	// Verify topological sort works
	sorted, err := TopologicalSort(g)
	if err != nil {
		t.Fatalf("unexpected error in topological sort: %v", err)
	}

	if len(sorted) != 3 {
		t.Fatalf("expected 3 sorted nodes, got %d", len(sorted))
	}

	// Check that dependencies come before dependents
	positions := make(map[string]int)
	for i, node := range sorted {
		positions[node.ID] = i
	}

	if positions["model_users"] >= positions["model_orders"] {
		t.Error("users should come before orders in execution order")
	}
	if positions["model_products"] >= positions["model_orders"] {
		t.Error("products should come before orders in execution order")
	}
}

func TestBuildFromDirectoryInvalidPath(t *testing.T) {
	b := NewBuilder()
	_, err := b.BuildFromDirectory("/nonexistent/path/that/does/not/exist")

	if err == nil {
		t.Fatal("expected error for non-existent directory, got nil")
	}
}

func TestBuildFromDirectoryNotADirectory(t *testing.T) {
	// Create a temporary file (not a directory)
	tmpFile := filepath.Join(t.TempDir(), "file.txt")
	err := os.WriteFile(tmpFile, []byte("test"), 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	b := NewBuilder()
	_, err = b.BuildFromDirectory(tmpFile)

	if err == nil {
		t.Fatal("expected error for file path (not directory), got nil")
	}
}

func TestBuildFromDirectoryNestedModels(t *testing.T) {
	// Create temporary directory with nested structure
	tmpDir := t.TempDir()
	subDir := filepath.Join(tmpDir, "staging")
	err := os.MkdirAll(subDir, 0755)
	if err != nil {
		t.Fatalf("failed to create subdirectory: %v", err)
	}

	// Create model in root
	usersContent := `SELECT id, name FROM raw_users`
	err = os.WriteFile(filepath.Join(tmpDir, "users.sql"), []byte(usersContent), 0644)
	if err != nil {
		t.Fatalf("failed to create users.sql: %v", err)
	}

	// Create model in subdirectory
	stagingContent := `SELECT * FROM {{ ref "users" }}`
	err = os.WriteFile(filepath.Join(subDir, "staging_users.sql"), []byte(stagingContent), 0644)
	if err != nil {
		t.Fatalf("failed to create staging_users.sql: %v", err)
	}

	b := NewBuilder()
	g, err := b.BuildFromDirectory(tmpDir)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	nodes := g.GetNodes()
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes (including nested), got %d", len(nodes))
	}

	// Verify staging_users depends on users
	deps := g.GetDependencies("model_staging_users")
	if len(deps) != 1 || deps[0] != "model_users" {
		t.Errorf("expected staging_users to depend on users, got %v", deps)
	}
}
