package dag

import (
	"testing"
)

func TestNewGraph(t *testing.T) {
	g := NewGraph()

	if g == nil {
		t.Fatal("expected NewGraph to return non-nil graph")
	}

	nodes := g.GetNodes()
	if len(nodes) != 0 {
		t.Errorf("expected empty graph, got %d nodes", len(nodes))
	}
}

func TestAddNode(t *testing.T) {
	g := NewGraph()

	node := &Node{
		ID:   "model_users",
		Name: "users",
		Type: "model",
	}

	err := g.AddNode(node)
	if err != nil {
		t.Fatalf("unexpected error adding node: %v", err)
	}

	retrieved, exists := g.GetNode("model_users")
	if !exists {
		t.Fatal("expected node to exist in graph")
	}
	if retrieved.ID != "model_users" {
		t.Errorf("expected retrieved node ID 'model_users', got '%s'", retrieved.ID)
	}
}

func TestAddNodeDuplicate(t *testing.T) {
	g := NewGraph()

	node1 := &Node{ID: "model_users", Name: "users", Type: "model"}
	node2 := &Node{ID: "model_users", Name: "users_v2", Type: "model"}

	err := g.AddNode(node1)
	if err != nil {
		t.Fatalf("unexpected error adding first node: %v", err)
	}

	err = g.AddNode(node2)
	if err == nil {
		t.Fatal("expected error adding duplicate node, got nil")
	}
}

func TestAddEdge(t *testing.T) {
	g := NewGraph()

	node1 := &Node{ID: "model_users", Name: "users", Type: "model"}
	node2 := &Node{ID: "model_orders", Name: "orders", Type: "model"}

	g.AddNode(node1)
	g.AddNode(node2)

	err := g.AddEdge("model_orders", "model_users")
	if err != nil {
		t.Fatalf("unexpected error adding edge: %v", err)
	}

	if !g.HasEdge("model_orders", "model_users") {
		t.Error("expected edge to exist")
	}
}

func TestAddEdgeMissingNode(t *testing.T) {
	g := NewGraph()

	node1 := &Node{ID: "model_users", Name: "users", Type: "model"}
	g.AddNode(node1)

	err := g.AddEdge("model_orders", "model_users")
	if err == nil {
		t.Fatal("expected error adding edge with missing 'from' node, got nil")
	}

	err = g.AddEdge("model_users", "model_missing")
	if err == nil {
		t.Fatal("expected error adding edge with missing 'to' node, got nil")
	}
}

func TestGetNode(t *testing.T) {
	g := NewGraph()

	node := &Node{ID: "model_users", Name: "users", Type: "model"}
	g.AddNode(node)

	retrieved, exists := g.GetNode("model_users")
	if !exists {
		t.Fatal("expected node to exist")
	}
	if retrieved.ID != "model_users" {
		t.Errorf("expected ID 'model_users', got '%s'", retrieved.ID)
	}

	_, exists = g.GetNode("model_nonexistent")
	if exists {
		t.Error("expected node not to exist")
	}
}

func TestGetDependencies(t *testing.T) {
	g := NewGraph()

	users := &Node{ID: "model_users", Name: "users", Type: "model"}
	products := &Node{ID: "model_products", Name: "products", Type: "model"}
	orders := &Node{ID: "model_orders", Name: "orders", Type: "model"}

	g.AddNode(users)
	g.AddNode(products)
	g.AddNode(orders)

	g.AddEdge("model_orders", "model_users")
	g.AddEdge("model_orders", "model_products")

	deps := g.GetDependencies("model_orders")
	if len(deps) != 2 {
		t.Errorf("expected 2 dependencies, got %d", len(deps))
	}

	depsMap := make(map[string]bool)
	for _, dep := range deps {
		depsMap[dep] = true
	}

	if !depsMap["model_users"] {
		t.Error("expected model_users in dependencies")
	}
	if !depsMap["model_products"] {
		t.Error("expected model_products in dependencies")
	}
}

func TestHasEdge(t *testing.T) {
	g := NewGraph()

	node1 := &Node{ID: "model_users", Name: "users", Type: "model"}
	node2 := &Node{ID: "model_orders", Name: "orders", Type: "model"}

	g.AddNode(node1)
	g.AddNode(node2)
	g.AddEdge("model_orders", "model_users")

	if !g.HasEdge("model_orders", "model_users") {
		t.Error("expected edge to exist")
	}

	if g.HasEdge("model_users", "model_orders") {
		t.Error("expected reverse edge not to exist")
	}

	if g.HasEdge("model_users", "model_nonexistent") {
		t.Error("expected edge to nonexistent node not to exist")
	}
}

func TestGetNodes(t *testing.T) {
	g := NewGraph()

	node1 := &Node{ID: "model_users", Name: "users", Type: "model"}
	node2 := &Node{ID: "model_orders", Name: "orders", Type: "model"}
	node3 := &Node{ID: "model_products", Name: "products", Type: "model"}

	g.AddNode(node1)
	g.AddNode(node2)
	g.AddNode(node3)

	nodes := g.GetNodes()
	if len(nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(nodes))
	}

	nodeMap := make(map[string]bool)
	for _, node := range nodes {
		nodeMap[node.ID] = true
	}

	if !nodeMap["model_users"] {
		t.Error("expected model_users in nodes")
	}
	if !nodeMap["model_orders"] {
		t.Error("expected model_orders in nodes")
	}
	if !nodeMap["model_products"] {
		t.Error("expected model_products in nodes")
	}
}
