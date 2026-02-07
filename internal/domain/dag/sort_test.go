package dag

import (
	"testing"
)

func TestTopologicalSortEmptyGraph(t *testing.T) {
	g := NewGraph()

	result, err := TopologicalSort(g)
	if err != nil {
		t.Fatalf("unexpected error for empty graph: %v", err)
	}

	if len(result) != 0 {
		t.Errorf("expected empty result for empty graph, got %d nodes", len(result))
	}
}

func TestTopologicalSortSingleNode(t *testing.T) {
	g := NewGraph()

	node := &Node{ID: "model_users", Name: "users", Type: "model"}
	g.AddNode(node)

	result, err := TopologicalSort(g)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 node, got %d", len(result))
	}

	if result[0].ID != "model_users" {
		t.Errorf("expected node 'model_users', got '%s'", result[0].ID)
	}
}

func TestTopologicalSortLinear(t *testing.T) {
	// Linear chain: A → B → C
	// C depends on B, B depends on A
	// Execution order should be: A, B, C
	g := NewGraph()

	nodeA := &Node{ID: "A", Name: "A", Type: "model"}
	nodeB := &Node{ID: "B", Name: "B", Type: "model"}
	nodeC := &Node{ID: "C", Name: "C", Type: "model"}

	g.AddNode(nodeA)
	g.AddNode(nodeB)
	g.AddNode(nodeC)

	g.AddEdge("B", "A") // B depends on A
	g.AddEdge("C", "B") // C depends on B

	result, err := TopologicalSort(g)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(result))
	}

	// Build map of positions
	positions := make(map[string]int)
	for i, node := range result {
		positions[node.ID] = i
	}

	// A must come before B, B must come before C
	if positions["A"] >= positions["B"] {
		t.Errorf("A should come before B in execution order")
	}
	if positions["B"] >= positions["C"] {
		t.Errorf("B should come before C in execution order")
	}
}

func TestTopologicalSortDiamond(t *testing.T) {
	// Diamond shape:
	//     A
	//    / \
	//   B   C
	//    \ /
	//     D
	// D depends on B and C, B and C depend on A
	// Valid orders: A,B,C,D or A,C,B,D
	g := NewGraph()

	nodeA := &Node{ID: "A", Name: "A", Type: "model"}
	nodeB := &Node{ID: "B", Name: "B", Type: "model"}
	nodeC := &Node{ID: "C", Name: "C", Type: "model"}
	nodeD := &Node{ID: "D", Name: "D", Type: "model"}

	g.AddNode(nodeA)
	g.AddNode(nodeB)
	g.AddNode(nodeC)
	g.AddNode(nodeD)

	g.AddEdge("B", "A") // B depends on A
	g.AddEdge("C", "A") // C depends on A
	g.AddEdge("D", "B") // D depends on B
	g.AddEdge("D", "C") // D depends on C

	result, err := TopologicalSort(g)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result) != 4 {
		t.Fatalf("expected 4 nodes, got %d", len(result))
	}

	// Build map of positions
	positions := make(map[string]int)
	for i, node := range result {
		positions[node.ID] = i
	}

	// Verify dependencies are respected
	if positions["A"] >= positions["B"] {
		t.Errorf("A should come before B")
	}
	if positions["A"] >= positions["C"] {
		t.Errorf("A should come before C")
	}
	if positions["B"] >= positions["D"] {
		t.Errorf("B should come before D")
	}
	if positions["C"] >= positions["D"] {
		t.Errorf("C should come before D")
	}
}

func TestTopologicalSortComplex(t *testing.T) {
	// More complex graph:
	//     A     E
	//    / \    |
	//   B   C   |
	//    \ / \ /
	//     D   F
	g := NewGraph()

	nodes := []*Node{
		{ID: "A", Name: "A", Type: "model"},
		{ID: "B", Name: "B", Type: "model"},
		{ID: "C", Name: "C", Type: "model"},
		{ID: "D", Name: "D", Type: "model"},
		{ID: "E", Name: "E", Type: "model"},
		{ID: "F", Name: "F", Type: "model"},
	}

	for _, node := range nodes {
		g.AddNode(node)
	}

	g.AddEdge("B", "A") // B depends on A
	g.AddEdge("C", "A") // C depends on A
	g.AddEdge("D", "B") // D depends on B
	g.AddEdge("D", "C") // D depends on C
	g.AddEdge("F", "C") // F depends on C
	g.AddEdge("F", "E") // F depends on E

	result, err := TopologicalSort(g)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result) != 6 {
		t.Fatalf("expected 6 nodes, got %d", len(result))
	}

	// Build map of positions
	positions := make(map[string]int)
	for i, node := range result {
		positions[node.ID] = i
	}

	// Verify all dependencies are respected
	if positions["A"] >= positions["B"] {
		t.Errorf("A should come before B")
	}
	if positions["A"] >= positions["C"] {
		t.Errorf("A should come before C")
	}
	if positions["B"] >= positions["D"] {
		t.Errorf("B should come before D")
	}
	if positions["C"] >= positions["D"] {
		t.Errorf("C should come before D")
	}
	if positions["C"] >= positions["F"] {
		t.Errorf("C should come before F")
	}
	if positions["E"] >= positions["F"] {
		t.Errorf("E should come before F")
	}
}

func TestTopologicalSortWithCycle(t *testing.T) {
	// Create a cycle: A → B → C → A
	g := NewGraph()

	nodeA := &Node{ID: "A", Name: "A", Type: "model"}
	nodeB := &Node{ID: "B", Name: "B", Type: "model"}
	nodeC := &Node{ID: "C", Name: "C", Type: "model"}

	g.AddNode(nodeA)
	g.AddNode(nodeB)
	g.AddNode(nodeC)

	g.AddEdge("A", "B") // A depends on B
	g.AddEdge("B", "C") // B depends on C
	g.AddEdge("C", "A") // C depends on A (cycle!)

	result, err := TopologicalSort(g)
	if err == nil {
		t.Fatal("expected error for graph with cycle, got nil")
	}

	if result != nil {
		t.Errorf("expected nil result for cyclic graph, got %v", result)
	}
}

func TestTopologicalSortMultipleValidOrders(t *testing.T) {
	// Graph with multiple valid topological orders:
	//   A   B
	//    \ /
	//     C
	// Valid orders: A,B,C or B,A,C
	g := NewGraph()

	nodeA := &Node{ID: "A", Name: "A", Type: "model"}
	nodeB := &Node{ID: "B", Name: "B", Type: "model"}
	nodeC := &Node{ID: "C", Name: "C", Type: "model"}

	g.AddNode(nodeA)
	g.AddNode(nodeB)
	g.AddNode(nodeC)

	g.AddEdge("C", "A") // C depends on A
	g.AddEdge("C", "B") // C depends on B

	result, err := TopologicalSort(g)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(result))
	}

	// Build map of positions
	positions := make(map[string]int)
	for i, node := range result {
		positions[node.ID] = i
	}

	// Both A and B must come before C
	if positions["A"] >= positions["C"] {
		t.Errorf("A should come before C")
	}
	if positions["B"] >= positions["C"] {
		t.Errorf("B should come before C")
	}

	// The order of A and B relative to each other doesn't matter
	// Both orders (A,B,C and B,A,C) are valid
}
