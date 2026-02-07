package dag

import (
	"strings"
	"testing"
)

func TestDetectCyclesNoCycle(t *testing.T) {
	// Valid DAG: A → B → C
	g := NewGraph()

	nodeA := &Node{ID: "A", Name: "A", Type: "model"}
	nodeB := &Node{ID: "B", Name: "B", Type: "model"}
	nodeC := &Node{ID: "C", Name: "C", Type: "model"}

	g.AddNode(nodeA)
	g.AddNode(nodeB)
	g.AddNode(nodeC)

	g.AddEdge("B", "A") // B depends on A
	g.AddEdge("C", "B") // C depends on B

	cycle, err := DetectCycles(g)
	if err != nil {
		t.Fatalf("unexpected error for valid DAG: %v", err)
	}

	if cycle != nil {
		t.Errorf("expected no cycle, got %v", cycle)
	}
}

func TestDetectCyclesSimpleCycle(t *testing.T) {
	// Simple cycle: A → B → A
	g := NewGraph()

	nodeA := &Node{ID: "A", Name: "A", Type: "model"}
	nodeB := &Node{ID: "B", Name: "B", Type: "model"}

	g.AddNode(nodeA)
	g.AddNode(nodeB)

	g.AddEdge("A", "B") // A depends on B
	g.AddEdge("B", "A") // B depends on A (cycle!)

	cycle, err := DetectCycles(g)
	if err == nil {
		t.Fatal("expected error for graph with cycle, got nil")
	}

	if cycle == nil {
		t.Fatal("expected cycle path, got nil")
	}

	// Verify the cycle path contains both nodes
	cycleMap := make(map[string]bool)
	for _, nodeID := range cycle {
		cycleMap[nodeID] = true
	}

	if !cycleMap["A"] || !cycleMap["B"] {
		t.Errorf("expected cycle to contain A and B, got %v", cycle)
	}
}

func TestDetectCyclesComplexCycle(t *testing.T) {
	// Complex cycle: A → B → C → A
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

	cycle, err := DetectCycles(g)
	if err == nil {
		t.Fatal("expected error for graph with cycle, got nil")
	}

	if cycle == nil {
		t.Fatal("expected cycle path, got nil")
	}

	// Verify the cycle path contains all three nodes
	cycleMap := make(map[string]bool)
	for _, nodeID := range cycle {
		cycleMap[nodeID] = true
	}

	if !cycleMap["A"] || !cycleMap["B"] || !cycleMap["C"] {
		t.Errorf("expected cycle to contain A, B, and C, got %v", cycle)
	}
}

func TestDetectCyclesSelfReference(t *testing.T) {
	// Self-reference: A → A
	g := NewGraph()

	nodeA := &Node{ID: "A", Name: "A", Type: "model"}

	g.AddNode(nodeA)
	g.AddEdge("A", "A") // A depends on itself

	cycle, err := DetectCycles(g)
	if err == nil {
		t.Fatal("expected error for self-referencing node, got nil")
	}

	if cycle == nil {
		t.Fatal("expected cycle path, got nil")
	}

	if len(cycle) < 2 {
		t.Errorf("expected cycle path with at least 2 entries (showing the loop), got %v", cycle)
	}

	if cycle[0] != "A" {
		t.Errorf("expected first node in cycle to be A, got %s", cycle[0])
	}
}

func TestValidateValidGraph(t *testing.T) {
	// Valid graph with no issues
	g := NewGraph()

	nodeA := &Node{ID: "A", Name: "A", Type: "model"}
	nodeB := &Node{ID: "B", Name: "B", Type: "model"}
	nodeC := &Node{ID: "C", Name: "C", Type: "model"}

	g.AddNode(nodeA)
	g.AddNode(nodeB)
	g.AddNode(nodeC)

	g.AddEdge("B", "A") // B depends on A
	g.AddEdge("C", "B") // C depends on B

	err := Validate(g)
	if err != nil {
		t.Fatalf("unexpected error for valid graph: %v", err)
	}
}

func TestValidateMissingDependency(t *testing.T) {
	// Graph with a dependency that doesn't exist as a node
	// This is actually prevented by AddEdge, so we need to test
	// a node that references a dependency that was removed or never added
	g := NewGraph()

	nodeA := &Node{ID: "A", Name: "A", Type: "model"}
	nodeB := &Node{ID: "B", Name: "B", Type: "model", Dependencies: []string{"C"}}

	g.AddNode(nodeA)
	g.AddNode(nodeB)

	// Note: We can't use AddEdge to create an edge to a non-existent node
	// But nodes can have Dependencies listed that aren't in the graph
	// Validate should detect this mismatch

	err := Validate(g)
	if err == nil {
		t.Fatal("expected error for node with missing dependency, got nil")
	}

	if !strings.Contains(err.Error(), "dependency") && !strings.Contains(err.Error(), "C") {
		t.Errorf("expected error message about missing dependency C, got: %v", err)
	}
}

func TestValidateWithCycle(t *testing.T) {
	// Validate should detect cycles
	g := NewGraph()

	nodeA := &Node{ID: "A", Name: "A", Type: "model"}
	nodeB := &Node{ID: "B", Name: "B", Type: "model"}

	g.AddNode(nodeA)
	g.AddNode(nodeB)

	g.AddEdge("A", "B") // A depends on B
	g.AddEdge("B", "A") // B depends on A (cycle!)

	err := Validate(g)
	if err == nil {
		t.Fatal("expected error for graph with cycle, got nil")
	}

	if !strings.Contains(err.Error(), "cycle") {
		t.Errorf("expected error message about cycle, got: %v", err)
	}
}

func TestValidateEmptyGraph(t *testing.T) {
	g := NewGraph()

	err := Validate(g)
	if err != nil {
		t.Fatalf("unexpected error for empty graph: %v", err)
	}
}

func TestValidateSelfReference(t *testing.T) {
	g := NewGraph()

	nodeA := &Node{ID: "A", Name: "A", Type: "model"}
	g.AddNode(nodeA)
	g.AddEdge("A", "A") // Self-reference

	err := Validate(g)
	if err == nil {
		t.Fatal("expected error for self-referencing node, got nil")
	}

	if !strings.Contains(err.Error(), "cycle") && !strings.Contains(err.Error(), "self") {
		t.Errorf("expected error message about cycle or self-reference, got: %v", err)
	}
}
