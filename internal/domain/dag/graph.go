package dag

import (
	"fmt"
)

// Graph represents a directed acyclic graph (DAG) of dependencies.
// It uses an adjacency list representation for efficient edge lookups.
type Graph struct {
	// nodes stores all nodes by their ID
	nodes map[string]*Node

	// edges stores adjacency list: from -> [to1, to2, ...]
	// If "model_orders" depends on "model_users", we have:
	// edges["model_orders"] = ["model_users"]
	edges map[string][]string
}

// NewGraph creates a new empty graph.
func NewGraph() *Graph {
	return &Graph{
		nodes: make(map[string]*Node),
		edges: make(map[string][]string),
	}
}

// AddNode adds a node to the graph.
// Returns an error if a node with the same ID already exists.
func (g *Graph) AddNode(node *Node) error {
	if node == nil {
		return fmt.Errorf("cannot add nil node")
	}

	if _, exists := g.nodes[node.ID]; exists {
		return fmt.Errorf("node with ID '%s' already exists", node.ID)
	}

	g.nodes[node.ID] = node
	return nil
}

// AddEdge adds a directed edge from 'from' to 'to', representing
// that 'from' depends on 'to'.
// Returns an error if either node doesn't exist.
func (g *Graph) AddEdge(from, to string) error {
	if _, exists := g.nodes[from]; !exists {
		return fmt.Errorf("node '%s' does not exist", from)
	}

	if _, exists := g.nodes[to]; !exists {
		return fmt.Errorf("node '%s' does not exist", to)
	}

	// Check if edge already exists to avoid duplicates
	for _, existing := range g.edges[from] {
		if existing == to {
			return nil // Edge already exists, silently succeed
		}
	}

	g.edges[from] = append(g.edges[from], to)
	return nil
}

// GetNode retrieves a node by its ID.
// Returns the node and true if found, nil and false otherwise.
func (g *Graph) GetNode(id string) (*Node, bool) {
	node, exists := g.nodes[id]
	return node, exists
}

// GetDependencies returns the list of node IDs that the given node depends on.
// Returns an empty slice if the node has no dependencies or doesn't exist.
func (g *Graph) GetDependencies(id string) []string {
	deps, exists := g.edges[id]
	if !exists {
		return []string{}
	}

	// Return a copy to prevent external modification
	result := make([]string, len(deps))
	copy(result, deps)
	return result
}

// GetNodes returns all nodes in the graph.
// The order is not guaranteed.
func (g *Graph) GetNodes() []*Node {
	nodes := make([]*Node, 0, len(g.nodes))
	for _, node := range g.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

// HasEdge checks if a directed edge exists from 'from' to 'to'.
func (g *Graph) HasEdge(from, to string) bool {
	deps, exists := g.edges[from]
	if !exists {
		return false
	}

	for _, dep := range deps {
		if dep == to {
			return true
		}
	}
	return false
}
