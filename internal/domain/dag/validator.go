package dag

import (
	"fmt"
	"strings"
)

// DetectCycles finds cycles in the graph using depth-first search (DFS).
// Returns the cycle path if found, nil if the graph is acyclic.
// If a cycle is found, also returns an error describing the cycle.
func DetectCycles(g *Graph) ([]string, error) {
	if g == nil {
		return nil, fmt.Errorf("cannot detect cycles in nil graph")
	}

	nodes := g.GetNodes()
	if len(nodes) == 0 {
		return nil, nil
	}

	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	// DFS function to detect cycles
	var dfs func(string, []string) ([]string, bool)
	dfs = func(nodeID string, path []string) ([]string, bool) {
		visited[nodeID] = true
		recStack[nodeID] = true
		path = append(path, nodeID)

		// Check all dependencies of this node
		deps := g.GetDependencies(nodeID)
		for _, dep := range deps {
			if !visited[dep] {
				// Visit unvisited dependency
				if cycle, found := dfs(dep, path); found {
					return cycle, true
				}
			} else if recStack[dep] {
				// Found a back edge - cycle detected
				// Build the cycle path from dep to current node and back to dep
				cyclePath := append(path, dep)
				return cyclePath, true
			}
		}

		// Remove from recursion stack when backtracking
		recStack[nodeID] = false
		return nil, false
	}

	// Try DFS from each unvisited node
	for _, node := range nodes {
		if !visited[node.ID] {
			if cycle, found := dfs(node.ID, []string{}); found {
				return cycle, fmt.Errorf("cycle detected: %s", strings.Join(cycle, " -> "))
			}
		}
	}

	return nil, nil
}

// Validate performs comprehensive validation on the graph.
// It checks for:
// - Cycles (including self-references)
// - Missing dependency nodes
// Returns an error if any validation fails.
func Validate(g *Graph) error {
	if g == nil {
		return fmt.Errorf("cannot validate nil graph")
	}

	nodes := g.GetNodes()
	if len(nodes) == 0 {
		return nil // Empty graph is valid
	}

	// Check for cycles
	cycle, err := DetectCycles(g)
	if err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}
	if cycle != nil {
		return fmt.Errorf("validation failed: cycle detected")
	}

	// Check for missing dependencies
	// Build a map of all node IDs
	nodeIDs := make(map[string]bool)
	for _, node := range nodes {
		nodeIDs[node.ID] = true
	}

	// Check each node's dependencies exist in the graph
	for _, node := range nodes {
		deps := g.GetDependencies(node.ID)
		for _, dep := range deps {
			if !nodeIDs[dep] {
				return fmt.Errorf("node '%s' has dependency '%s' which does not exist in the graph", node.ID, dep)
			}
		}

		// Also check Node.Dependencies field if it's set
		if node.Dependencies != nil {
			for _, dep := range node.Dependencies {
				if !nodeIDs[dep] {
					return fmt.Errorf("node '%s' references dependency '%s' which does not exist in the graph", node.ID, dep)
				}
			}
		}
	}

	return nil
}
