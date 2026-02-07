package dag

import (
	"fmt"
)

// TopologicalSort performs a topological sort on the graph using Kahn's algorithm.
// Returns an ordered slice of nodes where dependencies come before dependents.
// Returns an error if the graph contains a cycle.
func TopologicalSort(g *Graph) ([]*Node, error) {
	if g == nil {
		return nil, fmt.Errorf("cannot sort nil graph")
	}

	nodes := g.GetNodes()
	if len(nodes) == 0 {
		return []*Node{}, nil
	}

	// Step 1: Calculate in-degrees for all nodes
	// In-degree = number of dependencies a node has
	inDegree := make(map[string]int)
	for _, node := range nodes {
		deps := g.GetDependencies(node.ID)
		inDegree[node.ID] = len(deps)
	}

	// Step 2: Find all nodes with in-degree 0 and add to queue
	// These are nodes with no dependencies
	queue := []string{}
	for id, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, id)
		}
	}

	// Step 3: Process queue using Kahn's algorithm
	result := []*Node{}

	for len(queue) > 0 {
		// Dequeue a node with in-degree 0
		currentID := queue[0]
		queue = queue[1:]

		// Add to result
		node, _ := g.GetNode(currentID)
		result = append(result, node)

		// For each node that depends on currentID, reduce their in-degree
		// We need to find nodes where there's an edge from that node to currentID
		for _, n := range nodes {
			if g.HasEdge(n.ID, currentID) {
				inDegree[n.ID]--
				if inDegree[n.ID] == 0 {
					queue = append(queue, n.ID)
				}
			}
		}
	}

	// Step 4: Check if all nodes were processed
	// If not, there's a cycle
	if len(result) != len(nodes) {
		return nil, fmt.Errorf("cycle detected in graph: processed %d of %d nodes", len(result), len(nodes))
	}

	return result, nil
}
