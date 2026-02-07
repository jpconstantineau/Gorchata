package executor

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jpconstantineau/gorchata/internal/domain/dag"
	"github.com/jpconstantineau/gorchata/internal/domain/materialization"
	"github.com/jpconstantineau/gorchata/internal/platform"
	"github.com/jpconstantineau/gorchata/internal/template"
)

// Engine coordinates the execution of models
type Engine struct {
	adapter        platform.DatabaseAdapter
	templateEngine *template.Engine
}

// NewEngine creates a new execution engine
func NewEngine(adapter platform.DatabaseAdapter, templateEngine *template.Engine) (*Engine, error) {
	if adapter == nil {
		return nil, fmt.Errorf("database adapter cannot be nil")
	}
	if templateEngine == nil {
		return nil, fmt.Errorf("template engine cannot be nil")
	}

	return &Engine{
		adapter:        adapter,
		templateEngine: templateEngine,
	}, nil
}

// ExecuteModel executes a single model
func (e *Engine) ExecuteModel(ctx context.Context, model *Model) (ModelResult, error) {
	result := ModelResult{
		ModelID:   model.ID,
		Status:    StatusRunning,
		StartTime: time.Now(),
	}

	// Validate model has compiled SQL
	if model.CompiledSQL == "" {
		result.Status = StatusFailed
		result.Error = "model has no compiled SQL"
		result.EndTime = time.Now()
		return result, fmt.Errorf("model %s has no compiled SQL", model.ID)
	}

	// Check if this is raw DDL (CREATE TABLE, INSERT, UPDATE, DELETE, etc.)
	// If so, execute directly without materialization strategy
	trimmedSQL := strings.TrimSpace(strings.ToUpper(model.CompiledSQL))
	isRawDDL := strings.HasPrefix(trimmedSQL, "CREATE ") ||
		strings.HasPrefix(trimmedSQL, "INSERT ") ||
		strings.HasPrefix(trimmedSQL, "UPDATE ") ||
		strings.HasPrefix(trimmedSQL, "DELETE ") ||
		strings.HasPrefix(trimmedSQL, "DROP ") ||
		strings.HasPrefix(trimmedSQL, "ALTER ")

	if isRawDDL {
		// Execute raw DDL directly
		// Split by semicolons to handle multiple statements
		statements := strings.Split(model.CompiledSQL, ";")
		var sqlStatements []string

		for _, stmt := range statements {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			if err := e.adapter.ExecuteDDL(ctx, stmt); err != nil {
				result.Status = StatusFailed
				result.Error = fmt.Sprintf("failed to execute SQL: %v", err)
				result.EndTime = time.Now()
				return result, fmt.Errorf("failed to execute SQL for model %s: %w", model.ID, err)
			}
			sqlStatements = append(sqlStatements, stmt)
		}

		result.Status = StatusSuccess
		result.EndTime = time.Now()
		result.SQLStatements = sqlStatements
		return result, nil
	}

	// Get materialization strategy
	strategy, err := materialization.GetStrategyFromConfig(model.MaterializationConfig)
	if err != nil {
		result.Status = StatusFailed
		result.Error = fmt.Sprintf("failed to get strategy: %v", err)
		result.EndTime = time.Now()
		return result, fmt.Errorf("failed to get strategy for model %s: %w", model.ID, err)
	}

	// Generate SQL statements
	sqlStatements, err := strategy.Materialize(model.ID, model.CompiledSQL, model.MaterializationConfig)
	if err != nil {
		result.Status = StatusFailed
		result.Error = fmt.Sprintf("failed to generate SQL: %v", err)
		result.EndTime = time.Now()
		return result, fmt.Errorf("failed to generate SQL for model %s: %w", model.ID, err)
	}

	result.SQLStatements = sqlStatements

	// Execute SQL statements
	for _, sql := range sqlStatements {
		if err := e.adapter.ExecuteDDL(ctx, sql); err != nil {
			result.Status = StatusFailed
			result.Error = fmt.Sprintf("failed to execute SQL: %v", err)
			result.EndTime = time.Now()
			return result, fmt.Errorf("failed to execute SQL for model %s: %w", model.ID, err)
		}
	}

	// Mark as successful
	result.Status = StatusSuccess
	result.EndTime = time.Now()

	return result, nil
}

// ExecuteModels executes multiple models in topological order based on dependencies
func (e *Engine) ExecuteModels(ctx context.Context, models []*Model, failFast bool) (*ExecutionResult, error) {
	result := NewExecutionResult()
	result.Status = StatusRunning

	// Build dependency graph
	graph := dag.NewGraph()

	for _, model := range models {
		node := &dag.Node{
			ID:   model.ID,
			Name: model.ID,
			Type: "model",
		}
		if err := graph.AddNode(node); err != nil {
			result.Complete()
			return result, fmt.Errorf("failed to add node %s to graph: %w", model.ID, err)
		}
	}

	// Add edges for dependencies
	for _, model := range models {
		for _, dep := range model.Dependencies {
			if err := graph.AddEdge(model.ID, dep); err != nil {
				result.Complete()
				return result, fmt.Errorf("failed to add edge from %s to %s: %w", model.ID, dep, err)
			}
		}
	}

	// Perform topological sort
	sortedNodes, err := dag.TopologicalSort(graph)
	if err != nil {
		result.Complete()
		return result, fmt.Errorf("failed to sort DAG: %w", err)
	}

	// Create a map for quick model lookup
	modelMap := make(map[string]*Model)
	for _, model := range models {
		modelMap[model.ID] = model
	}

	// Execute models in topological order
	for _, node := range sortedNodes {
		model, exists := modelMap[node.ID]
		if !exists {
			// This shouldn't happen, but handle it gracefully
			continue
		}

		modelResult, err := e.ExecuteModel(ctx, model)
		result.AddModelResult(modelResult)

		if err != nil {
			if failFast {
				result.Complete()
				return result, fmt.Errorf("execution failed at model %s: %w", model.ID, err)
			}
			// Continue to next model if not fail-fast
		}
	}

	result.Complete()

	// Return error if there were any failures (but only if fail-fast is disabled)
	if !failFast && result.FailureCount() > 0 {
		return result, nil // Return result but no error to allow caller to inspect results
	}

	return result, nil
}
