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

	// If TemplateContent is set, render it with the correct incremental context
	if model.TemplateContent != "" {
		// Determine if this is an incremental run
		// First check if the table actually exists - if not, treat as full refresh (first run)
		tableExists := false
		if model.MaterializationConfig.Type == materialization.MaterializationIncremental {
			exists, err := e.adapter.TableExists(ctx, model.ID)
			if err != nil {
				// If we can't check table existence, log but continue (assume doesn't exist)
				tableExists = false
			} else {
				tableExists = exists
			}
		}

		isIncremental := model.MaterializationConfig.Type == materialization.MaterializationIncremental &&
			!model.MaterializationConfig.FullRefresh &&
			tableExists

		// Build template context with incremental settings
		tmplCtx := template.NewContext(
			template.WithCurrentModel(model.ID),
			template.WithIsIncremental(isIncremental),
			template.WithCurrentModelTable(model.ID),
		)

		// Parse the template
		tmpl, err := e.templateEngine.Parse(model.ID, model.TemplateContent)
		if err != nil {
			result.Status = StatusFailed
			result.Error = fmt.Sprintf("failed to parse template: %v", err)
			result.EndTime = time.Now()
			return result, fmt.Errorf("failed to parse template for model %s: %w", model.ID, err)
		}

		// Render the template with the incremental context
		rendered, err := template.Render(tmpl, tmplCtx, nil)
		if err != nil {
			result.Status = StatusFailed
			result.Error = fmt.Sprintf("failed to render template: %v", err)
			result.EndTime = time.Now()
			return result, fmt.Errorf("failed to render template for model %s: %w", model.ID, err)
		}

		// Update the compiled SQL with the newly rendered version
		model.SetCompiledSQL(rendered)
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
	// Strip SQL comments first to properly detect DDL statements
	sqlWithoutComments := stripSQLComments(model.CompiledSQL)
	trimmedSQL := strings.TrimSpace(strings.ToUpper(sqlWithoutComments))
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

// stripSQLComments removes SQL comments from a string
// Handles both single-line (--) and multi-line (/* */) comments
func stripSQLComments(sql string) string {
	var result strings.Builder
	inMultiLineComment := false
	inSingleLineComment := false
	inStringLiteral := false

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		// Check for string literals (don't process comments inside strings)
		if ch == '\'' && !inMultiLineComment && !inSingleLineComment {
			inStringLiteral = !inStringLiteral
			result.WriteByte(ch)
			continue
		}

		// If in string literal, just copy the character
		if inStringLiteral {
			result.WriteByte(ch)
			continue
		}

		// Check for end of single-line comment
		if inSingleLineComment {
			if ch == '\n' {
				inSingleLineComment = false
				result.WriteByte(ch) // Keep the newline
			}
			continue
		}

		// Check for end of multi-line comment
		if inMultiLineComment {
			if ch == '*' && i+1 < len(sql) && sql[i+1] == '/' {
				inMultiLineComment = false
				i++ // Skip the '/'
			}
			continue
		}

		// Check for start of single-line comment
		if ch == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			inSingleLineComment = true
			i++ // Skip the second '-'
			continue
		}

		// Check for start of multi-line comment
		if ch == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			inMultiLineComment = true
			i++ // Skip the '*'
			continue
		}

		// Regular character - add to result
		result.WriteByte(ch)
	}

	return result.String()

}
