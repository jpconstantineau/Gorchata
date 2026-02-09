package seeds

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"text/template"

	"github.com/jpconstantineau/gorchata/internal/platform"
)

// ExecuteSQLSeed executes a SQL seed file by rendering templates and executing statements
func ExecuteSQLSeed(ctx context.Context, adapter platform.DatabaseAdapter, sqlContent string, vars map[string]interface{}, config map[string]interface{}) error {
	// Input validation
	if adapter == nil {
		return fmt.Errorf("adapter cannot be nil")
	}

	// Handle empty SQL
	if strings.TrimSpace(sqlContent) == "" {
		return nil
	}

	// Render template with {{ var }} support only
	rendered, err := renderSQLSeedTemplate(sqlContent, vars)
	if err != nil {
		return fmt.Errorf("failed to render SQL template: %w", err)
	}

	// Split into statements by semicolons
	statements := splitSQLStatements(rendered)

	// Execute each statement
	for i, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		if err := adapter.ExecuteDDL(ctx, stmt); err != nil {
			return fmt.Errorf("failed to execute statement %d: %w", i+1, err)
		}
	}

	return nil
}

// renderSQLSeedTemplate renders a SQL template with {{ var }} support only
func renderSQLSeedTemplate(content string, vars map[string]interface{}) (string, error) {
	// Validate no forbidden functions
	if err := validateNoForbiddenFunctions(content); err != nil {
		return "", err
	}

	// If no template markers, return as-is
	if !strings.Contains(content, "{{") {
		return content, nil
	}

	// Create template with var function only
	tmpl, err := template.New("sql_seed").Funcs(template.FuncMap{
		"var": func(key string) (interface{}, error) {
			if vars == nil {
				return nil, fmt.Errorf("no variables provided for key: %s", key)
			}
			val, ok := vars[key]
			if !ok {
				return nil, fmt.Errorf("variable not found: %s", key)
			}
			return val, nil
		},
	}).Parse(content)

	if err != nil {
		return "", fmt.Errorf("failed to parse template: %w", err)
	}

	// Execute template
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, nil); err != nil {
		return "", fmt.Errorf("failed to execute template: %w", err)
	}

	return buf.String(), nil
}

// validateNoForbiddenFunctions checks that SQL doesn't contain ref/source/seed functions
func validateNoForbiddenFunctions(sql string) error {
	forbiddenFuncs := []string{"ref", "source", "seed"}

	for _, fn := range forbiddenFuncs {
		pattern := fmt.Sprintf("{{ %s ", fn)
		if strings.Contains(sql, pattern) {
			return fmt.Errorf("forbidden function '%s' used in SQL seed (only {{ var }} is allowed)", fn)
		}
		// Also check for {{ref, {{source, {{seed (no space)
		pattern = fmt.Sprintf("{{%s ", fn)
		if strings.Contains(sql, pattern) {
			return fmt.Errorf("forbidden function '%s' used in SQL seed (only {{ var }} is allowed)", fn)
		}
	}

	return nil
}

// splitSQLStatements splits SQL content into individual statements by semicolons
func splitSQLStatements(sql string) []string {
	// Simple split by semicolons
	// Note: This is a simple implementation that doesn't handle semicolons inside strings
	// For production use, you might want a more sophisticated SQL parser
	statements := strings.Split(sql, ";")

	// Filter out empty statements
	result := make([]string, 0, len(statements))
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt != "" {
			result = append(result, stmt)
		}
	}

	return result
}
