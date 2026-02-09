package template

import (
	"fmt"
	"os"
	"strings"
)

// DependencyTracker tracks dependencies between models.
type DependencyTracker interface {
	AddDependency(from, to string) error
}

// makeRefFunc creates a ref() function for template use.
// Returns fully qualified table name and registers dependency if tracker provided.
func makeRefFunc(ctx *Context, tracker DependencyTracker) func(string) string {
	return func(modelName string) string {
		// Register dependency if we have a current model and tracker
		if tracker != nil && ctx.CurrentModel != "" {
			_ = tracker.AddDependency(ctx.CurrentModel, modelName)
		}

		// Return qualified table name
		if ctx.Schema != "" {
			return fmt.Sprintf("%s.%s", ctx.Schema, modelName)
		}
		return modelName
	}
}

// makeVarFunc creates a var() function for template use.
// Retrieves value from Context.Vars map, returns error if not found.
func makeVarFunc(ctx *Context) func(string) (interface{}, error) {
	return func(varName string) (interface{}, error) {
		val, ok := ctx.Vars[varName]
		if !ok {
			return nil, fmt.Errorf("variable not found: %s", varName)
		}
		return val, nil
	}
}

// makeConfigFunc creates a config() function for template use.
// Access configuration from Context.Config, supports dot notation for nested keys.
func makeConfigFunc(ctx *Context) func(string) (interface{}, error) {
	return func(key string) (interface{}, error) {
		// Support dot notation for nested keys
		if strings.Contains(key, ".") {
			return getNestedConfig(ctx.Config, key)
		}

		// Simple key lookup
		val, ok := ctx.Config[key]
		if !ok {
			return nil, fmt.Errorf("config key not found: %s", key)
		}
		return val, nil
	}
}

// getNestedConfig retrieves a nested config value using dot notation.
func getNestedConfig(config map[string]interface{}, key string) (interface{}, error) {
	parts := strings.Split(key, ".")
	var current interface{} = config

	for i, part := range parts {
		m, ok := current.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("config path invalid at %s", strings.Join(parts[:i], "."))
		}

		val, ok := m[part]
		if !ok {
			return nil, fmt.Errorf("config key not found: %s", key)
		}
		current = val
	}

	return current, nil
}

// makeSourceFunc creates a source() function for template use.
// Returns qualified source table name.
func makeSourceFunc(ctx *Context) func(string, string) (string, error) {
	return func(sourceName, tableName string) (string, error) {
		// Check if source is configured
		source, ok := ctx.Sources[sourceName]
		if !ok {
			// Fall back to simple source.table format
			return fmt.Sprintf("%s.%s", sourceName, tableName), nil
		}

		// Look up table in source
		qualifiedName, ok := source[tableName]
		if !ok {
			return "", fmt.Errorf("table %s not found in source %s", tableName, sourceName)
		}

		return qualifiedName, nil
	}
}

// makeSeedFunc creates a seed() function for template use.
// Returns qualified table name for the specified seed.
func makeSeedFunc(ctx *Context) func(string) (string, error) {
	return func(seedName string) (string, error) {
		if seedName == "" {
			return "", fmt.Errorf("seed name cannot be empty")
		}

		tableName, ok := ctx.Seeds[seedName]
		if !ok {
			return "", fmt.Errorf("seed %q not found", seedName)
		}

		return tableName, nil
	}
}

// makeEnvVarFunc creates an env_var() function for template use.
// Gets environment variable with optional default value.
func makeEnvVarFunc() func(string, ...string) (string, error) {
	return func(key string, defaultVal ...string) (string, error) {
		val := os.Getenv(key)

		// If variable is set, return it
		if val != "" {
			return val, nil
		}

		// If default provided, use it
		if len(defaultVal) > 0 {
			return defaultVal[0], nil
		}

		// No variable and no default - error
		return "", fmt.Errorf("environment variable not set: %s", key)
	}
}

// makeIsIncrementalFunc creates an is_incremental() function for template use.
// Returns whether the current execution is an incremental run.
func makeIsIncrementalFunc(ctx *Context) func() bool {
	return func() bool {
		return ctx.IsIncremental
	}
}

// makeThisFunc creates a this() function for template use.
// Returns the current model's table name with schema qualification if set.
// Returns an error if CurrentModelTable is not set (protects against misuse).
func makeThisFunc(ctx *Context) func() (string, error) {
	return func() (string, error) {
		// Error if CurrentModelTable not set - protects against misuse
		if ctx.CurrentModelTable == "" {
			return "", fmt.Errorf("this() called but CurrentModelTable not set in context")
		}

		// Return qualified table name if schema is set
		if ctx.Schema != "" {
			return fmt.Sprintf("%s.%s", ctx.Schema, ctx.CurrentModelTable), nil
		}

		// Return just the table name
		return ctx.CurrentModelTable, nil
	}
}
