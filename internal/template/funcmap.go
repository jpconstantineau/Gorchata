package template

import (
	"text/template"
)

// BuildFuncMap creates a template.FuncMap with all custom functions.
// The functions are bound to the provided context and dependency tracker.
func BuildFuncMap(ctx *Context, tracker DependencyTracker) template.FuncMap {
	// Handle nil context gracefully
	if ctx == nil {
		ctx = NewContext()
	}

	return template.FuncMap{
		"ref":            makeRefFunc(ctx, tracker),
		"var":            makeVarFunc(ctx),
		"config":         makeConfigFunc(ctx), // For accessing config values; materialization directives parsed separately
		"source":         makeSourceFunc(ctx),
		"seed":           makeSeedFunc(ctx),
		"env_var":        makeEnvVarFunc(),
		"is_incremental": makeIsIncrementalFunc(ctx),
		"this":           makeThisFunc(ctx),
	}
}
