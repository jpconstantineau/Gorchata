package template

import (
	"bytes"
	"fmt"
)

// Render executes a template with the given context and data.
// The context provides custom functions (ref, var, config, etc.).
// The data is passed as the root data object to the template (accessible via .).
func Render(tmpl *Template, ctx *Context, data interface{}) (string, error) {
	if tmpl == nil {
		return "", fmt.Errorf("template is nil")
	}
	if ctx == nil {
		ctx = NewContext()
	}

	// Build FuncMap with the actual context
	funcMap := BuildFuncMap(ctx, tmpl.engine.tracker)

	// Clone the template and add functions
	// This allows us to use the same parsed template with different contexts
	clone, err := tmpl.tmpl.Clone()
	if err != nil {
		return "", fmt.Errorf("failed to clone template %q: %w", tmpl.name, err)
	}

	clone = clone.Funcs(funcMap)

	// Set option to error on missing keys
	clone = clone.Option("missingkey=error")

	// Execute the template
	var buf bytes.Buffer
	if err := clone.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("template %q execution failed: %w", tmpl.name, err)
	}

	return buf.String(), nil
}
