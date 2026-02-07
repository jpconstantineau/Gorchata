package template

import (
	"text/template"
)

// Engine wraps text/template and provides custom function support.
type Engine struct {
	tracker    DependencyTracker
	leftDelim  string
	rightDelim string
}

// EngineOption configures an Engine.
type EngineOption func(*Engine)

// New creates a new template engine with the given options.
func New(opts ...EngineOption) *Engine {
	engine := &Engine{
		leftDelim:  "{{",
		rightDelim: "}}",
	}

	for _, opt := range opts {
		opt(engine)
	}

	return engine
}

// WithDependencyTracker sets the dependency tracker for the engine.
func WithDependencyTracker(tracker DependencyTracker) EngineOption {
	return func(e *Engine) {
		e.tracker = tracker
	}
}

// WithDelimiters sets custom template delimiters.
func WithDelimiters(left, right string) EngineOption {
	return func(e *Engine) {
		e.leftDelim = left
		e.rightDelim = right
	}
}

// Parse parses a template with the given name and content.
func (e *Engine) Parse(name, content string) (*Template, error) {
	// Create a new text/template
	tmpl := template.New(name)

	// Set custom delimiters if configured
	tmpl = tmpl.Delims(e.leftDelim, e.rightDelim)

	// Add functions with a default context for parsing
	// These will be replaced with actual context during rendering
	funcMap := BuildFuncMap(NewContext(), e.tracker)
	tmpl = tmpl.Funcs(funcMap)

	// Parse the template content
	parsed, err := tmpl.Parse(content)
	if err != nil {
		return nil, err
	}

	return &Template{
		tmpl:   parsed,
		name:   name,
		engine: e,
	}, nil
}
