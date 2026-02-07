package template

import (
	"text/template"
)

// Template wraps a parsed text/template.
type Template struct {
	tmpl   *template.Template
	name   string
	engine *Engine
}

// Name returns the template name.
func (t *Template) Name() string {
	return t.name
}
