package app

import (
	"github.com/jpconstantineau/gorchata/internal/cli"
)

// App represents the Gorchata application
type App struct {
	// Future: Add configuration, logger, or other app-level dependencies here
}

// New creates and initializes a new App instance
func New() (*App, error) {
	app := &App{}
	return app, nil
}

// Run executes the application with the provided arguments
// This is the application wiring layer that delegates to the CLI router
func (a *App) Run(args []string) error {
	// Delegate to CLI router
	// Future: Could inject app-level dependencies (config, logger, etc.) into CLI here
	return cli.Run(args)
}
