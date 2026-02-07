package main

import (
	"fmt"
	"os"

	"github.com/pierre/gorchata/internal/app"
)

func main() {
	// Create the application instance
	application, err := app.New()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing application: %v\n", err)
		os.Exit(1)
	}

	// Run the application with command-line arguments
	if err := application.Run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
