package cli

import (
	"fmt"
)

const (
	version = "0.1.0"
)

// Run is the main entry point for the CLI, routing to subcommands
func Run(args []string) error {
	// Handle no arguments
	if len(args) == 0 {
		return fmt.Errorf("no command specified. Use 'gorchata --help' for usage information")
	}

	// Handle help flags
	if args[0] == "--help" || args[0] == "-h" || args[0] == "help" {
		printUsage()
		return nil
	}

	// Handle version flags
	if args[0] == "--version" || args[0] == "-v" || args[0] == "version" {
		printVersion()
		return nil
	}

	// Route to subcommands
	command := args[0]
	commandArgs := args[1:]

	switch command {
	case "init":
		return InitCommand(commandArgs)
	case "run":
		return RunCommand(commandArgs)
	case "compile":
		return CompileCommand(commandArgs)
	case "test":
		return TestCommand(commandArgs)
	case "build":
		return BuildCommand(commandArgs)
	case "docs":
		return DocsCommand(commandArgs)
	default:
		return fmt.Errorf("unknown command: %s. Use 'gorchata --help' for usage information", command)
	}
}

// printUsage prints the CLI usage information
func printUsage() {
	fmt.Println("Gorchata - SQL-first data transformation tool")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  gorchata [command] [options]")
	fmt.Println()
	fmt.Println("Available Commands:")
	fmt.Println("  init      Initialize a new Gorchata project")
	fmt.Println("  run       Execute SQL transformations against the database")
	fmt.Println("  compile   Compile SQL templates without executing them")
	fmt.Println("  test      Run data quality tests")
	fmt.Println("  build     Run models and tests (full build workflow)")
	fmt.Println("  docs      Generate documentation (not yet implemented)")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  -h, --help      Show help information")
	fmt.Println("  -v, --version   Show version information")
	fmt.Println()
	fmt.Println("Use 'gorchata [command] --help' for more information about a command.")
}

// printVersion prints the version information
func printVersion() {
	fmt.Printf("Gorchata version %s\n", version)
}
