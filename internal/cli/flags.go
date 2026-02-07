package cli

import (
	"flag"
)

// CommonFlags contains flags shared across multiple commands
type CommonFlags struct {
	Target   string
	Models   string
	FailFast bool
	Verbose  bool
}

// AddCommonFlags registers common flags to a FlagSet
func AddCommonFlags(fs *flag.FlagSet, cf *CommonFlags) {
	fs.StringVar(&cf.Target, "target", "", "Target environment (from profiles.yml)")
	fs.StringVar(&cf.Models, "models", "", "Comma-separated list of models to process")
	fs.BoolVar(&cf.FailFast, "fail-fast", false, "Stop execution on first error")
	fs.BoolVar(&cf.Verbose, "verbose", false, "Enable verbose output")
}
