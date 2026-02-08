package singular

import (
	"regexp"
	"strings"

	"github.com/jpconstantineau/gorchata/internal/domain/test"
)

// ParseTestMetadata extracts test configuration from SQL comments
// Looks for patterns like: -- config(severity='warn', store_failures=true)
func ParseTestMetadata(sqlContent string) (*test.TestConfig, error) {
	config := test.DefaultTestConfig()

	// Look for config comment pattern
	configPattern := regexp.MustCompile(`--\s*config\((.*?)\)`)
	matches := configPattern.FindStringSubmatch(sqlContent)

	if len(matches) < 2 {
		// No config found, return defaults
		return config, nil
	}

	configStr := matches[1]

	// Parse individual config options
	// Format: key='value' or key=value
	optionPattern := regexp.MustCompile(`(\w+)\s*=\s*'([^']*)'|(\w+)\s*=\s*(\w+)`)
	options := optionPattern.FindAllStringSubmatch(configStr, -1)

	for _, opt := range options {
		var key, value string
		if opt[1] != "" {
			// Quoted value
			key = opt[1]
			value = opt[2]
		} else {
			// Unquoted value
			key = opt[3]
			value = opt[4]
		}

		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)

		switch key {
		case "severity":
			switch value {
			case "warn":
				config.Severity = test.SeverityWarn
			case "error":
				config.Severity = test.SeverityError
			}
		case "store_failures":
			config.StoreFailures = (value == "true")
		case "where":
			config.Where = value
		}
	}

	return config, nil
}
