package generic

import (
	"fmt"
	"regexp"
	"strings"
)

// ParseTestTemplate parses a {% test %} block from SQL content
// Returns: test name, parameter list, SQL template, and any error
func ParseTestTemplate(sqlContent string) (string, []string, string, error) {
	// Pattern to match {% test name(params) %} ... {% endtest %}
	// (?s) flag enables dot to match newlines
	pattern := regexp.MustCompile(`(?s)\{%\s*test\s+(\w+)\s*\((.*?)\)\s*%\}(.*)\{%\s*endtest\s*%\}`)
	matches := pattern.FindStringSubmatch(sqlContent)

	if len(matches) < 4 {
		return "", nil, "", fmt.Errorf("invalid template syntax: missing test or endtest tags")
	}

	testName := strings.TrimSpace(matches[1])
	paramsStr := strings.TrimSpace(matches[2])
	sqlTemplate := strings.TrimSpace(matches[3])

	// Parse parameters
	var params []string
	if paramsStr != "" {
		// Split by comma and trim each param
		paramParts := strings.Split(paramsStr, ",")
		for _, p := range paramParts {
			param := strings.TrimSpace(p)
			if param != "" {
				params = append(params, param)
			}
		}
	}

	if testName == "" {
		return "", nil, "", fmt.Errorf("test name cannot be empty")
	}

	if sqlTemplate == "" {
		return "", nil, "", fmt.Errorf("SQL template cannot be empty")
	}

	return testName, params, sqlTemplate, nil
}
