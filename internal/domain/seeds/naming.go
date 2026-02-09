package seeds

import (
	"path/filepath"
	"strings"

	"github.com/jpconstantineau/gorchata/internal/config"
)

// ResolveTableName determines the target table name for a seed file
// based on the configured naming strategy.
//
// Strategies:
//   - filename: uses the CSV filename (without extension) as table name
//   - folder: uses the parent folder name as table name
//   - static: uses the configured static_name as table name
//
// If a prefix is configured, it will be prepended to the resolved name.
func ResolveTableName(seedFilePath string, namingConfig *config.NamingConfig) string {
	var baseName string

	switch namingConfig.Strategy {
	case config.NamingStrategyStatic:
		// Static strategy: use the configured static name
		baseName = namingConfig.StaticName

	case config.NamingStrategyFolder:
		// Folder strategy: extract parent directory name
		// Normalize path separators for cross-platform compatibility
		normalizedPath := filepath.ToSlash(seedFilePath)
		dir := filepath.Dir(normalizedPath)

		// Get the last component of the directory path
		baseName = filepath.Base(dir)

		// If we're at root (dir is "." or "/"), use empty or fallback
		if baseName == "." || baseName == "/" || baseName == "\\" {
			baseName = ""
		}

	case config.NamingStrategyFilename:
		fallthrough
	default:
		// Filename strategy (default): extract filename without extension
		// Normalize path separators for cross-platform compatibility
		normalizedPath := filepath.ToSlash(seedFilePath)
		filename := filepath.Base(normalizedPath)

		// Remove extension
		ext := filepath.Ext(filename)
		if ext != "" {
			baseName = strings.TrimSuffix(filename, ext)
		} else {
			baseName = filename
		}
	}

	// Apply prefix if configured
	if namingConfig.Prefix != "" {
		return namingConfig.Prefix + baseName
	}

	return baseName
}
