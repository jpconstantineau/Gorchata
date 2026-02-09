package seeds

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Scope constants define the discovery scope for seed files
const (
	ScopeFile   = "file"   // Single specified file
	ScopeFolder = "folder" // All files in one directory (non-recursive)
	ScopeTree   = "tree"   // All files recursively
)

// isSeedFile checks if a file is a valid seed file (CSV or SQL)
func isSeedFile(path string) bool {
	lowerPath := strings.ToLower(path)
	return strings.HasSuffix(lowerPath, ".csv") || strings.HasSuffix(lowerPath, ".sql")
}

// DiscoverSeeds discovers seed files based on the specified path and scope.
//
// Scopes:
//   - ScopeFile: Returns the single specified file if it exists and is a .csv or .sql
//   - ScopeFolder: Returns all .csv and .sql files directly in the specified directory (non-recursive)
//   - ScopeTree: Returns all .csv and .sql files in the directory tree (recursive)
//
// Returns a list of absolute file paths to discovered seed files.
func DiscoverSeeds(path string, scope string) ([]string, error) {
	switch scope {
	case ScopeFile:
		return discoverFile(path)
	case ScopeFolder:
		return discoverFolder(path)
	case ScopeTree:
		return discoverTree(path)
	default:
		return nil, fmt.Errorf("unknown scope: %s", scope)
	}
}

// discoverFile discovers a single file
func discoverFile(path string) ([]string, error) {
	// Check if file exists
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	// Check if it's actually a file (not a directory)
	if info.IsDir() {
		return nil, fmt.Errorf("path is a directory, not a file: %s", path)
	}

	// Check if it's a seed file (CSV or SQL)
	if !isSeedFile(path) {
		return nil, fmt.Errorf("file is not a seed file (.csv or .sql): %s", path)
	}

	// Convert to absolute path
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}

	return []string{absPath}, nil
}

// discoverFolder discovers all seed files in a single directory (non-recursive)
func discoverFolder(path string) ([]string, error) {
	// Check if directory exists
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("failed to stat directory: %w", err)
	}

	// Check if it's actually a directory
	if !info.IsDir() {
		return nil, fmt.Errorf("path is not a directory: %s", path)
	}

	// Read directory contents
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	// Filter seed files (CSV and SQL)
	var seedFiles []string
	for _, entry := range entries {
		// Skip directories
		if entry.IsDir() {
			continue
		}

		// Check if file is a seed file
		if isSeedFile(entry.Name()) {
			absPath := filepath.Join(path, entry.Name())
			seedFiles = append(seedFiles, absPath)
		}
	}

	return seedFiles, nil
}

// discoverTree discovers all seed files recursively in a directory tree
func discoverTree(path string) ([]string, error) {
	// Check if directory exists
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("failed to stat directory: %w", err)
	}

	// Check if it's actually a directory
	if !info.IsDir() {
		return nil, fmt.Errorf("path is not a directory: %s", path)
	}

	// Walk the directory tree
	var seedFiles []string
	err = filepath.Walk(path, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Check if file is a seed file
		if isSeedFile(filePath) {
			seedFiles = append(seedFiles, filePath)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to walk directory tree: %w", err)
	}

	return seedFiles, nil
}
