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

// DiscoverSeeds discovers seed files based on the specified path and scope.
//
// Scopes:
//   - ScopeFile: Returns the single specified file if it exists and is a .csv
//   - ScopeFolder: Returns all .csv files directly in the specified directory (non-recursive)
//   - ScopeTree: Returns all .csv files in the directory tree (recursive)
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

	// Check if it's a CSV file
	if !strings.HasSuffix(strings.ToLower(path), ".csv") {
		return nil, fmt.Errorf("file is not a CSV: %s", path)
	}

	// Convert to absolute path
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}

	return []string{absPath}, nil
}

// discoverFolder discovers all CSV files in a single directory (non-recursive)
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

	// Filter CSV files
	var csvFiles []string
	for _, entry := range entries {
		// Skip directories
		if entry.IsDir() {
			continue
		}

		// Check if file is a CSV
		if strings.HasSuffix(strings.ToLower(entry.Name()), ".csv") {
			absPath := filepath.Join(path, entry.Name())
			csvFiles = append(csvFiles, absPath)
		}
	}

	return csvFiles, nil
}

// discoverTree discovers all CSV files recursively in a directory tree
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
	var csvFiles []string
	err = filepath.Walk(path, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Check if file is a CSV
		if strings.HasSuffix(strings.ToLower(filePath), ".csv") {
			csvFiles = append(csvFiles, filePath)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to walk directory tree: %w", err)
	}

	return csvFiles, nil
}
