package seeds

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDiscoverSeeds_File tests discovery of a single CSV file
func TestDiscoverSeeds_File(t *testing.T) {
	// Setup: Create temp directory with test CSV file
	tmpDir := t.TempDir()
	csvFile := filepath.Join(tmpDir, "customers.csv")
	err := os.WriteFile(csvFile, []byte("id,name\n1,Alice\n2,Bob"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Act
	result, err := DiscoverSeeds(csvFile, ScopeFile)

	// Assert
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("Expected 1 file, got %d", len(result))
	}
	if result[0] != csvFile {
		t.Errorf("Expected %s, got %s", csvFile, result[0])
	}
}

// TestDiscoverSeeds_Folder tests discovery of all CSV files in one folder (non-recursive)
func TestDiscoverSeeds_Folder(t *testing.T) {
	// Setup: Create temp directory with multiple CSV files and a subdirectory
	tmpDir := t.TempDir()

	// Create CSV files in root
	csv1 := filepath.Join(tmpDir, "customers.csv")
	csv2 := filepath.Join(tmpDir, "orders.csv")
	err := os.WriteFile(csv1, []byte("id,name\n1,Alice"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	err = os.WriteFile(csv2, []byte("id,total\n1,100"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Create subdirectory with CSV (should be ignored in folder scope)
	subDir := filepath.Join(tmpDir, "subdir")
	err = os.Mkdir(subDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create subdirectory: %v", err)
	}
	csv3 := filepath.Join(subDir, "products.csv")
	err = os.WriteFile(csv3, []byte("id,product\n1,Widget"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Act
	result, err := DiscoverSeeds(tmpDir, ScopeFolder)

	// Assert
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("Expected 2 files (folder scope, non-recursive), got %d", len(result))
	}

	// Check that result contains both CSV files from root
	found := make(map[string]bool)
	for _, f := range result {
		found[f] = true
	}
	if !found[csv1] {
		t.Errorf("Expected to find %s", csv1)
	}
	if !found[csv2] {
		t.Errorf("Expected to find %s", csv2)
	}
	if found[csv3] {
		t.Errorf("Should not find subdirectory file %s in folder scope", csv3)
	}
}

// TestDiscoverSeeds_Tree tests discovery of all CSV files recursively
func TestDiscoverSeeds_Tree(t *testing.T) {
	// Setup: Create temp directory tree with CSV files
	tmpDir := t.TempDir()

	// Create CSV in root
	csv1 := filepath.Join(tmpDir, "root.csv")
	err := os.WriteFile(csv1, []byte("id,name\n1,Alice"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Create subdirectory with CSV
	subDir1 := filepath.Join(tmpDir, "level1")
	err = os.Mkdir(subDir1, 0755)
	if err != nil {
		t.Fatalf("Failed to create subdirectory: %v", err)
	}
	csv2 := filepath.Join(subDir1, "level1.csv")
	err = os.WriteFile(csv2, []byte("id,value\n1,100"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Create nested subdirectory with CSV
	subDir2 := filepath.Join(subDir1, "level2")
	err = os.Mkdir(subDir2, 0755)
	if err != nil {
		t.Fatalf("Failed to create subdirectory: %v", err)
	}
	csv3 := filepath.Join(subDir2, "level2.csv")
	err = os.WriteFile(csv3, []byte("id,product\n1,Widget"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Act
	result, err := DiscoverSeeds(tmpDir, ScopeTree)

	// Assert
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("Expected 3 files (tree scope, recursive), got %d", len(result))
	}

	// Check that result contains all CSV files
	found := make(map[string]bool)
	for _, f := range result {
		found[f] = true
	}
	if !found[csv1] {
		t.Errorf("Expected to find %s", csv1)
	}
	if !found[csv2] {
		t.Errorf("Expected to find %s", csv2)
	}
	if !found[csv3] {
		t.Errorf("Expected to find %s", csv3)
	}
}

// TestDiscoverSeeds_FilterNonCSV tests that non-CSV files are ignored
func TestDiscoverSeeds_FilterNonCSV(t *testing.T) {
	// Setup: Create temp directory with CSV and non-CSV files
	tmpDir := t.TempDir()

	// Create CSV file
	csvFile := filepath.Join(tmpDir, "data.csv")
	err := os.WriteFile(csvFile, []byte("id,name\n1,Alice"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Create non-CSV files
	txtFile := filepath.Join(tmpDir, "readme.txt")
	err = os.WriteFile(txtFile, []byte("Some text"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	sqlFile := filepath.Join(tmpDir, "query.sql")
	err = os.WriteFile(sqlFile, []byte("SELECT * FROM table"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Act
	result, err := DiscoverSeeds(tmpDir, ScopeFolder)

	// Assert
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("Expected 1 CSV file, got %d files", len(result))
	}
	if result[0] != csvFile {
		t.Errorf("Expected %s, got %s", csvFile, result[0])
	}
}

// TestDiscoverSeeds_EmptyDirectory tests discovery in an empty directory
func TestDiscoverSeeds_EmptyDirectory(t *testing.T) {
	// Setup: Create empty temp directory
	tmpDir := t.TempDir()

	// Act
	result, err := DiscoverSeeds(tmpDir, ScopeFolder)

	// Assert
	if err != nil {
		t.Fatalf("Expected no error for empty directory, got: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("Expected 0 files in empty directory, got %d", len(result))
	}
}

// TestDiscoverSeeds_NonExistent tests discovery with non-existent path
func TestDiscoverSeeds_NonExistent(t *testing.T) {
	// Setup: Use a path that doesn't exist
	nonExistentPath := filepath.Join(t.TempDir(), "does-not-exist")

	// Act
	result, err := DiscoverSeeds(nonExistentPath, ScopeFile)

	// Assert
	if err == nil {
		t.Fatal("Expected error for non-existent path, got nil")
	}
	if result != nil {
		t.Errorf("Expected nil result for non-existent path, got %v", result)
	}
}

// TestDiscoverSeeds_InvalidScope tests discovery with invalid scope
func TestDiscoverSeeds_InvalidScope(t *testing.T) {
	// Setup: Create temp directory
	tmpDir := t.TempDir()

	// Act
	result, err := DiscoverSeeds(tmpDir, "invalid-scope")

	// Assert
	if err == nil {
		t.Fatal("Expected error for invalid scope, got nil")
	}
	if result != nil {
		t.Errorf("Expected nil result for invalid scope, got %v", result)
	}
}
