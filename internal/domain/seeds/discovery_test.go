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
	// Should find CSV and SQL, but not TXT
	if len(result) != 2 {
		t.Fatalf("Expected 2 seed files (CSV + SQL), got %d files", len(result))
	}

	// Check that result contains both seed files but not txt
	found := make(map[string]bool)
	for _, f := range result {
		found[f] = true
	}
	if !found[csvFile] {
		t.Errorf("Expected to find CSV file %s", csvFile)
	}
	if !found[sqlFile] {
		t.Errorf("Expected to find SQL file %s", sqlFile)
	}
	if found[txtFile] {
		t.Errorf("Should not find TXT file %s", txtFile)
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

// TestDiscoverSeeds_MixedTypes tests discovery of both CSV and SQL files
func TestDiscoverSeeds_MixedTypes(t *testing.T) {
	// Setup: Create temp directory with both CSV and SQL files
	tmpDir := t.TempDir()

	// Create CSV files
	csv1 := filepath.Join(tmpDir, "customers.csv")
	csv2 := filepath.Join(tmpDir, "orders.csv")
	err := os.WriteFile(csv1, []byte("id,name\n1,Alice"), 0644)
	if err != nil {
		t.Fatalf("Failed to create CSV file: %v", err)
	}
	err = os.WriteFile(csv2, []byte("id,total\n1,100"), 0644)
	if err != nil {
		t.Fatalf("Failed to create CSV file: %v", err)
	}

	// Create SQL files
	sql1 := filepath.Join(tmpDir, "init_data.sql")
	sql2 := filepath.Join(tmpDir, "raw_events.sql")
	err = os.WriteFile(sql1, []byte("CREATE TABLE test (id INTEGER);"), 0644)
	if err != nil {
		t.Fatalf("Failed to create SQL file: %v", err)
	}
	err = os.WriteFile(sql2, []byte("INSERT INTO events VALUES (1, 'test');"), 0644)
	if err != nil {
		t.Fatalf("Failed to create SQL file: %v", err)
	}

	// Create non-seed file (should be ignored)
	txtFile := filepath.Join(tmpDir, "readme.txt")
	err = os.WriteFile(txtFile, []byte("This is a readme"), 0644)
	if err != nil {
		t.Fatalf("Failed to create text file: %v", err)
	}

	// Act
	result, err := DiscoverSeeds(tmpDir, ScopeFolder)

	// Assert
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Should find 2 CSV files + 2 SQL files = 4 total
	if len(result) != 4 {
		t.Fatalf("Expected 4 seed files (2 CSV + 2 SQL), got %d", len(result))
	}

	// Check that result contains all seed files
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
	if !found[sql1] {
		t.Errorf("Expected to find %s", sql1)
	}
	if !found[sql2] {
		t.Errorf("Expected to find %s", sql2)
	}
	if found[txtFile] {
		t.Errorf("Should not find non-seed file %s", txtFile)
	}
}

// TestDiscoverSeeds_SQLFile tests discovery of a single SQL file
func TestDiscoverSeeds_SQLFile(t *testing.T) {
	// Setup: Create temp directory with test SQL file
	tmpDir := t.TempDir()
	sqlFile := filepath.Join(tmpDir, "init.sql")
	err := os.WriteFile(sqlFile, []byte("CREATE TABLE test (id INTEGER);"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Act
	result, err := DiscoverSeeds(sqlFile, ScopeFile)

	// Assert
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("Expected 1 file, got %d", len(result))
	}
	if result[0] != sqlFile {
		t.Errorf("Expected %s, got %s", sqlFile, result[0])
	}
}

// TestDiscoverSeeds_NonSeedFile tests rejection of non-seed files
func TestDiscoverSeeds_NonSeedFile(t *testing.T) {
	// Setup: Create temp directory with non-seed file
	tmpDir := t.TempDir()
	txtFile := filepath.Join(tmpDir, "readme.txt")
	err := os.WriteFile(txtFile, []byte("This is not a seed file"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Act
	result, err := DiscoverSeeds(txtFile, ScopeFile)

	// Assert - should get an error
	if err == nil {
		t.Fatal("Expected error for non-seed file, got nil")
	}
	if result != nil {
		t.Errorf("Expected nil result for non-seed file, got %v", result)
	}
}
