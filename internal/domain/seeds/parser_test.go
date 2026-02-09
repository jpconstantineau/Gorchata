package seeds

import (
	"path/filepath"
	"testing"
)

func TestParseCSV_BasicCSV(t *testing.T) {
	testFile := filepath.Join("testdata", "valid.csv")

	records, err := ParseCSV(testFile)
	if err != nil {
		t.Fatalf("ParseCSV() error = %v, want nil", err)
	}

	// Should have headers + 2 data rows = 3 records
	if len(records) != 3 {
		t.Errorf("ParseCSV() returned %d records, want 3", len(records))
	}

	// Check headers
	expectedHeaders := []string{"id", "name", "age"}
	if len(records) > 0 {
		for i, header := range expectedHeaders {
			if records[0][i] != header {
				t.Errorf("Header[%d] = %v, want %v", i, records[0][i], header)
			}
		}
	}

	// Check first data row
	if len(records) > 1 {
		if records[1][0] != "1" {
			t.Errorf("records[1][0] = %v, want '1'", records[1][0])
		}
		if records[1][1] != "John Doe" {
			t.Errorf("records[1][1] = %v, want 'John Doe'", records[1][1])
		}
		if records[1][2] != "30" {
			t.Errorf("records[1][2] = %v, want '30'", records[1][2])
		}
	}
}

func TestParseCSV_WithQuotes(t *testing.T) {
	testFile := filepath.Join("testdata", "with_quotes.csv")

	records, err := ParseCSV(testFile)
	if err != nil {
		t.Fatalf("ParseCSV() error = %v, want nil", err)
	}

	// Should have headers + 2 data rows = 3 records
	if len(records) != 3 {
		t.Errorf("ParseCSV() returned %d records, want 3", len(records))
	}

	// Check that quoted fields with commas are handled correctly
	if len(records) > 1 {
		if records[1][1] != "Smith, John" {
			t.Errorf("records[1][1] = %v, want 'Smith, John'", records[1][1])
		}
		if records[1][2] != "123 Main St, Apt 4" {
			t.Errorf("records[1][2] = %v, want '123 Main St, Apt 4'", records[1][2])
		}
	}
}

func TestParseCSV_EmptyFile(t *testing.T) {
	testFile := filepath.Join("testdata", "empty.csv")

	_, err := ParseCSV(testFile)
	if err == nil {
		t.Error("ParseCSV() expected error for empty file, got nil")
	}
}

func TestParseCSV_MissingHeaders(t *testing.T) {
	testFile := filepath.Join("testdata", "no_headers.csv")

	records, err := ParseCSV(testFile)
	// This should parse successfully but first row will be empty/whitespace
	// We might want to validate this depending on requirements
	if err != nil {
		t.Fatalf("ParseCSV() error = %v", err)
	}

	// First row should be empty or contain empty string
	if len(records) > 0 && len(records[0]) > 0 && records[0][0] != "" {
		t.Logf("Warning: First row is not empty, got: %v", records[0])
	}
}

func TestParseCSV_MalformedRows(t *testing.T) {
	testFile := filepath.Join("testdata", "malformed.csv")

	_, err := ParseCSV(testFile)
	if err == nil {
		t.Error("ParseCSV() expected error for malformed CSV, got nil")
	}
}

func TestParseCSV_NonExistentFile(t *testing.T) {
	testFile := filepath.Join("testdata", "nonexistent.csv")

	_, err := ParseCSV(testFile)
	if err == nil {
		t.Error("ParseCSV() expected error for non-existent file, got nil")
	}
}
