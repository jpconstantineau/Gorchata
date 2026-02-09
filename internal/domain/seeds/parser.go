package seeds

import (
	"encoding/csv"
	"fmt"
	"os"
)

// ParseCSV reads and parses a CSV file, returning all records including headers
// Returns a 2D slice where first row is headers and subsequent rows are data
func ParseCSV(filePath string) ([][]string, error) {
	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	// Create CSV reader
	reader := csv.NewReader(file)

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %w", err)
	}

	// Validate non-empty
	if len(records) == 0 {
		return nil, fmt.Errorf("CSV file is empty")
	}

	// Validate consistent field counts
	if len(records) > 0 {
		expectedFields := len(records[0])
		for i, record := range records {
			if len(record) != expectedFields {
				return nil, fmt.Errorf("row %d has %d fields, expected %d", i, len(record), expectedFields)
			}
		}
	}

	return records, nil
}
