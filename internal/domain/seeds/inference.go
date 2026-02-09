package seeds

import (
	"fmt"
	"strconv"
	"strings"
)

// inferColumnType determines the SQL type for a column based on sample values.
// Priority: INTEGER > REAL > TEXT
// Returns "INTEGER", "REAL", or "TEXT"
func inferColumnType(values []string) string {
	// Filter out empty/whitespace-only values
	nonEmpty := make([]string, 0, len(values))
	for _, v := range values {
		trimmed := strings.TrimSpace(v)
		if trimmed != "" {
			nonEmpty = append(nonEmpty, trimmed)
		}
	}

	// If all values are empty, default to TEXT
	if len(nonEmpty) == 0 {
		return "TEXT"
	}

	// Check for leading zeros (should be TEXT, not numeric)
	hasLeadingZeros := false
	for _, v := range nonEmpty {
		// Leading zeros: length > 1, starts with '0', and not just "0" or negative
		if len(v) > 1 && v[0] == '0' && v != "0" {
			hasLeadingZeros = true
			break
		}
	}

	if hasLeadingZeros {
		return "TEXT"
	}

	// Try parsing all values as integers
	allIntegers := true
	for _, v := range nonEmpty {
		_, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			allIntegers = false
			break
		}
	}

	if allIntegers {
		return "INTEGER"
	}

	// Try parsing all values as floats
	allFloats := true
	for _, v := range nonEmpty {
		_, err := strconv.ParseFloat(v, 64)
		if err != nil {
			allFloats = false
			break
		}
	}

	if allFloats {
		return "REAL"
	}

	// Default to TEXT
	return "TEXT"
}

// InferSchema analyzes CSV rows and infers the SQL schema.
// The first row is expected to be headers, and subsequent rows are data.
// If sampleSize > 0, only the first sampleSize data rows are analyzed.
// If sampleSize = 0, all data rows are analyzed.
// Returns a SeedSchema with inferred column types or an error.
func InferSchema(rows [][]string, sampleSize int) (*SeedSchema, error) {
	// Validate input
	if len(rows) < 2 {
		return nil, fmt.Errorf("insufficient data: need at least headers + 1 data row")
	}

	// First row is headers
	headers := rows[0]
	if len(headers) == 0 {
		return nil, fmt.Errorf("no headers provided")
	}

	// Remaining rows are data
	dataRows := rows[1:]

	// Apply sample size if specified
	if sampleSize > 0 && sampleSize < len(dataRows) {
		dataRows = dataRows[:sampleSize]
	}

	// Build schema by inferring type for each column
	columns := make([]SeedColumn, len(headers))
	for colIdx, header := range headers {
		// Collect all values for this column
		colValues := make([]string, 0, len(dataRows))
		for _, row := range dataRows {
			// Handle rows with different lengths
			if colIdx < len(row) {
				colValues = append(colValues, row[colIdx])
			} else {
				colValues = append(colValues, "")
			}
		}

		// Infer the type for this column
		colType := inferColumnType(colValues)

		columns[colIdx] = SeedColumn{
			Name: header,
			Type: colType,
		}
	}

	return &SeedSchema{
		Columns: columns,
	}, nil
}
