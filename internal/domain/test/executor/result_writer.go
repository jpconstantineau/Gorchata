package executor

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/jpconstantineau/gorchata/internal/domain/test"
)

// ResultWriter defines the interface for writing test results
type ResultWriter interface {
	Write(result *test.TestResult) error
	WriteSummary(summary *test.TestSummary) error
}

// ConsoleResultWriter writes test results to console
type ConsoleResultWriter struct {
	writer io.Writer
	color  bool
}

// NewConsoleResultWriter creates a new console result writer
func NewConsoleResultWriter(writer io.Writer, color bool) *ConsoleResultWriter {
	return &ConsoleResultWriter{
		writer: writer,
		color:  color,
	}
}

// Write writes a single test result to console
func (w *ConsoleResultWriter) Write(result *test.TestResult) error {
	// Use uppercase status names for display
	statusName := ""
	switch result.Status {
	case test.StatusPassed:
		statusName = "PASS"
	case test.StatusFailed:
		statusName = "FAIL"
	case test.StatusWarning:
		statusName = "WARN"
	default:
		statusName = result.Status.String()
	}

	duration := result.Duration().Milliseconds()

	// Apply color if enabled
	if w.color {
		switch result.Status {
		case test.StatusPassed:
			statusName = fmt.Sprintf("\033[32m%s\033[0m", statusName) // Green
		case test.StatusFailed:
			statusName = fmt.Sprintf("\033[31m%s\033[0m", statusName) // Red
		case test.StatusWarning:
			statusName = fmt.Sprintf("\033[33m%s\033[0m", statusName) // Yellow
		}
	}

	status := statusName

	line := fmt.Sprintf("[%s] %s (%dms)", status, result.TestID, duration)

	if result.FailureCount > 0 {
		line += fmt.Sprintf(" - %d failures", result.FailureCount)
	}

	if result.ErrorMessage != "" {
		line += fmt.Sprintf(" - %s", result.ErrorMessage)
	}

	fmt.Fprintln(w.writer, line)
	return nil
}

// WriteSummary writes the test summary to console
func (w *ConsoleResultWriter) WriteSummary(summary *test.TestSummary) error {
	fmt.Fprintln(w.writer, "\n========================================")
	fmt.Fprintln(w.writer, "Test Summary")
	fmt.Fprintln(w.writer, "========================================")
	fmt.Fprintf(w.writer, "Total: %d\n", summary.TotalTests)
	fmt.Fprintf(w.writer, "Passed: %d\n", summary.PassedTests)
	fmt.Fprintf(w.writer, "Failed: %d\n", summary.FailedTests)
	fmt.Fprintf(w.writer, "Warnings: %d\n", summary.WarningTests)
	fmt.Fprintf(w.writer, "Skipped: %d\n", summary.SkippedTests)

	duration := summary.Duration().Milliseconds()
	fmt.Fprintf(w.writer, "Duration: %dms\n", duration)
	fmt.Fprintln(w.writer, "========================================")

	return nil
}

// JSONResultWriter writes test results to a JSON file
type JSONResultWriter struct {
	outputPath string
	results    []*test.TestResult
}

// NewJSONResultWriter creates a new JSON result writer
func NewJSONResultWriter(outputPath string) *JSONResultWriter {
	return &JSONResultWriter{
		outputPath: outputPath,
		results:    []*test.TestResult{},
	}
}

// Write collects a test result (to be written when WriteSummary is called)
func (w *JSONResultWriter) Write(result *test.TestResult) error {
	w.results = append(w.results, result)
	return nil
}

// WriteSummary writes all collected results and summary to JSON file
func (w *JSONResultWriter) WriteSummary(summary *test.TestSummary) error {
	// Create output directory if it doesn't exist
	dir := filepath.Dir(w.outputPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Build JSON structure
	output := map[string]interface{}{
		"summary": map[string]interface{}{
			"total_tests": summary.TotalTests,
			"passed":      summary.PassedTests,
			"failed":      summary.FailedTests,
			"warnings":    summary.WarningTests,
			"skipped":     summary.SkippedTests,
			"duration_ms": summary.Duration().Milliseconds(),
			"start_time":  summary.StartTime.Format("2006-01-02T15:04:05Z07:00"),
			"end_time":    summary.EndTime.Format("2006-01-02T15:04:05Z07:00"),
		},
		"results": w.buildResultsJSON(),
	}

	// Write JSON to file
	data, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	if err := os.WriteFile(w.outputPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write JSON file: %w", err)
	}

	return nil
}

// buildResultsJSON converts test results to JSON-friendly format
func (w *JSONResultWriter) buildResultsJSON() []map[string]interface{} {
	results := make([]map[string]interface{}, 0, len(w.results))

	for _, r := range w.results {
		result := map[string]interface{}{
			"test_id":       r.TestID,
			"status":        r.Status.String(),
			"duration_ms":   r.Duration().Milliseconds(),
			"failure_count": r.FailureCount,
		}

		if r.ErrorMessage != "" {
			result["error_message"] = r.ErrorMessage
		}

		results = append(results, result)
	}

	return results
}
