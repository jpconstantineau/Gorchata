package executor

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jpconstantineau/gorchata/internal/domain/test"
)

func TestConsoleResultWriter_Write_PassedTest(t *testing.T) {
	var buf bytes.Buffer
	writer := NewConsoleResultWriter(&buf, false) // no colors

	result := test.NewTestResult("test1", test.StatusPassed)
	result.Complete(test.StatusPassed, 0, "")

	err := writer.Write(result)

	if err != nil {
		t.Errorf("Write() error = %v, want nil", err)
	}

	output := buf.String()
	if !strings.Contains(output, "PASS") {
		t.Error("Output should contain PASS")
	}
	if !strings.Contains(output, "test1") {
		t.Error("Output should contain test ID")
	}
}

func TestConsoleResultWriter_Write_FailedTest(t *testing.T) {
	var buf bytes.Buffer
	writer := NewConsoleResultWriter(&buf, false)

	result := test.NewTestResult("test1", test.StatusFailed)
	result.Complete(test.StatusFailed, 5, "")

	err := writer.Write(result)

	if err != nil {
		t.Errorf("Write() error = %v, want nil", err)
	}

	output := buf.String()
	if !strings.Contains(output, "FAIL") {
		t.Error("Output should contain FAIL")
	}
	if !strings.Contains(output, "5") {
		t.Error("Output should contain failure count")
	}
}

func TestConsoleResultWriter_Write_Warning(t *testing.T) {
	var buf bytes.Buffer
	writer := NewConsoleResultWriter(&buf, false)

	result := test.NewTestResult("test1", test.StatusWarning)
	result.Complete(test.StatusWarning, 2, "")

	err := writer.Write(result)

	if err != nil {
		t.Errorf("Write() error = %v, want nil", err)
	}

	output := buf.String()
	if !strings.Contains(output, "WARN") {
		t.Error("Output should contain WARN")
	}
}

func TestConsoleResultWriter_WriteSummary(t *testing.T) {
	var buf bytes.Buffer
	writer := NewConsoleResultWriter(&buf, false)

	summary := test.NewTestSummary()
	result1 := test.NewTestResult("test1", test.StatusPassed)
	result1.Complete(test.StatusPassed, 0, "")
	summary.AddResult(result1)

	result2 := test.NewTestResult("test2", test.StatusFailed)
	result2.Complete(test.StatusFailed, 3, "")
	summary.AddResult(result2)

	summary.EndTime = time.Now()

	err := writer.WriteSummary(summary)

	if err != nil {
		t.Errorf("WriteSummary() error = %v, want nil", err)
	}

	output := buf.String()
	if !strings.Contains(output, "Passed: 1") {
		t.Error("Summary should show passed count")
	}
	if !strings.Contains(output, "Failed: 1") {
		t.Error("Summary should show failed count")
	}
	if !strings.Contains(output, "Total: 2") {
		t.Error("Summary should show total count")
	}
}

func TestConsoleResultWriter_WithColors(t *testing.T) {
	var buf bytes.Buffer
	writer := NewConsoleResultWriter(&buf, true) // with colors

	result := test.NewTestResult("test1", test.StatusPassed)
	result.Complete(test.StatusPassed, 0, "")

	err := writer.Write(result)

	if err != nil {
		t.Errorf("Write() error = %v, want nil", err)
	}

	// Should contain ANSI color codes
	output := buf.String()
	if !strings.Contains(output, "\033[") {
		t.Error("Output with colors should contain ANSI escape codes")
	}
}

func TestJSONResultWriter_Write(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test_results.json")

	writer := NewJSONResultWriter(outputPath)

	result := test.NewTestResult("test1", test.StatusPassed)
	result.Complete(test.StatusPassed, 0, "")

	err := writer.Write(result)

	if err != nil {
		t.Errorf("Write() error = %v, want nil", err)
	}
}

func TestJSONResultWriter_WriteSummary(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test_results.json")

	writer := NewJSONResultWriter(outputPath)

	summary := test.NewTestSummary()
	result1 := test.NewTestResult("test1", test.StatusPassed)
	result1.Complete(test.StatusPassed, 0, "")
	summary.AddResult(result1)

	result2 := test.NewTestResult("test2", test.StatusFailed)
	result2.Complete(test.StatusFailed, 3, "")
	summary.AddResult(result2)

	summary.EndTime = time.Now()

	// Write results first
	writer.Write(result1)
	writer.Write(result2)

	err := writer.WriteSummary(summary)

	if err != nil {
		t.Errorf("WriteSummary() error = %v, want nil", err)
	}

	// Read the JSON file
	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}

	// Parse JSON
	var jsonData map[string]interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	// Verify structure
	if _, ok := jsonData["summary"]; !ok {
		t.Error("JSON should contain 'summary' field")
	}
	if _, ok := jsonData["results"]; !ok {
		t.Error("JSON should contain 'results' field")
	}

	// Verify summary
	summaryData := jsonData["summary"].(map[string]interface{})
	if summaryData["total_tests"].(float64) != 2 {
		t.Errorf("total_tests = %v, want 2", summaryData["total_tests"])
	}
	if summaryData["passed"].(float64) != 1 {
		t.Errorf("passed = %v, want 1", summaryData["passed"])
	}
	if summaryData["failed"].(float64) != 1 {
		t.Errorf("failed = %v, want 1", summaryData["failed"])
	}

	// Verify results
	resultsData := jsonData["results"].([]interface{})
	if len(resultsData) != 2 {
		t.Errorf("results length = %d, want 2", len(resultsData))
	}
}

func TestJSONResultWriter_CreateDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "nested", "dir", "test_results.json")

	writer := NewJSONResultWriter(outputPath)

	summary := test.NewTestSummary()
	summary.EndTime = time.Now()

	err := writer.WriteSummary(summary)

	if err != nil {
		t.Errorf("WriteSummary() should create nested directories, error = %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		t.Error("JSON file should be created")
	}
}

func TestJSONResultWriter_ValidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test_results.json")

	writer := NewJSONResultWriter(outputPath)

	summary := test.NewTestSummary()
	result := test.NewTestResult("test1", test.StatusPassed)
	result.Complete(test.StatusPassed, 0, "")
	summary.AddResult(result)
	summary.EndTime = time.Now()

	writer.Write(result)
	writer.WriteSummary(summary)

	// Read and validate JSON
	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}

	var jsonData map[string]interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		t.Errorf("Invalid JSON: %v", err)
	}
}

func TestNewConsoleResultWriter(t *testing.T) {
	var buf bytes.Buffer
	writer := NewConsoleResultWriter(&buf, false)

	if writer == nil {
		t.Error("NewConsoleResultWriter() returned nil")
	}
}

func TestNewJSONResultWriter(t *testing.T) {
	writer := NewJSONResultWriter("test.json")

	if writer == nil {
		t.Error("NewJSONResultWriter() returned nil")
	}
}

func TestConsoleResultWriter_Duration(t *testing.T) {
	var buf bytes.Buffer
	writer := NewConsoleResultWriter(&buf, false)

	result := test.NewTestResult("test1", test.StatusPassed)
	time.Sleep(10 * time.Millisecond)
	result.Complete(test.StatusPassed, 0, "")

	err := writer.Write(result)

	if err != nil {
		t.Errorf("Write() error = %v, want nil", err)
	}

	output := buf.String()
	// Should show some duration
	if !strings.Contains(output, "ms") && !strings.Contains(output, "µs") {
		t.Error("Output should show duration")
	}
}

func TestConsoleResultWriter_EmptySummary(t *testing.T) {
	var buf bytes.Buffer
	writer := NewConsoleResultWriter(&buf, false)

	summary := test.NewTestSummary()
	summary.EndTime = time.Now()

	err := writer.WriteSummary(summary)

	if err != nil {
		t.Errorf("WriteSummary() error = %v, want nil", err)
	}

	output := buf.String()
	if !strings.Contains(output, "Total: 0") {
		t.Error("Empty summary should show Total: 0")
	}
}

func TestJSONResultWriter_NoResults(t *testing.T) {
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test_results.json")

	writer := NewJSONResultWriter(outputPath)

	summary := test.NewTestSummary()
	summary.EndTime = time.Now()

	err := writer.WriteSummary(summary)

	if err != nil {
		t.Errorf("WriteSummary() error = %v, want nil", err)
	}

	// Read the JSON file
	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}

	var jsonData map[string]interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	resultsData := jsonData["results"].([]interface{})
	if len(resultsData) != 0 {
		t.Errorf("results length = %d, want 0", len(resultsData))
	}
}
