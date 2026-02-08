package test

import (
	"testing"
	"time"
)

func TestNewTestResult(t *testing.T) {
	testID := "test_001"
	status := StatusPassed

	result := NewTestResult(testID, status)

	if result.TestID != testID {
		t.Errorf("TestID = %v, want %v", result.TestID, testID)
	}
	if result.Status != status {
		t.Errorf("Status = %v, want %v", result.Status, status)
	}
	if result.StartTime.IsZero() {
		t.Error("StartTime should be initialized")
	}
	if result.FailureCount != 0 {
		t.Errorf("FailureCount = %v, want 0", result.FailureCount)
	}
	if result.ErrorMessage != "" {
		t.Errorf("ErrorMessage = %v, want empty string", result.ErrorMessage)
	}
}

func TestTestResult_Validation(t *testing.T) {
	tests := []struct {
		name    string
		result  *TestResult
		wantErr bool
	}{
		{
			name: "valid result",
			result: &TestResult{
				TestID:       "test_001",
				Status:       StatusPassed,
				StartTime:    time.Now(),
				EndTime:      time.Now(),
				FailureCount: 0,
			},
			wantErr: false,
		},
		{
			name: "missing test id",
			result: &TestResult{
				TestID:    "",
				Status:    StatusPassed,
				StartTime: time.Now(),
				EndTime:   time.Now(),
			},
			wantErr: true,
		},
		{
			name: "missing status",
			result: &TestResult{
				TestID:    "test_001",
				Status:    "",
				StartTime: time.Now(),
				EndTime:   time.Now(),
			},
			wantErr: true,
		},
		{
			name: "zero start time",
			result: &TestResult{
				TestID:  "test_001",
				Status:  StatusPassed,
				EndTime: time.Now(),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.result.Validate()
			if tt.wantErr {
				if err == nil {
					t.Error("expected validation error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected validation error: %v", err)
				}
			}
		})
	}
}

func TestTestResult_Duration(t *testing.T) {
	start := time.Now()
	end := start.Add(5 * time.Second)

	result := &TestResult{
		TestID:    "test_001",
		Status:    StatusPassed,
		StartTime: start,
		EndTime:   end,
	}

	duration := result.Duration()

	if duration != 5*time.Second {
		t.Errorf("Duration = %v, want %v", duration, 5*time.Second)
	}
}

func TestTestResult_Duration_ZeroEndTime(t *testing.T) {
	start := time.Now()

	result := &TestResult{
		TestID:    "test_001",
		Status:    StatusRunning,
		StartTime: start,
		EndTime:   time.Time{}, // Zero value
	}

	duration := result.Duration()

	// Duration should be 0 when EndTime is not set
	if duration != 0 {
		t.Errorf("Duration = %v, want 0", duration)
	}
}

func TestTestResult_Complete(t *testing.T) {
	result := NewTestResult("test_001", StatusRunning)

	// Simulate some time passing
	time.Sleep(10 * time.Millisecond)

	result.Complete(StatusPassed, 0, "")

	if result.Status != StatusPassed {
		t.Errorf("Status = %v, want %v", result.Status, StatusPassed)
	}
	if result.EndTime.IsZero() {
		t.Error("EndTime should be set")
	}
	if result.Duration() <= 0 {
		t.Error("Duration should be greater than 0")
	}
}

func TestTestResult_Complete_WithFailures(t *testing.T) {
	result := NewTestResult("test_001", StatusRunning)

	failureCount := int64(5)
	errorMsg := "5 rows failed uniqueness check"

	result.Complete(StatusFailed, failureCount, errorMsg)

	if result.Status != StatusFailed {
		t.Errorf("Status = %v, want %v", result.Status, StatusFailed)
	}
	if result.FailureCount != failureCount {
		t.Errorf("FailureCount = %v, want %v", result.FailureCount, failureCount)
	}
	if result.ErrorMessage != errorMsg {
		t.Errorf("ErrorMessage = %v, want %v", result.ErrorMessage, errorMsg)
	}
}

func TestTestResult_AddFailedRows(t *testing.T) {
	result := NewTestResult("test_001", StatusRunning)

	failedRows := []map[string]interface{}{
		{"id": 1, "value": "invalid"},
		{"id": 2, "value": "invalid"},
	}

	result.AddFailedRows(failedRows)

	if len(result.FailedRows) != 2 {
		t.Errorf("FailedRows length = %v, want 2", len(result.FailedRows))
	}
	if result.FailedRows[0]["id"] != 1 {
		t.Errorf("FailedRows[0]['id'] = %v, want 1", result.FailedRows[0]["id"])
	}
}

func TestNewTestSummary(t *testing.T) {
	summary := NewTestSummary()

	if summary.TotalTests != 0 {
		t.Errorf("TotalTests = %v, want 0", summary.TotalTests)
	}
	if summary.PassedTests != 0 {
		t.Errorf("PassedTests = %v, want 0", summary.PassedTests)
	}
	if summary.FailedTests != 0 {
		t.Errorf("FailedTests = %v, want 0", summary.FailedTests)
	}
	if summary.WarningTests != 0 {
		t.Errorf("WarningTests = %v, want 0", summary.WarningTests)
	}
	if summary.SkippedTests != 0 {
		t.Errorf("SkippedTests = %v, want 0", summary.SkippedTests)
	}
	if summary.StartTime.IsZero() {
		t.Error("StartTime should be initialized")
	}
	if len(summary.TestResults) != 0 {
		t.Errorf("TestResults length = %v, want 0", len(summary.TestResults))
	}
}

func TestTestSummary_AddResult(t *testing.T) {
	summary := NewTestSummary()

	result1 := NewTestResult("test_001", StatusPassed)
	result1.Complete(StatusPassed, 0, "")

	result2 := NewTestResult("test_002", StatusFailed)
	result2.Complete(StatusFailed, 3, "3 rows failed")

	result3 := NewTestResult("test_003", StatusWarning)
	result3.Complete(StatusWarning, 1, "1 row warning")

	summary.AddResult(result1)
	summary.AddResult(result2)
	summary.AddResult(result3)

	if summary.TotalTests != 3 {
		t.Errorf("TotalTests = %v, want 3", summary.TotalTests)
	}
	if summary.PassedTests != 1 {
		t.Errorf("PassedTests = %v, want 1", summary.PassedTests)
	}
	if summary.FailedTests != 1 {
		t.Errorf("FailedTests = %v, want 1", summary.FailedTests)
	}
	if summary.WarningTests != 1 {
		t.Errorf("WarningTests = %v, want 1", summary.WarningTests)
	}
	if len(summary.TestResults) != 3 {
		t.Errorf("TestResults length = %v, want 3", len(summary.TestResults))
	}
}

func TestTestSummary_Complete(t *testing.T) {
	summary := NewTestSummary()

	result1 := NewTestResult("test_001", StatusPassed)
	result1.Complete(StatusPassed, 0, "")

	result2 := NewTestResult("test_002", StatusFailed)
	result2.Complete(StatusFailed, 3, "3 rows failed")

	summary.AddResult(result1)
	summary.AddResult(result2)

	// Simulate some time passing
	time.Sleep(10 * time.Millisecond)

	summary.Complete()

	if summary.EndTime.IsZero() {
		t.Error("EndTime should be set")
	}
	if summary.Duration() <= 0 {
		t.Error("Duration should be greater than 0")
	}
}

func TestTestSummary_HasFailures(t *testing.T) {
	tests := []struct {
		name    string
		results []*TestResult
		want    bool
	}{
		{
			name: "no failures",
			results: []*TestResult{
				{TestID: "test_001", Status: StatusPassed},
				{TestID: "test_002", Status: StatusPassed},
			},
			want: false,
		},
		{
			name: "has failures",
			results: []*TestResult{
				{TestID: "test_001", Status: StatusPassed},
				{TestID: "test_002", Status: StatusFailed},
			},
			want: true,
		},
		{
			name: "only warnings",
			results: []*TestResult{
				{TestID: "test_001", Status: StatusWarning},
				{TestID: "test_002", Status: StatusWarning},
			},
			want: false,
		},
		{
			name:    "no results",
			results: []*TestResult{},
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			summary := NewTestSummary()
			for _, result := range tt.results {
				summary.AddResult(result)
			}

			if got := summary.HasFailures(); got != tt.want {
				t.Errorf("HasFailures() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTestSummary_Duration(t *testing.T) {
	summary := NewTestSummary()

	// Simulate some time passing
	time.Sleep(10 * time.Millisecond)

	summary.Complete()

	duration := summary.Duration()

	if duration <= 0 {
		t.Error("Duration should be greater than 0")
	}
}
