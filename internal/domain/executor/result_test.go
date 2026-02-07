package executor

import (
	"testing"
	"time"
)

func TestNewExecutionResult(t *testing.T) {
	result := NewExecutionResult()

	if result == nil {
		t.Fatal("expected non-nil result")
	}

	if result.StartTime.IsZero() {
		t.Error("StartTime should be set")
	}

	if len(result.ModelResults) != 0 {
		t.Error("ModelResults should be empty initially")
	}

	if result.Status != StatusPending {
		t.Errorf("Status = %v, want %v", result.Status, StatusPending)
	}
}

func TestExecutionResult_AddModelResult(t *testing.T) {
	result := NewExecutionResult()

	modelResult := ModelResult{
		ModelID:   "test_model",
		Status:    StatusSuccess,
		StartTime: time.Now(),
		EndTime:   time.Now().Add(time.Second),
	}

	result.AddModelResult(modelResult)

	if len(result.ModelResults) != 1 {
		t.Errorf("ModelResults length = %d, want 1", len(result.ModelResults))
	}

	if result.ModelResults[0].ModelID != "test_model" {
		t.Errorf("ModelID = %v, want test_model", result.ModelResults[0].ModelID)
	}
}

func TestExecutionResult_Complete(t *testing.T) {
	result := NewExecutionResult()

	// Add successful model
	result.AddModelResult(ModelResult{
		ModelID:   "model1",
		Status:    StatusSuccess,
		StartTime: time.Now(),
		EndTime:   time.Now().Add(time.Second),
	})

	result.Complete()

	if result.EndTime.IsZero() {
		t.Error("EndTime should be set")
	}

	if result.Status != StatusSuccess {
		t.Errorf("Status = %v, want %v", result.Status, StatusSuccess)
	}

	if result.EndTime.Before(result.StartTime) {
		t.Error("EndTime should be after StartTime")
	}
}

func TestExecutionResult_Complete_WithFailures(t *testing.T) {
	result := NewExecutionResult()

	// Add successful model
	result.AddModelResult(ModelResult{
		ModelID: "model1",
		Status:  StatusSuccess,
	})

	// Add failed model
	result.AddModelResult(ModelResult{
		ModelID: "model2",
		Status:  StatusFailed,
		Error:   "some error",
	})

	result.Complete()

	if result.Status != StatusFailed {
		t.Errorf("Status = %v, want %v", result.Status, StatusFailed)
	}
}

func TestExecutionResult_SuccessCount(t *testing.T) {
	result := NewExecutionResult()

	result.AddModelResult(ModelResult{ModelID: "m1", Status: StatusSuccess})
	result.AddModelResult(ModelResult{ModelID: "m2", Status: StatusSuccess})
	result.AddModelResult(ModelResult{ModelID: "m3", Status: StatusFailed})

	count := result.SuccessCount()
	if count != 2 {
		t.Errorf("SuccessCount = %d, want 2", count)
	}
}

func TestExecutionResult_FailureCount(t *testing.T) {
	result := NewExecutionResult()

	result.AddModelResult(ModelResult{ModelID: "m1", Status: StatusSuccess})
	result.AddModelResult(ModelResult{ModelID: "m2", Status: StatusFailed})
	result.AddModelResult(ModelResult{ModelID: "m3", Status: StatusFailed})

	count := result.FailureCount()
	if count != 2 {
		t.Errorf("FailureCount = %d, want 2", count)
	}
}

func TestExecutionResult_Duration(t *testing.T) {
	result := NewExecutionResult()

	// Simulate execution time
	time.Sleep(10 * time.Millisecond)
	result.Complete()

	duration := result.Duration()
	if duration < 10*time.Millisecond {
		t.Errorf("Duration = %v, want >= 10ms", duration)
	}
}

func TestModelResult_Duration(t *testing.T) {
	start := time.Now()
	end := start.Add(5 * time.Second)

	modelResult := ModelResult{
		ModelID:   "test",
		StartTime: start,
		EndTime:   end,
		Status:    StatusSuccess,
	}

	duration := modelResult.Duration()
	if duration != 5*time.Second {
		t.Errorf("Duration = %v, want 5s", duration)
	}
}
