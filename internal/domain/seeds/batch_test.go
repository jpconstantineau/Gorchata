package seeds

import (
	"errors"
	"testing"
)

func TestBatchProcessor_SmallBatch(t *testing.T) {
	bp := NewBatchProcessor(2)

	rows := [][]string{
		{"1", "Alice"},
		{"2", "Bob"},
		{"3", "Charlie"},
		{"4", "Diana"},
		{"5", "Eve"},
	}

	var batches [][]string
	err := bp.Process(rows, func(batch [][]string) error {
		// Collect each batch for verification
		batches = append(batches, batch...)
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have processed 3 batches: [0,1], [2,3], [4]
	expectedBatches := 3
	actualBatches := (len(rows) + bp.BatchSize - 1) / bp.BatchSize
	if actualBatches != expectedBatches {
		t.Errorf("expected %d batches, got %d", expectedBatches, actualBatches)
	}

	// Verify all rows were processed
	if len(batches) != len(rows) {
		t.Errorf("expected %d rows processed, got %d", len(rows), len(batches))
	}
}

func TestBatchProcessor_ExactBatch(t *testing.T) {
	bp := NewBatchProcessor(5)

	rows := [][]string{
		{"1", "A"}, {"2", "B"}, {"3", "C"}, {"4", "D"}, {"5", "E"},
		{"6", "F"}, {"7", "G"}, {"8", "H"}, {"9", "I"}, {"10", "J"},
	}

	batchCount := 0
	err := bp.Process(rows, func(batch [][]string) error {
		batchCount++
		// Each batch should have exactly 5 rows
		if len(batch) != 5 {
			t.Errorf("batch %d: expected 5 rows, got %d", batchCount, len(batch))
		}
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have exactly 2 batches
	if batchCount != 2 {
		t.Errorf("expected 2 batches, got %d", batchCount)
	}
}

func TestBatchProcessor_LargeBatch(t *testing.T) {
	bp := NewBatchProcessor(1000)

	rows := [][]string{
		{"1", "Alice"},
		{"2", "Bob"},
		{"3", "Charlie"},
	}

	batchCount := 0
	err := bp.Process(rows, func(batch [][]string) error {
		batchCount++
		// Should process all rows in a single batch
		if len(batch) != 3 {
			t.Errorf("expected 3 rows in batch, got %d", len(batch))
		}
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have exactly 1 batch
	if batchCount != 1 {
		t.Errorf("expected 1 batch, got %d", batchCount)
	}
}

func TestBatchProcessor_ErrorHandling(t *testing.T) {
	bp := NewBatchProcessor(2)

	rows := [][]string{
		{"1", "Alice"},
		{"2", "Bob"},
		{"3", "Charlie"},
		{"4", "Diana"},
	}

	expectedError := errors.New("processing error")
	batchCount := 0

	err := bp.Process(rows, func(batch [][]string) error {
		batchCount++
		// Return error on second batch
		if batchCount == 2 {
			return expectedError
		}
		return nil
	})

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, expectedError) {
		t.Errorf("expected error %v, got %v", expectedError, err)
	}

	// Should have stopped at second batch
	if batchCount != 2 {
		t.Errorf("expected to stop at batch 2, processed %d batches", batchCount)
	}
}

func TestNewBatchProcessor_InvalidSize(t *testing.T) {
	tests := []struct {
		name      string
		batchSize int
		wantPanic bool
	}{
		{
			name:      "zero batch size",
			batchSize: 0,
			wantPanic: true,
		},
		{
			name:      "negative batch size",
			batchSize: -1,
			wantPanic: true,
		},
		{
			name:      "negative large batch size",
			batchSize: -100,
			wantPanic: true,
		},
		{
			name:      "valid batch size 1",
			batchSize: 1,
			wantPanic: false,
		},
		{
			name:      "valid batch size 100",
			batchSize: 100,
			wantPanic: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if tt.wantPanic && r == nil {
					t.Error("expected panic, but didn't panic")
				}
				if !tt.wantPanic && r != nil {
					t.Errorf("unexpected panic: %v", r)
				}
			}()

			bp := NewBatchProcessor(tt.batchSize)
			if !tt.wantPanic && bp.BatchSize != tt.batchSize {
				t.Errorf("expected batch size %d, got %d", tt.batchSize, bp.BatchSize)
			}
		})
	}
}
