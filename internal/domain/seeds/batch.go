package seeds

import "fmt"

// BatchProcessor processes data in configurable batch sizes
type BatchProcessor struct {
	BatchSize int
}

// NewBatchProcessor creates a new batch processor with the specified batch size
func NewBatchProcessor(batchSize int) *BatchProcessor {
	if batchSize <= 0 {
		panic(fmt.Sprintf("batch size must be greater than 0, got %d", batchSize))
	}
	return &BatchProcessor{
		BatchSize: batchSize,
	}
}

// Process processes rows in batches, calling the provided function for each batch
func (bp *BatchProcessor) Process(rows [][]string, fn func(batch [][]string) error) error {
	for i := 0; i < len(rows); i += bp.BatchSize {
		end := i + bp.BatchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[i:end]
		if err := fn(batch); err != nil {
			return err
		}
	}
	return nil
}
