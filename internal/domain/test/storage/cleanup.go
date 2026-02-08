package storage

import (
	"context"
	"fmt"
)

// CleanupConfig defines retention policy for test failures
type CleanupConfig struct {
	RetentionDays int  // Number of days to retain failures
	Enabled       bool // Whether cleanup is enabled
}

// DefaultCleanupConfig returns sensible defaults for cleanup
func DefaultCleanupConfig() CleanupConfig {
	return CleanupConfig{
		RetentionDays: 30,
		Enabled:       true,
	}
}

// Validate checks if the cleanup config is valid
func (c CleanupConfig) Validate() error {
	if c.Enabled && c.RetentionDays <= 0 {
		return fmt.Errorf("retention days must be positive when cleanup is enabled")
	}
	return nil
}

// CleanupOldFailures removes failures older than retention period
func CleanupOldFailures(ctx context.Context, store FailureStore, config CleanupConfig) error {
	if !config.Enabled {
		return nil
	}

	if err := config.Validate(); err != nil {
		return fmt.Errorf("invalid cleanup config: %w", err)
	}

	return store.CleanupOldFailures(ctx, config.RetentionDays)
}
