package storage

import (
	"context"
	"testing"
)

// TestCleanupConfig_Default tests default cleanup configuration
func TestCleanupConfig_Default(t *testing.T) {
	config := DefaultCleanupConfig()

	if config.RetentionDays <= 0 {
		t.Errorf("RetentionDays = %d, want > 0", config.RetentionDays)
	}
	if config.RetentionDays != 30 {
		t.Errorf("RetentionDays = %d, want 30", config.RetentionDays)
	}
	if !config.Enabled {
		t.Error("Enabled = false, want true")
	}
}

// TestCleanupConfig_Custom tests custom cleanup configuration
func TestCleanupConfig_Custom(t *testing.T) {
	config := CleanupConfig{
		RetentionDays: 7,
		Enabled:       false,
	}

	if config.RetentionDays != 7 {
		t.Errorf("RetentionDays = %d, want 7", config.RetentionDays)
	}
	if config.Enabled {
		t.Error("Enabled = true, want false")
	}
}

// TestCleanupOldFailures_Disabled tests cleanup when disabled
func TestCleanupOldFailures_Disabled(t *testing.T) {
	ctx := context.Background()
	mock := &MockFailureStore{}
	config := CleanupConfig{
		RetentionDays: 30,
		Enabled:       false,
	}

	err := CleanupOldFailures(ctx, mock, config)
	if err != nil {
		t.Errorf("CleanupOldFailures() error = %v, want nil", err)
	}

	// Should not call store when disabled
	if len(mock.CleanupCalls) != 0 {
		t.Errorf("CleanupOldFailures() called store %d times, want 0", len(mock.CleanupCalls))
	}
}

// TestCleanupOldFailures_Enabled tests cleanup when enabled
func TestCleanupOldFailures_Enabled(t *testing.T) {
	ctx := context.Background()
	mock := &MockFailureStore{}
	config := CleanupConfig{
		RetentionDays: 30,
		Enabled:       true,
	}

	err := CleanupOldFailures(ctx, mock, config)
	if err != nil {
		t.Errorf("CleanupOldFailures() error = %v, want nil", err)
	}

	// Should call store when enabled
	if len(mock.CleanupCalls) != 1 {
		t.Errorf("CleanupOldFailures() called store %d times, want 1", len(mock.CleanupCalls))
	}
	if mock.CleanupCalls[0] != 30 {
		t.Errorf("CleanupOldFailures() retention = %d, want 30", mock.CleanupCalls[0])
	}
}

// TestCleanupOldFailures_Error tests cleanup error handling
func TestCleanupOldFailures_Error(t *testing.T) {
	ctx := context.Background()
	mock := &MockFailureStore{
		CleanupError: context.Canceled,
	}
	config := CleanupConfig{
		RetentionDays: 30,
		Enabled:       true,
	}

	err := CleanupOldFailures(ctx, mock, config)
	if err == nil {
		t.Error("CleanupOldFailures() error = nil, want error")
	}
	if err != context.Canceled {
		t.Errorf("CleanupOldFailures() error = %v, want context.Canceled", err)
	}
}

// TestCleanupConfig_Validate tests validation of cleanup config
func TestCleanupConfig_Validate(t *testing.T) {
	tests := []struct {
		name      string
		config    CleanupConfig
		wantError bool
	}{
		{
			name: "valid config",
			config: CleanupConfig{
				RetentionDays: 30,
				Enabled:       true,
			},
			wantError: false,
		},
		{
			name: "zero retention days",
			config: CleanupConfig{
				RetentionDays: 0,
				Enabled:       true,
			},
			wantError: true,
		},
		{
			name: "negative retention days",
			config: CleanupConfig{
				RetentionDays: -1,
				Enabled:       true,
			},
			wantError: true,
		},
		{
			name: "disabled with invalid retention",
			config: CleanupConfig{
				RetentionDays: -1,
				Enabled:       false,
			},
			wantError: false, // Should not validate when disabled
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantError {
				t.Errorf("Validate() error = %v, wantError %v", err, tt.wantError)
			}
		})
	}
}
