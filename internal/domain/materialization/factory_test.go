package materialization

import (
	"testing"
)

func TestGetStrategy(t *testing.T) {
	tests := []struct {
		name         string
		matType      MaterializationType
		wantStrategy string
		wantErr      bool
	}{
		{
			name:         "returns view strategy",
			matType:      MaterializationView,
			wantStrategy: "view",
			wantErr:      false,
		},
		{
			name:         "returns table strategy",
			matType:      MaterializationTable,
			wantStrategy: "table",
			wantErr:      false,
		},
		{
			name:         "returns incremental strategy",
			matType:      MaterializationIncremental,
			wantStrategy: "incremental",
			wantErr:      false,
		},
		{
			name:         "returns error for unknown strategy",
			matType:      MaterializationType("unknown"),
			wantStrategy: "",
			wantErr:      true,
		},
		{
			name:         "returns error for empty strategy",
			matType:      MaterializationType(""),
			wantStrategy: "",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy, err := GetStrategy(tt.matType)

			if (err != nil) != tt.wantErr {
				t.Errorf("GetStrategy() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if strategy == nil {
					t.Errorf("GetStrategy() returned nil strategy")
					return
				}
				if got := strategy.Name(); got != tt.wantStrategy {
					t.Errorf("GetStrategy() returned strategy %v, want %v", got, tt.wantStrategy)
				}
			}
		})
	}
}

func TestGetStrategyFromConfig(t *testing.T) {
	tests := []struct {
		name         string
		config       MaterializationConfig
		wantStrategy string
		wantErr      bool
	}{
		{
			name: "gets strategy from config type",
			config: MaterializationConfig{
				Type: MaterializationView,
			},
			wantStrategy: "view",
			wantErr:      false,
		},
		{
			name: "defaults to table for empty config",
			config: MaterializationConfig{
				Type: "",
			},
			wantStrategy: "table",
			wantErr:      false,
		},
		{
			name: "returns incremental strategy",
			config: MaterializationConfig{
				Type:      MaterializationIncremental,
				UniqueKey: []string{"id"},
			},
			wantStrategy: "incremental",
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy, err := GetStrategyFromConfig(tt.config)

			if (err != nil) != tt.wantErr {
				t.Errorf("GetStrategyFromConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if strategy == nil {
					t.Errorf("GetStrategyFromConfig() returned nil strategy")
					return
				}
				if got := strategy.Name(); got != tt.wantStrategy {
					t.Errorf("GetStrategyFromConfig() returned strategy %v, want %v", got, tt.wantStrategy)
				}
			}
		})
	}
}

func TestDefaultStrategy(t *testing.T) {
	config := DefaultConfig()
	strategy, err := GetStrategyFromConfig(config)

	if err != nil {
		t.Errorf("GetStrategyFromConfig() with default config returned error: %v", err)
		return
	}

	if strategy.Name() != "table" {
		t.Errorf("Default strategy should be 'table', got: %v", strategy.Name())
	}
}
