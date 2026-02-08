package test

import (
	"testing"
)

func TestDefaultTestConfig(t *testing.T) {
	config := DefaultTestConfig()

	if config.Severity != SeverityError {
		t.Errorf("Severity = %v, want %v", config.Severity, SeverityError)
	}
	if config.StoreFailures {
		t.Error("StoreFailures = true, want false")
	}
	if config.Where != "" {
		t.Errorf("Where = %v, want empty string", config.Where)
	}
	if config.SampleSize != 0 {
		t.Errorf("SampleSize = %v, want 0", config.SampleSize)
	}
	if len(config.Tags) != 0 {
		t.Errorf("Tags length = %v, want 0", len(config.Tags))
	}
	if config.CustomName != "" {
		t.Errorf("CustomName = %v, want empty string", config.CustomName)
	}
}

func TestTestConfig_Validation(t *testing.T) {
	tests := []struct {
		name    string
		config  *TestConfig
		wantErr bool
	}{
		{
			name: "valid config with defaults",
			config: &TestConfig{
				Severity:      SeverityError,
				StoreFailures: false,
			},
			wantErr: false,
		},
		{
			name: "valid config with all fields",
			config: &TestConfig{
				Severity:      SeverityWarn,
				StoreFailures: true,
				Where:         "created_at > '2024-01-01'",
				SampleSize:    100,
				Tags:          []string{"critical", "daily"},
				CustomName:    "Custom Test Name",
			},
			wantErr: false,
		},
		{
			name: "invalid severity",
			config: &TestConfig{
				Severity: "",
			},
			wantErr: true,
		},
		{
			name: "negative sample size",
			config: &TestConfig{
				Severity:   SeverityError,
				SampleSize: -1,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
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

func TestSeverity_String(t *testing.T) {
	tests := []struct {
		name     string
		severity Severity
		want     string
	}{
		{
			name:     "error severity",
			severity: SeverityError,
			want:     "error",
		},
		{
			name:     "warn severity",
			severity: SeverityWarn,
			want:     "warn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.severity.String(); got != tt.want {
				t.Errorf("Severity.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTestConfig_SetSeverity(t *testing.T) {
	config := DefaultTestConfig()

	config.SetSeverity(SeverityWarn)

	if config.Severity != SeverityWarn {
		t.Errorf("Severity = %v, want %v", config.Severity, SeverityWarn)
	}
}

func TestTestConfig_SetStoreFailures(t *testing.T) {
	config := DefaultTestConfig()

	config.SetStoreFailures(true)

	if !config.StoreFailures {
		t.Error("StoreFailures = false, want true")
	}
}

func TestTestConfig_SetWhere(t *testing.T) {
	config := DefaultTestConfig()

	whereClause := "created_at > '2024-01-01'"
	config.SetWhere(whereClause)

	if config.Where != whereClause {
		t.Errorf("Where = %v, want %v", config.Where, whereClause)
	}
}

func TestTestConfig_SetSampleSize(t *testing.T) {
	config := DefaultTestConfig()

	sampleSize := 100
	config.SetSampleSize(sampleSize)

	if config.SampleSize != sampleSize {
		t.Errorf("SampleSize = %v, want %v", config.SampleSize, sampleSize)
	}
}

func TestTestConfig_AddTag(t *testing.T) {
	config := DefaultTestConfig()

	config.AddTag("critical")
	config.AddTag("daily")

	if len(config.Tags) != 2 {
		t.Errorf("Tags length = %v, want 2", len(config.Tags))
	}
	if config.Tags[0] != "critical" {
		t.Errorf("Tags[0] = %v, want critical", config.Tags[0])
	}
	if config.Tags[1] != "daily" {
		t.Errorf("Tags[1] = %v, want daily", config.Tags[1])
	}

	// Add duplicate tag (should not add)
	config.AddTag("critical")

	if len(config.Tags) != 2 {
		t.Errorf("Tags length = %v, want 2 (duplicate not added)", len(config.Tags))
	}
}

func TestTestConfig_SetCustomName(t *testing.T) {
	config := DefaultTestConfig()

	customName := "My Custom Test"
	config.SetCustomName(customName)

	if config.CustomName != customName {
		t.Errorf("CustomName = %v, want %v", config.CustomName, customName)
	}
}

func TestConditionalThreshold_Evaluate(t *testing.T) {
	tests := []struct {
		name      string
		threshold ConditionalThreshold
		rowCount  int64
		want      bool
	}{
		{
			name: "greater than - true",
			threshold: ConditionalThreshold{
				Operator: OperatorGreaterThan,
				Value:    10,
			},
			rowCount: 15,
			want:     true,
		},
		{
			name: "greater than - false",
			threshold: ConditionalThreshold{
				Operator: OperatorGreaterThan,
				Value:    10,
			},
			rowCount: 5,
			want:     false,
		},
		{
			name: "greater than or equal - true (equal)",
			threshold: ConditionalThreshold{
				Operator: OperatorGreaterThanOrEqual,
				Value:    10,
			},
			rowCount: 10,
			want:     true,
		},
		{
			name: "greater than or equal - true (greater)",
			threshold: ConditionalThreshold{
				Operator: OperatorGreaterThanOrEqual,
				Value:    10,
			},
			rowCount: 15,
			want:     true,
		},
		{
			name: "greater than or equal - false",
			threshold: ConditionalThreshold{
				Operator: OperatorGreaterThanOrEqual,
				Value:    10,
			},
			rowCount: 5,
			want:     false,
		},
		{
			name: "equals - true",
			threshold: ConditionalThreshold{
				Operator: OperatorEquals,
				Value:    10,
			},
			rowCount: 10,
			want:     true,
		},
		{
			name: "equals - false",
			threshold: ConditionalThreshold{
				Operator: OperatorEquals,
				Value:    10,
			},
			rowCount: 5,
			want:     false,
		},
		{
			name: "not equals - true",
			threshold: ConditionalThreshold{
				Operator: OperatorNotEquals,
				Value:    10,
			},
			rowCount: 5,
			want:     true,
		},
		{
			name: "not equals - false",
			threshold: ConditionalThreshold{
				Operator: OperatorNotEquals,
				Value:    10,
			},
			rowCount: 10,
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.threshold.Evaluate(tt.rowCount); got != tt.want {
				t.Errorf("ConditionalThreshold.Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTestConfig_WithConditionalThresholds(t *testing.T) {
	config := DefaultTestConfig()

	errorThreshold := ConditionalThreshold{
		Operator: OperatorGreaterThan,
		Value:    10,
	}
	warnThreshold := ConditionalThreshold{
		Operator: OperatorGreaterThan,
		Value:    5,
	}

	config.SetErrorIf(errorThreshold)
	config.SetWarnIf(warnThreshold)

	if config.ErrorIf == nil {
		t.Error("ErrorIf should be set")
	}
	if config.ErrorIf.Operator != OperatorGreaterThan {
		t.Errorf("ErrorIf.Operator = %v, want %v", config.ErrorIf.Operator, OperatorGreaterThan)
	}
	if config.ErrorIf.Value != 10 {
		t.Errorf("ErrorIf.Value = %v, want 10", config.ErrorIf.Value)
	}

	if config.WarnIf == nil {
		t.Error("WarnIf should be set")
	}
	if config.WarnIf.Operator != OperatorGreaterThan {
		t.Errorf("WarnIf.Operator = %v, want %v", config.WarnIf.Operator, OperatorGreaterThan)
	}
	if config.WarnIf.Value != 5 {
		t.Errorf("WarnIf.Value = %v, want 5", config.WarnIf.Value)
	}
}

func TestComparisonOperator_String(t *testing.T) {
	tests := []struct {
		name     string
		operator ComparisonOperator
		want     string
	}{
		{
			name:     "greater than",
			operator: OperatorGreaterThan,
			want:     ">",
		},
		{
			name:     "greater than or equal",
			operator: OperatorGreaterThanOrEqual,
			want:     ">=",
		},
		{
			name:     "equals",
			operator: OperatorEquals,
			want:     "=",
		},
		{
			name:     "not equals",
			operator: OperatorNotEquals,
			want:     "!=",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.operator.String(); got != tt.want {
				t.Errorf("ComparisonOperator.String() = %v, want %v", got, tt.want)
			}
		})
	}
}
