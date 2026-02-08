package test

import (
	"testing"
)

func TestNewTest_ValidInput(t *testing.T) {
	tests := []struct {
		name        string
		id          string
		testName    string
		modelName   string
		columnName  string
		testType    TestType
		sqlTemplate string
		wantErr     bool
	}{
		{
			name:        "valid generic test",
			id:          "test_001",
			testName:    "unique_user_id",
			modelName:   "stg_users",
			columnName:  "user_id",
			testType:    GenericTest,
			sqlTemplate: "SELECT * FROM {{.ModelName}} WHERE {{.ColumnName}} IS NULL",
			wantErr:     false,
		},
		{
			name:        "valid singular test",
			id:          "test_002",
			testName:    "row_count_check",
			modelName:   "stg_orders",
			columnName:  "",
			testType:    SingularTest,
			sqlTemplate: "SELECT * FROM {{.ModelName}} WHERE amount < 0",
			wantErr:     false,
		},
		{
			name:        "empty id",
			id:          "",
			testName:    "test",
			modelName:   "model",
			testType:    GenericTest,
			sqlTemplate: "SELECT 1",
			wantErr:     true,
		},
		{
			name:        "empty test name",
			id:          "test_001",
			testName:    "",
			modelName:   "model",
			testType:    GenericTest,
			sqlTemplate: "SELECT 1",
			wantErr:     true,
		},
		{
			name:        "empty model name",
			id:          "test_001",
			testName:    "test",
			modelName:   "",
			testType:    GenericTest,
			sqlTemplate: "SELECT 1",
			wantErr:     true,
		},
		{
			name:        "empty sql template",
			id:          "test_001",
			testName:    "test",
			modelName:   "model",
			testType:    GenericTest,
			sqlTemplate: "",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			test, err := NewTest(tt.id, tt.testName, tt.modelName, tt.columnName, tt.testType, tt.sqlTemplate)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if test.ID != tt.id {
				t.Errorf("ID = %v, want %v", test.ID, tt.id)
			}
			if test.Name != tt.testName {
				t.Errorf("Name = %v, want %v", test.Name, tt.testName)
			}
			if test.ModelName != tt.modelName {
				t.Errorf("ModelName = %v, want %v", test.ModelName, tt.modelName)
			}
			if test.ColumnName != tt.columnName {
				t.Errorf("ColumnName = %v, want %v", test.ColumnName, tt.columnName)
			}
			if test.Type != tt.testType {
				t.Errorf("Type = %v, want %v", test.Type, tt.testType)
			}
			if test.SQLTemplate != tt.sqlTemplate {
				t.Errorf("SQLTemplate = %v, want %v", test.SQLTemplate, tt.sqlTemplate)
			}
			// Check default config is initialized
			if test.Config == nil {
				t.Error("Config should be initialized")
			}
		})
	}
}

func TestNewTest_InvalidInput(t *testing.T) {
	tests := []struct {
		name     string
		id       string
		testName string
		model    string
		testType TestType
		sql      string
	}{
		{
			name:     "all empty fields",
			id:       "",
			testName: "",
			model:    "",
			testType: GenericTest,
			sql:      "",
		},
		{
			name:     "missing id",
			id:       "",
			testName: "test",
			model:    "model",
			testType: GenericTest,
			sql:      "SELECT 1",
		},
		{
			name:     "missing name",
			id:       "id",
			testName: "",
			model:    "model",
			testType: GenericTest,
			sql:      "SELECT 1",
		},
		{
			name:     "missing model",
			id:       "id",
			testName: "test",
			model:    "",
			testType: GenericTest,
			sql:      "SELECT 1",
		},
		{
			name:     "missing sql",
			id:       "id",
			testName: "test",
			model:    "model",
			testType: GenericTest,
			sql:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewTest(tt.id, tt.testName, tt.model, "", tt.testType, tt.sql)
			if err == nil {
				t.Error("expected error for invalid input, got nil")
			}
		})
	}
}

func TestTest_Validation(t *testing.T) {
	tests := []struct {
		name    string
		test    *Test
		wantErr bool
	}{
		{
			name: "valid test",
			test: &Test{
				ID:          "test_001",
				Name:        "unique_check",
				ModelName:   "stg_users",
				Type:        GenericTest,
				SQLTemplate: "SELECT * FROM {{.ModelName}}",
				Config:      DefaultTestConfig(),
			},
			wantErr: false,
		},
		{
			name: "missing id",
			test: &Test{
				ID:          "",
				Name:        "unique_check",
				ModelName:   "stg_users",
				Type:        GenericTest,
				SQLTemplate: "SELECT * FROM {{.ModelName}}",
				Config:      DefaultTestConfig(),
			},
			wantErr: true,
		},
		{
			name: "missing name",
			test: &Test{
				ID:          "test_001",
				Name:        "",
				ModelName:   "stg_users",
				Type:        GenericTest,
				SQLTemplate: "SELECT * FROM {{.ModelName}}",
				Config:      DefaultTestConfig(),
			},
			wantErr: true,
		},
		{
			name: "missing model name",
			test: &Test{
				ID:          "test_001",
				Name:        "unique_check",
				ModelName:   "",
				Type:        GenericTest,
				SQLTemplate: "SELECT * FROM {{.ModelName}}",
				Config:      DefaultTestConfig(),
			},
			wantErr: true,
		},
		{
			name: "missing sql template",
			test: &Test{
				ID:          "test_001",
				Name:        "unique_check",
				ModelName:   "stg_users",
				Type:        GenericTest,
				SQLTemplate: "",
				Config:      DefaultTestConfig(),
			},
			wantErr: true,
		},
		{
			name: "nil config",
			test: &Test{
				ID:          "test_001",
				Name:        "unique_check",
				ModelName:   "stg_users",
				Type:        GenericTest,
				SQLTemplate: "SELECT * FROM {{.ModelName}}",
				Config:      nil,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.test.Validate()
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

func TestTestType_String(t *testing.T) {
	tests := []struct {
		name     string
		testType TestType
		want     string
	}{
		{
			name:     "generic test",
			testType: GenericTest,
			want:     "generic",
		},
		{
			name:     "singular test",
			testType: SingularTest,
			want:     "singular",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.testType.String(); got != tt.want {
				t.Errorf("TestType.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTestStatus_String(t *testing.T) {
	tests := []struct {
		name   string
		status TestStatus
		want   string
	}{
		{
			name:   "pending",
			status: StatusPending,
			want:   "pending",
		},
		{
			name:   "running",
			status: StatusRunning,
			want:   "running",
		},
		{
			name:   "passed",
			status: StatusPassed,
			want:   "passed",
		},
		{
			name:   "failed",
			status: StatusFailed,
			want:   "failed",
		},
		{
			name:   "warning",
			status: StatusWarning,
			want:   "warning",
		},
		{
			name:   "skipped",
			status: StatusSkipped,
			want:   "skipped",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.status.String(); got != tt.want {
				t.Errorf("TestStatus.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTest_SetConfig(t *testing.T) {
	test, err := NewTest("test_001", "test", "model", "", GenericTest, "SELECT 1")
	if err != nil {
		t.Fatalf("failed to create test: %v", err)
	}

	config := &TestConfig{
		Severity:      SeverityError,
		StoreFailures: true,
	}

	test.SetConfig(config)

	if test.Config != config {
		t.Error("Config was not set correctly")
	}
	if test.Config.Severity != SeverityError {
		t.Errorf("Config.Severity = %v, want %v", test.Config.Severity, SeverityError)
	}
	if !test.Config.StoreFailures {
		t.Error("Config.StoreFailures = false, want true")
	}
}
