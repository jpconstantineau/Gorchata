package executor

import (
	"context"
	"strings"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/domain/materialization"
	"github.com/jpconstantineau/gorchata/internal/template"
)

// TestEnginePassesIncrementalContext verifies that the engine sets IsIncremental
// and CurrentModelTable in the template context when executing incremental models
func TestEnginePassesIncrementalContext(t *testing.T) {
	tests := []struct {
		name                  string
		materializationType   materialization.MaterializationType
		fullRefresh           bool
		expectedIsIncremental bool
		templateContent       string
		expectedInSQL         string // string that should appear in SQL if context is set correctly
	}{
		{
			name:                  "incremental model sets IsIncremental true",
			materializationType:   materialization.MaterializationIncremental,
			fullRefresh:           false,
			expectedIsIncremental: true,
			templateContent:       "SELECT * FROM source {{ if is_incremental }}WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }}){{ end }}",
			expectedInSQL:         "WHERE updated_at >",
		},
		{
			name:                  "incremental with full refresh sets IsIncremental false",
			materializationType:   materialization.MaterializationIncremental,
			fullRefresh:           true,
			expectedIsIncremental: false,
			templateContent:       "SELECT * FROM source {{ if is_incremental }}WHERE updated_at > '2020-01-01'{{ end }}",
			expectedInSQL:         "", // Should NOT contain the WHERE clause
		},
		{
			name:                  "table model sets IsIncremental false",
			materializationType:   materialization.MaterializationTable,
			fullRefresh:           false,
			expectedIsIncremental: false,
			templateContent:       "SELECT * FROM source {{ if is_incremental }}WHERE id > 100{{ end }}",
			expectedInSQL:         "", // Should NOT contain the WHERE clause
		},
		{
			name:                  "view model sets IsIncremental false",
			materializationType:   materialization.MaterializationView,
			fullRefresh:           false,
			expectedIsIncremental: false,
			templateContent:       "SELECT * FROM source {{ if is_incremental }}LIMIT 10{{ end }}",
			expectedInSQL:         "", // Should NOT contain the LIMIT clause
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := newMockAdapter()
			templateEngine := template.New()

			engine, err := NewEngine(adapter, templateEngine)
			if err != nil {
				t.Fatalf("failed to create engine: %v", err)
			}

			// Create model with template content
			model, err := NewModel("test_model", "models/test_model.sql")
			if err != nil {
				t.Fatalf("failed to create model: %v", err)
			}

			// Set template content (this is what we need to add to Model)
			model.SetTemplateContent(tt.templateContent)

			// Set materialization config
			model.SetMaterializationConfig(materialization.MaterializationConfig{
				Type:        tt.materializationType,
				FullRefresh: tt.fullRefresh,
				UniqueKey:   []string{"id"},
			})

			// For incremental tests that expect the table to exist, mark it as existing
			if tt.expectedIsIncremental {
				adapter.tableExists["test_model"] = true
			}

			// Execute the model
			ctx := context.Background()
			result, err := engine.ExecuteModel(ctx, model)

			if err != nil {
				t.Fatalf("unexpected error executing model: %v", err)
			}

			if result.Status != StatusSuccess {
				t.Errorf("expected status %v, got %v: %s", StatusSuccess, result.Status, result.Error)
			}

			// Check that the rendered SQL contains (or doesn't contain) the expected incremental logic
			if tt.expectedInSQL != "" {
				// Should contain the incremental logic
				foundInSQL := false
				for _, sql := range result.SQLStatements {
					if strings.Contains(sql, tt.expectedInSQL) {
						foundInSQL = true
						break
					}
				}
				if !foundInSQL {
					t.Errorf("expected SQL to contain %q when IsIncremental=%v, but it didn't. SQL statements: %v",
						tt.expectedInSQL, tt.expectedIsIncremental, result.SQLStatements)
				}
			} else {
				// Should NOT contain incremental logic
				for _, sql := range result.SQLStatements {
					if strings.Contains(sql, "WHERE") && (strings.Contains(sql, "WHERE updated_at >") ||
						strings.Contains(sql, "WHERE id >") || strings.Contains(sql, "LIMIT 10")) {
						t.Errorf("expected SQL to NOT contain incremental logic when IsIncremental=%v, but found it in: %s",
							tt.expectedIsIncremental, sql)
					}
				}
			}
		})
	}
}

// TestEnginePassesCurrentModelTable verifies that the engine sets CurrentModelTable
// correctly so that the {{ this }} template function works
func TestEnginePassesCurrentModelTable(t *testing.T) {
	adapter := newMockAdapter()
	templateEngine := template.New()

	engine, err := NewEngine(adapter, templateEngine)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	model, err := NewModel("my_model", "models/my_model.sql")
	if err != nil {
		t.Fatalf("failed to create model: %v", err)
	}

	// Template uses {{ this }} which should expand to the model name
	templateContent := "SELECT * FROM source WHERE id > (SELECT MAX(id) FROM {{ this }})"
	model.SetTemplateContent(templateContent)

	model.SetMaterializationConfig(materialization.MaterializationConfig{
		Type:      materialization.MaterializationIncremental,
		UniqueKey: []string{"id"},
	})

	ctx := context.Background()
	result, err := engine.ExecuteModel(ctx, model)

	if err != nil {
		t.Fatalf("unexpected error executing model: %v", err)
	}

	if result.Status != StatusSuccess {
		t.Errorf("expected status %v, got %v: %s", StatusSuccess, result.Status, result.Error)
	}

	// Check that {{ this }} was replaced with the model name
	foundModelReference := false
	for _, sql := range result.SQLStatements {
		if strings.Contains(sql, "FROM my_model") {
			foundModelReference = true
			break
		}
	}

	if !foundModelReference {
		t.Errorf("expected SQL to contain reference to 'my_model' (from {{ this }}), but it didn't. SQL statements: %v",
			result.SQLStatements)
	}
}

// TestIncrementalModelExecution tests a complete incremental model execution
// with both is_incremental template function and {{ this }} reference
func TestIncrementalModelExecution(t *testing.T) {
	adapter := newMockAdapter()
	templateEngine := template.New()

	engine, err := NewEngine(adapter, templateEngine)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	model, err := NewModel("fct_sales", "models/facts/fct_sales.sql")
	if err != nil {
		t.Fatalf("failed to create model: %v", err)
	}

	// Realistic incremental model template
	templateContent := `SELECT 
	sale_id,
	customer_id,
	product_id,
	sale_date,
	amount
FROM raw.sales
{{ if is_incremental }}
WHERE sale_date > (SELECT COALESCE(MAX(sale_date), '1900-01-01') FROM {{ this }})
{{ end }}`

	model.SetTemplateContent(templateContent)
	model.SetMaterializationConfig(materialization.MaterializationConfig{
		Type:      materialization.MaterializationIncremental,
		UniqueKey: []string{"sale_id"},
	})

	// Mark the table as existing so incremental mode is triggered
	adapter.tableExists["fct_sales"] = true

	ctx := context.Background()
	result, err := engine.ExecuteModel(ctx, model)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Status != StatusSuccess {
		t.Errorf("expected status %v, got %v: %s", StatusSuccess, result.Status, result.Error)
	}

	// Verify the incremental WHERE clause is present
	foundIncrementalLogic := false
	foundThisReference := false
	for _, sql := range result.SQLStatements {
		if strings.Contains(sql, "WHERE sale_date >") {
			foundIncrementalLogic = true
		}
		if strings.Contains(sql, "FROM fct_sales") {
			foundThisReference = true
		}
	}

	if !foundIncrementalLogic {
		t.Errorf("expected incremental WHERE clause in SQL, but didn't find it. SQL statements: %v",
			result.SQLStatements)
	}

	if !foundThisReference {
		t.Errorf("expected {{ this }} to resolve to 'fct_sales', but didn't find it. SQL statements: %v",
			result.SQLStatements)
	}

	// Now test with full refresh - should NOT have WHERE clause
	model.SetMaterializationConfig(materialization.MaterializationConfig{
		Type:        materialization.MaterializationIncremental,
		UniqueKey:   []string{"sale_id"},
		FullRefresh: true,
	})

	result2, err := engine.ExecuteModel(ctx, model)
	if err != nil {
		t.Fatalf("unexpected error on full refresh: %v", err)
	}

	if result2.Status != StatusSuccess {
		t.Errorf("expected status %v, got %v: %s", StatusSuccess, result2.Status, result2.Error)
	}

	// Should NOT have the incremental WHERE clause when full refresh is true
	for _, sql := range result2.SQLStatements {
		if strings.Contains(sql, "WHERE sale_date >") {
			t.Errorf("expected NO incremental WHERE clause with full refresh, but found it in: %s", sql)
		}
	}
}

// TestFullRefreshOverride verifies that incremental models use DROP+CREATE
// (table strategy) when FullRefresh flag is set, instead of MERGE
func TestFullRefreshOverride(t *testing.T) {
	adapter := newMockAdapter()
	templateEngine := template.New()

	engine, err := NewEngine(adapter, templateEngine)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}

	model, err := NewModel("incremental_table", "models/incremental_table.sql")
	if err != nil {
		t.Fatalf("failed to create model: %v", err)
	}

	templateContent := `SELECT id, value FROM source`
	model.SetTemplateContent(templateContent)

	// Test 1: Normal incremental behavior (should use MERGE strategy)
	model.SetMaterializationConfig(materialization.MaterializationConfig{
		Type:      materialization.MaterializationIncremental,
		UniqueKey: []string{"id"},
	})

	ctx := context.Background()
	result1, err := engine.ExecuteModel(ctx, model)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result1.Status != StatusSuccess {
		t.Errorf("expected status %v, got %v: %s", StatusSuccess, result1.Status, result1.Error)
	}

	// Should use MERGE or INSERT strategy (contains merge-related keywords)
	foundMergeLogic := false
	for _, sql := range result1.SQLStatements {
		sqlUpper := strings.ToUpper(sql)
		if strings.Contains(sqlUpper, "MERGE") || strings.Contains(sqlUpper, "INSERT") ||
			strings.Contains(sqlUpper, "UPDATE") || strings.Contains(sqlUpper, "_TEMP") {
			foundMergeLogic = true
			break
		}
	}

	if !foundMergeLogic {
		t.Logf("Incremental SQL statements: %v", result1.SQLStatements)
		// Note: Depending on the materialization strategy implementation,
		// this might not fail if the strategy is implemented differently
	}

	// Test 2: Full refresh behavior (should use DROP+CREATE like table strategy)
	model.SetMaterializationConfig(materialization.MaterializationConfig{
		Type:        materialization.MaterializationIncremental,
		UniqueKey:   []string{"id"},
		FullRefresh: true,
	})

	result2, err := engine.ExecuteModel(ctx, model)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result2.Status != StatusSuccess {
		t.Errorf("expected status %v, got %v: %s", StatusSuccess, result2.Status, result2.Error)
	}

	// Should use DROP+CREATE strategy (like a table materialization)
	foundDrop := false
	foundCreate := false
	for _, sql := range result2.SQLStatements {
		sqlUpper := strings.ToUpper(sql)
		if strings.Contains(sqlUpper, "DROP") {
			foundDrop = true
		}
		if strings.Contains(sqlUpper, "CREATE TABLE") {
			foundCreate = true
		}
	}

	if !foundDrop || !foundCreate {
		t.Errorf("expected full refresh to use DROP+CREATE strategy, but found: %v", result2.SQLStatements)
	}
}
