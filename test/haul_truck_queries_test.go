package test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestHaulTruckAnalyticsQueriesExist validates that all analytical query SQL files are created
func TestHaulTruckAnalyticsQueriesExist(t *testing.T) {
	repoRoot := getRepoRoot(t)
	analyticsDir := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "models", "analytics")

	requiredFiles := []string{
		"worst_performing_trucks.sql",
		"bottleneck_analysis.sql",
		"payload_compliance.sql",
		"shift_performance.sql",
		"fuel_efficiency.sql",
		"operator_performance.sql",
	}

	for _, file := range requiredFiles {
		filePath := filepath.Join(analyticsDir, file)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			t.Errorf("Required analytics query file does not exist: %s", file)
		}
	}
}

// TestWorstPerformingTrucksQuery validates ranking by lowest tons per hour
func TestWorstPerformingTrucksQuery(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "models", "analytics", "worst_performing_trucks.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read worst_performing_trucks.sql: %v", err)
	}

	sql := string(content)

	// Verify it references truck_daily_productivity
	if !strings.Contains(sql, "truck_daily_productivity") {
		t.Error("worst_performing_trucks.sql should reference truck_daily_productivity")
	}

	// Verify it references dim_truck
	if !strings.Contains(sql, "dim_truck") {
		t.Error("worst_performing_trucks.sql should reference dim_truck")
	}

	// Verify it includes ranking
	if !strings.Contains(sql, "RANK()") && !strings.Contains(sql, "ROW_NUMBER()") {
		t.Error("worst_performing_trucks.sql should include ranking logic (RANK() or ROW_NUMBER())")
	}

	// Verify key metrics
	requiredMetrics := []string{"tons_per_hour", "payload_utilization", "cycle_time"}
	for _, metric := range requiredMetrics {
		if !strings.Contains(strings.ToLower(sql), strings.ToLower(metric)) {
			t.Errorf("worst_performing_trucks.sql should include metric: %s", metric)
		}
	}

	// Verify it uses CTE for clarity
	if !strings.Contains(sql, "WITH") {
		t.Error("worst_performing_trucks.sql should use CTE (WITH clause)")
	}

	// Verify it orders by performance (ASC for worst first)
	if !strings.Contains(sql, "ORDER BY") {
		t.Error("worst_performing_trucks.sql should have ORDER BY clause")
	}
}

// TestHaulTruckBottleneckAnalysisQuery validates bottleneck identification (crusher vs shovel)
func TestHaulTruckBottleneckAnalysisQuery(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "models", "analytics", "bottleneck_analysis.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read bottleneck_analysis.sql: %v", err)
	}

	sql := string(content)

	// Verify it references queue_analysis
	if !strings.Contains(sql, "queue_analysis") {
		t.Error("bottleneck_analysis.sql should reference queue_analysis")
	}

	// Verify it analyzes both crusher and shovel queues
	if !strings.Contains(strings.ToLower(sql), "crusher") {
		t.Error("bottleneck_analysis.sql should analyze crusher queues")
	}
	if !strings.Contains(strings.ToLower(sql), "shovel") {
		t.Error("bottleneck_analysis.sql should analyze shovel queues")
	}

	// Verify key metrics
	requiredMetrics := []string{"queue", "avg", "max"}
	for _, metric := range requiredMetrics {
		if !strings.Contains(strings.ToLower(sql), metric) {
			t.Errorf("bottleneck_analysis.sql should include metric: %s", metric)
		}
	}

	// Verify it uses CTE
	if !strings.Contains(sql, "WITH") {
		t.Error("bottleneck_analysis.sql should use CTE (WITH clause)")
	}

	// Verify it identifies constraints/bottlenecks
	if !strings.Contains(strings.ToLower(sql), "constraint") && !strings.Contains(strings.ToLower(sql), "bottleneck") {
		t.Error("bottleneck_analysis.sql should identify constraints/bottlenecks")
	}
}

// TestPayloadComplianceQuery validates underload/overload frequency analysis
func TestPayloadComplianceQuery(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "models", "analytics", "payload_compliance.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read payload_compliance.sql: %v", err)
	}

	sql := string(content)

	// Verify it references fact_haul_cycle
	if !strings.Contains(sql, "fact_haul_cycle") {
		t.Error("payload_compliance.sql should reference fact_haul_cycle")
	}

	// Verify it references dim_truck for capacity
	if !strings.Contains(sql, "dim_truck") {
		t.Error("payload_compliance.sql should reference dim_truck")
	}

	// Verify it checks payload thresholds (85%, 105%)
	payloadChecks := []string{"85", "105", "payload", "utilization"}
	foundChecks := 0
	for _, check := range payloadChecks {
		if strings.Contains(sql, check) {
			foundChecks++
		}
	}
	if foundChecks < 2 {
		t.Error("payload_compliance.sql should include payload threshold checks (85%, 105%, utilization)")
	}

	// Verify it categorizes cycles (underload, optimal, overload)
	categories := []string{"underload", "overload", "optimal"}
	foundCategories := 0
	for _, cat := range categories {
		if strings.Contains(strings.ToLower(sql), cat) {
			foundCategories++
		}
	}
	if foundCategories < 2 {
		t.Error("payload_compliance.sql should categorize cycles (underload, optimal, overload)")
	}

	// Verify it uses CTE
	if !strings.Contains(sql, "WITH") {
		t.Error("payload_compliance.sql should use CTE (WITH clause)")
	}
}

// TestShiftPerformanceQuery validates day vs night shift productivity comparison
func TestShiftPerformanceQuery(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "models", "analytics", "shift_performance.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read shift_performance.sql: %v", err)
	}

	sql := string(content)

	// Verify it references fact_haul_cycle
	if !strings.Contains(sql, "fact_haul_cycle") {
		t.Error("shift_performance.sql should reference fact_haul_cycle")
	}

	// Verify it references dim_shift
	if !strings.Contains(sql, "dim_shift") {
		t.Error("shift_performance.sql should reference dim_shift")
	}

	// Verify it analyzes shift performance
	if !strings.Contains(strings.ToLower(sql), "shift") {
		t.Error("shift_performance.sql should analyze by shift")
	}

	// Verify key metrics
	requiredMetrics := []string{"tons", "cycle", "productivity"}
	foundMetrics := 0
	for _, metric := range requiredMetrics {
		if strings.Contains(strings.ToLower(sql), metric) {
			foundMetrics++
		}
	}
	if foundMetrics < 2 {
		t.Error("shift_performance.sql should include key productivity metrics")
	}

	// Verify it uses CTE
	if !strings.Contains(sql, "WITH") {
		t.Error("shift_performance.sql should use CTE (WITH clause)")
	}

	// Verify it compares shifts (percentage difference)
	if !strings.Contains(strings.ToLower(sql), "pct") && !strings.Contains(strings.ToLower(sql), "percent") && !strings.Contains(strings.ToLower(sql), "difference") {
		t.Error("shift_performance.sql should compare shift performance (percentage/difference)")
	}
}

// TestFuelEfficiencyQuery validates liters per ton and fuel efficiency calculations
func TestFuelEfficiencyQuery(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "models", "analytics", "fuel_efficiency.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read fuel_efficiency.sql: %v", err)
	}

	sql := string(content)

	// Verify it references fact_haul_cycle
	if !strings.Contains(sql, "fact_haul_cycle") {
		t.Error("fuel_efficiency.sql should reference fact_haul_cycle")
	}

	// Verify it references dim_truck
	if !strings.Contains(sql, "dim_truck") {
		t.Error("fuel_efficiency.sql should reference dim_truck")
	}

	// Verify it calculates fuel metrics
	fuelMetrics := []string{"fuel", "liter", "ton"}
	for _, metric := range fuelMetrics {
		if !strings.Contains(strings.ToLower(sql), metric) {
			t.Errorf("fuel_efficiency.sql should include %s in calculations", metric)
		}
	}

	// Verify it calculates distance-based metrics (ton-miles)
	if !strings.Contains(strings.ToLower(sql), "distance") {
		t.Error("fuel_efficiency.sql should include distance-based calculations")
	}

	// Verify it includes ranking
	if !strings.Contains(sql, "RANK()") && !strings.Contains(sql, "ROW_NUMBER()") && !strings.Contains(sql, "ORDER BY") {
		t.Error("fuel_efficiency.sql should include ranking logic")
	}

	// Verify it uses CTE
	if !strings.Contains(sql, "WITH") {
		t.Error("fuel_efficiency.sql should use CTE (WITH clause)")
	}
}

// TestOperatorPerformanceQuery validates operator ranking by efficiency
func TestOperatorPerformanceQuery(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "models", "analytics", "operator_performance.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read operator_performance.sql: %v", err)
	}

	sql := string(content)

	// Verify it references fact_haul_cycle
	if !strings.Contains(sql, "fact_haul_cycle") {
		t.Error("operator_performance.sql should reference fact_haul_cycle")
	}

	// Verify it references dim_operator
	if !strings.Contains(sql, "dim_operator") {
		t.Error("operator_performance.sql should reference dim_operator")
	}

	// Verify it groups/analyzes by operator
	if !strings.Contains(strings.ToLower(sql), "operator") {
		t.Error("operator_performance.sql should analyze by operator")
	}

	// Verify key performance metrics
	performanceMetrics := []string{"cycle", "payload", "efficiency"}
	foundMetrics := 0
	for _, metric := range performanceMetrics {
		if strings.Contains(strings.ToLower(sql), metric) {
			foundMetrics++
		}
	}
	if foundMetrics < 2 {
		t.Error("operator_performance.sql should include key performance metrics (cycle time, payload utilization, efficiency)")
	}

	// Verify it includes ranking
	if !strings.Contains(sql, "RANK()") && !strings.Contains(sql, "ROW_NUMBER()") && !strings.Contains(sql, "ORDER BY") {
		t.Error("operator_performance.sql should include ranking logic")
	}

	// Verify it uses CTE
	if !strings.Contains(sql, "WITH") {
		t.Error("operator_performance.sql should use CTE (WITH clause)")
	}
}

// TestQueryResultStructure validates basic SQL structure of all queries
func TestQueryResultStructure(t *testing.T) {
	repoRoot := getRepoRoot(t)
	analyticsDir := filepath.Join(repoRoot, "examples", "haul_truck_analytics", "models", "analytics")

	queryFiles := []string{
		"worst_performing_trucks.sql",
		"bottleneck_analysis.sql",
		"payload_compliance.sql",
		"shift_performance.sql",
		"fuel_efficiency.sql",
		"operator_performance.sql",
	}

	for _, file := range queryFiles {
		t.Run(file, func(t *testing.T) {
			filePath := filepath.Join(analyticsDir, file)
			content, err := os.ReadFile(filePath)
			if err != nil {
				t.Fatalf("Failed to read %s: %v", file, err)
			}

			sql := string(content)

			// All queries should have SELECT statement
			if !strings.Contains(strings.ToUpper(sql), "SELECT") {
				t.Errorf("%s should contain SELECT statement", file)
			}

			// All queries should have FROM clause
			if !strings.Contains(strings.ToUpper(sql), "FROM") {
				t.Errorf("%s should contain FROM clause", file)
			}

			// All queries should use CTEs for clarity
			if !strings.Contains(sql, "WITH") {
				t.Errorf("%s should use CTE (WITH clause) for clarity", file)
			}

			// Queries should not be empty
			if len(strings.TrimSpace(sql)) < 100 {
				t.Errorf("%s appears to be too short or empty", file)
			}
		})
	}
}
