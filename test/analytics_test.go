package test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestAnalyticsQueriesExist validates that all analytical query SQL files are created
func TestAnalyticsQueriesExist(t *testing.T) {
	repoRoot := getRepoRoot(t)
	analyticsDir := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "analytics")

	requiredFiles := []string{
		"corridor_comparison.sql",
		"bottleneck_analysis.sql",
		"straggler_trends.sql",
		"cycle_time_optimization.sql",
		"queue_impact.sql",
		"power_efficiency.sql",
		"seasonal_patterns.sql",
	}

	for _, file := range requiredFiles {
		filePath := filepath.Join(analyticsDir, file)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			t.Errorf("Required analytics query file does not exist: %s", file)
		}
	}
}

// TestCorridorComparisonQuery validates corridor performance comparison query
func TestCorridorComparisonQuery(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "analytics", "corridor_comparison.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read corridor_comparison.sql: %v", err)
	}

	sql := string(content)

	// Verify it references agg_corridor_weekly_metrics
	if !strings.Contains(sql, "agg_corridor_weekly_metrics") {
		t.Error("corridor_comparison.sql should reference agg_corridor_weekly_metrics")
	}

	// Verify it includes ranking or ordering
	if !strings.Contains(sql, "RANK()") && !strings.Contains(sql, "ROW_NUMBER()") && !strings.Contains(sql, "ORDER BY") {
		t.Error("corridor_comparison.sql should include ranking or ordering logic")
	}

	// Verify it compares transit times
	if !strings.Contains(sql, "transit") {
		t.Error("corridor_comparison.sql should analyze transit times")
	}

	// Verify it includes key metrics
	requiredMetrics := []string{"straggler", "queue"}
	for _, metric := range requiredMetrics {
		if !strings.Contains(strings.ToLower(sql), metric) {
			t.Errorf("corridor_comparison.sql should include metric: %s", metric)
		}
	}

	// Verify it uses CTE for clarity
	if !strings.Contains(sql, "WITH") {
		t.Error("corridor_comparison.sql should use CTE (WITH clause)")
	}
}

// TestBottleneckAnalysisQuery validates bottleneck identification query
func TestBottleneckAnalysisQuery(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "analytics", "bottleneck_analysis.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read bottleneck_analysis.sql: %v", err)
	}

	sql := string(content)

	// Verify it references agg_queue_analysis
	if !strings.Contains(sql, "agg_queue_analysis") {
		t.Error("bottleneck_analysis.sql should reference agg_queue_analysis")
	}

	// Verify it references fact_train_trip
	if !strings.Contains(sql, "fact_train_trip") {
		t.Error("bottleneck_analysis.sql should reference fact_train_trip")
	}

	// Verify it identifies longest queue times
	if !strings.Contains(strings.ToLower(sql), "queue") {
		t.Error("bottleneck_analysis.sql should analyze queue times")
	}

	// Verify it identifies delays
	if !strings.Contains(strings.ToLower(sql), "delay") {
		t.Error("bottleneck_analysis.sql should analyze delays")
	}

	// Verify it uses CTE for clarity
	if !strings.Contains(sql, "WITH") {
		t.Error("bottleneck_analysis.sql should use CTE (WITH clause)")
	}
}

// TestStragglerTrendsQuery validates straggler pattern analysis over time
func TestStragglerTrendsQuery(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "analytics", "straggler_trends.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read straggler_trends.sql: %v", err)
	}

	sql := string(content)

	// Verify it references agg_straggler_impact
	if !strings.Contains(sql, "agg_straggler_impact") {
		t.Error("straggler_trends.sql should reference agg_straggler_impact")
	}

	// Verify it analyzes trends over time (week)
	if !strings.Contains(strings.ToLower(sql), "week") {
		t.Error("straggler_trends.sql should analyze trends by week")
	}

	// Verify it analyzes straggler rates
	if !strings.Contains(strings.ToLower(sql), "straggler") && !strings.Contains(strings.ToLower(sql), "rate") {
		t.Error("straggler_trends.sql should analyze straggler rates")
	}

	// Verify it analyzes by corridor
	if !strings.Contains(strings.ToLower(sql), "corridor") {
		t.Error("straggler_trends.sql should analyze by corridor")
	}

	// Verify it uses CTE for clarity
	if !strings.Contains(sql, "WITH") {
		t.Error("straggler_trends.sql should use CTE (WITH clause)")
	}
}

// TestCycleTimeOptimizationQuery validates cycle time breakdown and optimization opportunities
func TestCycleTimeOptimizationQuery(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "analytics", "cycle_time_optimization.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read cycle_time_optimization.sql: %v", err)
	}

	sql := string(content)

	// Verify it references agg_corridor_weekly_metrics
	if !strings.Contains(sql, "agg_corridor_weekly_metrics") {
		t.Error("cycle_time_optimization.sql should reference agg_corridor_weekly_metrics")
	}

	// Verify it references turnaround metrics
	if !strings.Contains(sql, "agg_origin_turnaround") || !strings.Contains(sql, "agg_destination_turnaround") {
		t.Error("cycle_time_optimization.sql should reference turnaround metrics")
	}

	// Verify it breaks down cycle time components
	componentsFound := 0
	if strings.Contains(strings.ToLower(sql), "transit") {
		componentsFound++
	}
	if strings.Contains(strings.ToLower(sql), "queue") {
		componentsFound++
	}
	if strings.Contains(strings.ToLower(sql), "turnaround") {
		componentsFound++
	}

	if componentsFound < 3 {
		t.Error("cycle_time_optimization.sql should analyze all three cycle time components: transit, queue, turnaround")
	}

	// Verify it uses CTE for clarity
	if !strings.Contains(sql, "WITH") {
		t.Error("cycle_time_optimization.sql should use CTE (WITH clause)")
	}
}

// TestQueueImpactQuery validates queue waiting impact quantification
func TestQueueImpactQuery(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "analytics", "queue_impact.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read queue_impact.sql: %v", err)
	}

	sql := string(content)

	// Verify it references agg_queue_analysis
	if !strings.Contains(sql, "agg_queue_analysis") {
		t.Error("queue_impact.sql should reference agg_queue_analysis")
	}

	// Verify it quantifies hours lost
	if !strings.Contains(strings.ToLower(sql), "hours") {
		t.Error("queue_impact.sql should quantify hours lost to queuing")
	}

	// Verify it calculates percentages or ratios
	if !strings.Contains(strings.ToLower(sql), "percent") && !strings.Contains(sql, "/") && !strings.Contains(sql, "*") {
		t.Error("queue_impact.sql should calculate percentages of cycle time")
	}

	// Verify it analyzes queue impact by location
	if !strings.Contains(strings.ToLower(sql), "location") && !strings.Contains(strings.ToLower(sql), "origin") && !strings.Contains(strings.ToLower(sql), "destination") {
		t.Error("queue_impact.sql should analyze queue impact by location")
	}

	// Verify it uses CTE for clarity
	if !strings.Contains(sql, "WITH") {
		t.Error("queue_impact.sql should use CTE (WITH clause)")
	}
}

// TestPowerEfficiencyQuery validates locomotive power transfer pattern analysis
func TestPowerEfficiencyQuery(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "analytics", "power_efficiency.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read power_efficiency.sql: %v", err)
	}

	sql := string(content)

	// Verify it references agg_power_efficiency
	if !strings.Contains(sql, "agg_power_efficiency") {
		t.Error("power_efficiency.sql should reference agg_power_efficiency")
	}

	// Verify it analyzes repower frequency or patterns
	if !strings.Contains(strings.ToLower(sql), "repower") && !strings.Contains(strings.ToLower(sql), "power") {
		t.Error("power_efficiency.sql should analyze repower patterns")
	}

	// Verify it compares same-power vs different-power
	if !strings.Contains(strings.ToLower(sql), "same") && !strings.Contains(strings.ToLower(sql), "different") {
		t.Error("power_efficiency.sql should compare same-power vs different-power patterns")
	}

	// Verify it analyzes by corridor
	if !strings.Contains(strings.ToLower(sql), "corridor") {
		t.Error("power_efficiency.sql should analyze by corridor")
	}

	// Verify it uses CTE for clarity
	if !strings.Contains(sql, "WITH") {
		t.Error("power_efficiency.sql should use CTE (WITH clause)")
	}
}

// TestSeasonalPatternsQuery validates seasonal effect detection (weeks 5 & 8)
func TestSeasonalPatternsQuery(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "analytics", "seasonal_patterns.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read seasonal_patterns.sql: %v", err)
	}

	sql := string(content)

	// Verify it references agg_corridor_weekly_metrics or agg_straggler_impact
	if !strings.Contains(sql, "agg_corridor_weekly_metrics") && !strings.Contains(sql, "agg_straggler_impact") {
		t.Error("seasonal_patterns.sql should reference weekly aggregated metrics")
	}

	// Verify it analyzes by week
	if !strings.Contains(strings.ToLower(sql), "week") {
		t.Error("seasonal_patterns.sql should analyze patterns by week")
	}

	// Verify it detects anomalies or patterns (week 5 and 8)
	if !strings.Contains(sql, "5") && !strings.Contains(sql, "8") {
		t.Error("seasonal_patterns.sql should detect week 5 and week 8 patterns")
	}

	// Verify it compares metrics over time
	if !strings.Contains(strings.ToLower(sql), "avg") && !strings.Contains(strings.ToLower(sql), "baseline") && !strings.Contains(strings.ToLower(sql), "compare") {
		t.Error("seasonal_patterns.sql should compare metrics to baseline or average")
	}

	// Verify it uses CTE for clarity
	if !strings.Contains(sql, "WITH") {
		t.Error("seasonal_patterns.sql should use CTE (WITH clause)")
	}
}
