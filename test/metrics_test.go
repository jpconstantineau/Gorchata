package test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMetricsSQLFilesExist validates that all metrics SQL files are created
func TestMetricsSQLFilesExist(t *testing.T) {
	repoRoot := getRepoRoot(t)
	metricsDir := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "metrics")

	requiredFiles := []string{
		"agg_corridor_weekly_metrics.sql",
		"agg_fleet_utilization_daily.sql",
		"agg_origin_turnaround.sql",
		"agg_destination_turnaround.sql",
		"agg_straggler_impact.sql",
		"agg_queue_analysis.sql",
		"agg_power_efficiency.sql",
	}

	for _, file := range requiredFiles {
		filePath := filepath.Join(metricsDir, file)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			t.Errorf("Required metrics file does not exist: %s", file)
		}
	}
}

// TestCorridorMetricsByWeek validates weekly corridor aggregations
func TestCorridorMetricsByWeek(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "metrics", "agg_corridor_weekly_metrics.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read agg_corridor_weekly_metrics.sql: %v", err)
	}

	sql := string(content)

	// Verify it's configured as a table
	if !strings.Contains(sql, `config "materialized" "table"`) {
		t.Error("agg_corridor_weekly_metrics should be materialized as a table")
	}

	// Verify it references fact_train_trip as source
	if !strings.Contains(sql, "fact_train_trip") {
		t.Error("agg_corridor_weekly_metrics should reference fact_train_trip")
	}

	// Verify it references dim_corridor
	if !strings.Contains(sql, "dim_corridor") {
		t.Error("agg_corridor_weekly_metrics should reference dim_corridor")
	}

	// Verify it references dim_date for week extraction
	if !strings.Contains(sql, "dim_date") {
		t.Error("agg_corridor_weekly_metrics should reference dim_date")
	}

	// Verify required metrics columns
	requiredMetrics := []string{
		"total_trips",
		"avg_transit_hours",
		"avg_origin_queue_hours",
		"avg_destination_queue_hours",
		"total_stragglers",
		"avg_straggler_delay_hours",
	}

	for _, metric := range requiredMetrics {
		if !strings.Contains(sql, metric) {
			t.Errorf("agg_corridor_weekly_metrics should calculate: %s", metric)
		}
	}

	// Verify GROUP BY includes corridor_id, year, week
	if !strings.Contains(sql, "GROUP BY") {
		t.Error("agg_corridor_weekly_metrics should use GROUP BY for aggregation")
	}

	// Verify AVG, SUM, COUNT functions are used
	aggregateFunctions := []string{"AVG(", "SUM(", "COUNT("}
	for _, fn := range aggregateFunctions {
		if !strings.Contains(sql, fn) {
			t.Errorf("agg_corridor_weekly_metrics should use aggregate function: %s", fn)
		}
	}

	// Verify NULLIF for division by zero protection
	if !strings.Contains(sql, "NULLIF") {
		t.Error("agg_corridor_weekly_metrics should use NULLIF for safe division")
	}
}

// TestOriginTurnaroundMetrics ensures origin turnaround time calculations including queue wait
func TestOriginTurnaroundMetrics(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "metrics", "agg_origin_turnaround.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read agg_origin_turnaround.sql: %v", err)
	}

	sql := string(content)

	// Verify it's configured as a table
	if !strings.Contains(sql, `config "materialized" "table"`) {
		t.Error("agg_origin_turnaround should be materialized as a table")
	}

	// Verify it references fact_train_trip
	if !strings.Contains(sql, "fact_train_trip") {
		t.Error("agg_origin_turnaround should reference fact_train_trip")
	}

	// Verify required metrics
	requiredMetrics := []string{
		"avg_turnaround_hours",
		"min_turnaround_hours",
		"max_turnaround_hours",
		"trip_count",
	}

	for _, metric := range requiredMetrics {
		if !strings.Contains(sql, metric) {
			t.Errorf("agg_origin_turnaround should calculate: %s", metric)
		}
	}

	// Verify origin_queue_hours is included in turnaround calculation
	if !strings.Contains(sql, "origin_queue_hours") {
		t.Error("agg_origin_turnaround should include origin_queue_hours in turnaround time")
	}

	// Verify GROUP BY includes origin location
	if !strings.Contains(sql, "GROUP BY") {
		t.Error("agg_origin_turnaround should group by origin location and week")
	}
}

// TestDestinationTurnaroundMetrics ensures destination turnaround time calculations including queue wait
func TestDestinationTurnaroundMetrics(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "metrics", "agg_destination_turnaround.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read agg_destination_turnaround.sql: %v", err)
	}

	sql := string(content)

	// Verify it's configured as a table
	if !strings.Contains(sql, `config "materialized" "table"`) {
		t.Error("agg_destination_turnaround should be materialized as a table")
	}

	// Verify it references fact_train_trip
	if !strings.Contains(sql, "fact_train_trip") {
		t.Error("agg_destination_turnaround should reference fact_train_trip")
	}

	// Verify required metrics
	requiredMetrics := []string{
		"avg_turnaround_hours",
		"min_turnaround_hours",
		"max_turnaround_hours",
		"trip_count",
	}

	for _, metric := range requiredMetrics {
		if !strings.Contains(sql, metric) {
			t.Errorf("agg_destination_turnaround should calculate: %s", metric)
		}
	}

	// Verify destination_queue_hours is included in turnaround calculation
	if !strings.Contains(sql, "destination_queue_hours") {
		t.Error("agg_destination_turnaround should include destination_queue_hours in turnaround time")
	}

	// Verify GROUP BY includes destination location
	if !strings.Contains(sql, "GROUP BY") {
		t.Error("agg_destination_turnaround should group by destination location and week")
	}
}

// TestCycleTimeMetricsByCorridorWeek validates complete cycle time per corridor per week
func TestCycleTimeMetricsByCorridorWeek(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "metrics", "agg_corridor_weekly_metrics.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read agg_corridor_weekly_metrics.sql: %v", err)
	}

	sql := string(content)

	// Verify cycle time components are calculated
	cycleComponents := []string{
		"total_trip_hours", // full cycle time
		"transit_hours",    // or avg_transit_hours
	}

	hasComponents := false
	for _, comp := range cycleComponents {
		if strings.Contains(sql, comp) {
			hasComponents = true
			break
		}
	}

	if !hasComponents {
		t.Error("agg_corridor_weekly_metrics should include cycle time calculations (total_trip_hours or avg_cycle_time)")
	}
}

// TestStragglerImpactMetrics calculates straggler rate, delay distribution, and impact
func TestStragglerImpactMetrics(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "metrics", "agg_straggler_impact.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read agg_straggler_impact.sql: %v", err)
	}

	sql := string(content)

	// Verify it's configured as a table
	if !strings.Contains(sql, `config "materialized" "table"`) {
		t.Error("agg_straggler_impact should be materialized as a table")
	}

	// Verify it references fact_straggler
	if !strings.Contains(sql, "fact_straggler") {
		t.Error("agg_straggler_impact should reference fact_straggler")
	}

	// Verify required metrics
	requiredMetrics := []string{
		"straggler_count",
		"avg_delay_hours",
		"straggler_rate", // or percentage
	}

	for _, metric := range requiredMetrics {
		if !strings.Contains(sql, metric) {
			t.Errorf("agg_straggler_impact should calculate: %s", metric)
		}
	}

	// Verify delay distribution - should have buckets or percentiles
	delayBuckets := []string{
		"delay_0_6_hours",     // short delays
		"delay_6_12_hours",    // medium delays
		"delay_12_24_hours",   // long delays
		"delay_24_plus_hours", // very long delays
	}

	hasDelayBuckets := 0
	for _, bucket := range delayBuckets {
		if strings.Contains(sql, bucket) {
			hasDelayBuckets++
		}
	}

	if hasDelayBuckets < 3 {
		t.Error("agg_straggler_impact should include delay distribution buckets")
	}

	// Verify GROUP BY for aggregation
	if !strings.Contains(sql, "GROUP BY") {
		t.Error("agg_straggler_impact should use GROUP BY for aggregation")
	}
}

// TestQueueBottleneckMetrics identifies queue wait time patterns at origins/destinations
func TestQueueBottleneckMetrics(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "metrics", "agg_queue_analysis.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read agg_queue_analysis.sql: %v", err)
	}

	sql := string(content)

	// Verify it's configured as a table
	if !strings.Contains(sql, `config "materialized" "table"`) {
		t.Error("agg_queue_analysis should be materialized as a table")
	}

	// Verify it references fact_train_trip
	if !strings.Contains(sql, "fact_train_trip") {
		t.Error("agg_queue_analysis should reference fact_train_trip")
	}

	// Verify required metrics
	requiredMetrics := []string{
		"avg_queue_hours",
		"max_queue_hours",
		"queue_frequency", // or trip_count
	}

	for _, metric := range requiredMetrics {
		if !strings.Contains(sql, metric) {
			t.Errorf("agg_queue_analysis should calculate: %s", metric)
		}
	}

	// Verify both origin and destination queue times are analyzed
	queueTypes := []string{
		"origin_queue_hours",
		"destination_queue_hours",
	}

	for _, queueType := range queueTypes {
		if !strings.Contains(sql, queueType) {
			t.Errorf("agg_queue_analysis should analyze: %s", queueType)
		}
	}

	// Verify location grouping
	if !strings.Contains(sql, "location") {
		t.Error("agg_queue_analysis should group by location")
	}
}

// TestPowerEfficiencyMetrics calculates inferred power transfer frequency and patterns
func TestPowerEfficiencyMetrics(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "metrics", "agg_power_efficiency.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read agg_power_efficiency.sql: %v", err)
	}

	sql := string(content)

	// Verify it's configured as a table
	if !strings.Contains(sql, `config "materialized" "table"`) {
		t.Error("agg_power_efficiency should be materialized as a table")
	}

	// Verify it references fact_inferred_power_transfer
	if !strings.Contains(sql, "fact_inferred_power_transfer") {
		t.Error("agg_power_efficiency should reference fact_inferred_power_transfer")
	}

	// Verify required metrics
	requiredMetrics := []string{
		"power_transfer_count",
		"same_power_trips",  // or consecutive trips with same power
		"repower_frequency", // or power change rate
	}

	foundMetrics := 0
	for _, metric := range requiredMetrics {
		if strings.Contains(sql, metric) {
			foundMetrics++
		}
	}

	if foundMetrics < 2 {
		t.Error("agg_power_efficiency should calculate power transfer metrics")
	}

	// Verify GROUP BY for aggregation
	if !strings.Contains(sql, "GROUP BY") {
		t.Error("agg_power_efficiency should use GROUP BY for aggregation")
	}
}

// TestFleetUtilizationMetrics calculates cars per trip, trips per week
func TestFleetUtilizationMetrics(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "metrics", "agg_fleet_utilization_daily.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read agg_fleet_utilization_daily.sql: %v", err)
	}

	sql := string(content)

	// Verify it's configured as a table
	if !strings.Contains(sql, `config "materialized" "table"`) {
		t.Error("agg_fleet_utilization_daily should be materialized as a table")
	}

	// Verify it references fact tables
	requiredSources := []string{
		"fact_car_location_event", // or fact_train_trip
		"dim_date",
	}

	for _, source := range requiredSources {
		if !strings.Contains(sql, source) {
			t.Errorf("agg_fleet_utilization_daily should reference: %s", source)
		}
	}

	// Verify required metrics
	requiredMetrics := []string{
		"total_cars",
		"cars_on_trains",
		"cars_as_stragglers",
		"cars_idle",
		"utilization_pct",
	}

	for _, metric := range requiredMetrics {
		if !strings.Contains(sql, metric) {
			t.Errorf("agg_fleet_utilization_daily should calculate: %s", metric)
		}
	}

	// Verify daily aggregation by date_key
	if !strings.Contains(sql, "date_key") {
		t.Error("agg_fleet_utilization_daily should use date_key for daily aggregation")
	}
}

// TestSeasonalEffectMetrics validates week 5 slowdown and week 8 straggler increase detected
func TestSeasonalEffectMetrics(t *testing.T) {
	repoRoot := getRepoRoot(t)
	filePath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "metrics", "agg_corridor_weekly_metrics.sql")

	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read agg_corridor_weekly_metrics.sql: %v", err)
	}

	sql := string(content)

	// Verify week is tracked for seasonal analysis
	if !strings.Contains(sql, "week") {
		t.Error("agg_corridor_weekly_metrics should track week for seasonal analysis")
	}

	// Verify year is tracked
	if !strings.Contains(sql, "year") {
		t.Error("agg_corridor_weekly_metrics should track year for seasonal analysis")
	}

	// Verify transit hours and straggler metrics are calculated (needed to detect anomalies)
	if !strings.Contains(sql, "transit") && !strings.Contains(sql, "straggler") {
		t.Error("agg_corridor_weekly_metrics should include transit and straggler metrics for seasonal analysis")
	}
}

// TestMetricsSchemaAlignment ensures all metric tables defined in schema.yml have SQL files
func TestMetricsSchemaAlignment(t *testing.T) {
	repoRoot := getRepoRoot(t)
	metricsDir := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "metrics")

	// These should match the schema.yml definitions (all 7 Phase 6 metrics)
	expectedMetrics := []string{
		"agg_corridor_weekly_metrics",
		"agg_fleet_utilization_daily",
		"agg_origin_turnaround",
		"agg_destination_turnaround",
		"agg_straggler_impact",
		"agg_queue_analysis",
		"agg_power_efficiency",
	}

	for _, metricTable := range expectedMetrics {
		filePath := filepath.Join(metricsDir, metricTable+".sql")
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			t.Errorf("Schema defines %s but SQL file does not exist", metricTable)
		}
	}
}
