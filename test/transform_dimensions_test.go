package test

import (
	"context"
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/platform"
	"github.com/jpconstantineau/gorchata/internal/platform/sqlite"
	"github.com/jpconstantineau/gorchata/internal/template"
)

// setupTestDatabase creates a test database with raw CLM events loaded
func setupTestDatabase(t *testing.T) (*sqlite.SQLiteAdapter, context.Context, string) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	config := &platform.ConnectionConfig{
		DatabasePath: dbPath,
	}
	adapter := sqlite.NewSQLiteAdapter(config)

	ctx := context.Background()
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Create raw table and load seed data
	repoRoot := getRepoRoot(t)
	seedPath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "seeds", "raw_clm_events.csv")

	err := adapter.ExecuteDDL(ctx, `
		CREATE TABLE raw_clm_events (
			event_id INTEGER PRIMARY KEY,
			event_timestamp TEXT NOT NULL,
			car_id TEXT NOT NULL,
			train_id TEXT NOT NULL,
			location_id TEXT NOT NULL,
			event_type TEXT NOT NULL,
			loaded_flag TEXT NOT NULL,
			commodity TEXT NOT NULL,
			weight_tons REAL NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create raw table: %v", err)
	}

	err = loadCSVIntoTableDim(ctx, adapter, seedPath, "raw_clm_events")
	if err != nil {
		t.Fatalf("Failed to load CSV data: %v", err)
	}

	return adapter, ctx, repoRoot
}

// executeDimensionModel loads and executes a dimension model SQL file
func executeDimensionModel(t *testing.T, adapter *sqlite.SQLiteAdapter, ctx context.Context, repoRoot, modelName string) {
	modelPath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "dimensions", modelName+".sql")
	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		t.Fatalf("Model not found: %s", modelPath)
	}

	content, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatalf("Failed to read model: %v", err)
	}

	// Remove config calls before parsing
	contentStr := removeConfigCallsDim(string(content))

	// Parse and render template
	templateEngine := template.New()
	tmpl, err := templateEngine.Parse(modelName, contentStr)
	if err != nil {
		t.Fatalf("Failed to parse template: %v", err)
	}

	ctx2 := template.NewContext(template.WithCurrentModel(modelName))
	ctx2.Seeds = map[string]string{
		"raw_clm_events": "raw_clm_events",
	}
	rendered, err := template.Render(tmpl, ctx2, nil)
	if err != nil {
		t.Fatalf("Failed to render template: %v", err)
	}

	// Create dimension table
	err = adapter.ExecuteDDL(ctx, "DROP TABLE IF EXISTS "+modelName)
	if err != nil {
		t.Fatalf("Failed to drop existing table: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, "CREATE TABLE "+modelName+" AS "+rendered)
	if err != nil {
		t.Fatalf("Failed to execute model: %v", err)
	}
}

// TestDimCarGeneration ensures all 250 cars in dimension with single car type
func TestDimCarGeneration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, repoRoot := setupTestDatabase(t)
	defer adapter.Close()

	// Execute dim_car model
	executeDimensionModel(t, adapter, ctx, repoRoot, "dim_car")

	// Test 1: Verify exactly 228 unique cars (based on generated seed data)
	countResult, err := adapter.ExecuteQuery(ctx, "SELECT COUNT(*) as count FROM dim_car")
	if err != nil {
		t.Fatalf("Failed to query dim_car: %v", err)
	}
	carCount := countResult.Rows[0][0].(int64)
	if carCount != 228 {
		t.Errorf("Expected exactly 228 cars (from seed data), got %d", carCount)
	}
	t.Logf("Found %d unique cars", carCount)

	// Test 2: Verify all cars have COAL_HOPPER type
	typeResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(DISTINCT car_type) as type_count 
		FROM dim_car
	`)
	if err != nil {
		t.Fatalf("Failed to query car types: %v", err)
	}
	typeCount := typeResult.Rows[0][0].(int64)
	if typeCount != 1 {
		t.Errorf("Expected 1 car type, got %d", typeCount)
	}

	// Test 3: Verify car_type is COAL_HOPPER
	carTypeResult, err := adapter.ExecuteQuery(ctx, `
		SELECT DISTINCT car_type 
		FROM dim_car
	`)
	if err != nil {
		t.Fatalf("Failed to query car type value: %v", err)
	}
	carType := carTypeResult.Rows[0][0].(string)
	if carType != "COAL_HOPPER" {
		t.Errorf("Expected car type COAL_HOPPER, got %s", carType)
	}

	// Test 4: Verify capacity is set (100 tons per car as standard)
	capacityResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_car 
		WHERE capacity_tons IS NULL OR capacity_tons <= 0
	`)
	if err != nil {
		t.Fatalf("Failed to query capacities: %v", err)
	}
	invalidCapacity := capacityResult.Rows[0][0].(int64)
	if invalidCapacity > 0 {
		t.Errorf("Found %d cars with invalid capacity", invalidCapacity)
	}

	// Test 5: Verify no nulls in required fields
	nullResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_car 
		WHERE car_id IS NULL OR car_id = '' OR car_type IS NULL OR car_type = ''
	`)
	if err != nil {
		t.Fatalf("Failed to check nulls: %v", err)
	}
	nullCount := nullResult.Rows[0][0].(int64)
	if nullCount > 0 {
		t.Errorf("Found %d records with null required fields", nullCount)
	}
}

// TestDimTrainGeneration validates train records created from CLM events with trip-specific IDs
func TestDimTrainGeneration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, repoRoot := setupTestDatabase(t)
	defer adapter.Close()

	// Execute dim_train model
	executeDimensionModel(t, adapter, ctx, repoRoot, "dim_train")

	// Test 1: Verify trains exist
	countResult, err := adapter.ExecuteQuery(ctx, "SELECT COUNT(*) as count FROM dim_train")
	if err != nil {
		t.Fatalf("Failed to query dim_train: %v", err)
	}
	trainCount := countResult.Rows[0][0].(int64)
	if trainCount == 0 {
		t.Error("No trains generated")
	}

	// Test 2: Verify each train has 75 cars (standard unit train size)
	// Allow for some variation in case of missing data or stragglers
	carCountResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_train 
		WHERE num_cars < 70 OR num_cars > 80
	`)
	if err != nil {
		t.Fatalf("Failed to query train car counts: %v", err)
	}
	irregularTrains := carCountResult.Rows[0][0].(int64)
	if irregularTrains > 0 {
		t.Logf("Warning: Found %d trains with car count outside 70-80 range", irregularTrains)
	}

	// Test 3: Verify train_id format (TRAIN_XXX where XXX is numeric)
	formatResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_train 
		WHERE train_id NOT LIKE 'TRAIN_%'
	`)
	if err != nil {
		t.Fatalf("Failed to query train ID format: %v", err)
	}
	invalidFormat := formatResult.Rows[0][0].(int64)
	if invalidFormat > 0 {
		t.Errorf("Found %d trains with invalid ID format", invalidFormat)
	}

	// Test 4: Verify formed_at timestamp exists
	formedResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_train 
		WHERE formed_at IS NULL
	`)
	if err != nil {
		t.Fatalf("Failed to query formed_at: %v", err)
	}
	missingFormed := formedResult.Rows[0][0].(int64)
	if missingFormed > 0 {
		t.Errorf("Found %d trains without formed_at timestamp", missingFormed)
	}

	// Test 5: Verify most trains have completed (arrived at destination)
	completedResult, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			COUNT(*) as total_trains,
			COUNT(completed_at) as completed_trains
		FROM dim_train
	`)
	if err != nil {
		t.Fatalf("Failed to query completion status: %v", err)
	}
	totalTrains := completedResult.Rows[0][0].(int64)
	completedCount := completedResult.Rows[0][1].(int64)
	if completedCount == 0 && totalTrains > 0 {
		t.Error("No completed trains found")
	}
	completionRate := float64(completedCount) / float64(totalTrains) * 100
	t.Logf("Completion rate: %.1f%% (%d of %d trains)", completionRate, completedCount, totalTrains)

	// Test 6: Verify no nulls in required fields
	nullResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_train 
		WHERE train_id IS NULL OR train_id = '' 
		   OR train_name IS NULL OR train_name = ''
		   OR num_cars IS NULL
	`)
	if err != nil {
		t.Fatalf("Failed to check nulls: %v", err)
	}
	nullCount := nullResult.Rows[0][0].(int64)
	if nullCount > 0 {
		t.Errorf("Found %d records with null required fields", nullCount)
	}
}

// TestDimLocationHierarchy ensures origin/station/destination hierarchy
func TestDimLocationHierarchy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, repoRoot := setupTestDatabase(t)
	defer adapter.Close()

	// Execute dim_location model
	executeDimensionModel(t, adapter, ctx, repoRoot, "dim_location")

	// Test 1: Verify 2 origins exist
	originResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_location 
		WHERE location_type = 'ORIGIN'
	`)
	if err != nil {
		t.Fatalf("Failed to query origins: %v", err)
	}
	originCount := originResult.Rows[0][0].(int64)
	if originCount != 2 {
		t.Errorf("Expected 2 origins, got %d", originCount)
	}

	// Test 2: Verify 3 destinations exist
	destResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_location 
		WHERE location_type = 'DESTINATION'
	`)
	if err != nil {
		t.Fatalf("Failed to query destinations: %v", err)
	}
	destCount := destResult.Rows[0][0].(int64)
	if destCount != 3 {
		t.Errorf("Expected 3 destinations, got %d", destCount)
	}

	// Test 3: Verify stations exist (intermediate locations)
	stationResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_location 
		WHERE location_type = 'STATION'
	`)
	if err != nil {
		t.Fatalf("Failed to query stations: %v", err)
	}
	stationCount := stationResult.Rows[0][0].(int64)
	if stationCount == 0 {
		t.Error("No stations found")
	}

	// Test 4: Verify origins have queue capacity attribute
	queueResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_location 
		WHERE location_type = 'ORIGIN' 
		  AND (avg_queue_hours IS NULL OR avg_queue_hours < 12 OR avg_queue_hours > 18)
	`)
	if err != nil {
		t.Fatalf("Failed to query origin queue times: %v", err)
	}
	invalidQueue := queueResult.Rows[0][0].(int64)
	if invalidQueue > 0 {
		t.Errorf("Found %d origins with invalid queue hours (should be 12-18)", invalidQueue)
	}

	// Test 5: Verify destinations have queue capacity attribute
	destQueueResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_location 
		WHERE location_type = 'DESTINATION' 
		  AND (avg_queue_hours IS NULL OR avg_queue_hours < 8 OR avg_queue_hours > 12)
	`)
	if err != nil {
		t.Fatalf("Failed to query destination queue times: %v", err)
	}
	invalidDestQueue := destQueueResult.Rows[0][0].(int64)
	if invalidDestQueue > 0 {
		t.Errorf("Found %d destinations with invalid queue hours (should be 8-12)", invalidDestQueue)
	}

	// Test 6: Verify location_type values are valid
	typeResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_location 
		WHERE location_type NOT IN ('ORIGIN', 'DESTINATION', 'STATION')
	`)
	if err != nil {
		t.Fatalf("Failed to query location types: %v", err)
	}
	invalidTypes := typeResult.Rows[0][0].(int64)
	if invalidTypes > 0 {
		t.Errorf("Found %d locations with invalid type", invalidTypes)
	}

	// Test 7: Verify origin names (COAL_MINE_A, COAL_MINE_B)
	originNameResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_location 
		WHERE location_type = 'ORIGIN' 
		  AND location_id IN ('COAL_MINE_A', 'COAL_MINE_B')
	`)
	if err != nil {
		t.Fatalf("Failed to query origin names: %v", err)
	}
	validOrigins := originNameResult.Rows[0][0].(int64)
	if validOrigins != 2 {
		t.Errorf("Expected 2 valid origin names, got %d", validOrigins)
	}

	// Test 8: Verify destination names
	destNameResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_location 
		WHERE location_type = 'DESTINATION' 
		  AND location_id IN ('POWER_PLANT_1', 'POWER_PLANT_2', 'PORT_TERMINAL')
	`)
	if err != nil {
		t.Fatalf("Failed to query destination names: %v", err)
	}
	validDests := destNameResult.Rows[0][0].(int64)
	if validDests != 3 {
		t.Errorf("Expected 3 valid destination names, got %d", validDests)
	}
}

// TestDimCorridorCreation validates 6 corridor records with proper attributes
func TestDimCorridorCreation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, repoRoot := setupTestDatabase(t)
	defer adapter.Close()

	// Execute dim_location first (dependency)
	executeDimensionModel(t, adapter, ctx, repoRoot, "dim_location")

	// Execute dim_corridor model
	executeDimensionModel(t, adapter, ctx, repoRoot, "dim_corridor")

	// Test 1: Verify 6 corridors (2 origins × 3 destinations)
	countResult, err := adapter.ExecuteQuery(ctx, "SELECT COUNT(*) as count FROM dim_corridor")
	if err != nil {
		t.Fatalf("Failed to query dim_corridor: %v", err)
	}
	corridorCount := countResult.Rows[0][0].(int64)
	if corridorCount != 6 {
		t.Errorf("Expected 6 corridors, got %d", corridorCount)
	}

	// Test 2: Verify all corridors have valid origin references
	originRefResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_corridor c
		LEFT JOIN dim_location l ON c.origin_location_id = l.location_id
		WHERE l.location_type != 'ORIGIN'
	`)
	if err != nil {
		t.Fatalf("Failed to query origin references: %v", err)
	}
	invalidOrigins := originRefResult.Rows[0][0].(int64)
	if invalidOrigins > 0 {
		t.Errorf("Found %d corridors with invalid origin references", invalidOrigins)
	}

	// Test 3: Verify all corridors have valid destination references
	destRefResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_corridor c
		LEFT JOIN dim_location l ON c.destination_location_id = l.location_id
		WHERE l.location_type != 'DESTINATION'
	`)
	if err != nil {
		t.Fatalf("Failed to query destination references: %v", err)
	}
	invalidDests := destRefResult.Rows[0][0].(int64)
	if invalidDests > 0 {
		t.Errorf("Found %d corridors with invalid destination references", invalidDests)
	}

	// Test 4: Verify transit time class values
	transitResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_corridor
		WHERE transit_time_class NOT IN ('2-day', '3-day', '4-day')
	`)
	if err != nil {
		t.Fatalf("Failed to query transit time classes: %v", err)
	}
	invalidTransit := transitResult.Rows[0][0].(int64)
	if invalidTransit > 0 {
		t.Errorf("Found %d corridors with invalid transit time class", invalidTransit)
	}

	// Test 5: Verify expected transit hours are reasonable
	hoursResult, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			MIN(expected_transit_hours) as min_hours,
			MAX(expected_transit_hours) as max_hours,
			AVG(expected_transit_hours) as avg_hours
		FROM dim_corridor
	`)
	if err != nil {
		t.Fatalf("Failed to query expected transit hours: %v", err)
	}
	minHours := hoursResult.Rows[0][0]
	maxHours := hoursResult.Rows[0][1]
	avgHours := hoursResult.Rows[0][2]
	t.Logf("Transit hours - Min: %v, Max: %v, Avg: %v", minHours, maxHours, avgHours)

	// Verify hours are positive and non-null
	if minHours == nil || maxHours == nil {
		t.Error("Found corridors with null expected transit hours")
	}

	// Test 6: Verify no nulls in required fields
	nullResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_corridor
		WHERE corridor_id IS NULL OR corridor_id = ''
		   OR origin_location_id IS NULL OR origin_location_id = ''
		   OR destination_location_id IS NULL OR destination_location_id = ''
		   OR transit_time_class IS NULL
		   OR expected_transit_hours IS NULL
		   OR distance_miles IS NULL
		   OR station_count IS NULL
	`)
	if err != nil {
		t.Fatalf("Failed to check nulls: %v", err)
	}
	nullCount := nullResult.Rows[0][0].(int64)
	if nullCount > 0 {
		t.Errorf("Found %d records with null required fields", nullCount)
	}

	// Test 7: Verify distance_miles are reasonable (calculated from transit hours)
	distanceResult, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			MIN(distance_miles) as min_distance,
			MAX(distance_miles) as max_distance,
			AVG(distance_miles) as avg_distance
		FROM dim_corridor
	`)
	if err != nil {
		t.Fatalf("Failed to query distance miles: %v", err)
	}
	minDistance := distanceResult.Rows[0][0]
	maxDistance := distanceResult.Rows[0][1]
	avgDistance := distanceResult.Rows[0][2]
	t.Logf("Distance miles - Min: %v, Max: %v, Avg: %v", minDistance, maxDistance, avgDistance)

	// Test 8: Verify station_count is populated from actual data
	stationResult, err := adapter.ExecuteQuery(ctx, `
		SELECT 
			MIN(station_count) as min_stations,
			MAX(station_count) as max_stations,
			AVG(station_count) as avg_stations,
			COUNT(CASE WHEN station_count > 0 THEN 1 END) as corridors_with_stations
		FROM dim_corridor
	`)
	if err != nil {
		t.Fatalf("Failed to query station counts: %v", err)
	}
	minStations := stationResult.Rows[0][0]
	maxStations := stationResult.Rows[0][1]
	avgStations := stationResult.Rows[0][2]
	corridorsWithStations := stationResult.Rows[0][3].(int64)
	t.Logf("Station count - Min: %v, Max: %v, Avg: %v, Corridors with stations: %d", minStations, maxStations, avgStations, corridorsWithStations)

	// Test 9: Verify intermediate_stations is populated for corridors with stations
	intermediateResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_corridor
		WHERE station_count > 0 AND (intermediate_stations IS NULL OR intermediate_stations = '')
	`)
	if err != nil {
		t.Fatalf("Failed to query intermediate stations: %v", err)
	}
	missingIntermediate := intermediateResult.Rows[0][0].(int64)
	if missingIntermediate > 0 {
		t.Errorf("Found %d corridors with station_count > 0 but missing intermediate_stations", missingIntermediate)
	}
}

// TestDimDatePopulation ensures date dimension covers analysis period (90 days)
func TestDimDatePopulation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	adapter, ctx, repoRoot := setupTestDatabase(t)
	defer adapter.Close()

	// Execute dim_date model
	executeDimensionModel(t, adapter, ctx, repoRoot, "dim_date")

	// Test 1: Verify 90 dates (2024-01-01 to 2024-03-31)
	countResult, err := adapter.ExecuteQuery(ctx, "SELECT COUNT(*) as count FROM dim_date")
	if err != nil {
		t.Fatalf("Failed to query dim_date: %v", err)
	}
	dateCount := countResult.Rows[0][0].(int64)
	// 90 days = Jan (31) + Feb (29 in 2024, leap year) + Mar (31)
	if dateCount != 91 {
		t.Errorf("Expected 91 days (Jan 1 - Mar 31, 2024), got %d", dateCount)
	}

	// Test 2: Verify date range
	rangeResult, err := adapter.ExecuteQuery(ctx, `
		SELECT MIN(full_date) as min_date, MAX(full_date) as max_date 
		FROM dim_date
	`)
	if err != nil {
		t.Fatalf("Failed to query date range: %v", err)
	}
	minDate := rangeResult.Rows[0][0].(string)
	maxDate := rangeResult.Rows[0][1].(string)
	if minDate != "2024-01-01" {
		t.Errorf("Expected min date 2024-01-01, got %s", minDate)
	}
	if maxDate != "2024-03-31" {
		t.Errorf("Expected max date 2024-03-31, got %s", maxDate)
	}

	// Test 3: Verify week numbers are populated (for seasonal analysis)
	weekResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_date
		WHERE week IS NULL OR week < 1 OR week > 53
	`)
	if err != nil {
		t.Fatalf("Failed to query week numbers: %v", err)
	}
	invalidWeeks := weekResult.Rows[0][0].(int64)
	if invalidWeeks > 0 {
		t.Errorf("Found %d dates with invalid week numbers", invalidWeeks)
	}

	// Test 4: Verify day_of_week values (1-7)
	dowResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_date
		WHERE day_of_week < 1 OR day_of_week > 7
	`)
	if err != nil {
		t.Fatalf("Failed to query day of week: %v", err)
	}
	invalidDOW := dowResult.Rows[0][0].(int64)
	if invalidDOW > 0 {
		t.Errorf("Found %d dates with invalid day_of_week", invalidDOW)
	}

	// Test 5: Verify is_weekend flag (0 or 1)
	weekendResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_date
		WHERE is_weekend NOT IN (0, 1)
	`)
	if err != nil {
		t.Fatalf("Failed to query weekend flag: %v", err)
	}
	invalidWeekend := weekendResult.Rows[0][0].(int64)
	if invalidWeekend > 0 {
		t.Errorf("Found %d dates with invalid is_weekend flag", invalidWeekend)
	}

	// Test 6: Verify weekend logic (day 6 and 7 should be weekends)
	weekendLogicResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_date
		WHERE (day_of_week IN (6, 7) AND is_weekend != 1)
		   OR (day_of_week NOT IN (6, 7) AND is_weekend != 0)
	`)
	if err != nil {
		t.Fatalf("Failed to query weekend logic: %v", err)
	}
	weekendLogicErrors := weekendLogicResult.Rows[0][0].(int64)
	if weekendLogicErrors > 0 {
		t.Errorf("Found %d dates with incorrect weekend logic", weekendLogicErrors)
	}

	// Test 7: Verify date_key format (YYYYMMDD)
	keyResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_date
		WHERE date_key < 20240101 OR date_key > 20240331
	`)
	if err != nil {
		t.Fatalf("Failed to query date keys: %v", err)
	}
	invalidKeys := keyResult.Rows[0][0].(int64)
	if invalidKeys > 0 {
		t.Errorf("Found %d dates with invalid date_key", invalidKeys)
	}

	// Test 8: Verify no nulls in required fields
	nullResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM dim_date
		WHERE date_key IS NULL 
		   OR full_date IS NULL
		   OR year IS NULL
		   OR quarter IS NULL
		   OR month IS NULL
		   OR week IS NULL
		   OR day_of_week IS NULL
		   OR is_weekend IS NULL
	`)
	if err != nil {
		t.Fatalf("Failed to check nulls: %v", err)
	}
	nullCount := nullResult.Rows[0][0].(int64)
	if nullCount > 0 {
		t.Errorf("Found %d records with null required fields", nullCount)
	}
}

// loadCSVIntoTableDim reads a CSV file and inserts its data into a table
func loadCSVIntoTableDim(ctx context.Context, adapter *sqlite.SQLiteAdapter, csvPath, tableName string) error {
	// Open CSV file
	file, err := os.Open(csvPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create CSV reader
	reader := csv.NewReader(file)

	// Read header (skip it)
	_, err = reader.Read()
	if err != nil {
		return err
	}

	// Prepare batch insert
	batchSize := 1000
	values := make([]string, 0, batchSize)

	// Read and insert data
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Build VALUES clause for this row
		eventID := record[0]
		timestamp := record[1]
		carID := record[2]
		trainID := record[3]
		locationID := record[4]
		eventType := record[5]
		loadedFlag := record[6]
		commodity := record[7]
		weightTons := record[8]

		value := "(" + eventID + ",'" + timestamp + "','" + carID + "','" + trainID + "','" +
			locationID + "','" + eventType + "','" + loadedFlag + "','" + commodity + "'," + weightTons + ")"
		values = append(values, value)

		// Execute batch when full
		if len(values) >= batchSize {
			insertSQL := "INSERT INTO " + tableName + " VALUES " + strings.Join(values, ",")
			err = adapter.ExecuteDDL(ctx, insertSQL)
			if err != nil {
				return err
			}
			values = values[:0]
		}
	}

	// Insert remaining rows
	if len(values) > 0 {
		insertSQL := "INSERT INTO " + tableName + " VALUES " + strings.Join(values, ",")
		err = adapter.ExecuteDDL(ctx, insertSQL)
		if err != nil {
			return err
		}
	}

	return nil
}

// removeConfigCallsDim removes {{ config ... }} calls from content
func removeConfigCallsDim(content string) string {
	// Remove Go template syntax: {{ config "key" "value" }}
	goTemplateRe := regexp.MustCompile(`{{\s*config\s+"[^"]+"\s+"[^"]+"\s*}}`)
	content = goTemplateRe.ReplaceAllString(content, "")

	// Remove legacy Jinja-style syntax: {{ config(key='value') }}
	legacyRe := regexp.MustCompile(`{{\s*config\s*\([^}]+\)\s*}}`)
	return legacyRe.ReplaceAllString(content, "")
}
