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

// TestStagingTableLoad validates raw CLM CSV data loads correctly into staging table
func TestStagingTableLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Create temporary database
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create SQLite adapter
	config := &platform.ConnectionConfig{
		DatabasePath: dbPath,
	}
	adapter := sqlite.NewSQLiteAdapter(config)

	ctx := context.Background()
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer adapter.Close()

	// Load seed CSV data
	repoRoot := getRepoRoot(t)
	seedPath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "seeds", "raw_clm_events.csv")

	if _, err := os.Stat(seedPath); os.IsNotExist(err) {
		t.Fatalf("Seed file not found: %s", seedPath)
	}

	// Create raw seed table
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

	// Load CSV data using helper function
	err = loadCSVIntoTable(ctx, adapter, seedPath, "raw_clm_events")
	if err != nil {
		t.Fatalf("Failed to load CSV data: %v", err)
	}

	// Verify raw data loaded
	countResult, err := adapter.ExecuteQuery(ctx, "SELECT COUNT(*) as count FROM raw_clm_events")
	if err != nil {
		t.Fatalf("Failed to query raw table: %v", err)
	}

	rawCount := countResult.Rows[0][0].(int64)
	if rawCount != 125926 {
		t.Errorf("Expected 125926 raw records, got %d", rawCount)
	}

	// Load and execute staging model
	stagingModelPath := filepath.Join(repoRoot, "examples", "unit_train_analytics", "models", "staging", "stg_clm_events.sql")
	if _, err := os.Stat(stagingModelPath); os.IsNotExist(err) {
		t.Fatalf("Staging model not found: %s", stagingModelPath)
	}

	content, err := os.ReadFile(stagingModelPath)
	if err != nil {
		t.Fatalf("Failed to read staging model: %v", err)
	}

	// Remove config calls before parsing
	contentStr := removeConfigCallsStaging(string(content))

	// Parse and render template
	templateEngine := template.New()
	tmpl, err := templateEngine.Parse("stg_clm_events", contentStr)
	if err != nil {
		t.Fatalf("Failed to parse template: %v", err)
	}

	ctx2 := template.NewContext(template.WithCurrentModel("stg_clm_events"))
	ctx2.Seeds = map[string]string{
		"raw_clm_events": "raw_clm_events",
	}
	rendered, err := template.Render(tmpl, ctx2, nil)
	if err != nil {
		t.Fatalf("Failed to render template: %v", err)
	}

	// Create staging view/table
	err = adapter.ExecuteDDL(ctx, "DROP TABLE IF EXISTS stg_clm_events")
	if err != nil {
		t.Fatalf("Failed to drop existing staging table: %v", err)
	}

	err = adapter.ExecuteDDL(ctx, "CREATE TABLE stg_clm_events AS "+rendered)
	if err != nil {
		t.Fatalf("Failed to execute staging model: %v", err)
	}

	// Verify staging table created and populated
	stgCountResult, err := adapter.ExecuteQuery(ctx, "SELECT COUNT(*) as count FROM stg_clm_events")
	if err != nil {
		t.Fatalf("Failed to query staging table: %v", err)
	}

	stgCount := stgCountResult.Rows[0][0].(int64)
	if stgCount != rawCount {
		t.Errorf("Staging table count mismatch: expected %d, got %d", rawCount, stgCount)
	}
}

// TestCSVParsingLogic validates CSV parsing handles all expected formats
func TestCSVParsingLogic(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Create temporary database
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
	defer adapter.Close()

	// Load seed data
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
		t.Fatalf("Failed to create table: %v", err)
	}

	err = loadCSVIntoTable(ctx, adapter, seedPath, "raw_clm_events")
	if err != nil {
		t.Fatalf("Failed to load CSV: %v", err)
	}

	// Test 1: Verify timestamp parsing
	timestampResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM raw_clm_events 
		WHERE event_timestamp NOT LIKE '____-__-__ __:__:__'
	`)
	if err != nil {
		t.Fatalf("Failed to query timestamps: %v", err)
	}
	invalidTimestamps := timestampResult.Rows[0][0].(int64)
	if invalidTimestamps > 0 {
		t.Errorf("Found %d invalid timestamp formats", invalidTimestamps)
	}

	// Test 2: Verify boolean parsing (loaded_flag should be 'true' or 'false')
	boolResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM raw_clm_events 
		WHERE loaded_flag NOT IN ('true', 'false')
	`)
	if err != nil {
		t.Fatalf("Failed to query boolean flags: %v", err)
	}
	invalidBools := boolResult.Rows[0][0].(int64)
	if invalidBools > 0 {
		t.Errorf("Found %d invalid boolean values", invalidBools)
	}

	// Test 3: Verify no nulls in required fields (except train_id which can be empty for stragglers)
	nullCheckFields := []string{"car_id", "location_id", "event_type", "commodity"}
	for _, field := range nullCheckFields {
		nullResult, err := adapter.ExecuteQuery(ctx, "SELECT COUNT(*) as count FROM raw_clm_events WHERE "+field+" IS NULL OR "+field+" = ''")
		if err != nil {
			t.Fatalf("Failed to check nulls for %s: %v", field, err)
		}
		nullCount := nullResult.Rows[0][0].(int64)
		if nullCount > 0 {
			t.Errorf("Found %d null/empty values in %s", nullCount, field)
		}
	}

	// Test 4: Verify numeric ranges
	weightResult, err := adapter.ExecuteQuery(ctx, `
		SELECT COUNT(*) as count 
		FROM raw_clm_events 
		WHERE weight_tons < 0 OR weight_tons > 200
	`)
	if err != nil {
		t.Fatalf("Failed to check weight range: %v", err)
	}
	invalidWeights := weightResult.Rows[0][0].(int64)
	if invalidWeights > 0 {
		t.Errorf("Found %d weights outside valid range (0-200 tons)", invalidWeights)
	}
}

// loadCSVIntoTable reads a CSV file and inserts its data into a table
func loadCSVIntoTable(ctx context.Context, adapter *sqlite.SQLiteAdapter, csvPath, tableName string) error {
	// Open CSV file
	file, err := os.Open(csvPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create CSV reader
	reader := csv.NewReader(file)

	// Read header (skip it as we create table with proper column names)
	_, err = reader.Read()
	if err != nil {
		return err
	}

	// Prepare batch insert
	batchSize := 1000
	values := make([]string, 0, batchSize)
	count := 0

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
		// Record format: event_id, event_timestamp, car_id, train_id, location_id, event_type, loaded_flag, commodity, weight_tons
		eventID := record[0]
		timestamp := record[1]
		carID := record[2]
		trainID := record[3]
		locationID := record[4]
		eventType := record[5]
		loadedFlag := record[6]
		commodity := record[7]
		weightTons := record[8]

		// Escape strings
		value := "(" + eventID + ",'" + timestamp + "','" + carID + "','" + trainID + "','" +
			locationID + "','" + eventType + "','" + loadedFlag + "','" + commodity + "'," + weightTons + ")"
		values = append(values, value)
		count++

		// Execute batch when full
		if len(values) >= batchSize {
			insertSQL := "INSERT INTO " + tableName + " VALUES " + strings.Join(values, ",")
			err = adapter.ExecuteDDL(ctx, insertSQL)
			if err != nil {
				return err
			}
			values = values[:0] // Clear slice
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

// removeConfigCallsStaging removes {{ config ... }} calls from content
func removeConfigCallsStaging(content string) string {
	// Remove Go template syntax: {{ config "key" "value" }}
	goTemplateRe := regexp.MustCompile(`{{\s*config\s+"[^"]+"\s+"[^"]+"\s*}}`)
	content = goTemplateRe.ReplaceAllString(content, "")

	// Remove legacy Jinja-style syntax: {{ config(key='value') }}
	legacyRe := regexp.MustCompile(`{{\s*config\s*\([^}]+\)\s*}}`)
	return legacyRe.ReplaceAllString(content, "")
}
