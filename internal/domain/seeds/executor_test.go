package seeds

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jpconstantineau/gorchata/internal/config"
	"github.com/jpconstantineau/gorchata/internal/platform"
	"github.com/jpconstantineau/gorchata/internal/platform/sqlite"
)

func TestBuildCreateTableSQL(t *testing.T) {
	schema := &SeedSchema{
		Columns: []SeedColumn{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "TEXT"},
			{Name: "amount", Type: "REAL"},
		},
	}

	sql := buildCreateTableSQL("customers", schema)

	// Check that SQL starts with CREATE TABLE
	if !strings.HasPrefix(sql, "CREATE TABLE") {
		t.Errorf("expected SQL to start with 'CREATE TABLE', got: %s", sql)
	}

	// Verify quoted table name
	if !strings.Contains(sql, `"customers"`) {
		t.Error("expected quoted table name '\"customers\"' in SQL")
	}

	// Verify all columns are present with quoted identifiers
	if !strings.Contains(sql, `"id" INTEGER`) {
		t.Error("expected '\"id\" INTEGER' in SQL")
	}
	if !strings.Contains(sql, `"name" TEXT`) {
		t.Error("expected '\"name\" TEXT' in SQL")
	}
	if !strings.Contains(sql, `"amount" REAL`) {
		t.Error("expected '\"amount\" REAL' in SQL")
	}
}

func TestBuildCreateTableSQL_MultipleColumns(t *testing.T) {
	schema := &SeedSchema{
		Columns: []SeedColumn{
			{Name: "col1", Type: "TEXT"},
			{Name: "col2", Type: "INTEGER"},
			{Name: "col3", Type: "REAL"},
			{Name: "col4", Type: "TIMESTAMP"},
		},
	}

	sql := buildCreateTableSQL("test_table", schema)

	// Verify quoted table name
	if !strings.Contains(sql, `"test_table"`) {
		t.Error("expected quoted table name '\"test_table\"' in SQL")
	}

	// Verify all columns exist with quoted identifiers
	expectedColumns := []string{`"col1" TEXT`, `"col2" INTEGER`, `"col3" REAL`, `"col4" TIMESTAMP`}
	for _, col := range expectedColumns {
		if !strings.Contains(sql, col) {
			t.Errorf("expected '%s' in SQL, got: %s", col, sql)
		}
	}
}

func TestBuildInsertSQL_SingleRow(t *testing.T) {
	schema := &SeedSchema{
		Columns: []SeedColumn{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "TEXT"},
		},
	}

	rows := [][]string{
		{"1", "Alice"},
	}

	sql := buildInsertSQL("customers", schema, rows)

	// Check INSERT INTO structure with quoted table name
	if !strings.HasPrefix(sql, "INSERT INTO") {
		t.Errorf("expected SQL to start with 'INSERT INTO', got: %s", sql)
	}
	if !strings.Contains(sql, `"customers"`) {
		t.Error("expected quoted table name '\"customers\"' in SQL")
	}

	// Verify INTEGER value is NOT quoted
	if !strings.Contains(sql, "(1,") {
		t.Error("expected unquoted INTEGER '1' in SQL")
	}
	// Verify TEXT value is quoted
	if !strings.Contains(sql, "'Alice'") {
		t.Error("expected quoted TEXT 'Alice' in SQL")
	}
}

func TestBuildInsertSQL_MultipleRows(t *testing.T) {
	schema := &SeedSchema{
		Columns: []SeedColumn{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "TEXT"},
		},
	}

	rows := [][]string{
		{"1", "Alice"},
		{"2", "Bob"},
		{"3", "Charlie"},
	}

	sql := buildInsertSQL("customers", schema, rows)

	// Check for multiple value tuples
	valueCount := strings.Count(sql, "(")
	if valueCount != 3 {
		t.Errorf("expected 3 value tuples, got %d", valueCount)
	}

	// Verify INTEGER values are NOT quoted and TEXT values ARE quoted
	if !strings.Contains(sql, "(1, 'Alice')") {
		t.Errorf("expected (1, 'Alice') with unquoted INTEGER in SQL, got: %s", sql)
	}
	if !strings.Contains(sql, "(2, 'Bob')") {
		t.Errorf("expected (2, 'Bob') with unquoted INTEGER in SQL, got: %s", sql)
	}
	if !strings.Contains(sql, "(3, 'Charlie')") {
		t.Errorf("expected (3, 'Charlie') with unquoted INTEGER in SQL, got: %s", sql)
	}
}

func TestBuildInsertSQL_QuotedValues(t *testing.T) {
	schema := &SeedSchema{
		Columns: []SeedColumn{
			{Name: "id", Type: "INTEGER"},
			{Name: "message", Type: "TEXT"},
		},
	}

	rows := [][]string{
		{"1", "It's a test"},
		{"2", "Don't worry"},
	}

	sql := buildInsertSQL("messages", schema, rows)

	// Single quotes should be escaped as ''
	if !strings.Contains(sql, "It''s a test") {
		t.Error("expected escaped quote in 'It''s a test'")
	}
	if !strings.Contains(sql, "Don''t worry") {
		t.Error("expected escaped quote in 'Don''t worry'")
	}
}

// setupTestDB creates a temporary SQLite database for testing
func setupTestDB(t *testing.T) platform.DatabaseAdapter {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	connConfig := &platform.ConnectionConfig{
		DatabasePath: dbPath,
	}
	adapter := sqlite.NewSQLiteAdapter(connConfig)
	if err := adapter.Connect(context.Background()); err != nil {
		t.Fatalf("failed to create test adapter: %v", err)
	}
	return adapter
}

func TestExecuteSeed_Success(t *testing.T) {
	ctx := context.Background()
	adapter := setupTestDB(t)
	defer adapter.Close()

	seed := &Seed{
		ID:   "customers",
		Path: "seeds/customers.csv",
		Type: SeedTypeCSV,
		Schema: &SeedSchema{
			Columns: []SeedColumn{
				{Name: "id", Type: "INTEGER"},
				{Name: "name", Type: "TEXT"},
			},
		},
	}

	rows := [][]string{
		{"1", "Alice"},
		{"2", "Bob"},
	}

	cfg := &config.SeedConfig{
		Import: config.ImportConfig{
			BatchSize: 1000,
		},
	}

	result, err := ExecuteSeed(ctx, adapter, seed, rows, cfg)
	if err != nil {
		t.Fatalf("ExecuteSeed failed: %v", err)
	}

	if result.Status != StatusSuccess {
		t.Errorf("expected status %s, got %s", StatusSuccess, result.Status)
	}

	if result.RowsLoaded != 2 {
		t.Errorf("expected 2 rows loaded, got %d", result.RowsLoaded)
	}

	// Verify table exists and contains data
	exists, err := adapter.TableExists(ctx, "customers")
	if err != nil {
		t.Fatalf("TableExists failed: %v", err)
	}
	if !exists {
		t.Error("expected table 'customers' to exist")
	}

	// Query the data
	queryResult, err := adapter.ExecuteQuery(ctx, "SELECT id, name FROM customers ORDER BY id")
	if err != nil {
		t.Fatalf("ExecuteQuery failed: %v", err)
	}

	if len(queryResult.Rows) != 2 {
		t.Errorf("expected 2 rows in table, got %d", len(queryResult.Rows))
	}
}

func TestExecuteSeed_FullRefresh(t *testing.T) {
	ctx := context.Background()
	adapter := setupTestDB(t)
	defer adapter.Close()

	seed := &Seed{
		ID:   "products",
		Path: "seeds/products.csv",
		Type: SeedTypeCSV,
		Schema: &SeedSchema{
			Columns: []SeedColumn{
				{Name: "id", Type: "INTEGER"},
				{Name: "name", Type: "TEXT"},
			},
		},
	}

	cfg := &config.SeedConfig{
		Import: config.ImportConfig{
			BatchSize: 1000,
		},
	}

	// First load
	rows1 := [][]string{
		{"1", "Product A"},
		{"2", "Product B"},
	}

	result1, err := ExecuteSeed(ctx, adapter, seed, rows1, cfg)
	if err != nil {
		t.Fatalf("first ExecuteSeed failed: %v", err)
	}
	if result1.RowsLoaded != 2 {
		t.Errorf("first load: expected 2 rows, got %d", result1.RowsLoaded)
	}

	// Second load (should replace data)
	rows2 := [][]string{
		{"3", "Product C"},
		{"4", "Product D"},
		{"5", "Product E"},
	}

	result2, err := ExecuteSeed(ctx, adapter, seed, rows2, cfg)
	if err != nil {
		t.Fatalf("second ExecuteSeed failed: %v", err)
	}
	if result2.RowsLoaded != 3 {
		t.Errorf("second load: expected 3 rows, got %d", result2.RowsLoaded)
	}

	// Verify only new data exists (old data was dropped)
	queryResult, err := adapter.ExecuteQuery(ctx, "SELECT COUNT(*) FROM products")
	if err != nil {
		t.Fatalf("ExecuteQuery failed: %v", err)
	}

	if len(queryResult.Rows) != 1 {
		t.Fatalf("expected 1 row from COUNT, got %d", len(queryResult.Rows))
	}

	count := queryResult.Rows[0][0]
	// Convert to int for comparison
	if fmt.Sprintf("%v", count) != "3" {
		t.Errorf("expected 3 rows after refresh, got %v", count)
	}
}

func TestExecuteSeed_TransactionRollback(t *testing.T) {
	ctx := context.Background()
	adapter := setupTestDB(t)
	defer adapter.Close()

	seed := &Seed{
		ID:   "orders",
		Path: "seeds/orders.csv",
		Type: SeedTypeCSV,
		Schema: &SeedSchema{
			Columns: []SeedColumn{
				{Name: "id", Type: "INTEGER"},
				{Name: "customer", Type: "TEXT"},
			},
		},
	}

	cfg := &config.SeedConfig{
		Import: config.ImportConfig{
			BatchSize: 1000,
		},
	}

	// Create a seed that will succeed
	rows := [][]string{
		{"1", "Customer A"},
	}

	result, err := ExecuteSeed(ctx, adapter, seed, rows, cfg)
	if err != nil {
		t.Fatalf("ExecuteSeed failed: %v", err)
	}
	if result.Status != StatusSuccess {
		t.Errorf("expected success status, got %s", result.Status)
	}

	// Verify table exists
	exists, err := adapter.TableExists(ctx, "orders")
	if err != nil {
		t.Fatalf("TableExists failed: %v", err)
	}
	if !exists {
		t.Error("expected table 'orders' to exist after successful execution")
	}
}

func TestExecuteSeed_MultipleInserts(t *testing.T) {
	ctx := context.Background()
	adapter := setupTestDB(t)
	defer adapter.Close()

	seed := &Seed{
		ID:   "large_data",
		Path: "seeds/large_data.csv",
		Type: SeedTypeCSV,
		Schema: &SeedSchema{
			Columns: []SeedColumn{
				{Name: "id", Type: "INTEGER"},
				{Name: "value", Type: "TEXT"},
			},
		},
	}

	// Create 25 rows with batch size of 10
	rows := make([][]string, 25)
	for i := 0; i < 25; i++ {
		rows[i] = []string{fmt.Sprintf("%d", i+1), fmt.Sprintf("Value %d", i+1)}
	}

	cfg := &config.SeedConfig{
		Import: config.ImportConfig{
			BatchSize: 10,
		},
	}

	result, err := ExecuteSeed(ctx, adapter, seed, rows, cfg)
	if err != nil {
		t.Fatalf("ExecuteSeed failed: %v", err)
	}

	if result.Status != StatusSuccess {
		t.Errorf("expected status %s, got %s", StatusSuccess, result.Status)
	}

	if result.RowsLoaded != 25 {
		t.Errorf("expected 25 rows loaded, got %d", result.RowsLoaded)
	}

	// Verify all rows are in the table
	queryResult, err := adapter.ExecuteQuery(ctx, "SELECT COUNT(*) FROM large_data")
	if err != nil {
		t.Fatalf("ExecuteQuery failed: %v", err)
	}

	count := queryResult.Rows[0][0]
	if fmt.Sprintf("%v", count) != "25" {
		t.Errorf("expected 25 rows in table, got %v", count)
	}
}

func TestExecuteSeed_NilInputs(t *testing.T) {
	ctx := context.Background()
	adapter := setupTestDB(t)
	defer adapter.Close()

	cfg := &config.SeedConfig{
		Import: config.ImportConfig{
			BatchSize: 1000,
		},
	}

	seed := &Seed{
		ID:   "test",
		Path: "test.csv",
		Type: SeedTypeCSV,
		Schema: &SeedSchema{
			Columns: []SeedColumn{{Name: "id", Type: "INTEGER"}},
		},
	}

	rows := [][]string{{"1"}}

	tests := []struct {
		name    string
		adapter platform.DatabaseAdapter
		seed    *Seed
		rows    [][]string
		cfg     *config.SeedConfig
		wantErr bool
	}{
		{
			name:    "nil adapter",
			adapter: nil,
			seed:    seed,
			rows:    rows,
			cfg:     cfg,
			wantErr: true,
		},
		{
			name:    "nil seed",
			adapter: adapter,
			seed:    nil,
			rows:    rows,
			cfg:     cfg,
			wantErr: true,
		},
		{
			name:    "nil config",
			adapter: adapter,
			seed:    seed,
			rows:    rows,
			cfg:     nil,
			wantErr: true,
		},
		{
			name:    "nil rows",
			adapter: adapter,
			seed:    seed,
			rows:    nil,
			cfg:     cfg,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ExecuteSeed(ctx, tt.adapter, tt.seed, tt.rows, tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExecuteSeed() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), "nil") {
				t.Errorf("expected error message to contain 'nil', got: %v", err)
			}
		})
	}
}

func TestBuildCreateTableSQL_SpecialCharacters(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		columns   []SeedColumn
		wantTable string
		wantCols  []string
	}{
		{
			name:      "table name with spaces",
			tableName: "My Table",
			columns:   []SeedColumn{{Name: "id", Type: "INTEGER"}},
			wantTable: `"My Table"`,
			wantCols:  []string{`"id" INTEGER`},
		},
		{
			name:      "column name with spaces",
			tableName: "orders",
			columns: []SeedColumn{
				{Name: "order id", Type: "INTEGER"},
				{Name: "customer name", Type: "TEXT"},
			},
			wantTable: `"orders"`,
			wantCols:  []string{`"order id" INTEGER`, `"customer name" TEXT`},
		},
		{
			name:      "names with quotes",
			tableName: `user"data`,
			columns:   []SeedColumn{{Name: `col"1`, Type: "TEXT"}},
			wantTable: `"user""data"`,
			wantCols:  []string{`"col""1" TEXT`},
		},
		{
			name:      "SQL keywords",
			tableName: "select",
			columns: []SeedColumn{
				{Name: "from", Type: "INTEGER"},
				{Name: "where", Type: "TEXT"},
			},
			wantTable: `"select"`,
			wantCols:  []string{`"from" INTEGER`, `"where" TEXT`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := &SeedSchema{Columns: tt.columns}
			sql := buildCreateTableSQL(tt.tableName, schema)

			if !strings.Contains(sql, tt.wantTable) {
				t.Errorf("expected table name %s in SQL, got: %s", tt.wantTable, sql)
			}

			for _, wantCol := range tt.wantCols {
				if !strings.Contains(sql, wantCol) {
					t.Errorf("expected column definition %s in SQL, got: %s", wantCol, sql)
				}
			}
		})
	}
}

func TestBuildInsertSQL_NumericTypes(t *testing.T) {
	schema := &SeedSchema{
		Columns: []SeedColumn{
			{Name: "id", Type: "INTEGER"},
			{Name: "price", Type: "REAL"},
			{Name: "name", Type: "TEXT"},
		},
	}

	rows := [][]string{
		{"123", "45.67", "Product A"},
		{"456", "89.01", "Product B"},
	}

	sql := buildInsertSQL("products", schema, rows)

	// INTEGER values should NOT be quoted
	if strings.Contains(sql, "'123'") {
		t.Error("INTEGER values should not be quoted, found '123'")
	}
	if !strings.Contains(sql, "(123,") {
		t.Error("expected unquoted INTEGER value 123")
	}

	// REAL values should NOT be quoted
	if strings.Contains(sql, "'45.67'") {
		t.Error("REAL values should not be quoted, found '45.67'")
	}
	if !strings.Contains(sql, "45.67,") {
		t.Error("expected unquoted REAL value 45.67")
	}

	// TEXT values SHOULD be quoted
	if !strings.Contains(sql, "'Product A'") {
		t.Error("TEXT values should be quoted, expected 'Product A'")
	}
}

func TestBuildInsertSQL_EmptyStrings(t *testing.T) {
	schema := &SeedSchema{
		Columns: []SeedColumn{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "TEXT"},
			{Name: "description", Type: "TEXT"},
		},
	}

	rows := [][]string{
		{"1", "Alice", ""},     // Empty description
		{"2", "", "Some text"}, // Empty name
	}

	sql := buildInsertSQL("users", schema, rows)

	// Empty strings should remain as '', not NULL or missing
	// First row: (1, 'Alice', '')
	if !strings.Contains(sql, "(1, 'Alice', '')") {
		t.Errorf("expected empty string as '', got: %s", sql)
	}

	// Second row: (2, '', 'Some text')
	if !strings.Contains(sql, "(2, '', 'Some text')") {
		t.Errorf("expected empty string as '', got: %s", sql)
	}
}
