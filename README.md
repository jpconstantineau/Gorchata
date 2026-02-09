# Gorchata 🌶️

**Gorchata** is a SQL-first data transformation tool inspired by dbt, designed for Go developers who want a lightweight, dependency-free solution for managing data transformations.

## Why Gorchata?

- **SQL-First**: Write your transformations in SQL, the language of data
- **Go-Powered**: Fast, compiled binary with no runtime dependencies
- **Zero CGO**: Pure Go implementation with SQLite (no C dependencies)
- **dbt-Compatible**: Familiar project structure and concepts for dbt users
- **Lightweight**: Single binary, minimal footprint
- **Cross-Platform**: Works on Windows, Linux, and macOS

## Features

- 📊 **SQL-first data transformations** - Write models as SQL SELECT statements
- 🔄 **Dependency resolution** - Automatic DAG-based execution order
- 🎯 **Incremental models** - Efficient updates for large datasets
- 🗃️ **Multiple materializations** - Table, view, incremental
- 📝 **Go text/template engine** - dbt-compatible functions: `{{ ref "..." }}`, `{{ source "..." "..." }}`, variables, conditionals
- ✅ **Data quality testing** - 14 built-in tests + custom SQL tests
- 🌱 **Seeds** - Load CSV/SQL data files with schema inference and overrides
- 💾 **Pure Go SQLite** - No CGO dependencies, cross-platform
- ⚙️ **Profile management** - Multiple environments (dev, prod)

## Requirements

- **Go 1.25+** (for building from source)
- Terminal / Command Prompt

## Installation

### Option 1: Install via go install

```bash
go install github.com/jpconstantineau/gorchata/cmd/gorchata@latest
```

### Option 2: Build from Source

```bash
git clone https://github.com/jpconstantineau/gorchata.git
cd gorchata
scripts/build.ps1 -Task build
```

The binary will be created in `bin/gorchata.exe` (Windows) or `bin/gorchata` (Unix).

## Quick Start

### 1. Initialize a New Project

```bash
gorchata init my_project
cd my_project
```

This creates a new Gorchata project with the following structure:
```
my_project/
├── gorchata_project.yml    # Project configuration
├── profiles.yml            # Connection profiles
├── models/                 # SQL transformation models
│   ├── stg_users.sql      # Sample staging model
│   ├── stg_orders.sql     # Sample staging model
│   └── fct_order_summary.sql  # Sample fact table
├── seeds/                  # CSV/SQL seed data files
├── tests/                  # Data quality tests
└── macros/                 # Reusable SQL macros (planned feature)
```

**Init Command Options:**

```bash
# Initialize with sample models (default)
gorchata init my_project

# Initialize empty project without sample models
gorchata init my_project --empty

# Force initialization even if directory exists
gorchata init my_project --force

# Show help
gorchata init --help
```

### 2. Configure Your Project

**gorchata_project.yml** (project root):
```yaml
name: my_project
version: 1.0.0
profile: dev

model-paths:
  - models
seed-paths:
  - seeds
test-paths:
  - tests
macro-paths:
  - macros
```

**profiles.yml** (typically `~/.gorchata/profiles.yml` or project root):
```yaml
default:
  target: dev
  outputs:
    dev:
      type: sqlite
      database: ${GORCHATA_DB_PATH:./gorchata.db}
    prod:
      type: sqlite
      database: ${GORCHATA_PROD_DB:/data/gorchata_prod.db}
```

**Environment Variables in profiles.yml:**
- `${VAR}`: Required variable - error if not set
- `${VAR:default}`: Optional variable with default value

Example environment variable usage:
```bash
# Set database path via environment variable
export GORCHATA_DB_PATH=/custom/path/to/db.db

# Run gorchata (will use the env var)
gorchata run
```

### 3. Create Your First Model

Models are SQL files with optional configuration and template functions.

**models/staging/stg_users.sql**:
```sql
-- Staging model for users
{{ config "materialized" "view" }}

SELECT
    id,
    name,
    email,
    created_at
FROM raw_users
WHERE deleted_at IS NULL
```

**models/marts/fct_orders.sql**:
```sql
-- Fact table combining users and orders
{{ config "materialized" "table" }}

SELECT
    u.id as user_id,
    u.name as user_name,
    COUNT(o.id) as order_count,
    SUM(o.amount) as total_amount
FROM {{ ref "stg_users" }} u
LEFT JOIN {{ ref "stg_orders" }} o ON u.id = o.user_id
GROUP BY u.id, u.name
```

**Template Syntax:**
- `{{ config "materialized" "view" }}` - Set materialization strategy (view, table, incremental)
- `{{ ref "model_name" }}` - Reference another model (creates dependency)
- `{{ source "source_name" "table_name" }}` - Reference a source table
- `{{ if condition }}...{{ end }}` - Conditional logic

### 4. Run Your Models

```bash
# Run all models in dependency order
gorchata run

# Run with verbose output
gorchata run --verbose

# Run specific models
gorchata run --models stg_users,fct_orders

# Stop on first error
gorchata run --fail-fast
```

## How to Run

### Via PowerShell Script (Recommended for Development)

```powershell
# Run tests
scripts/build.ps1 -Task test

# Build the binary
scripts/build.ps1 -Task build

# Run the application
scripts/build.ps1 -Task run

# Run with arguments
scripts/build.ps1 -Task run -- --help

# Clean build artifacts
scripts/build.ps1 -Task clean
```

### Via Go Command

```bash
# Run directly without building
go run ./cmd/gorchata

# Build and run
go build -o bin/gorchata ./cmd/gorchata
./bin/gorchata
```

### Via Installed Binary

```bash
gorchata run
gorchata compile
gorchata test
gorchata docs
```

## Commands

### `init`
Initialize a new Gorchata project with configuration files and optional sample models.

```bash
gorchata init my_project           # Create project with sample models
gorchata init my_project --empty   # Create empty project
gorchata init my_project --force   # Force init even if directory exists
gorchata init --help               # Show init help
```

Creates:
- `gorchata_project.yml` - Project configuration
- `profiles.yml` - Connection profiles with SQLite defaults
- `models/` - Directory for SQL models (with samples unless --empty)
- `tests/` - Directory for data quality tests (see "Testing Your Data" section)
- `seeds/` - Directory for CSV/SQL seed data files (see "Seeds" section)
- `macros/` - Empty directory for planned macros feature

**Flags:**
- `--empty` - Skip creating sample models
- `--force` - Overwrite existing files if project directory exists

### `run`
Execute all models in dependency order.

```bash
gorchata run                       # Run all models
gorchata run --verbose             # Show detailed output
gorchata run --models customers    # Run specific model(s)
gorchata run --fail-fast           # Stop on first error
gorchata run --target prod         # Use specific target from profiles
gorchata run --full-refresh        # Force full refresh for incremental models
```

### `compile`
Compile templates without executing them (validate SQL).

```bash
gorchata compile                   # Compile all models
gorchata compile --models orders   # Compile specific models
```

### `test`
Run data quality tests on your models.

```bash
gorchata test                      # Run all tests
gorchata test --select "not_null_*"  # Run tests matching pattern
gorchata test --exclude "*_temp_*"   # Exclude tests matching pattern
gorchata test --models "users,orders"  # Test specific models
gorchata test --tags "critical,finance"  # Test with tags
gorchata test --fail-fast            # Stop on first failure
```

### `build`
Run models then run tests.

```bash
gorchata build                     # Run all models then all tests
gorchata build --profile prod      # Use specific profile
```

### `docs`
Generate documentation from your models.

**Status**: Coming Soon

The `docs` command is planned for a future release and will include:
- Data lineage visualization
- Data dictionary generation
- Model dependency graphs
- Column-level documentation
- Test coverage reports

```bash
# Future usage
gorchata docs generate
```

## Testing Your Data

Gorchata includes comprehensive data quality testing inspired by dbt. Define tests in your schema files or as standalone SQL queries to validate your data transformations.

### Quick Start

Define tests in your `schema.yml`:

```yaml
models:
  - name: users
    columns:
      - name: email
        tests:
          - not_null
          - unique
      - name: status
        tests:
          - accepted_values:
              values: ['active', 'inactive', 'pending']
```

Run your tests:

```bash
# Run all tests
gorchata test

# Run specific tests
gorchata test --select "not_null_*"

# Run tests for specific models
gorchata test --models "users,orders"

# Build models and run tests
gorchata build
```

### Built-in Generic Tests

Gorchata includes 14 generic tests out of the box:

| Test | Description | Required Config | Optional Config |
|------|-------------|-----------------|-----------------|
| `not_null` | Validates column has no NULL values | - | `severity`, `where` |
| `unique` | Validates column values are unique | - | `severity`, `where` |
| `accepted_values` | Validates column contains only specified values | `values: [...]` | `severity`, `where`, `quote: true/false` |
| `relationships` | Validates foreign key relationship exists | `to: model`, `field: column` | `severity`, `where` |
| `not_empty_string` | Validates string column has no empty strings | - | `severity`, `where` |
| `at_least_one` | Validates table has at least one row matching condition | - | `group_by_columns`, `severity`, `where` |
| `not_constant` | Validates column has more than one distinct value | - | `severity`, `where` |
| `unique_combination_of_columns` | Validates combination of columns is unique | `combination_of_columns: [...]` | `severity`, `where` |
| `relationships_where` | Validates foreign key with conditional logic | `to: model`, `field: column`, `from_condition: where`, `to_condition: where` | `severity` |
| `accepted_range` | Validates column values within numeric range | `min_value: N`, `max_value: N` | `severity`, `where` |
| `recency` | Validates table has recent data within timeframe | `datepart: day/hour`, `field: timestamp_col`, `interval: N` | `severity` |
| `equal_rowcount` | Validates two models have the same rowcount | `compare_model: other_model` | `severity` |
| `sequential_values` | Validates column contains sequential values | `interval: N` | `severity`, `where` |
| `mutually_exclusive_ranges` | Validates date/number ranges don't overlap | `lower_bound_column: col1`, `upper_bound_column: col2`, `partition_by: col3`, `gaps: allowed/not_allowed` | `severity` |

### Test Configuration

All tests support these configuration options:

- **`severity`**: `error` (default) or `warn` - determines test status on failure
- **`where`**: SQL WHERE clause to filter rows before testing
- **`store_failures`**: `true` or `false` - persist failing rows to audit tables
- **`store_failures_as`**: Custom table name for storing failures (default: `dbt_test__audit_{test_id}`)
- **`error_if`**: Conditional threshold for errors (e.g., `">100"`, `">5%"`)
- **`warn_if`**: Conditional threshold for warnings
- **`sample_size`**: Override adaptive sampling (e.g., `500000`, `null` to disable)
- **`tags`**: List of tags for test selection

Example with configuration:

```yaml
models:
  - name: orders
    columns:
      - name: total_amount
        tests:
          - not_null:
              severity: error
              where: "status = 'completed'"
              store_failures: true
              tags: ["finance", "critical"]
```

### Singular Tests

Create custom SQL tests in your `tests/` directory:

```sql
-- tests/orders_total_positive.sql
-- Tests that all completed orders have positive totals
SELECT order_id, total_amount
FROM {{ ref "stg_orders" }}
WHERE status = 'completed' 
  AND total_amount <= 0
```

Any rows returned = test failure.

### Storing Test Failures

Persist failing rows for analysis:

```yaml
tests:
  - unique:
      store_failures: true
      store_failures_as: "duplicate_emails"
```

When a test fails with `store_failures: true`, Gorchata creates an audit table in the `dbt_test__audit` schema and stores the failing rows with metadata:

```sql
-- Query stored failures
SELECT test_run_id, failed_at, * 
FROM dbt_test__audit.duplicate_emails
ORDER BY failed_at DESC 
LIMIT 100;
```

Failures are automatically cleaned up after 30 days.

### Test Selection

Filter tests with CLI flags:

```bash
# By name pattern
gorchata test --select "not_null_*"

# By tag
gorchata test --tags "critical,finance"

# By model
gorchata test --models "users,orders"

# Exclude patterns
gorchata test --exclude "*_temp_*"

# Stop on first failure
gorchata test --fail-fast
```

### CLI Commands for Testing

**`gorchata test`** - Run tests only
```bash
gorchata test [--select pattern] [--exclude pattern] [--models models] [--tags tags] [--fail-fast]
```

**`gorchata build`** - Run models then tests
```bash
gorchata build [--profile profile] [--target target]
```

**`gorchata run --test`** - Run models with optional testing
```bash
gorchata run --test [--profile profile] [--target target]
```

### Adaptive Sampling

For large tables (≥1 million rows), Gorchata automatically samples 100,000 rows for testing to improve performance:

```yaml
# Override sampling behavior per test
tests:
  - unique:
      sample_size: 500000  # Use 500K sample
      
  - not_null:
      sample_size: null  # Disable sampling, scan all rows
```

### Test Results

Results are output to:
- **Console**: Color-coded output (GREEN=PASS, RED=FAIL, YELLOW=WARN)
- **JSON**: `target/test_results.json` with detailed results

Example JSON output:
```json
{
  "summary": {
    "total_tests": 10,
    "passed": 8,
    "failed": 1,
    "warnings": 1,
    "duration_ms": 1234
  },
  "results": [
    {
      "test_id": "not_null_users_email",
      "test_name": "not_null",
      "model": "users",
      "column": "email",
      "status": "passed",
      "duration_ms": 45,
      "failure_count": 0
    }
  ]
}
```

## Seeds: Loading Static Data

Seeds allow you to version-control and load CSV data files into your database. Perfect for reference data, lookup tables, and test fixtures.

### Quick Start

1. **Create a CSV file** in your `seeds/` directory:

```csv
# seeds/countries.csv
iso_code,name,region
US,United States,Americas
CA,Canada,Americas
UK,United Kingdom,Europe
```

2. **Load seeds** into your database:

```bash
gorchata seed                    # Load all seeds
gorchata seed --show             # Show seed files without loading
gorchata seed --select countries # Load specific seed(s)
```

3. **Reference seeds** in your models:

```sql
SELECT
  o.order_id,
  c.name AS country_name
FROM {{ ref "orders" }} o
JOIN {{ seed "countries" }} c ON o.country_code = c.iso_code
```

### Seed Configuration

Configure seeds in `seeds/seed.yml`:

```yaml
version: 1

naming:
  strategy: filename  # Use filename as table name
  prefix: ""          # Optional prefix (e.g., "seed_")

import:
  batch_size: 1000    # Rows per batch
  scope: folder       # Discovery scope: file, folder, tree
```

### Schema Overrides

Override inferred column types in your `models/schema.yml`:

```yaml
version: 2

seeds:
  - name: countries
    config:
      column_types:
        iso_code: TEXT      # Force TEXT (prevents "01" becoming 1)
        population: INTEGER
        gdp: REAL
```

### SQL Seeds (Advanced)

For complex setup, use SQL seed files with {{ var }} template support:

```sql
-- seeds/initialize.sql
CREATE TABLE config AS
SELECT
  '{{ var "env" }}' AS environment,
  '{{ var "region" }}' AS region;
```

Load with variables:

```bash
gorchata seed --vars '{"env":"prod","region":"us-west"}'
```

**Note**: SQL seeds support only `{{ var }}` - no `{{ ref }}`, `{{ source }}`, or `{{ seed }}`.

### Seed Discovery

Gorchata discovers seed files based on the `scope` setting:

- **`file`**: Single specified file
- **`folder`** (default): All CSV/SQL files in `seeds/` (non-recursive)
- **`tree`**: All CSV/SQL files recursively

### Seed Materialization

Seeds are materialized as tables with full refresh on each run:

1. Drop existing table (if exists)
2. Infer schema from CSV headers and data
3. Create table with inferred schema
4. Load data in batches (configurable)

### Best Practices

- ✅ Keep seeds small (<1MB) - they're version-controlled
- ✅ Use for reference data that changes infrequently
- ✅ Commit seeds to version control
- ✅ Use schema overrides for IDs with leading zeros
- ❌ Don't use seeds for large datasets - use source tables instead

## Known Limitations

### Testing Features

The data quality testing framework is fully functional with some documented limitations:

**Schema Test Discovery**
- Currently, only singular SQL test files (`.sql` in `tests/` directory) are reliably discovered
- Generic tests defined in `schema.yml` files may not be fully discovered in some scenarios
- **Workaround**: Use singular tests for critical validations until schema discovery is enhanced
- **Status**: 24 of 27 integration tests passing (89% success rate)
- **Impact**: 3 tests affected by schema discovery limitation (ExecuteTestsEndToEnd, SingularTest, TestSelection)

**Performance**
- Adaptive sampling automatically activates for tables ≥1 million rows (100,000 row sample)
- Can be overridden per-test with `sample_size: null` to scan all rows
- Large dataset testing (1.5M rows) takes 5-10 minutes, gated behind `GORCHATA_RUN_PERF_TESTS=1` environment variable

### Core Functionality Status

All critical functionality is fully operational:
- ✅ 14 generic tests with full configuration options
- ✅ Singular tests (custom SQL queries)
- ✅ Test execution with adaptive sampling
- ✅ Failure storage in `dbt_test__audit` schema
- ✅ CLI integration (`test`, `build`, `run --test`)
- ✅ Test selection by name, model, tag, pattern
- ✅ Severity levels (error/warn) and conditional thresholds

These limitations are documented as known issues and will be addressed in future releases.

## Template Engine

Gorchata uses Go's `text/template` engine with dbt-compatible template functions. This provides a familiar experience for dbt users while leveraging Go's robust templating system.

### Template Syntax

**Go text/template** uses space-separated arguments for functions:
```sql
{{ ref "model_name" }}                    -- Reference a model
{{ source "raw_data" "users" }}           -- Reference a source
{{ config "materialized" "view" }}        -- Set materialization
{{ if is_incremental }}...{{ end }}       -- Conditional logic
```

All template functions use consistent Go template syntax with space-separated string arguments.

### Available Functions

| Function | Syntax | Description |
|----------|--------|-------------|
| `ref` | `{{ ref "model" }}` | Reference another model, creates dependency |
| `source` | `{{ source "src" "table" }}` | Reference a source table |
| `this` | `{{ this }}` | Current model's table name (for incremental models) |
| `is_incremental` | `{{ if is_incremental }}` | Check if running in incremental mode |
| `var` | `{{ var "name" }}` | Access variable from context |
| `env_var` | `{{ env_var "VAR" "default" }}` | Get environment variable |
| `config` | `{{ config "key" }}` | Access configuration value |

## Materialization Strategies

Gorchata supports three materialization strategies:

### View (Default)
Creates a SQL view. Fast to build, always reflects current data.

```sql
{{ config "materialized" "view" }}

SELECT * FROM source_table
```

### Table
Creates a physical table using `CREATE TABLE AS SELECT`. Full refresh on each run.

```sql
{{ config "materialized" "table" }}

SELECT * FROM source_table
```

### Incremental
Appends new records to existing table based on unique key. Use the `is_incremental` template function to add filtering logic that only applies during incremental runs.

```sql
{{ config "materialized" "incremental" }}

SELECT 
  id,
  name,
  created_at
FROM source_table
{{ if is_incremental }}
WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{{ end }}
```

**Template Functions:**
- `{{ if is_incremental }}...{{ end }}` - Conditional logic for incremental runs
- `{{ this }}` - Returns the current model's table name (use in FROM clause)
- Functions use Go text/template syntax with space-separated arguments

**Full Refresh:**
Force a full refresh (rebuild from scratch) using the `--full-refresh` flag:

```bash
gorchata run --full-refresh
```

When `--full-refresh` is used, `is_incremental` returns false and the model is rebuilt using DROP+CREATE.

## Sample Project Structure

```
my_project/
├── gorchata_project.yml    # Project configuration
├── profiles.yml            # Connection profiles
├── models/
│   ├── staging/
│   │   ├── stg_users.sql
│   │   ├── stg_orders.sql
│   │   └── schema.yml      # Model and test configuration
│   └── marts/
│       └── fct_order_summary.sql
├── tests/                  # Data quality tests (singular SQL tests)
│   └── test_order_totals.sql
├── seeds/                  # CSV data files (planned feature)
└── macros/                 # Reusable SQL macros (planned feature)
```

See [test/fixtures/sample_project](test/fixtures/sample_project) for a complete working example.

## Troubleshooting

### Database Location

By default, Gorchata stores its SQLite database at the path specified in your `profiles.yml`.
Example: `./gorchata.db` (in project root)

To find your database:
```bash
# Check your profiles.yml
cat profiles.yml | grep database
```

### Reset Database

To start fresh with a clean database:

```bash
# Delete the database file
rm gorchata.db

# Run gorchata again (will create new database)
gorchata run
```

### Common Errors

**Error: "no such table: raw_users"**
- Your model references a source table that doesn't exist
- Create the source table or seed data first
-Or modify your model to use existing tables

**Error: "cycle detected in graph"**
- You have circular dependencies (Model A depends on B, B depends on A)
- Review your `{{ ref "..." }}` calls to break the cycle

**Error: "failed to load config"**
- Check that `gorchata_project.yml` and `profiles.yml` exist
- Validate YAML syntax (use a YAML validator)
- Ensure `default` profile exists in `profiles.yml`

**Error: "template execution failed"**
- Check template syntax: use `{{ ref "model" }}` with space-separated arguments (Go text/template style)
- NOT Jinja style: `{{ ref('model') }}` won't work
- Ensure all referenced models exist
- Validate SQL syntax
- Check conditionals use `{{ if condition }}...{{ end }}` format

### Execution Order

Models are executed in topological order based on dependencies (via `{{ ref "..." }}`).
Use `--verbose` flag to see execution order:

```bash
gorchata run --verbose
```

### Performance Tips

1. Use `views` for transformations that don't need to be persisted
2. Use `tables` for large aggregations or expensive queries
3. Use `--models` flag to run only specific models during development
4. Consider database indexing for foreign keys and join columns

## Configuration

### Configuration Files

Gorchata uses two main configuration files:

1. **gorchata_project.yml** - Project-specific settings (checked into version control)
2. **profiles.yml** - Environment/connection settings (typically not in version control)

See example configuration files in [configs/](configs/) directory:
- [configs/gorchata_project.example.yml](configs/gorchata_project.example.yml)
- [configs/profiles.example.yml](configs/profiles.example.yml)

### Configuration Discovery

Gorchata searches for config files in the following order:
1. Current directory
2. Parent directories (recursively up to root)
3. `~/.gorchata/` (for profiles.yml only)

This allows you to run `gorchata` from subdirectories within your project.

## Developer Workflow

### Running Tests

```bash
# Via PowerShell
scripts/build.ps1 -Task test

# Via Go command
go test ./...
```

### Building

```bash
# Via PowerShell
scripts/build.ps1 -Task build

# Via Go command (with CGO disabled)
CGO_ENABLED=0 go build -o bin/gorchata ./cmd/gorchata
```

### Test-Driven Development (TDD)

This project follows strict TDD principles:

1. **Write tests first** - Define expected behavior
2. **Run tests** - Confirm they fail (red phase)
3. **Implement** - Write minimal code to pass
4. **Run tests** - Confirm they pass (green phase)
5. **Refactor** - Improve code while keeping tests green
6. **Build/Run** - Verify end-to-end via PowerShell script

All contributions must include tests.

## Architecture

Gorchata follows Clean Architecture principles:

```
cmd/gorchata/          # Thin entrypoint (main.go)
internal/
├── app/               # Application wiring, dependency injection
├── cli/               # Command-line interface (Bubble Tea)
├── domain/            # Business logic (pure, no dependencies)
│   ├── model/         # Model domain logic
│   ├── project/       # Project configuration
│   └── compilation/   # Template compilation
├── config/            # Configuration loading/parsing
├── template/          # SQL template engine (Jinja-like)
└── platform/          # External dependencies
    ├── sqlite/        # SQLite adapter
    └── fs/            # Filesystem adapter
```

### Key Principles

- **Domain is pure**: No external dependencies (UI, DB, filesystem)
- **Ports and Adapters**: Interfaces defined in domain, implemented in platform
- **Dependency Injection**: All dependencies injected via constructors
- **No CGO**: Pure Go, cross-platform compilation

## Troubleshooting

### Reset Database

If you encounter database issues:

```bash
# Delete the database file
rm .gorchata/gorchata.db   # Unix/Mac
del .gorchata\gorchata.db  # Windows

# Re-run Gorchata to recreate
gorchata run
```

### Build Errors

**Error: "cgo is required"**
- Ensure `CGO_ENABLED=0` is set in your environment
- Use the provided PowerShell script which sets this automatically

**Error: "cannot find package"**
- Run `go mod tidy` to download dependencies
- Ensure Go 1.25+ is installed: `go version`

**Error: "binary not found"**
- Ensure you've run `scripts/build.ps1 -Task build` first
- Check that `bin/gorchata.exe` exists

### Runtime Errors

**Error: "profiles.yml not found"**
- Create `profiles.yml` in project root or `~/.gorchata/`
- Copy from `configs/profiles.example.yml` and customize for your environment

**Error: "gorchata_project.yml not found"**
- Ensure you're running Gorchata from a project root directory
- Initialize a new project with `gorchata init`

## Roadmap

### Completed ✅

- [x] Phase 1: Project scaffolding and build infrastructure
- [x] Phase 2: Configuration parsing (YAML with environment variable support)
- [x] Phase 3: SQL template engine (Go templates with Jinja-like syntax)
- [x] Phase 4: Model compilation and dependency resolution
- [x] Phase 5: SQLite execution engine
- [x] Phase 6: Incremental materialization with `is_incremental` and `{{ this }}`
- [x] Phase 7: `gorchata init` command with project scaffolding
- [x] Phase 8: Data quality testing framework (14 generic tests + singular tests)
- [x] Phase 9: Seeds system (CSV/SQL data loading with schema overrides)
- [x] Complete working examples (Star Schema, DCS Alarm Analytics)

### In Progress 🚧

- [ ] Phase 10: Macros system (reusable SQL snippets)
- [ ] Phase 11: Documentation generation (`gorchata docs generate`)

### Future Enhancements 🔮

- [ ] Additional database adapters (PostgreSQL, BigQuery, Snowflake)
- [ ] Pre/post hooks for models
- [ ] Parallel execution support
- [ ] Advanced testing features:
  - Table-level monitors (freshness, volume anomalies, schema drift)
  - Statistical profiling and baseline generation
  - Anomaly detection using historical patterns
  - Segmented testing (metrics by dimensions)

## Contributing

Contributions are welcome! Please ensure:

1. All tests pass (`scripts/build.ps1 -Task test`)
2. New features include tests (TDD)
3. Code follows idiomatic Go style (`gofmt`)
4. Documentation is updated (README.md)

## License

See [LICENSE](LICENSE) file for details.

## Support

- **Issues**: https://github.com/jpconstantineau/gorchata/issues
- **Discussions**: https://github.com/jpconstantineau/gorchata/discussions

---

*Made with ❤️ and Go*
