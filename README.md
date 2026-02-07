# Gorchata 🌶️

**Gorchata** is a SQL-first data transformation tool inspired by dbt, designed for Go developers who want a lightweight, dependency-free solution for managing data transformations.

## Why Gorchata?

- **SQL-First**: Write your transformations in SQL, the language of data
- **Go-Powered**: Fast, compiled binary with no runtime dependencies
- **Zero CGO**: Pure Go implementation with SQLite (no C dependencies)
- **dbt-Compatible**: Familiar project structure and concepts for dbt users
- **Lightweight**: Single binary, minimal footprint
- **Cross-Platform**: Works on Windows, Linux, and macOS

## Requirements

- **Go 1.25+** (for building from source)
- Terminal / Command Prompt

## Installation

### Option 1: Install via go install

```bash
go install github.com/pierre/gorchata/cmd/gorchata@latest
```

### Option 2: Build from Source

```bash
git clone https://github.com/pierre/gorchata.git
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
├── seeds/                  # CSV data files (for future use)
├── tests/                  # Data quality tests (for future use)
└── macros/                 # Reusable SQL macros (for future use)
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
{{ config(materialized='view') }}

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
{{ config(materialized='table') }}

SELECT
    u.id as user_id,
    u.name as user_name,
    COUNT(o.id) as order_count,
    SUM(o.amount) as total_amount
FROM {{ ref "stg_users" }} u
LEFT JOIN {{ ref "stg_orders" }} o ON u.id = o.user_id
GROUP BY u.id, u.name
```

**Key Template Functions:**
- `{{ config(materialized='view') }}` - Set materialization strategy (view, table, incremental)
- `{{ ref "model_name" }}` - Reference another model (creates dependency)

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
- `seeds/`, `tests/`, `macros/` - Empty directories for future use

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
```

### `compile`
Compile templates without executing them (validate SQL).

```bash
gorchata compile                   # Compile all models
gorchata compile --models orders   # Compile specific models
```

### `test` (Coming Soon)
Run data quality tests on your models.

```bash
gorchata test
```

### `docs` (Coming Soon)
Generate documentation from your models.

```bash
gorchata docs generate
```

## Materialization Strategies

Gorchata supports three materialization strategies:

### View (Default)
Creates a SQL view. Fast to build, always reflects current data.

```sql
{{ config(materialized='view') }}

SELECT * FROM source_table
```

### Table
Creates a physical table using `CREATE TABLE AS SELECT`. Full refresh on each run.

```sql
{{ config(materialized='table') }}

SELECT * FROM source_table
```

### Incremental (Coming Soon)
Appends new records to existing table based on unique key.  

```sql
{{ config(materialized='incremental', unique_key=['id']) }}

SELECT * FROM source_table
WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
```

## Sample Project Structure

```
my_project/
├── gorchata_project.yml    # Project configuration
├── profiles.yml            # Connection profiles
├── models/
│   ├── staging/
│   │   ├── stg_users.sql
│   │   └── stg_orders.sql
│   └── marts/
│       └── fct_order_summary.sql
├── seeds/                  # CSV data files (coming soon)
├── tests/                  # Data quality tests (coming soon)
└── macros/                 # Reusable SQL macros (coming soon)
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
- Check template syntax: use `{{ ref "model" }}` not `{{ ref('model') }}`
- Ensure all referenced models exist
- Validate SQL syntax

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
- Cx] Phase 2: Configuration parsing (YAML with environment variable supportr the correct format

**Error: "gorchata_project.yml not found"**
- Ensure you're running Gorchata from a project root directory
- Initialize a new project with `gorchata init`

## Roadmap

- [x] Phase 1: Project scaffolding and build infrastructure
- [ ] Phase 2: Configuration parsing (YAML)
- [ ] Phase 3: SQL template engine (Jinja-like)
- [ ] Phase 4: Model compilation and dependency resolution
- [ ] Phase 5: SQLite execution engine
- [ ] Phase 6: CLI interface (Bubble Tea)
- [ ] Phase 7: Seeds and tests
- [ ] Phase 8: Macros and documentation generation

## Contributing

Contributions are welcome! Please ensure:

1. All tests pass (`scripts/build.ps1 -Task test`)
2. New features include tests (TDD)
3. Code follows idiomatic Go style (`gofmt`)
4. Documentation is updated (README.md)

## License

See [LICENSE](LICENSE) file for details.

## Support

- **Issues**: https://github.com/pierre/gorchata/issues
- **Discussions**: https://github.com/pierre/gorchata/discussions

---

*Made with ❤️ and Go*
