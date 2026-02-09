## Plan: Phase 9 - Seeds System (CSV/SQL Data Loading)

A complete seed data loading system supporting CSV files and SQL scripts with flexible configuration for table naming strategies, batch processing, and import scope. Seeds can be referenced by models via a new `seed()` template function, replacing the current workaround of inline VALUES clauses. Infrastructure (config, directories) is already in place from Phase 7.

**Phases: 7 phases**

### 1. **Phase 1: Seed Domain Types and CSV Parser Foundation**
- **Objective:** Create the core Seed domain types and implement basic CSV file parsing using Go's stdlib `encoding/csv`
- **Files/Functions to Modify/Create:**
  - `internal/domain/seeds/seed.go` (new) - Seed, SeedType, SeedColumn, SeedSchema structs
  - `internal/domain/seeds/parser.go` (new) - ParseCSV(), readCSVFile()
  - `internal/domain/seeds/seed_test.go` (new) - Seed struct validation tests
  - `internal/domain/seeds/parser_test.go` (new) - CSV parsing tests
  - `internal/domain/seeds/testdata/` (new) - Test CSV fixtures
- **Tests to Write:**
  - TestSeedValidation - validates Seed struct fields (ID, Path, Type)
  - TestParseCSV_BasicCSV - parses simple CSV with headers
  - TestParseCSV_WithQuotes - handles quoted fields
  - TestParseCSV_EmptyFile - handles empty CSV gracefully
  - TestParseCSV_MissingHeaders - errors on CSV without headers
  - TestParseCSV_MalformedRows - errors on inconsistent field counts
- **Steps:**
  1. Write tests for Seed struct validation (ID required, Type validation)
  2. Run tests (fail)
  3. Implement Seed, SeedType, SeedColumn, SeedSchema struct definitions
  4. Write tests for basic CSV parsing (headers, data rows)
  5. Run tests (fail)
  6. Implement ParseCSV() using encoding/csv.NewReader()
  7. Run tests until passing
  8. Refactor CSV parsing to handle edge cases (quotes, empty values)

### 2. **Phase 2: Seed Configuration System with Naming Strategies**
- **Objective:** Implement seed.yml configuration supporting flexible table naming (filename, folder, static) and batch/import settings
- **Files/Functions to Modify/Create:**
  - `internal/config/seed_config.go` (new) - SeedConfig, NamingStrategy, ImportConfig structs
  - `internal/config/seed_config_test.go` (new) - Seed config parsing tests
  - `configs/seed.example.yml` (new) - Example seed configuration
  - `internal/domain/seeds/naming.go` (new) - ResolveTableName() based on strategy
  - `internal/domain/seeds/naming_test.go` (new) - Naming strategy tests
- **Tests to Write:**
  - TestParseSeedConfig_NamingStrategy - parses naming: filename/folder/static
  - TestParseSeedConfig_ImportConfig - parses batch_size, scope (file/folder/tree)
  - TestResolveTableName_Filename - generates table name from CSV filename
  - TestResolveTableName_Folder - generates table name from parent folder
  - TestResolveTableName_Static - uses configured static name
  - TestResolveTableName_Precedence - handles override precedence
- **Steps:**
  1. Write tests for SeedConfig struct with naming strategy fields
  2. Run tests (fail)
  3. Implement SeedConfig struct with NamingStrategy (filename/folder/static), ImportConfig (batch_size, scope)
  4. Write tests for parsing seed.yml with different naming configs
  5. Run tests (fail)
  6. Implement YAML unmarshaling for SeedConfig
  7. Write tests for ResolveTableName() with each strategy
  8. Run tests (fail)
  9. Implement ResolveTableName() switching on NamingStrategy
  10. Create example seed.yml demonstrating all options
  11. Run tests until passing
  12. Refactor for clear separation of naming logic

### 3. **Phase 3: Type Inference and Schema Generation**
- **Objective:** Implement automatic type inference from CSV data to generate SQL schema (INTEGER, REAL, TEXT) with configurable sampling
- **Files/Functions to Modify/Create:**
  - `internal/domain/seeds/inference.go` (new) - InferSchema(), inferColumnType()
  - `internal/domain/seeds/inference_test.go` (new) - Type inference tests
- **Tests to Write:**
  - TestInferSchema_AllIntegers - infers INTEGER for numeric columns
  - TestInferSchema_Decimals - infers REAL for columns with decimals
  - TestInferSchema_Mixed - infers TEXT for mixed-type columns
  - TestInferSchema_WithNulls - ignores NULL/empty values during inference
  - TestInferColumnType_Priority - tests type inference priority (INTEGER > REAL > TEXT)
  - TestInferColumnType_EdgeCases - handles edge cases (leading zeros, scientific notation)
- **Steps:**
  1. Write tests for inferColumnType() with various data samples
  2. Run tests (fail)
  3. Implement inferColumnType() with regex/parsing logic for INTEGER/REAL/TEXT
  4. Write tests for InferSchema() sampling first N rows
  5. Run tests (fail)
  6. Implement InferSchema() that samples data and calls inferColumnType() per column
  7. Run tests until passing
  8. Refactor to handle NULL values and edge cases gracefully

### 4. **Phase 4: Seed Executor with Configurable Batch Processing**
- **Objective:** Implement seed execution logic with configurable batch sizes and import scopes (file/folder/tree)
- **Files/Functions to Modify/Create:**
  - `internal/domain/seeds/executor.go` (new) - ExecuteSeed(), buildCreateTableSQL(), buildInsertSQL()
  - `internal/domain/seeds/executor_test.go` (new) - Seed execution integration tests
  - `internal/domain/seeds/result.go` (new) - SeedResult struct
  - `internal/domain/seeds/batch.go` (new) - BatchProcessor with configurable size
- **Tests to Write:**
  - TestBuildCreateTableSQL - generates correct CREATE TABLE statement
  - TestBuildInsertSQL_ConfigurableBatch - respects batch_size config
  - TestBatchProcessor_SmallBatch - handles 100-row batches
  - TestBatchProcessor_LargeBatch - handles 10000-row batches
  - TestExecuteSeed_Success - end-to-end test with temp database
  - TestExecuteSeed_FullRefresh - verifies DROP TABLE IF EXISTS behavior
  - TestExecuteSeed_TransactionRollback - verifies error handling and rollback
- **Steps:**
  1. Write tests for buildCreateTableSQL() with inferred schema
  2. Run tests (fail)
  3. Implement buildCreateTableSQL() to generate DDL from SeedSchema
  4. Write tests for BatchProcessor with configurable batch_size
  5. Run tests (fail)
  6. Implement BatchProcessor that chunks rows based on config
  7. Write tests for buildInsertSQL() using BatchProcessor
  8. Run tests (fail)
  9. Implement buildInsertSQL() generating parameterized VALUES for each batch
  10. Write integration test ExecuteSeed() with temp SQLite database
  11. Run test (fail)
  12. Implement ExecuteSeed() orchestrating DROP/CREATE/batched-INSERT within transaction
  13. Run tests until passing
  14. Refactor for optimal performance and clear batch boundaries

### 5. **Phase 5: CLI Seed Command with Scoped Discovery**
- **Objective:** Implement `gorchata seed` CLI command with configurable discovery scope (single file, folder, folder tree)
- **Files/Functions to Modify/Create:**
  - `internal/cli/seed.go` (new) - SeedCommand(), discoverSeeds(), executeSeeds()
  - `internal/cli/seed_test.go` (new) - CLI command tests
  - `internal/domain/seeds/discovery.go` (new) - DiscoverSeeds() with scope support
  - `internal/domain/seeds/discovery_test.go` (new) - Discovery tests
  - `cmd/gorchata/main.go` - Add "seed" subcommand routing
- **Tests to Write:**
  - TestDiscoverSeeds_File - discovers single specified CSV file
  - TestDiscoverSeeds_Folder - discovers all CSVs in one folder (non-recursive)
  - TestDiscoverSeeds_Tree - discovers all CSVs recursively in folder tree
  - TestDiscoverSeeds_FilterNonCSV - ignores non-.csv files
  - TestSeedCommand_SelectFlag - runs only specified seeds
  - TestSeedCommand_FullRefreshFlag - passes full-refresh to executor
  - TestSeedCommand_NoSeedsFound - handles empty seed directories gracefully
- **Steps:**
  1. Write tests for DiscoverSeeds() with scope parameter (file/folder/tree)
  2. Run tests (fail)
  3. Implement DiscoverSeeds() with conditional logic: os.Stat for file, os.ReadDir for folder, filepath.Walk for tree
  4. Write tests for SeedCommand() flag parsing (--select, --full-refresh, --scope)
  5. Run tests (fail)
  6. Implement SeedCommand() with flag.FlagSet following RunCommand() pattern
  7. Write integration test executing seeds with mock adapter
  8. Run test (fail)
  9. Implement executeSeeds() orchestrating discovery → parsing → execution
  10. Update `cmd/gorchata/main.go` to route "seed" subcommand
  11. Run tests until passing
  12. Refactor for consistent error reporting and verbose output

### 6. **Phase 6: Template seed() Function and Schema References**
- **Objective:** Add seed() template function and support schema.yml references to separate seed config files
- **Files/Functions to Modify/Create:**
  - `internal/template/functions.go` - makeSeedFunc()
  - `internal/template/funcmap.go` - Register seed() in BuildFuncMap()
  - `internal/template/context.go` - Add Seeds map[string]string field
  - `internal/template/functions_test.go` - seed() function tests
  - `internal/config/schema.go` - Add seed_config_path reference support
  - `internal/config/schema_test.go` - Schema reference tests
- **Tests to Write:**
  - TestSeedFunc_ExistingSeed - returns qualified table name for valid seed
  - TestSeedFunc_WithSchema - includes schema prefix when configured
  - TestSeedFunc_NonexistentSeed - returns error for unknown seed
  - TestSeedFunc_EmptyName - handles empty seed name
  - TestParseSchemaYAML_SeedConfigRef - parses seed_config_path reference
  - TestIntegration_SeedInModel - tests model using seed() in template
- **Steps:**
  1. Write tests for makeSeedFunc() following makeSourceFunc() pattern
  2. Run tests (fail)
  3. Implement makeSeedFunc() that looks up seed in Context.Seeds map
  4. Write test for Context.Seeds population
  5. Run test (fail)
  6. Update Context struct with Seeds field and initialization
  7. Register seed() in BuildFuncMap()
  8. Write tests for schema.yml parsing with seed_config_path field
  9. Run tests (fail)
  10. Implement parsing logic to load referenced seed config files
  11. Write integration test compiling model template with seed() reference
  12. Run test (fail)
  13. Update model compilation to populate Context.Seeds before template execution
  14. Run tests until passing
  15. Refactor for consistent qualified name generation (schema.table)

### 7. **Phase 7: SQL Seeds, Schema Overrides, and Example Migration**
- **Objective:** Support SQL seed files with {{ var }} substitution, schema.yml column type overrides, and migrate examples to CSV seeds
- **Files/Functions to Modify/Create:**
  - `internal/domain/seeds/sql_seed.go` (new) - ExecuteSQLSeed() with var support
  - `internal/domain/seeds/sql_seed_test.go` (new) - SQL seed tests
  - `internal/config/schema.go` - Parse seeds section with column_types
  - `internal/cli/seed.go` - Support .sql files in discovery
  - `examples/star_schema_example/seeds/raw_sales.csv` (new) - Migrate from VALUES
  - `examples/star_schema_example/seeds/seed.yml` (new) - Seed configuration
  - `examples/star_schema_example/schema.yml` - Add seeds section
  - `examples/dcs_alarm_example/seeds/raw_alarm_events.csv` (new) - Migrate from VALUES
  - `examples/dcs_alarm_example/seeds/seed.yml` (new) - Seed configuration
  - `README.md` - Update seeds documentation (remove "planned" status)
- **Tests to Write:**
  - TestExecuteSQLSeed_WithVarSubstitution - handles {{ var "key" }} replacement
  - TestExecuteSQLSeed_NoTemplates - executes plain SQL without ref/source/seed
  - TestParseSchemaYAML_SeedsColumnTypes - parses seeds with column_types override
  - TestApplySchemaOverride - applies manual types over inferred types
  - TestDiscoverSeeds_MixedTypes - discovers both CSV and SQL seeds
  - TestEndToEnd_ModelReferencesSeed - full pipeline test
- **Steps:**
  1. Write tests for ExecuteSQLSeed() with {{ var }} template support
  2. Run tests (fail)
  3. Implement ExecuteSQLSeed() reading .sql file, rendering var templates only, executing statements
  4. Write tests for validating SQL seeds don't contain ref/source/seed functions
  5. Run tests (fail)
  6. Implement validation logic rejecting forbidden template functions
  7. Write tests for parsing seeds section in schema.yml with column_types
  8. Run tests (fail)
  9. Extend schema YAML parser to handle seeds.config.column_types
  10. Write tests for applying schema overrides to inferred types
  11. Run tests (fail)
  12. Implement schema override logic in InferSchema()
  13. Update discoverSeeds() to include .sql files
  14. Create raw_sales.csv and raw_alarm_events.csv from example data
  15. Create seed.yml files for both examples with appropriate naming strategies
  16. Delete old models/sources/*.sql files with inline VALUES
  17. Add seeds section to schema.yml for examples
  18. Write end-to-end test: seed → model → verify
  19. Run test (fail)
  20. Fix any integration issues in seed discovery/execution pipeline
  21. Update `README.md` Seeds section (planned → complete, usage examples, config docs)
  22. Run all tests until passing
  23. Refactor and clean up temporary test artifacts

**Open Questions (Resolved):**

1. ✅ **Seed table naming**: Use configurable YAML with strategies: `filename`, `folder`, or `static` name
2. ✅ **Batch insert size**: Configurable batch_size in seed.yml; support import scopes: `file`, `folder`, or `tree`
3. ✅ **Schema.yml location**: Schema in same location but may reference separate seed config files via `seed_config_path`
4. ✅ **SQL seed template support**: Allow `{{ var }}` for environment variables only; forbid `{{ ref }}/{{ source }}/{{ seed }}`
5. ✅ **Error handling**: Fail-fast by default; add `--continue-on-error` and `--continue-and-quarantine` flags in future phases

---

## Example seed.yml Configuration

```yaml
version: 1

# Table naming strategy
naming:
  strategy: filename  # Options: filename, folder, static
  # static_name: my_table  # Only used when strategy: static
  # prefix: seed_  # Optional prefix for all seed tables

# Import configuration
import:
  batch_size: 1000  # Rows per INSERT batch
  scope: tree  # Options: file, folder, tree

# Column type overrides (optional)
column_types:
  customer_id: INTEGER
  signup_date: TEXT
  revenue: REAL

# Additional configuration
config:
  quote_columns: true
  full_refresh: true
```

---

## Example schema.yml with Seeds Reference

```yaml
version: 2

# Reference external seed configuration
seed_config_path: seeds/seed.yml

models:
  - name: customer_analysis
    columns:
      - name: customer_id
        data_tests: [unique, not_null]

# Inline seeds configuration (alternative to seed_config_path)
seeds:
  - name: customers
    config:
      column_types:
        customer_id: INTEGER
        name: TEXT
        email: TEXT
```
