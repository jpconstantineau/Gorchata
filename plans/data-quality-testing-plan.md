## Plan: Data Quality Testing Framework for Gorchata

Implement a comprehensive data quality testing framework inspired by DBT's test architecture with 14-18 built-in generic tests, custom test templates, and extensibility for future Monte Carlo-style observability features. The framework provides DBT-compatible test configuration, adaptive sampling for performance, multiple execution modes, and robust failure tracking - all in pure Go with no CGO dependencies.

**Phases: 8 phases (core testing + examples) + 4 future phases (observability)**

### 1. **Phase 1: Test Domain Models & Core Abstractions**
   - **Objective:** Design and implement the core domain models for representing tests, test results, and test execution context
   - **Files/Functions to Modify/Create:**
     - [internal/domain/test/test.go](internal/domain/test/test.go) - Test struct, TestType, TestStatus enums
     - [internal/domain/test/result.go](internal/domain/test/result.go) - TestResult, TestSummary structs
     - [internal/domain/test/config.go](internal/domain/test/config.go) - TestConfig, Severity levels, thresholds
   - **Tests to Write:**
     - `TestNewTest_ValidInput`
     - `TestNewTest_InvalidInput`
     - `TestTestResult_Validation`
     - `TestTestConfig_Defaults`
     - `TestSeverityLevels`
   - **Steps:**
     1. Write test cases defining expected behavior for test domain objects
     2. Run tests and confirm failures
     3. Implement Test, TestResult, TestConfig domain models with validation
     4. Implement TestType enum (GenericTest, SingularTest, future: ProfileTest)
     5. Implement TestStatus enum (Pending, Running, Passed, Failed, Warning, Skipped)
     6. Implement Severity enum (Error, Warn)
     7. Run tests until passing

### 2. **Phase 2: Generic Test Implementations (Core + Extended Tests)**
   - **Objective:** Implement 4 core DBT tests + 10-15 extended dbt-utils style tests
   - **Files/Functions to Modify/Create:**
     - **Core Tests (DBT built-in):**
       - [internal/domain/test/generic/not_null.go](internal/domain/test/generic/not_null.go) - NotNullTest
       - [internal/domain/test/generic/unique.go](internal/domain/test/generic/unique.go) - UniqueTest
       - [internal/domain/test/generic/accepted_values.go](internal/domain/test/generic/accepted_values.go) - AcceptedValuesTest
       - [internal/domain/test/generic/relationships.go](internal/domain/test/generic/relationships.go) - RelationshipsTest
     - **Extended Tests (dbt-utils style):**
       - [internal/domain/test/generic/not_empty_string.go](internal/domain/test/generic/not_empty_string.go) - NotEmptyStringTest
       - [internal/domain/test/generic/at_least_one.go](internal/domain/test/generic/at_least_one.go) - AtLeastOneTest
       - [internal/domain/test/generic/not_constant.go](internal/domain/test/generic/not_constant.go) - NotConstantTest
       - [internal/domain/test/generic/unique_combination_of_columns.go](internal/domain/test/generic/unique_combination_of_columns.go) - UniqueCombinationTest
       - [internal/domain/test/generic/relationships_where.go](internal/domain/test/generic/relationships_where.go) - RelationshipsWhereTest
       - [internal/domain/test/generic/accepted_range.go](internal/domain/test/generic/accepted_range.go) - AcceptedRangeTest
       - [internal/domain/test/generic/recency.go](internal/domain/test/generic/recency.go) - RecencyTest
       - [internal/domain/test/generic/equal_rowcount.go](internal/domain/test/generic/equal_rowcount.go) - EqualRowcountTest
       - [internal/domain/test/generic/sequential_values.go](internal/domain/test/generic/sequential_values.go) - SequentialValuesTest
       - [internal/domain/test/generic/mutually_exclusive_ranges.go](internal/domain/test/generic/mutually_exclusive_ranges.go) - MutuallyExclusiveRangesTest
     - **Infrastructure:**
       - [internal/domain/test/generic/registry.go](internal/domain/test/generic/registry.go) - Test registration and factory
       - [internal/domain/test/generic/base.go](internal/domain/test/generic/base.go) - Shared test execution logic
   - **Tests to Write:**
     - Core tests (8 tests): `TestNotNull_*`, `TestUnique_*`, `TestAcceptedValues_*`, `TestRelationships_*`
     - Extended tests (20+ tests): One passing + one failing test per extended test type
     - `TestGenericTestRegistry_AllTests` - Verify all 14-18 tests registered
   - **Steps:**
     1. Write comprehensive test cases for core generic tests
     2. Run tests and confirm failures
     3. Implement base test infrastructure (registry, execution engine, SQL helpers)
     4. Implement 4 core tests with SQL generation and execution
     5. Write test cases for extended tests
     6. Run tests and confirm failures
     7. Implement 10-15 extended tests
     8. Implement GenericTestRegistry with all tests registered
     9. Run all tests until passing

### 3. **Phase 3: Singular Test Support & Custom Generic Test Templates**
   - **Objective:** Implement singular test execution (custom SQL) and SQL template-based custom generic tests
   - **Files/Functions to Modify/Create:**
     - **Singular Tests:**
       - [internal/domain/test/singular/loader.go](internal/domain/test/singular/loader.go) - LoadSingularTests(), loadTestFromFile()
       - [internal/domain/test/singular/executor.go](internal/domain/test/singular/executor.go) - ExecuteSingularTest()
       - [internal/domain/test/singular/parser.go](internal/domain/test/singular/parser.go) - ParseTestMetadata() for extracting config
     - **Custom Generic Test Templates:**
       - [internal/domain/test/generic/template_loader.go](internal/domain/test/generic/template_loader.go) - LoadCustomGenericTests()
       - [internal/domain/test/generic/template_executor.go](internal/domain/test/generic/template_executor.go) - ExecuteTemplateTest()
       - [internal/domain/test/generic/template_parser.go](internal/domain/test/generic/template_parser.go) - ParseTestTemplate() with {% test %} syntax
   - **Tests to Write:**
     - `TestLoadSingularTests_ValidDirectory`
     - `TestLoadSingularTests_EmptyDirectory`
     - `TestExecuteSingularTest_WithFailingRows`
     - `TestExecuteSingularTest_WithoutFailingRows`
     - `TestParseTestMetadata_WithConfig`
     - `TestLoadCustomGenericTests_FromDirectory`
     - `TestParseTestTemplate_ValidTemplate`
     - `TestExecuteTemplateTest_WithArguments`
   - **Steps:**
     1. Write test cases for singular test loading and parsing
     2. Run tests and confirm failures
     3. Implement directory scanning for .sql files in tests/
     4. Implement SQL file parsing with template support ({{ ref() }}, {{ source() }})
     5. Implement metadata extraction from SQL comments (-- config(severity='warn'))
     6. Implement singular test execution (0 rows = pass, >0 rows = fail)
     7. Write test cases for custom generic test templates
     8. Run tests and confirm failures
     9. Implement {% test name(model, args) %} parser
     10. Implement template argument substitution
     11. Register custom generic tests dynamically
     12. Run all tests until passing

### 4. **Phase 4: Test Configuration via YAML Schema Files**
   - **Objective:** Implement schema.yml parsing to configure generic tests on models and columns
   - **Files/Functions to Modify/Create:**
     - [internal/domain/test/schema/schema.go](internal/domain/test/schema/schema.go) - SchemaFile, ModelSchema, ColumnSchema structs
     - [internal/domain/test/schema/parser.go](internal/domain/test/schema/parser.go) - ParseSchemaFile(), LoadSchemaFiles()
     - [internal/domain/test/schema/builder.go](internal/domain/test/schema/builder.go) - BuildTestsFromSchema()
   - **Tests to Write:**
     - `TestParseSchemaFile_ValidYAML`
     - `TestParseSchemaFile_InvalidYAML`
     - `TestLoadSchemaFiles_MultipleFiles`
     - `TestBuildTestsFromSchema_ColumnTests`
     - `TestBuildTestsFromSchema_WithConfiguration`
     - `TestBuildTestsFromSchema_WithWhereClause`
   - **Steps:**
     1. Write test cases for schema file parsing with sample YAML
     2. Run tests and confirm failures
     3. Define SchemaFile struct matching DBT schema.yml structure
     4. Implement YAML parsing using gopkg.in/yaml.v3
     5. Implement schema file discovery (recursively find *schema.yml in models/)
     6. Implement BuildTestsFromSchema to instantiate generic tests from config
     7. Handle test configuration: severity, where clauses, custom names, tags
     8. Run tests until passing

### 5. **Phase 5: Test Execution Engine & Multi-Mode CLI Integration**
   - **Objective:** Implement the test execution engine with adaptive sampling and integrate all three execution modes
   - **Files/Functions to Modify/Create:**
     - [internal/domain/test/executor/engine.go](internal/domain/test/executor/engine.go) - TestEngine, ExecuteTests(), ExecuteTest()
     - [internal/domain/test/executor/selector.go](internal/domain/test/executor/selector.go) - Test selection logic (by name, tag, model)
     - [internal/domain/test/executor/sampler.go](internal/domain/test/executor/sampler.go) - Adaptive sampling logic (>1M rows)
     - [internal/domain/test/executor/result_writer.go](internal/domain/test/executor/result_writer.go) - Console + JSON writers
     - [internal/cli/test.go](internal/cli/test.go) - Replace placeholder with full TestCommand
     - [internal/cli/run.go](internal/cli/run.go) - Add --test flag support
     - [internal/cli/build.go](internal/cli/build.go) - New build command (models + tests)
   - **Tests to Write:**
     - `TestExecuteTests_AllPass`
     - `TestExecuteTests_SomeFailures`
     - `TestExecuteTests_WithWarnings`
     - `TestTestSelector_ByName`
     - `TestTestSelector_ByTag`
     - `TestTestSelector_ByModel`
     - `TestAdaptiveSampler_SmallTable`
     - `TestAdaptiveSampler_LargeTable`
     - `TestResultWriter_ConsoleOutput`
     - `TestResultWriter_JSONOutput`
     - `TestBuildCommand_ModelsAndTests`
   - **Steps:**
     1. Write test cases for test engine with mock tests and database adapter
     2. Run tests and confirm failures
     3. Implement TestEngine with ExecuteTests() orchestration
     4. Implement adaptive sampling: check row count, use TABLESAMPLE for >1M rows
     5. Implement test discovery (load singular tests + parse schema files)
     6. Implement test selection logic (all, by name pattern, by tag, by model)
     7. Implement parallel test execution with goroutines and error handling
     8. Implement result aggregation and summary
     9. Implement console result writer with colored output
     10. Implement JSON result writer to target/test_results.json
     11. Implement CLI test command with flags (--select, --exclude, --verbose, --fail-fast)
     12. Update CLI run command to support --test flag
     13. Create new CLI build command that orchestrates models then tests
     14. Run all tests until passing

### 6. **Phase 6: Test Result Storage & Failure Tracking in Test Schema**
   - **Objective:** Implement store_failures capability in `dbt_test__audit` schema (DBT-compatible)
   - **Files/Functions to Modify/Create:**
     - [internal/domain/test/storage/failure_store.go](internal/domain/test/storage/failure_store.go) - FailureStore interface, SQLiteFailureStore
     - [internal/domain/test/storage/schema.go](internal/domain/test/storage/schema.go) - CreateTestSchema(), CreateFailureTable(), DropFailureTable()
     - [internal/domain/test/storage/cleanup.go](internal/domain/test/storage/cleanup.go) - CleanupOldFailures() with retention policy
     - [internal/domain/test/executor/engine.go](internal/domain/test/executor/engine.go) - Modify ExecuteTest() to store failures
   - **Tests to Write:**
     - `TestCreateTestSchema_FirstTime`
     - `TestCreateTestSchema_AlreadyExists`
     - `TestStoreFailures_CreateTable`
     - `TestStoreFailures_InsertFailures`
     - `TestStoreFailures_QueryFailures`
     - `TestCleanupOldFailures_ExpiredRecords`
     - `TestExecuteTest_WithStoreFailures`
   - **Steps:**
     1. Write test cases for test schema and failure storage
     2. Run tests and confirm failures
     3. Define FailureStore interface (StoreFailures, GetFailures, ClearFailures)
     4. Implement CreateTestSchema() to create `dbt_test__audit` schema if not exists
     5. Design failure table schema: test_name, model, column, failed_row_json, execution_time, created_at
     6. Implement SQLiteFailureStore with dynamic table creation per test
     7. Modify test execution to capture failing rows when store_failures=true
     8. Implement JSON serialization of failing row data
     9. Implement CleanupOldFailures with configurable retention (default 30 days)
     10. Run all tests until passing

### 7. **Phase 7: Documentation & Core Integration Testing**
   - **Objective:** Create comprehensive documentation and framework-level integration tests
   - **Files/Functions to Modify/Create:**
     - [README.md](README.md) - Add "Testing Your Data" section
     - [test/integration_test_framework.go](test/integration_test_framework.go) - End-to-end test framework
     - [docs/testing-guide.md](docs/testing-guide.md) - Comprehensive testing guide (if needed)
   - **Tests to Write:**
     - `TestIntegration_TestCommand_CoreGenericTests`
     - `TestIntegration_TestCommand_ExtendedGenericTests`
     - `TestIntegration_TestCommand_SingularTests`
     - `TestIntegration_TestCommand_CustomGenericTests`
     - `TestIntegration_TestCommand_WithFailures`
     - `TestIntegration_BuildCommand_ModelsAndTests`
     - `TestIntegration_AdaptiveSampling_LargeTable`
   - **Steps:**
     1. Write end-to-end integration tests for all test execution modes
     2. Run tests and confirm failures
     3. Implement integration test framework with temp databases
     4. Test all three execution modes (test, run --test, build)
     5. Test adaptive sampling with large dataset
     6. Test failure storage in dbt_test__audit schema
     7. Update README.md with:
        - "gorchata test" command documentation
        - Generic tests reference table (all 14-18 tests)
        - How to write singular tests
        - How to write custom generic test templates
        - Configuration options (severity, where, store_failures, sample_size)
        - Troubleshooting guide
     8. Run integration tests and verify all workflows
     9. Build and run via PowerShell script
     10. Confirm framework functionality

### 8. **Phase 8: Example Project Test Implementation & Validation**
   - **Objective:** Add comprehensive data quality tests to all example projects and validate they work correctly
   - **Files/Functions to Modify/Create:**
     - [examples/dcs_alarm_example/models/schema.yml](examples/dcs_alarm_example/models/schema.yml) - Add generic tests config
     - [examples/dcs_alarm_example/tests/test_alarm_lifecycle.sql](examples/dcs_alarm_example/tests/test_alarm_lifecycle.sql) - Singular test
     - [examples/dcs_alarm_example/tests/test_standing_alarm_duration.sql](examples/dcs_alarm_example/tests/test_standing_alarm_duration.sql) - Business rule test
     - [examples/dcs_alarm_example/tests/test_chattering_detection.sql](examples/dcs_alarm_example/tests/test_chattering_detection.sql) - Chattering alarm test
     - [examples/dcs_alarm_example/tests/generic/test_valid_timestamp.sql](examples/dcs_alarm_example/tests/generic/test_valid_timestamp.sql) - Custom generic test
     - [examples/star_schema_example/models/schema.yml](examples/star_schema_example/models/schema.yml) - Add generic tests
     - [examples/star_schema_example/tests/test_fact_integrity.sql](examples/star_schema_example/tests/test_fact_integrity.sql) - Fact table tests
     - [examples/dcs_alarm_example/README.md](examples/dcs_alarm_example/README.md) - Document test coverage
     - [examples/star_schema_example/README.md](examples/star_schema_example/README.md) - Document test coverage
   - **Tests to Write:**
     - `TestDCSAlarmExample_AllTestsPass` - Verify all DCS alarm tests execute successfully
     - `TestStarSchemaExample_AllTestsPass` - Verify all star schema tests execute successfully
     - `TestExamples_TestDiscovery` - Verify test files are discovered correctly
   - **Steps:**
     1. Write integration tests for example test execution
     2. Run tests and confirm failures
     3. Create schema.yml for DCS alarm example with 20+ generic tests
     4. Create 3 singular test SQL files for alarm analytics (lifecycle, standing alarms, chattering)
     5. Create custom generic test template for timestamp validation
     6. Create schema.yml for star schema example with 25+ generic tests
     7. Create singular test for fact table integrity
     8. Create comprehensive schema.yml for star schema example:
        - Dimension uniqueness tests
        - Fact table FK integrity tests
        - Not null on key columns
        - SCD Type 2 validity tests (effective dates)
     9. Create singular tests for star schema business rules
     10. Update example README files documenting test coverage
     11. Run `gorchata test` in each example directory
     12. Verify all tests pass (except intentional failure examples)
     13. Test failure storage and debugging workflow
     14. Validate adaptive sampling on larger datasets (if applicable)
     15. Build and run via PowerShell script for end-to-end validation

---

## Future Phases (Statistical Testing & Observability)

Given the decision to defer statistical testing (Q7: Defer to Phase 9+), here are planned future enhancements:

### **Phase 9: Table-Level Monitors (Monte Carlo-Inspired)**
   - **Objective:** Implement table-level monitoring for freshness, volume anomalies, and schema drift
   - **Key Features:**
     - Freshness monitoring: Alert on stale data based on timestamp columns
     - Volume anomaly detection: Warn on unusual row count changes (configurable thresholds)
     - Schema drift detection: Track schema evolution and breaking changes
     - Metadata-based checks (no expensive full-table queries)
   - **Implementation:**
     - Monitor tables at metadata layer (row counts, last modified times)
     - Store baselines and track deviations
     - Alert on significant changes (>20% volume deviation, >24hr stale data)

### **Phase 10: Statistical Profiling & Baseline Generation**
   - **Objective:** Automatic metric collection and baseline generation for distribution testing
   - **Key Features:**
     - Automatic metric collection: null%, cardinality, min/max, mean, stdev
     - Store baselines in test history tables
     - Detect distribution drift over time
     - Percentile-based alerting (P50, P95, P99)
   - **Implementation:**
     - Profile tables on each test run
     - Store metrics with timestamps
     - Compare current metrics against historical baselines
     - Alert when metrics deviate beyond thresholds

### **Phase 11: Anomaly Detection & ML-Based Testing**
   - **Objective:** ML-based anomaly detection using historical test result patterns
   - **Key Features:**
     - Time-series anomaly detection on test metrics
     - Seasonal pattern recognition (day-of-week, monthly cycles)
     - Predictive thresholds (95th percentile from history)
     - Integration with test result storage for trend analysis
   - **Implementation:**
     - Analyze test result history for patterns
     - Use simple statistical methods (Z-score, IQR) initially
     - Detect unexpected spikes/drops in failure rates
     - Adapt thresholds based on historical variance

### **Phase 12: Segmented Testing**
   - **Objective:** Test metrics within dimensional segments to detect subgroup issues
   - **Key Features:**
     - Test metrics by dimensions (area_code, priority_code, equipment_type, etc.)
     - Detect subgroup anomalies masked by aggregates
     - Relative distribution testing (segment % changes)
     - Dimensional drill-down on test failures
   - **Implementation:**
     - Extend tests to group by dimensional attributes
     - Track per-segment metrics over time
     - Alert when segment distribution shifts significantly
     - Provide drill-down capabilities in test results

---

## Research Summary

### DBT Testing Capabilities (Phase 1-2 Implementation)
**Built-in Generic Tests:**
- `not_null`: No null values in column
- `unique`: No duplicate values
- `accepted_values`: Values in allowed set
- `relationships`: Referential integrity (FK validation)

**Test Configuration:**
- Severity levels: `error` (default) vs `warn`
- Conditional execution: `where` clauses for filtering
- Custom thresholds: `error_if`, `warn_if` with row count conditions
- Test storage: `store_failures` to save failing records
- Test naming: Auto-generated or custom
- Tags: For test organization and selection

**Test Types:**
- **Generic Tests**: Reusable parameterized tests (column-level or table-level)
- **Singular Tests**: One-off SQL queries returning failing rows

**Execution Features:**
- Test selection by name, model, tag, directory
- Parallel execution
- Fail-fast mode
- Rich console output with pass/fail/warn status
- JSON result artifacts

### DBT Packages Extended Tests (Future Phases)
**dbt-utils** (50+ tests):
- Table shape: `equal_rowcount`, `fewer_rows_than`, `recency`
- Advanced column: `not_constant`, `not_empty_string`, `cardinality_equality`
- Multi-column: `unique_combination_of_columns`, `mutually_exclusive_ranges`
- Relationships: `relationships_where` (conditional FK checks)
- Ranges: `accepted_range`, `sequential_values`

**dbt-expectations** (80+ tests):
- Statistical: `expect_column_mean_to_be_between`, `expect_column_stdev_to_be_between`
- String patterns: `expect_column_values_to_match_regex`
- Aggregate validation: `expect_column_sum_to_be_between`
- Distribution: `expect_column_values_to_be_within_n_stdevs`
- Multi-column: `expect_column_pair_values_A_to_be_greater_than_B`

### Monte Carlo Observability Concepts (Future Evolution)
**Complementary Approaches** (not duplicating DBT):
- **Anomaly Detection**: ML-based detection of "unknown unknowns" vs rule-based tests
- **Metadata Monitoring**: Freshness/volume checks without querying data
- **Segmentation**: Test metrics sliced by dimensions (detect issues in subgroups)
- **Profiling**: Automatic metric collection (null%, mean, stdev, cardinality) for baseline
- **Lineage-Aware**: Root cause analysis traversing data lineage on test failures

**Future Phase Ideas:**
- Phase 8: Table-level monitors (freshness, volume, schema drift)
- Phase 9: Statistical profiling and baseline generation
- Phase 10: Anomaly detection using historical test result patterns
- Phase 11: Segmented testing (test metrics by category/group)

---

## Design Decisions ✅

### **1. Test Scope: Full DBT Parity**
Implement all 4 core generic tests + singular tests + schema.yml configuration + full CLI (all 7 phases)

### **2. Extended Generic Tests: Include Now**
Add 10-15 most useful dbt-utils style tests in Phase 2:
- `not_empty_string`, `at_least_one`, `not_constant`
- `unique_combination_of_columns`, `mutually_exclusive_ranges`
- `recency`, `equal_rowcount`, `relationships_where`
- `accepted_range`, `sequential_values`

### **3. Test Execution Strategy: All Three Patterns**
- `gorchata test` - Standalone test execution
- `gorchata run --test` - Run models then tests
- `gorchata build` - Orchestrate both (like dbt build)

### **4. Custom Generic Tests: SQL Templates**
Users write SQL files in `tests/generic/` with `{{ arguments }}` placeholders:
```sql
-- tests/generic/test_positive_values.sql
{% test positive_values(model, column_name) %}
SELECT {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} <= 0
{% endtest %}
```

### **5. Test Result Reporting: Console + JSON**
- Colored terminal output for human readability
- `target/test_results.json` for CI/CD integration and tooling

### **6. Performance Strategy: Adaptive Sampling**
- Tables <1M rows: Full scan (accurate)
- Tables ≥1M rows: Random sample (configurable, default 100K rows)
- Users can override with `sample_size: <N>` or `sample_size: null` (force full scan)

### **7. Statistical Testing: Defer to Phase 8+**
Focus on rule-based testing initially; add profiling/anomaly detection after core is stable

### **8. Failure Storage: Test Schema in Same Database**
Store failing rows in `dbt_test__audit` schema (DBT-compatible):
- Tables: `dbt_test__audit.<test_name>`
- Automatic cleanup of old failures (configurable retention, default 30 days)
- Schema created automatically on first test with `store_failures: true`

---

## Implementation Notes

**Architecture Alignment:**
- Follows Gorchata's clean architecture: domain models → executor → CLI
- Pure Go, no CGO (all SQLite via modernc.org/sqlite)
- TDD throughout all phases
- Integrates with existing template engine for {{ ref() }}, {{ source() }} support
- Reuses existing DatabaseAdapter interface
- Adaptive sampling for performance (>1M rows)

**DBT Compatibility:**
- Schema.yml syntax for test configuration
- Test naming conventions match DBT
- Severity levels (error/warn) and conditional thresholds
- Store failures in `dbt_test__audit` schema
- SQL-first approach with template support

**Extensibility:**
- Custom generic tests via SQL templates with {% test %} blocks
- Generic test registry pattern allows adding tests without core changes
- Storage interface for alternate backends
- Foundation for future statistical/anomaly detection capabilities

**Performance:**
- Parallel test execution with goroutines
- Adaptive sampling: full scan <1M rows, sample ≥1M rows
- Metadata-only operations where possible
- Configurable sample sizes per test

**Quality Gates:**
- Each phase includes comprehensive unit tests
- Phase 7 includes end-to-end integration tests
- PowerShell script validation at phase completion
- README must stay synchronized with capabilities

**Cross-Platform:**
- Pure Go implementation works on Windows, Linux, macOS
- PowerShell build/test scripts for Windows
- No external dependencies beyond Go standard library + yaml + sqlite
