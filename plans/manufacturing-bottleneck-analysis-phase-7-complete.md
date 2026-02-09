## Phase 7 Complete: Data Quality Tests

Phase 7 implements comprehensive data quality tests using schema.yml (generic tests following DBT pattern) and custom SQL tests (singular tests) to validate data integrity and business rules across all models in the bottleneck analysis example.

**Files created/changed:**
- examples/bottleneck_analysis/models/schema.yml
- examples/bottleneck_analysis/tests/test_operation_lifecycle.sql
- examples/bottleneck_analysis/tests/test_valid_timestamps.sql
- examples/bottleneck_analysis/tests/test_utilization_bounds.sql
- examples/bottleneck_analysis/bottleneck_analysis_test.go (extended with 8 new tests)

**Functions created/changed:**
- TestSchemaYMLExists (validates schema.yml file presence)
- TestSchemaYMLIsValidYAML (validates YAML structure)
- TestSchemaYMLDefinesAllKeyTables (validates coverage of key tables)
- TestSchemaYMLDefinesDataQualityTests (validates test definitions)
- TestCustomSQLTestFilesExist (validates custom SQL test files)
- TestOperationLifecycleTestStructure (validates operation sequence test logic)
- TestValidTimestampsTestStructure (validates timestamp boundary tests)
- TestUtilizationBoundsTestStructure (validates utilization range tests)

**Tests created/changed:**
- 8 new test functions for data quality validation
- Total: 47 tests passing (all phases 1-7)

**Review Status:** APPROVED (after critical column name fixes applied)

**Data Quality Test Coverage:**

**schema.yml Generic Tests (55+ assertions):**
- **Uniqueness** (7 tests): All surrogate and natural keys validated
- **Completeness** (28 tests): Not-null constraints on critical fields
- **Referential Integrity** (7 tests): Foreign key relationships across all fact tables
- **Domain Constraints** (2 tests): Boolean flags constrained to valid values [0,1]

**Tables Covered:**
- dim_resource (8 columns with tests)
- dim_work_order (10 columns with tests)
- fct_operation (14 columns with tests, 5 relationships validated)
- int_resource_daily_utilization (8 columns with tests)
- rollup_bottleneck_ranking (10 columns with tests)

**Custom SQL Tests (Business Rules):**

**test_operation_lifecycle.sql:**
- Sequential operation numbering per work order (increments by 10)
- End timestamps after start timestamps
- Quantity completed ≤ work order quantity
- Positive cycle times
- Returns 0 rows if all rules satisfied

**test_valid_timestamps.sql:**
- No null timestamps in operations
- No timestamps beyond analysis_end_date
- No timestamps before analysis_start_date
- Uses project variables: {{ var("analysis_start_date") }}, {{ var("analysis_end_date") }}

**test_utilization_bounds.sql:**
- utilization_pct between 0-100%
- adjusted_utilization_pct between 0-100%
- No negative values
- No values exceeding 100%

**Critical Fixes Applied:**
- Corrected schema.yml column names to match actual SQL model outputs
- Fixed dim_resource: is_bottleneck → is_bottleneck_candidate
- Fixed dim_work_order: product_code → part_number
- Fixed int_resource_daily_utilization: total_available_minutes → available_minutes_per_day, total_actual_minutes → total_processing_minutes
- Added missing columns across all table definitions
- Moved test_utilization_bounds.sql from tests/generic/ to tests/ (correct location for singular tests)

**Git Commit Message:**
feat: Add comprehensive data quality tests with schema.yml and custom SQL tests

- Add schema.yml defining 55+ generic data quality tests (unique, not_null, relationships, accepted_values)
- Add test_operation_lifecycle.sql validating operation sequences, temporal logic, and quantity constraints
- Add test_valid_timestamps.sql checking timestamp nulls and boundary conditions using project vars
- Add test_utilization_bounds.sql ensuring utilization percentages within 0-100% range
- Cover all key tables: dim_resource, dim_work_order, fct_operation, int_resource_daily_utilization, rollup_bottleneck_ranking
- Validate 7 foreign key relationships for referential integrity
- Fix schema.yml column names to match actual SQL model outputs
- Move test_utilization_bounds.sql to correct location (tests/ not tests/generic/)
- Add 8 test functions to bottleneck_analysis_test.go validating test file structure
- All 47 tests passing
