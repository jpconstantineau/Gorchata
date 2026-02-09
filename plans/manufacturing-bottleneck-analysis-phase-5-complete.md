## Phase 5 Complete: Intermediate Transformations - Resource Metrics

Phase 5 builds intermediate rollup tables calculating resource-level aggregations for utilization analysis and downtime tracking. These intermediate tables provide the foundation for bottleneck identification in Phase 6.

**Files created/changed:**
- examples/bottleneck_analysis/models/rollups/int_downtime_summary.sql
- examples/bottleneck_analysis/models/rollups/int_resource_daily_utilization.sql
- examples/bottleneck_analysis/gorchata_project.yml (date configuration alignment)
- examples/bottleneck_analysis/bottleneck_analysis_test.go (extended with 9 new tests)

**Functions created/changed:**
- TestIntermediateTableSQLFilesExist (validates intermediate SQL files)
- TestDowntimeSummaryStructure (validates downtime calculation logic)
- TestDowntimeDurationCalculation (validates JULIANDAY arithmetic)
- TestDowntimeCategorization (validates SCHEDULED/UNSCHEDULED flags)
- TestUtilizationStructure (validates utilization table structure)
- TestUtilizationCalculation (validates utilization percentage formula)
- TestUtilizationPercentageBounds (validates percentage rounding)
- TestOperationTypeFiltering (validates PROCESSING/SETUP filter)
- TestCapacityCalculation (validates available minutes calculation)

**Tests created/changed:**
- 9 new test functions for intermediate table validation
- Total: 28 tests passing (4 Phase 1 + 6 Phase 2 + 3 Phase 3 + 6 Phase 4 + 9 Phase 5)

**Review Status:** APPROVED (after fixing date configuration mismatch)

**Key Features:**

**int_downtime_summary.sql:**
- Grain: One row per downtime event (~35 events)
- Duration calculation: CAST((JULIANDAY(end) - JULIANDAY(start)) × 24 × 60 AS INTEGER)
- Categorization: is_scheduled and is_unscheduled flags
- Foreign keys: resource_key, date_key
- Supports root cause analysis via reason_code

**int_resource_daily_utilization.sql:**
- Grain: One row per resource per day
- Key metrics:
  - total_processing_minutes: SUM of cycle times for PROCESSING and SETUP operations
  - available_minutes_per_day: shifts × hours × 60
  - effective_available_minutes: available - downtime
  - utilization_pct: (processing / effective_available) × 100
  - adjusted_utilization_pct: (processing / available) × 100
- Division-by-zero protection: NULLIF on denominator
- Null handling: COALESCE for resources with no operations
- Date filtering: Uses {{ var "analysis_start_date" }} and {{ var "analysis_end_date" }}

**Configuration Fix:**
- Updated gorchata_project.yml dates from 2026-01-01/2026-03-31 to 2024-01-01/2024-01-14 to match seed data
- Updated SQL to use template variables instead of hardcoded dates

**Git Commit Message:**
feat: Add resource utilization and downtime intermediate rollups

- Add int_downtime_summary.sql with duration calculations and scheduled/unscheduled categorization
- Add int_resource_daily_utilization.sql with dual utilization perspectives (effective vs raw capacity)
- Implement utilization formula: (processing_minutes / effective_available_minutes) × 100
- Add division-by-zero protection with NULLIF and null handling with COALESCE
- Filter PROCESSING and SETUP operations only for utilization calculations
- Update gorchata_project.yml dates to match seed data (2024-01-01 to 2024-01-14)
- Update SQL to use template variables for date filtering
- Add 9 comprehensive tests validating calculations, structure, and edge cases
- All 28 tests passing
