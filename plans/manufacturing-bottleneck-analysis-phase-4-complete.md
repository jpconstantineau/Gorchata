## Phase 4 Complete: Fact Table - Operation Events

Phase 4 creates the core fact table capturing granular operation events with calculated metrics for queue time and cycle time. This fact table provides the detailed data needed to identify bottlenecks through WIP accumulation and processing delays.

**Files created/changed:**
- examples/bottleneck_analysis/models/facts/fct_operation.sql
- examples/bottleneck_analysis/bottleneck_analysis_test.go (extended with 6 new tests)

**Functions created/changed:**
- TestFactTableSQLFilesExist (validates fact table file existence)
- TestFactOperationSQLStructure (validates SQL syntax and structure)
- TestFactOperationForeignKeys (validates dimension key joins)
- TestFactOperationQueueTimeCalculation (validates LAG window function logic)
- TestFactOperationCycleTimeCalculation (validates timestamp arithmetic)
- TestFactOperationMeasures (validates quantity measures)

**Tests created/changed:**
- 6 new test functions for fact table validation
- Total: 23 tests passing (4 Phase 1 + 6 Phase 2 + 3 Phase 3 + 6 Phase 4 + 4 dimension subtests)

**Review Status:** APPROVED

**Key Fact Table Features:**
- Grain: One row per operation per work order (~255 operations expected)
- Foreign Keys: 5 dimension references (resource, work_order, part, start_date, end_date)
- Calculated Metrics:
  - queue_time_minutes: Uses LAG() window function to calculate wait time before processing
  - cycle_time_minutes: Processing duration from start to end timestamp
- Queue Time Logic: Handles first operation specially (uses work order release_timestamp), subsequent operations use previous operation end_timestamp
- Timestamp Arithmetic: JULIANDAY conversion with proper minute calculation (× 24 × 60)

**SQL Design Pattern:**
- Multi-stage CTE transformation: operations_source → operations_with_arrival → operations_with_queue → fact_with_keys
- LAG() window function: PARTITION BY work_order_id ORDER BY operation_seq
- COALESCE for first operation handling
- All dimensions joined using LEFT JOIN on natural keys

**Git Commit Message:**
feat: Add fact table for operation-level bottleneck analysis

- Add fct_operation.sql with operation event grain and dimension foreign keys
- Implement queue_time_minutes calculation using LAG() window function for wait time analysis
- Implement cycle_time_minutes calculation for processing duration tracking
- Handle first operation specially (queue from work order release vs previous operation end)
- Join to 5 dimensions (resource, work_order, part, start_date, end_date) for analytical context
- Add 6 comprehensive tests validating SQL structure, foreign keys, and metric calculations
- All tests passing (23 total across phases 1-4)
