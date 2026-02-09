## Plan: Manufacturing Bottleneck Analysis Example

Build a comprehensive data warehouse example modeling the UniCo plant from "The Goal", demonstrating Theory of Constraints principles through synthetic manufacturing data. The example will identify bottlenecks by tracking work order flow, cycle times, queue accumulation, and resource utilization across critical departments (NCX-10 and Heat Treat).

**Phases: 8**

1. **Phase 1: Project Setup and Configuration**
   - **Objective:** Create the foundational project structure, configuration files, and directory hierarchy following Gorchata patterns.
   - **Files/Functions to Modify/Create:**
     - `examples/bottleneck_analysis/gorchata_project.yml`
     - `examples/bottleneck_analysis/profiles.yml`
     - `examples/bottleneck_analysis/README.md` (initial version)
     - Directory structure: `seeds/`, `models/`, `tests/`, `docs/`
   - **Tests to Write:**
     - `bottleneck_analysis_test.go` (test file structure and configuration loading)
   - **Steps:**
     1. Write test to verify project structure exists
     2. Create `examples/bottleneck_analysis/` directory
     3. Create `gorchata_project.yml` with project name "bottleneck_analysis", version 1.0, paths configuration (model-paths, seed-paths, test-paths)
     4. Create `profiles.yml` with SQLite database configuration
     5. Create subdirectories: `seeds/`, `models/{sources,dimensions,facts,rollups}/`, `tests/generic/`, `docs/`
     6. Create initial `README.md` with project overview, Theory of Constraints context, and UniCo plant description
     7. Run test to confirm configuration loads correctly

2. **Phase 2: Seed Data - Raw Manufacturing Events**
   - **Objective:** Generate realistic synthetic seed data representing work orders, operations, resources, and downtime events for the UniCo plant.
   - **Files/Functions to Modify/Create:**
     - `examples/bottleneck_analysis/seeds/seed.yml`
     - `examples/bottleneck_analysis/seeds/raw_work_orders.csv`
     - `examples/bottleneck_analysis/seeds/raw_operations.csv`
     - `examples/bottleneck_analysis/seeds/raw_resources.csv`
     - `examples/bottleneck_analysis/seeds/raw_downtime.csv`
   - **Tests to Write:**
     - `test_seed_data_validity` (verify CSV structure and basic constraints)
   - **Steps:**
     1. Write tests to verify seed data can be loaded and has expected columns
     2. Create `seed.yml` with import configuration (batch_size: 1000, naming_strategy: filename)
     3. Create `raw_resources.csv` with UniCo resources:
        - Include: NCX-10 (CNC), Heat Treat (Furnace), Milling, Assembly, Grinding, QA Inspection
        - Columns: resource_id, resource_name, resource_type, available_hours_per_shift, shifts_per_day, theoretical_capacity_per_hour
     4. Create `raw_work_orders.csv` with ~50 work orders:
        - Columns: work_order_id, part_number, quantity, release_timestamp, due_timestamp, priority, status
        - Include various part types requiring different routing paths
     5. Create `raw_operations.csv` with ~300 operation events:
        - Columns: operation_id, work_order_id, operation_seq, resource_id, operation_type (SETUP/PROCESSING/MOVE), start_timestamp, end_timestamp, quantity_completed, quantity_scrapped
        - Model realistic flow where NCX-10 and Heat Treat show queue buildup patterns
        - Include statistical fluctuations in processing times
     6. Create `raw_downtime.csv` with ~30 downtime events:
        - Columns: downtime_id, resource_id, downtime_start, downtime_end, downtime_type (SCHEDULED/UNSCHEDULED), reason_code
        - Higher downtime frequency at NCX-10 (bottleneck characteristic)
     7. Run tests to verify seed data structure and basic validity

3. **Phase 3: Dimension Tables** ✅ **COMPLETE**
   - **Objective:** Build dimension tables providing business context for resources, work orders, parts, and time hierarchies.
   - **Files/Functions to Modify/Create:**
     - `examples/bottleneck_analysis/models/dimensions/dim_resource.sql` ✅
     - `examples/bottleneck_analysis/models/dimensions/dim_work_order.sql` ✅
     - `examples/bottleneck_analysis/models/dimensions/dim_part.sql` ✅
     - `examples/bottleneck_analysis/models/dimensions/dim_date.sql` ✅
   - **Tests to Write:**
     - `test_dim_resource_uniqueness` (verify resource_key uniqueness) ✅
     - `test_dim_work_order_integrity` (verify work order attributes) ✅
   - **Steps:**
     1. Write tests expecting dimension tables to exist with surrogate keys ✅
     2. Create `dim_resource.sql`: ✅
        - Surrogate key: resource_key (ROW_NUMBER())
        - Natural key: resource_id
        - Attributes: resource_name, resource_type, capacity metrics, is_current flag
        - Calculated fields: daily_capacity, is_bottleneck_candidate
        - Source from: `{{ seed "raw_resources" }}`
     3. Create `dim_work_order.sql`: ✅
        - Surrogate key: work_order_key
        - Natural key: work_order_id
        - Attributes: part_number, quantity, priority, release_date, due_date
        - Calculated fields: lead_time_days, priority_rank
        - Source from: `{{ seed "raw_work_orders" }}`
     4. Create `dim_part.sql`: ✅
        - Surrogate key: part_key
        - Natural key: part_number
        - Attributes: part_description, part_family, routing_complexity
        - Derived from distinct parts in work orders
     5. Create `dim_date.sql`: ✅
        - Date dimension with calendar hierarchy using recursive CTE
        - Attributes: date_key (YYYYMMDD), full_date, day_of_week, week_number, month, quarter, year, is_weekend, is_holiday
        - Generate dates covering the analysis period (2024-01-01 to 2024-01-31)
     6. Run tests to verify dimension table integrity ✅

4. **Phase 4: Fact Table - Operation Events**
   - **Objective:** Create the core fact table capturing granular operation events with calculated metrics like queue time and cycle time.
   - **Files/Functions to Modify/Create:**
     - `examples/bottleneck_analysis/models/facts/fct_operation.sql`
   - **Tests to Write:**
     - `test_fct_operation_grain` (verify one row per operation)
     - `test_fct_operation_referential_integrity` (verify foreign keys to dimensions)
     - `test_fct_operation_queue_time_calculation` (verify queue time logic)
   - **Steps:**
     1. Write tests expecting fact table with correct grain and calculated metrics
     2. Create `fct_operation.sql`:
        - Grain: One row per operation per work order
        - Foreign keys: resource_key, work_order_key, part_key, start_date_key, end_date_key
        - Measures: 
          - `cycle_time_minutes` = TIMESTAMPDIFF(start_timestamp, end_timestamp)
          - `queue_time_minutes` = time between arrival and start (use LAG window function)
          - `quantity_completed`, `quantity_scrapped`
        - Attributes: operation_seq, operation_type
        - Source from: `{{ seed "raw_operations" }}`, join to dimensions
     3. Implement queue time calculation using window functions:
        - Partition by work_order_id, ORDER BY operation_seq
        - LAG(end_timestamp) OVER (partition) to get previous operation completion
        - Queue time = start_timestamp - LAG(end_timestamp)
     4. Handle first operation (queue time = start_timestamp - work_order_release_timestamp)
     5. Run tests to verify fact table correctness and metric calculations

5. **Phase 5: Intermediate Transformations - Resource Metrics**
   - **Objective:** Build intermediate tables calculating resource-level aggregations: utilization, throughput rates, and downtime summaries.
   - **Files/Functions to Modify/Create:**
     - `examples/bottleneck_analysis/models/rollups/int_resource_daily_utilization.sql`
     - `examples/bottleneck_analysis/models/rollups/int_downtime_summary.sql`
   - **Tests to Write:**
     - `test_utilization_calculation` (verify utilization formula correctness)
     - `test_utilization_range` (verify utilization between 0-100%)
   - **Steps:**
     1. Write tests expecting utilization metrics calculated correctly
     2. Create `int_resource_daily_utilization.sql`:
        - Grain: One row per resource per day
        - Calculate: 
          - `total_processing_minutes` = SUM(cycle_time_minutes) from fct_operation
          - `available_minutes` = shifts_per_day × hours_per_shift × 60
          - `utilization_pct` = (total_processing_minutes / available_minutes) × 100
        - Group by: resource_key, date_key
        - Source from: `{{ ref "fct_operation" }}`, `{{ ref "dim_resource" }}`
     3. Create `int_downtime_summary.sql`:
        - Grain: One row per downtime event
        - Calculate: downtime_duration_minutes
        - Categorize: scheduled vs unscheduled
        - Source from: `{{ seed "raw_downtime" }}`
     4. Adjust utilization calculation to account for downtime:
        - `effective_available_minutes` = available_minutes - downtime_minutes
        - `adjusted_utilization_pct` = (total_processing_minutes / effective_available_minutes) × 100
     5. Run tests to verify utilization calculations

6. **Phase 6: Bottleneck Analytics Rollups**
   - **Objective:** Create analytical rollup tables specifically designed to identify and visualize bottlenecks through WIP accumulation, queue patterns, and utilization rankings.
   - **Files/Functions to Modify/Create:**
     - `examples/bottleneck_analysis/models/rollups/rollup_wip_by_resource.sql`
     - `examples/bottleneck_analysis/models/rollups/rollup_queue_analysis.sql`
     - `examples/bottleneck_analysis/models/rollups/rollup_bottleneck_ranking.sql`
   - **Tests to Write:**
     - `test_wip_accumulation_logic` (verify WIP counting)
     - `test_bottleneck_identification` (verify NCX-10 and Heat Treat rank highest)
   - **Steps:**
     1. Write tests expecting bottleneck identification logic to highlight NCX-10 and Heat Treat
     2. Create `rollup_wip_by_resource.sql`:
        - Grain: One row per resource per hour
        - Calculate: count of work orders with status IN ('QUEUED', 'IN_PROCESS') at each resource
        - Identify accumulation patterns over time
        - Source from: `{{ ref "fct_operation" }}`
     3. Create `rollup_queue_analysis.sql`:
        - Grain: One row per resource (summary)
        - Calculate:
          - `avg_queue_time_minutes`
          - `max_queue_time_minutes`
          - `median_queue_time_minutes`
          - `total_operations_processed`
        - Rank resources by queue time
        - Source from: `{{ ref "fct_operation" }}`
     4. Create `rollup_bottleneck_ranking.sql`:
        - Grain: One row per resource (summary)
        - Combine metrics:
          - Average utilization (from int_resource_daily_utilization)
          - Average queue time (from rollup_queue_analysis)
          - WIP accumulation score (from rollup_wip_by_resource)
        - Calculate composite bottleneck score: `(utilization_pct × 0.4) + (avg_queue_time × 0.3) + (wip_score × 0.3)`
        - Rank resources; top ranks indicate bottlenecks
        - Include flags: `is_potential_bottleneck` (utilization > 85% OR avg_queue_time > threshold)
     5. Run tests to verify NCX-10 and Heat Treat identified as bottlenecks

7. **Phase 7: Data Quality Tests**
   - **Objective:** Implement comprehensive data quality tests using schema.yml (generic tests) and custom SQL tests (singular tests) to validate data integrity and business rules.
   - **Files/Functions to Modify/Create:**
     - `examples/bottleneck_analysis/models/schema.yml`
     - `examples/bottleneck_analysis/tests/test_operation_lifecycle.sql`
     - `examples/bottleneck_analysis/tests/test_valid_timestamps.sql`
     - `examples/bottleneck_analysis/tests/generic/test_utilization_bounds.sql`
   - **Tests to Write:**
     - Generic tests in schema.yml for all tables
     - Custom SQL tests for domain-specific validations
   - **Steps:**
     1. Write schema.yml structure expecting all tables and tests to be defined
     2. Create `schema.yml` with generic tests:
        - For dimensions: test uniqueness of surrogate keys, not_null on natural keys
        - For fct_operation: test referential integrity (foreign keys exist in dimensions), not_null on required fields, accepted_range for metrics (e.g., cycle_time > 0)
        - For rollups: test not_null on calculated metrics, utilization_pct between 0-100
     3. Create `test_operation_lifecycle.sql`:
        - Validate operation sequence integrity (operations numbered sequentially per work order)
        - Validate temporal logic (end_timestamp > start_timestamp)
        - Validate quantity completed ≤ work order quantity
     4. Create `test_valid_timestamps.sql`:
        - Check for null timestamps
        - Check for timestamps in the future
        - Check for timestamps before project start date
     5. Create `test_utilization_bounds.sql`:
        - Ensure utilization percentages are between 0 and 100
        - Flag any resources with utilization > 100% (data quality issue)
     6. Run all tests and verify they pass (or identify expected failures for edge cases)

8. **Phase 8: Documentation and Verification**
   - **Objective:** Complete comprehensive documentation including schema diagrams, Theory of Constraints explanations, how-to-run instructions, and verification queries for manual validation.
   - **Files/Functions to Modify/Create:**
     - `examples/bottleneck_analysis/README.md` (complete version)
     - `examples/bottleneck_analysis/docs/schema_diagram.md`
     - `examples/bottleneck_analysis/verify_bottleneck_analysis.sql`
   - **Tests to Write:**
     - `test_documentation_exists` (verify key documentation files present)
   - **Steps:**
     1. Write test expecting documentation files to exist
     2. Update `README.md` with complete content:
        - Overview of Theory of Constraints and The Goal context
        - UniCo plant description (NCX-10, Heat Treat, workflow)
        - Key metrics explained (throughput, WIP, utilization, queue time)
        - How to run the example (commands, configuration)
        - Directory structure explanation
        - Expected outputs and how to interpret bottleneck rankings
        - References and further reading
     3. Create `docs/schema_diagram.md`:
        - ERD diagram (ASCII art) showing dimensions, facts, and rollups
        - Data flow diagram: Seeds → Dimensions/Facts → Intermediate → Rollups → Analytics
        - Table specifications with column details
        - Grain definitions for each table
        - Calculation formulas explained
     4. Create `verify_bottleneck_analysis.sql`:
        - Query 1: Top 5 resources by utilization
        - Query 2: Resources with highest average queue times
        - Query 3: WIP accumulation over time (visual data)
        - Query 4: Final bottleneck ranking showing NCX-10 and Heat Treat at top
        - Query 5: Throughput comparison (before/after hypothetical bottleneck elevation)
     5. Run tests to verify documentation exists
     6. Manually run verification queries to confirm expected results

**Implementation Decisions:**

1. **Time Period**: 2 weeks of operations data (sufficient pattern visibility without excessive volume)
2. **Shift Configuration**: 2 shifts/day, 5 days/week (realistic manufacturing pattern)
3. **Product Mix**: 10-15 distinct part numbers with varying routing requirements
4. **Visualization**: README will include descriptions of expected outputs and queries to pull data for external visualization tools
