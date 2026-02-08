## Plan: DCS Alarm Analytics with ISA 18.2 Compliance

Create a comprehensive example demonstrating data warehouse design and analytics for refinery DCS (Distributed Control System) alarm and event streams. This example showcases time-series event data transformation, ISA 18.2 alarm management standard compliance metrics, and actionable operational intelligence for process control optimization.

**Implementation Approach:**
- Use Go's text/template library syntax for model templating
- All models materialized as full-refresh tables (daily batch pattern)
- Focus on 2 process areas (C-100 Crude Distillation, D-200 Vacuum Distillation)
- C-100 area: multiple standing alarms demonstrating chronic issues
- D-200 area: excessive operator loading during alarm storm window
- Include dim_time table with pre-computed 10-minute buckets for performance

**Phases: 7**

---

### **Phase 1: Project Structure and Configuration**

**Objective:** Set up the dcs_alarm_example project structure, configuration files, and database initialization following Gorchata patterns.

**Files/Functions to Create:**
- [examples/dcs_alarm_example/gorchata_project.yml](examples/dcs_alarm_example/gorchata_project.yml)
- [examples/dcs_alarm_example/profiles.yml](examples/dcs_alarm_example/profiles.yml)
- [examples/dcs_alarm_example/README.md](examples/dcs_alarm_example/README.md) (initial skeleton)
- [examples/dcs_alarm_example/models/](examples/dcs_alarm_example/models/) directory structure

**Tests to Write:**
- [examples/dcs_alarm_example/dcs_alarm_test.go](examples/dcs_alarm_example/dcs_alarm_test.go) - TestProjectConfigExists, TestDatabaseConnection, TestDirectoryStructure

**Steps:**
1. Create dcs_alarm_example directory under examples/
2. Write Go tests to verify: project config loads, database path resolves, model directories exist
3. Run tests (expect failures - files don't exist yet)
4. Create gorchata_project.yml with name, version, profile, model-paths, and vars (date ranges, ISA thresholds)
5. Create profiles.yml with SQLite connection using environment variable expansion: ${DCS_ALARM_DB:./examples/dcs_alarm_example/dcs_alarms.db}
6. Create models/ subdirectory structure: sources/, dimensions/, facts/, rollups/
7. Create skeleton README with project overview, ISA 18.2 context, and Go text/template usage
8. Run tests until passing
9. Verify project can be loaded by Gorchata CLI

---

### **Phase 2: Source Models with DCS Alarm Event Data**

**Objective:** Create source models containing realistic DCS alarm event streams with inline seed data demonstrating normal alarms, chattering, standing alarms, and alarm storms across two process areas.

**Files/Functions to Create:**
- [examples/dcs_alarm_example/models/sources/raw_alarm_events.sql](examples/dcs_alarm_example/models/sources/raw_alarm_events.sql)
- [examples/dcs_alarm_example/models/sources/raw_alarm_config.sql](examples/dcs_alarm_example/models/sources/raw_alarm_config.sql)

**Tests to Write:**
- TestSourceModelsExist, TestRawAlarmEventsParse, TestAlarmConfigParse
- TestAlarmEventData (verify 30+ events, correct columns, timestamp format)
- TestTwoProcessAreas (verify C-100 and D-200 areas present)

**Steps:**
1. Write Go tests verifying source model files exist and contain Go template config headers
2. Run tests (expect failures)
3. Create raw_alarm_events.sql with:
   - Go template config header: `{{ config(materialized='table') }}`
   - 30+ sample alarm events using inline VALUES statements
   - Events covering: normal alarms, chattering, standing (C-100 area focus), alarm storm (D-200 area focus)
   - Realistic DCS tag names (FIC-101, TIC-205, PSH-310, LIC-115, FIC-220, TAH-210, etc.)
   - C-100 area: 5+ standing alarms with durations >10 minutes
   - D-200 area: concentrated alarm storm creating excessive operator loading (>10 alarms in 10-minute window)
   - Timestamps, priorities (CRITICAL, HIGH, MEDIUM, LOW), states, alarm values
4. Create raw_alarm_config.sql with:
   - Alarm tag configuration metadata (tag descriptions, setpoints, equipment associations)
   - Two process areas: C-100 (Crude Distillation Unit), D-200 (Vacuum Distillation Unit)
   - Equipment types (COLUMN, PUMP, HEAT_EXCHANGER, REACTOR)
5. Add SQL comments documenting alarm scenarios and ISA 18.2 patterns
6. Run tests until passing
7. Build and verify data loads using PowerShell script

---
dates, and time buckets, enabling multi-dimensional analysis with optimized time-based queries.

**Files/Functions to Create:**
- [examples/dcs_alarm_example/models/dimensions/dim_alarm_tag.sql](examples/dcs_alarm_example/models/dimensions/dim_alarm_tag.sql)
- [examples/dcs_alarm_example/models/dimensions/dim_equipment.sql](examples/dcs_alarm_example/models/dimensions/dim_equipment.sql)
- [examples/dcs_alarm_example/models/dimensions/dim_process_area.sql](examples/dcs_alarm_example/models/dimensions/dim_process_area.sql)
- [examples/dcs_alarm_example/models/dimensions/dim_operator.sql](examples/dcs_alarm_example/models/dimensions/dim_operator.sql)
- [examples/dcs_alarm_example/models/dimensions/dim_priority.sql](examples/dcs_alarm_example/models/dimensions/dim_priority.sql)
- [examples/dcs_alarm_example/models/dimensions/dim_dates.sql](examples/dcs_alarm_example/models/dimensions/dim_dates.sql)
- [examples/dcs_alarm_example/models/dimensions/dim_time.sql](examples/dcs_alarm_example/models/dimensions/dim_time.sql)

**Tests to Write:**
- TestDimensionModelsExist, TestDimensionReferences (verify {{ ref }} usage)
- TestAlarmTagDimension (verify SCD Type 2 structure with valid_from/valid_to)
- TestTwoProcessAreas (verify exactly C-100 and D-200 exist)
- TestTimeBuckets (verify 144 10-minute buckets per day, 0-143)

**Steps:**
1. Write Go tests verifying dimension files exist and parse correctly
2. Run tests (expect failures)
3. Create dim_alarm_tag.sql with SCD Type 2 structure:
   - Surrogate key (tag_key), natural key (tag_id), tag_name
   - Equipment and area associations
   - is_safety_critical flag, alarm_type, is_active
   - valid_from, valid_to, is_current for temporal tracking
4. Create dim_equipment.sql with hierarchical structure:
   - Equipment for C-100 and D-200 areas only
   - Equipment types, parent_equipment_id for hierarchy
   - Process area associations
5. Create dim_process_area.sql with two areas:
   - C-100: Crude Distillation Unit (standing alarm focus)
   - D-200: Vacuum Distillation Unit (operator loading focus)
6. Create dim_operator.sql with operator profiles:
   - Operator names, shift assignments, console IDs, experience levels
7. Create dim_priority.sql with alarm priority levels:
   - Priority codes, numeric ordering, response time targets, color codes
8. Create dim_dates.sql with standard date dimension:
   - Date attributes (year, quarter, month, day, day_of_week)
   - Shift calendar attributes
9. Create dim_time.sql with pre-computed time buckets:
   - 144 rows for 10-minute buckets (00:00:00 to 23:50:00)
   - time_bucket_10min (0-143), hour, minute, shift
   - Join optimization for operator loading calculations
10. Use {{ ref "raw_alarm_config" }} where appropriate
11. Run tests until passing
12 - Shift calendar attributes
9. Use {{ ref "raw_alarm_config" }} where appropriate
10. Run tests until passing
11. Build and verify dimensions populate correctly

---

### **Phase 4: Fact Tables for Alarm Occurrences**

**Objective:** Create fact tables capturing alarm lifecycle events with timestamps, durations, operator actions, and derived ISA 18.2 metrics.

**Files/Functions to Create:**
- [examples/dcs_alarm_example/models/facts/fct_alarm_occurrence.sql](examples/dcs_alarm_example/models/facts/fct_alarm_occurrence.sql)
- [examples/dcs_alarm_example/models/facts/fct_alarm_state_change.sql](examples/dcs_alarm_example/models/facts/fct_alarm_state_change.sql)

**Tests to Write:**
- TestFactTablesExist, TestFactTableJoins (verify foreign keys to dimensions)
- TestAlarmDurationCalculations (verify duration_to_ack_sec, duration_to_resolve_sec)
- TestStandingAlarmFlags (verify is_standing_10min, is_standing_24hr)

**Steps:**
1. Write Go tests verifying fact table structure and calculations
2. Run tests (expect failures)
3. Create fct_alarm_occurrence.sql with:
   - Grain: one row per alarm activation/lifecycle
   - Foreign keys to all dimensions using {{ ref }} syntax
   - Activation, acknowledged, and inactive timestamps
   - Calculated metrics: duration_to_ack_sec, duration_to_resolve_sec
   - Boolean flags: is_standing_10min, is_standing_24hr, is_fleeting, is_acknowledged, is_resolved
   - Alarm values and setpoints
   - Point-in-time joins to dim_alarm_tag using temporal validity
4. Create fct_alarm_state_change.sql with:
   - Grain: one row per state transition (for chattering detection)
   - Foreign key to parent alarm occurrence
   - from_state, to_state, change_timestamp
   - sequence_number, time_since_last_change_sec
   - Generate 15 state change events for the chattering alarm examples
5. Add SQL comments documenting join logic and metric calculations
6. Use JULIANDAY for SQLite-compatible duration calculations
7. Run tests until passing
8. Build and verify fact data loads with correct calculations

---

### **Phase 5: ISA 18.2 Metrics - Operator Loading and Alarm Duration**

**Objective:** Create rollup tables calculating operator loading (alarms per hour) and standing alarm duration metrics per ISA 18.2 standards.

**Files/Functions to Create:**
- [examples/dcs_alarm_example/models/rollups/rollup_operator_loading_hourly.sql](examples/dcs_alarm_example/models/rollups/rollup_operator_loading_hourly.sql)
- [examples/dcs_alarm_example/models/rollups/rollup_standing_alarms.sql](examples/dcs_alarm_example/models/rollups/rollup_standing_alarms.sql)

**Tests to Write:**
- TestOperatorLoadingCalculation (verify 10-minute bucketing, ISA categories)
- TestAlarmFloodDetection (verify >10 alarms/10min flagging)
- TestStandingAlarmDuration (verify average, max, total duration calculations)

**Steps:**
1. Write Go tests verifying ISA 18.2 calculation logic
2. Run tests (expect failures)
3. Create rollup_operator_loading_hourly.sql with:
   - Grain: one row per operator per 10-minute bucket
   - Calculate alarm_count per bucket using strftime for time bucketing
   - Derive loading_category: ACCEPTABLE (1-2), MANAGEABLE (3-10), UNACCEPTABLE (>10)
   - Flag is_alarm_flood for >10 alarms in 10 minutes
   - Break down counts by priority (critical, high, medium, low)
   - Calculate avg_time_to_ack_sec, max_time_to_ack_sec
4. Create rollup_standing_alarms.sql with:
   - Grain: one row per tag with standing alarm occurrences
   - Calculate total_standing_duration_hrs, avg_standing_duration_min, max_standing_duration_hrs
   - Count standing alarm episodes per tag
   - Calculate pct_time_in_alarm for chronic standing issues
5. Use {{ ref "fct_alarm_occurrence" }} as source
6. Add comments explaining ISA 18.2 threshold rationale
7. Run tests until passing
8. Build and verify rollup calculations match expected values

---

### **Phase 6: ISA 18.2 Metrics - Chattering, Bad Actors, and System Health**

**Objective:** Create rollup tables identifying chattering alarms, bad actor tags (high frequency offenders), and overall alarm system health summary metrics.

**Files/Functions to Create:**
- [examples/dcs_alarm_example/models/rollups/rollup_chattering_alarms.sql](examples/dcs_alarm_example/models/rollups/rollup_chattering_alarms.sql)
- [examples/dcs_alarm_example/models/rollups/rollup_bad_actor_tags.sql](examples/dcs_alarm_example/models/rollups/rollup_bad_actor_tags.sql)
- [examples/dcs_alarm_example/models/rollups/rollup_alarm_system_health.sql](examples/dcs_alarm_example/models/rollups/rollup_alarm_system_health.sql)

**Tests to Write:**
- TestChatteringDetection (verify >=5 activations in 10 minutes logic)
- TestBadActorRanking (verify Pareto analysis, composite scoring)
- TestSystemHealthMetrics (verify ISA compliance score calculation)

**Steps:**
1. Write Go tests verifying advanced ISA 18.2 analytics
2. Run tests (expect failures)
3. Create rollup_chattering_alarms.sql with:
   - Grain: one row per tag exhibiting chattering behavior
   - Use LAG window function to calculate intervals between activations
   - Detect windows with >=5 activations within 10 minutes
   - Calculate chattering_episode_count, max_activations_per_hour
   - Calculate avg_cycle_time_sec, min_cycle_time_sec
4. Create rollup_bad_actor_tags.sql with:
   - Grain: one row per tag ranked by alarm contribution
   - Calculate total_activations, avg_activations_per_day
   - Perform Pareto ranking: cumulative_pct, percentile_rank
   - Calculate composite bad_actor_score combining frequency, standing duration, chattering
   - Identify is_top_10_pct contributors
   - Include recommended_action field (ADJUST_SETPOINT, ADD_DEADBAND, DISABLE)
5. Create rollup_alarm_system_health.sql with:
   - Grain: one row per day (or shift) for overall system KPIs
   - Calculate avg_alarms_per_hour, peak_alarms_per_10min
   - Calculate pct_time_acceptable, pct_time_manageable, pct_time_unacceptable
   - Count alarm_flood_count, avg_standing_alarms, chattering_tag_count
   - Calculate top_10_contribution_pct (% from worst 10 tags)
   - Compute composite isa_compliance_score (0-100)
6. Use window functions and CTEs for complex calculations
7. Run tests until passing
8. Build and verify all ISA 18.2 metrics calculate correctly

---

### **Phase 7: Documentation, Verification Queries, and Testing**

**Objective:** Complete comprehensive documentation with ISA 18.2 context, schema diagrams, sample queries, and full Go test suite for data integrity verification.

**Files/Functions to Create:**
- [examples/dcs_alarm_example/README.md](examples/dcs_alarm_example/README.md) (complete version)
- [examples/dcs_alarm_example/docs/alarm_schema_diagram.md](examples/dcs_alarm_example/docs/alarm_schema_diagram.md)
- [examples/dcs_alarm_example/verify_alarm_data.sql](examples/dcs_alarm_example/verify_alarm_data.sql)
- Complete test suite in [examples/dcs_alarm_example/dcs_alarm_test.go](examples/dcs_alarm_example/dcs_alarm_test.go)

**Tests to Write:**
- TestDataIntegrity (verify no orphan foreign keys, timestamp ordering)
- TestISAMetricThresholds (verify ISA 18.2 calculations against known sample data)
- TestSampleQueries (verify example queries execute and return expected results)

**Steps:**
1. Write Go tests for data integrity and query validation
2. Run tests (expect failures initially)
3. Expand README.md with:
   - DCS/SCADA background and ISA 18.2 standard overview
   - Business value of alarm analytics
   - Detailed schema explanation (sources → dimensions → facts → rollups)
   - ISA 18.2 metrics definitions (operator loading, standing, chattering, bad actors)
   - How to run the example (go commands, PowerShell scripts)
   - Configuration (environment variables, date range parameters)
   - Troubleshooting section
4. Create docs/alarm_schema_diagram.md with:
   - ERD showing table relationships and cardinality
   - Column-level documentation for each table
   - Explanation of grain for each fact/rollup table
5. Create verify_alarm_data.sql with sample queries:
   - Top 10 bad actor tags by alarm frequency
   - Daily ISA compliance scorecard
   - Alarm storm event analysis (Feb 7, 08:00-08:08)
   - Chattering alarm episodes for TIC-205
   - Operator performance comparison
   - Standing alarm duration by equipment
   - Hourly alarm loading trend chart data
6. Enhance dcs_alarm_test.go with:
   - TestModelDependencies (verify build order via {{ ref }} dependencies)
   - TestAlarmEventSequencing (verify acknowledged >= activation timestamps)
   - TestChatteringTagIdentification (verify TIC-205 and LIC-115 flagged)
   - TestBadActorTop10 (verify top contributors correctly ranked)
  Design Decisions:**

1. **Templating:** Use Go's text/template library syntax (matching Gorchata's implementation)
2. **Refresh Pattern:** All models materialize as full-refresh tables (daily batch pattern for this example)
3. **Time Dimension:** Include dim_time table with 144 pre-computed 10-minute buckets for join optimization
4. **Metric Scope:** Focus on four core ISA 18.2 metrics (operator loading, standing duration, chattering, bad actors)
5. **Process Areas:** Two areas with distinct characteristics:
   - C-100 (Crude Distillation): Multiple standing alarms demonstrating chronic issues
   - D-200 (Vacuum Distillation): Concentrated alarm storm with excessive operator loading
6. **Rationalization:** Not included in this example to maintain focus on core analytics
7. **Sample Data Volume:** 30+ alarm events across 2 days demonstrating all ISA 18.2 patterns

2. **Time Dimension Granularity:** Do you want a separate dim_time table with pre-computed 10-minute buckets, or is in-query bucketing using strftime() sufficient?

3. **Additional ISA Metrics:** Should we include stale alarms (configured but never activated) and fleeting alarms (<2 seconds active) in Phase 6, or keep the scope focused on the four main metrics requested?

4. **Alarm Rationalization Workflow:** Should we include fields and sample data demonstrating alarm rationalization status tracking (last_review_date, rationalization_status), or is this beyond the scope of the initial example?

5. **Multiple Process Areas:** Should the sample data include alarms from all four process areas (C-100, D-200, E-300, F-400), or focus on 1-2 areas to keep the example more focused?
