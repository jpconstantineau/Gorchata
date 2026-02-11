## Plan: OSB Machine Event Data to OEE Analytics

Building a comprehensive Oriented Strand Board (OSB) manufacturing analytics example that transforms raw machine event logs into an integrated OEE (Overall Equipment Effectiveness) and operational intelligence platform, tracking the complete production flow from raw log handling through board pressing and finishing operations with focus on availability losses, performance degradation, quality defects, downtime propagation through process buffers, and constraint identification for targeted capacity improvements.

**Key Domain Constraints:**
- **OSB Production Flow**: Log pond storage → Debarking → Stranding → Green strand bins (buffer) → Drying → Dry fiber silos (buffer) → Screening → Blending with resin → Forming → Mat buffer → Continuous press → Cooling → Sawing → Stacking → Warehouse
- **Machine States**: Running, Idle, Starved (upstream shortage), Blocked (downstream full), Unplanned Downtime (breakdown), Planned Maintenance, Setup/Adjustment, Quality Hold
- **OEE Time Model**: Planned Production Time = Calendar Time - Planned Downtime; Availability = Uptime / Planned Production Time; Performance = (Actual Output / Planned Output); Quality = (Good Output / Total Output); OEE = Availability × Performance × Quality
- **Buffer Capacities**: Green strand bins (~4 hours production), Dry fiber silos (~8 hours production), Mat buffer (~30 minutes production)
- **Critical Equipment**: Primary stranders (2×), Rotary dryer (single-point bottleneck risk), Continuous press (longest cycle time), Saws (multiple for redundancy)
- **Production Metrics**: Panels per hour, Panel thickness (9/16", 7/16", 3/8"), Density (38-42 lbs/ft³), Edge trim waste %
- **Downtime Categories**: Mechanical failures, Electrical/controls, Process upsets (strand bridging, resin issues), Quality holds, Changeovers, Raw material issues
- **Shift Operations**: 3 shifts × 8 hours with 30-minute overlap for handover
- **Quality Standards**: Thickness tolerance ±0.015", Density ±2 lbs/ft³, Delamination testing (random samples)
- **Maintenance Strategy**: Preventive maintenance windows (weekly), Condition-based maintenance triggers, Run-to-failure for non-critical

**Phases: 8**

### Phase 1: Schema Design and DDL Generation
- **Objective:** Define star schema for OSB manufacturing analytics including dimensions (equipment, production area, reason codes mapped to OEE model, shifts, products), staging tables for raw machine events, state duration logic, and fact tables for equipment performance, production output, and quality results
- **Files/Functions to Modify/Create:**
  - `examples/osb_machine_event_oee/schema.yml`
  - `test/osb_oee_schema_test.go`
  - Generated DDL files in `examples/osb_machine_event_oee/schema/`
- **Tests to Write:**
  - `TestOSBSchemaValidation` - validates schema YAML structure
  - `TestOSBSchemaParsing` - ensures schema parses correctly
  - `TestOSBDimensionTables` - verifies all required dimensions exist (equipment, area, reason_code, shift, date, product_spec)
  - `TestOSBStagingTables` - validates raw event staging structure
  - `TestOSBFactTables` - verifies equipment state facts, production facts, and quality facts
  - `TestReasonCodeOEEMapping` - validates reason codes correctly map to Planned/Unplanned and OEE loss categories
  - `TestBufferInventoryTracking` - validates buffer level tracking structure
- **Steps:**
  1. Write tests for schema validation and structure
  2. Create schema.yml defining dimensions:
     - dim_equipment (equipment_id, equipment_name, equipment_type, production_area, ideal_cycle_time_sec, rated_capacity_units_hr, installation_date, criticality_level)
     - dim_production_area (area_id, area_name, sequence_order, upstream_area_id, downstream_area_id, buffer_capacity_hours)
     - dim_reason_code (reason_code_id, reason_code, reason_category, oee_time_model_class [Planned/Unplanned], oee_loss_type [Availability/Performance/Quality], equipment_type, typical_duration_min, maintenance_action_required)
     - dim_shift (shift_id, shift_name, shift_start_time, shift_end_time, crew_size)
     - dim_date (standard date dimension)
     - dim_product_spec (product_id, thickness_inches, density_lbft3, grade, target_thickness, thickness_tolerance_plus, thickness_tolerance_minus, target_density, density_tolerance)
  3. Define staging table: stg_machine_events (event_id, equipment_id, event_timestamp, state, reason_code_id, operator_notes)
  4. Define state history table: stg_equipment_state_history (equipment_id, state_start_timestamp, state_end_timestamp, state_duration_min, machine_state, reason_code_id, shift_id)
  5. Define production tracking: stg_production_output (equipment_id, production_timestamp, output_quantity, product_id, batch_id)
  6. Define quality testing: stg_quality_tests (test_id, batch_id, test_timestamp, test_type, measured_thickness, measured_density, pass_fail, defect_type)
  7. Define buffer monitoring: stg_buffer_levels (buffer_name, timestamp, inventory_level_tons, capacity_utilization_pct, hours_of_supply)
  8. Define fact tables:
     - fact_equipment_state (equipment_id, shift_id, date_id, state_start, state_end, duration_min, machine_state, reason_code_id, oee_loss_category)
     - fact_production_output (production_id, equipment_id, shift_id, date_id, timestamp, quantity, product_id, batch_id, cycle_time_sec, performance_pct)
     - fact_quality_results (quality_id, batch_id, product_id, date_id, panels_produced, panels_tested, panels_passed, panels_downgraded, panels_scrapped, thickness_avg, thickness_stdev, density_avg, density_stdev)
  9. Run tests and verify DDL generation works correctly

### Phase 2: Seed Configuration for Machine Event Generation
- **Objective:** Create seed configuration that generates realistic machine event streams for an OSB plant with 15+ pieces of equipment across 7 production areas, operating over 90 days with realistic state transitions, downtime patterns, buffer dynamics, quality holds, shift handovers, and maintenance windows
- **Files/Functions to Modify/Create:**
  - `examples/osb_machine_event_oee/seed.yml`
  - `internal/domain/osb_event_generator.go` (if OSB-specific patterns needed)
  - `test/osb_seed_test.go`
- **Tests to Write:**
  - `TestOSBSeedConfiguration` - validates seed YAML parses
  - `TestEquipmentInventory` - ensures all equipment defined: 2 debarkers, 2 stranders, 1 dryer, 2 screens, 2 blenders, 1 forming line, 1 press, 1 cooling conveyor, 4 saws
  - `TestProductionFlowSequence` - validates equipment sequence matches OSB process flow
  - `TestDryerAsBottleneck` - ensures dryer capacity constraints modeled (rated for 85% of upstream stranding capacity)
  - `TestBufferCapacityRules` - validates green strand bins (4 hrs), dry fiber silos (8 hrs), mat buffer (30 min)
  - `TestStateTransitionRealism` - ensures valid state transitions (Running→Idle, Running→Starved, Running→Blocked, Running→Breakdown, etc.)
  - `TestDowntimePropagation` - validates that dryer downtime causes strander blocking then starves forming
  - `TestShiftPatterns` - validates 3×8hr shifts with handover delays
  - `TestMaintenanceWindows` - ensures planned maintenance scheduled appropriately
  - `TestQualityHoldEvents` - validates quality issues trigger holds and testing
  - `TestBreakdownMTBFDistribution` - ensures realistic failure intervals (MTBF varies by equipment: critical=200hrs, standard=500hrs)
  - `TestRepairMTTRDistribution` - validates repair durations follow realistic distributions (MTTR varies by failure type: 15min-8hrs)
  - `TestPerformanceLosses` - validates minor stops and speed losses modeled (1-5% performance degradation from ideal)
- **Steps:**
  1. Write tests for seed configuration validation
  2. Define equipment inventory:
     - Log Yard: 2 debarkers (DEBARK-01, DEBARK-02) - 120 logs/hr each
     - Stranding: 2 stranders (STRAND-01, STRAND-02) - 6 tons/hr each
     - Drying: 1 rotary dryer (DRYER-01) - 10 tons/hr (bottleneck at 83% of upstream capacity)
     - Screening: 2 screens (SCREEN-01, SCREEN-02) - parallel processing
     - Blending: 2 blenders (BLEND-01, BLEND-02) - resin application
     - Forming: 1 forming line (FORM-01) - mat formation
     - Pressing: 1 continuous press (PRESS-01) - 18 ft/min press speed, 8-min press time
     - Finishing: 1 cooling conveyor (COOL-01), 4 saws (SAW-01 to SAW-04) - redundant capacity
  3. Define buffer capacities:
     - Green strand bins: 48 tons (4 hours at 12 tons/hr consumption)
     - Dry fiber silos: 80 tons (8 hours at 10 tons/hr consumption)
     - Mat buffer: 5 mats (~30 minutes of press feed)
  4. Configure production targets:
     - Plant target: 700 panels/day (3/8" equivalent basis)
     - Press target cycle time: 8 minutes for 7/16" panels
     - Product mix: 50% 7/16", 30% 3/8", 20% 9/16"
  5. Define downtime patterns:
     - Mechanical failures: Stranders (bearing failures: MTBF=250hrs, MTTR=2-4hrs), Dryer (burner trips: MTBF=300hrs, MTTR=1-2hrs; gear failures: MTBF=2000hrs, MTTR=24hrs), Press (hydraulic leaks: MTBF=400hrs, MTTR=3-6hrs)
     - Process upsets: Strand bridging in bins (15min clears), Resin mix ratio deviation (30-60min to correct), Mat folds (10-20min recovery)
     - Quality holds: Thickness out-of-spec (1-2hr investigation + adjustment), Delamination test failures (4-8hr process review)
  6. Configure shift patterns: Day (06:00-14:00), Swing (14:00-22:00), Night (22:00-06:00) with 30-min overlap handovers
  7. Add planned maintenance: Weekly 8-hour windows on Sunday night shift for critical equipment
  8. Model buffer dynamics:
     - Green strand bins fill when stranders run faster than dryer consumes
     - Dryer outage depletes bins → stranders blocked when bins full
     - Dry fiber silos deplete when forming runs faster than dryer supplies
     - Forming starved when silos reach <10% capacity
     - Mat buffer rarely depletes (press is constraint)
  9. Add quality variation:
     - 2% of panels fail thickness spec (require repress or downgrade)
     - 0.5% fail delamination testing (scrap)
     - Quality holds impact 1-2% of production time
  10. Add performance losses:
     - Minor stops: Automatic stops <5 min (strand bridging, photo-eye glitches) - 2-3% of time
     - Speed losses: Press run at 95-98% of ideal speed due to process variability
  11. Generate realistic event sequences with timestamp precision to 1 second
  12. Run tests and verify event stream generation produces valid CSV data

### Phase 3: State Duration Calculation Logic
- **Objective:** Implement SQL transformations that process discrete machine state events into continuous state history records with calculated durations using window functions (LEAD) to identify state boundaries and compute time intervals
- **Files/Functions to Modify/Create:**
  - `examples/osb_machine_event_oee/models/staging/stg_equipment_state_history.sql`
  - `test/osb_state_duration_test.go`
- **Tests to Write:**
  - `TestStateDurationCalculation` - validates LEAD window function correctly calculates state_end from next state_start
  - `TestStateCompleteness` - ensures every event assigned a duration (handle last event per equipment)
  - `TestStateCategorization` - validates states correctly classified (Running, Idle, Starved, Blocked, etc.)
  - `TestReasonCodeJoin` - ensures reason codes correctly joined and OEE classification applied
  - `TestShiftAssignment` - validates correct shift_id assigned based on timestamp
  - `TestDateAssignment` - validates correct date_id assigned
  - `TestZeroDurationHandling` - ensures instantaneous state changes handled appropriately
  - `TestMultiDayPeriods` - validates state periods spanning midnight correctly split by day
- **Steps:**
  1. Write tests for state duration calculation logic
  2. Implement SQL query structure:
     ```sql
     WITH state_periods AS (
       SELECT 
         equipment_id,
         event_timestamp AS state_start_timestamp,
         LEAD(event_timestamp) OVER (PARTITION BY equipment_id ORDER BY event_timestamp) AS state_end_timestamp,
         state AS machine_state,
         reason_code_id
       FROM stg_machine_events
     )
     SELECT 
       equipment_id,
       state_start_timestamp,
       COALESCE(state_end_timestamp, CURRENT_TIMESTAMP) AS state_end_timestamp,
       DATEDIFF(minute, state_start_timestamp, COALESCE(state_end_timestamp, CURRENT_TIMESTAMP)) AS state_duration_min,
       machine_state,
       reason_code_id,
       -- join to get shift_id based on timestamp
       -- join to get date_id
     FROM state_periods
     ```
  3. Handle edge case: last event per equipment has no end time (use analysis end time or CURRENT_TIMESTAMP)
  4. Split states spanning midnight into separate records for each calendar day
  5. Join to dim_shift to assign shift_id based on state_start_timestamp
  6. Join to dim_date to assign date_id
  7. Run tests and verify state history table populated correctly

### Phase 4: OEE Calculation (Availability, Performance, Quality)
- **Objective:** Implement OEE calculation logic that aggregates equipment state durations and production output to compute Availability Loss, Performance Loss, and Quality Loss, producing daily OEE scores per equipment and shift following standard OEE calculation methodology
- **Files/Functions to Modify/Create:**
  - `examples/osb_machine_event_oee/models/facts/fact_equipment_daily_oee.sql`
  - `test/osb_oee_calculation_test.go`
- **Tests to Write:**
  - `TestPlannedProductionTimeCalculation` - validates Planned Production Time = Calendar Time - Planned Downtime
  - `TestAvailabilityCalculation` - validates Availability = Operating Time / Planned Production Time (excludes unplanned downtime)
  - `TestPerformanceCalculation` - validates Performance = (Actual Output / Ideal Output) where Ideal Output = Operating Time × Ideal Cycle Time
  - `TestQualityCalculation` - validates Quality = Good Output / Total Output (excludes scrap and downgrade)
  - `TestOEECalculation` - validates OEE = Availability × Performance × Quality
  - `TestSixBigLossesClassification` - validates downtime/losses correctly map to Six Big Losses categories
  - `TestEquipmentWithoutProductionHandling` - ensures non-production equipment (conveyors, buffers) handled appropriately
  - `TestMultiShiftAggregation` - validates OEE calculated separately per shift and as daily aggregate
- **Steps:**
  1. Write tests for OEE calculation logic
  2. Calculate Planned Production Time per equipment per day:
     - Start with total calendar time (e.g., 24 hours = 1440 minutes)
     - Subtract planned downtime (maintenance, shift handovers where applicable)
  3. Calculate Availability:
     - Operating Time = Planned Production Time - Unplanned Downtime
     - Unplanned Downtime = SUM(state_duration_min) WHERE reason_code.oee_time_model_class = 'Unplanned'
     - Availability = Operating Time / Planned Production Time
  4. Calculate Performance:
     - Join production output: actual_output = COUNT(panels) for equipment during operating time
     - Calculate ideal output: ideal_output = (Operating Time / Ideal Cycle Time)
     - Performance = actual_output / ideal_output
     - Account for speed losses and minor stops embedded in operating time
  5. Calculate Quality:
     - good_output = panels_passed
     - total_output = panels_produced
     - Quality = good_output / total_output
  6. Calculate OEE:
     - OEE = Availability × Performance × Quality
  7. Classify losses into Six Big Losses:
     - Availability Loss: Equipment Failures (breakdowns), Setup/Adjustments
     - Performance Loss: Minor Stops (<5 min), Reduced Speed
     - Quality Loss: Startup Rejects, Production Rejects
  8. Aggregate by equipment, shift, and day
  9. Run tests and verify OEE calculations match expected results

### Phase 5: Downtime Analysis and Reliability Metrics (MTBF, MTTR)
- **Objective:** Create detailed downtime analysis tables that aggregate failure events by equipment, categorize by failure mode, calculate Mean Time Between Failures (MTBF), Mean Time To Repair (MTTR), failure frequency, and identify chronic reliability issues for maintenance prioritization
- **Files/Functions to Modify/Create:**
  - `examples/osb_machine_event_oee/models/metrics/equipment_downtime_analysis.sql`
  - `examples/osb_machine_event_oee/models/metrics/failure_mode_pareto.sql`
  - `examples/osb_machine_event_oee/models/metrics/equipment_reliability_metrics.sql`
  - `test/osb_downtime_analysis_test.go`
- **Tests to Write:**
  - `TestDowntimeByReasonAggregation` - validates downtime correctly summed by reason code
  - `TestMTBFCalculation` - validates MTBF = Total Operating Time / Number of Failures
  - `TestMTTRCalculation` - validates MTTR = Total Downtime / Number of Failures
  - `TestFailureFrequencyCalculation` - validates failure count and failures per day/week/month
  - `TestChronicFailureIdentification` - validates identification of failure modes occurring >3 times/week
  - `TestParetoAnalysis` - validates failures ranked by cumulative impact (frequency × duration)
  - `TestComparisonToBaseline` - validates current MTBF/MTTR vs historical baseline
  - `TestCriticalEquipmentPrioritization` - validates critical equipment flagged (dryer, press)
- **Steps:**
  1. Write tests for downtime analysis logic
  2. Aggregate downtime events:
     ```sql
     SELECT 
       equipment_id,
       reason_code_id,
       COUNT(*) AS failure_count,
       SUM(state_duration_min) AS total_downtime_min,
       AVG(state_duration_min) AS avg_downtime_per_event_min,
       MIN(state_duration_min) AS min_downtime_min,
       MAX(state_duration_min) AS max_downtime_min
     FROM fact_equipment_state
     WHERE machine_state = 'Unplanned Downtime'
     GROUP BY equipment_id, reason_code_id
     ```
  3. Calculate MTBF per equipment:
     - Total operating time = SUM(duration) WHERE state = 'Running'
     - Failure count = COUNT(*) WHERE state = 'Unplanned Downtime'
     - MTBF = Total operating time / Failure count
  4. Calculate MTTR per equipment and reason code:
     - MTTR = AVG(duration) WHERE state = 'Unplanned Downtime'
  5. Create Pareto analysis of failure modes:
     - Rank reason codes by (failure_count × avg_downtime) DESC
     - Calculate cumulative percentage contribution to total downtime
     - Identify top 20% of failure modes causing 80% of downtime
  6. Identify "bad actor" equipment:
     - Equipment with MTBF < target threshold (e.g., <200 hours for critical, <500 for standard)
     - Equipment with chronic failures (same reason code >3 times/week)
  7. Calculate reliability trends over time (rolling 30-day MTBF/MTTR)
  8. Run tests and verify downtime analysis accuracy

### Phase 6: Buffer and Constraint Analysis
- **Objective:** Implement logic to track buffer inventory levels over time, identify starved and blocked conditions, analyze downtime propagation through production stages, and perform constraint analysis to identify the current system bottleneck and quantify economic impact of capacity improvements
- **Files/Functions to Modify/Create:**
  - `examples/osb_machine_event_oee/models/metrics/buffer_utilization_analysis.sql`
  - `examples/osb_machine_event_oee/models/metrics/starvation_blocking_analysis.sql`
  - `examples/osb_machine_event_oee/models/metrics/constraint_analysis.sql`
  - `test/osb_buffer_constraint_test.go`
- **Tests to Write:**
  - `TestBufferLevelTracking` - validates buffer inventory levels calculated correctly over time
  - `TestBufferCapacityUtilization` - validates % utilization calculated (current_level / capacity)
  - `TestStarvationEventDetection` - validates equipment starved events correlated with upstream buffer depletion
  - `TestBlockingEventDetection` - validates equipment blocked events correlated with downstream buffer full
  - `TestDowntimePropagationAnalysis` - validates dryer outage causes upstream blocking and downstream starvation
  - `TestBufferSizingImpact` - validates analysis of "what if" buffer capacity changes
  - `TestConstraintIdentification` - validates identification of system constraint (equipment with highest utilization + causing most downstream starvation)
  - `TestThroughputCalculation` - validates plant throughput limited by constraint resource
  - `TestCapacityGapAnalysis` - validates quantification of capacity gap (demand vs constraint capacity)
- **Steps:**
  1. Write tests for buffer and constraint analysis
  2. Track buffer inventory over time:
     - Start with buffer capacity definitions from dim_production_area
     - Calculate inflow rate (tons/hr from upstream equipment when running)
     - Calculate outflow rate (tons/hr to downstream equipment when running)
     - Simulate inventory level: inventory(t) = inventory(t-1) + inflow(t) - outflow(t)
     - Flag when buffer reaches capacity (100%) → upstream blocked
     - Flag when buffer depletes (<10%) → downstream starved
  3. Analyze starvation and blocking:
     ```sql
     SELECT 
       equipment_id,
       COUNT(*) AS starved_event_count,
       SUM(state_duration_min) AS total_starved_time_min,
       -- correlate with upstream buffer depletion
     FROM fact_equipment_state
     WHERE machine_state = 'Starved'
     GROUP BY equipment_id
     ```
  4. Perform constraint analysis:
     - Calculate utilization per equipment: utilization = operating_time / available_time
     - Identify equipment with highest utilization (likely constraint)
     - Analyze downstream impact: time downstream starved due to constraint
     - Quantify throughput gap: plant_demand - constraint_capacity
  5. Model "what-if" scenarios:
     - If dryer capacity increased 15%, what is impact on plant throughput?
     - If green strand bin capacity doubled, what is reduction in strander blocking time?
  6. Calculate economic impact:
     - Lost production (tons) = downtime_hours × constraint_capacity_tons_per_hour
     - Lost revenue = lost_production_tons × $/ton selling price
     - Prioritize investments: $/ton capacity increase for each bottleneck option
  7. Run tests and verify constraint identification and buffer analysis

### Phase 7: Advanced Analytics and Improvement Opportunities
- **Objective:** Create advanced analytical views that identify operational improvement opportunities including "bad actor" equipment prioritization for reliability improvements, shift and crew performance comparisons, quality issue root cause analysis, and maintenance strategy optimization recommendations
- **Files/Functions to Modify/Create:**
  - `examples/osb_machine_event_oee/models/analytics/bad_actor_prioritization.sql`
  - `examples/osb_machine_event_oee/models/analytics/shift_performance_comparison.sql`
  - `examples/osb_machine_event_oee/models/analytics/quality_root_cause_analysis.sql`
  - `examples/osb_machine_event_oee/models/analytics/maintenance_strategy_recommendations.sql`
  - `test/osb_advanced_analytics_test.go`
- **Tests to Write:**
  - `TestBadActorScoring` - validates equipment scored by impact (downtime × frequency × criticality)
  - `TestShiftPerformanceComparison` - validates OEE and downtime metrics compared across shifts
  - `TestQualityIssueCorrelation` - validates quality defects correlated with process parameters and equipment states
  - `TestMaintenanceEffectivenessAnalysis` - validates PM vs breakdown maintenance ratio and cost analysis
  - `TestImprovementROICalculation` - validates ROI calculated for proposed improvements (MTBF increase, capacity additions)
  - `TestTrendAnalysis` - validates trending of key metrics (OEE, MTBF, quality rate) over time
- **Steps:**
  1. Write tests for advanced analytics
  2. Implement bad actor prioritization:
     ```sql
     SELECT 
       e.equipment_id,
       e.equipment_name,
       e.criticality_level,
       rm.mtbf_hours,
       rm.mttr_hours,
       da.total_downtime_hours,
       da.failure_count,
       -- Calculate impact score
       (da.total_downtime_hours * da.failure_count * 
        CASE e.criticality_level 
          WHEN 'Critical' THEN 3 
          WHEN 'Important' THEN 2 
          ELSE 1 
        END) AS impact_score
     FROM dim_equipment e
     JOIN equipment_reliability_metrics rm ON e.equipment_id = rm.equipment_id
     JOIN equipment_downtime_analysis da ON e.equipment_id = da.equipment_id
     ORDER BY impact_score DESC
     LIMIT 10
     ```
  3. Implement shift performance comparison:
     - Compare OEE, availability, performance, quality by shift
     - Identify shifts with lower performance (training opportunities)
     - Analyze handover effectiveness (production dip after shift change?)
  4. Implement quality root cause analysis:
     - Correlate quality defects (thickness, density, delamination) with:
       - Equipment states at time of production (was press running at reduced speed?)
       - Process parameters (resin mix ratio, press temperature)
       - Shift and crew
     - Identify patterns: "Thickness issues occur 70% during swing shift" or "Delamination correlates with dryer temperature <350°F"
  5. Implement maintenance strategy analysis:
     - Calculate preventive maintenance effectiveness: % of downtime that is planned vs unplanned
     - Identify equipment running past PM intervals (increased breakdown risk)
     - Analyze cost: PM cost vs breakdown cost × probability
     - Recommend transition from reactive to preventive/predictive for chronic bad actors
  6. Implement improvement opportunity identification:
     - Rank opportunities by ROI:
       - Increase dryer capacity 15% → add $X revenue/year, costs $Y investment
       - Improve strander MTBF from 250hr to 400hr → reduce downtime by Z hours/year → add $revenue
     - Prioritize based on payback period
  7. Add trend analysis:
     - Rolling 30-day OEE trend by equipment
     - MTBF trend (improving or degrading?)
     - Quality defect rate trend
  8. Run tests and verify analytics provide actionable insights

### Phase 8: Documentation, Example Queries, and Visualization Guidance
- **Objective:** Create comprehensive documentation including README with domain overview, data dictionary, example SQL queries for common analytics scenarios, visualization guidance for dashboards (OEE trending, Pareto charts, buffer utilization), and troubleshooting guide for common data quality issues
- **Files/Functions to Modify/Create:**
  - `examples/osb_machine_event_oee/README.md`
  - `examples/osb_machine_event_oee/DATA_DICTIONARY.md`
  - `examples/osb_machine_event_oee/EXAMPLE_QUERIES.md`
  - `examples/osb_machine_event_oee/VISUALIZATION_GUIDE.md`
  - `test/osb_documentation_test.go`
- **Tests to Write:**
  - `TestREADMECompleteness` - validates README includes all required sections
  - `TestDataDictionaryCompleteness` - validates all tables and columns documented
  - `TestExampleQueriesExecutable` - validates all example queries execute without errors
  - `TestVisualizationGuidanceIncludes` - validates visualization guide includes chart type recommendations
  - `TestEndToEndWorkflow` - validates complete workflow from seed generation through analytics execution
- **Steps:**
  1. Write tests for documentation completeness
  2. Create README.md:
     - OSB manufacturing process overview with flow diagram
     - OEE methodology explanation (Availability, Performance, Quality)
     - Project structure explanation
     - Quick start guide: How to generate data, run transformations, query results
     - Key metrics definitions (OEE, MTBF, MTTR, utilization, etc.)
     - Business context: How plant managers would use these analytics
  3. Create DATA_DICTIONARY.md:
     - Document every table: purpose, grain, key columns
     - Document every dimension: attributes and business meaning
     - Document every fact table: measures and foreign keys
     - Document calculated fields: formulas and business rules
  4. Create EXAMPLE_QUERIES.md with common analytics queries:
     - Query 1: Equipment OEE by day and shift
     - Query 2: Top 10 downtime reasons (Pareto analysis)
     - Query 3: Bad actor equipment identification
     - Query 4: Buffer utilization over time
     - Query 5: Constraint analysis (which equipment limits plant throughput?)
     - Query 6: Quality defect correlation analysis
     - Query 7: Shift performance comparison
     - Query 8: Maintenance strategy effectiveness
     - Query 9: Lost production quantification (economic impact)
     - Query 10: Rolling 30-day OEE trend
  5. Create VISUALIZATION_GUIDE.md:
     - Dashboard layout recommendations:
       - Executive Summary: Plant OEE, Daily production vs target, Top 3 issues
       - Equipment Performance: OEE by equipment (heatmap or bar chart), Availability/Performance/Quality breakdown
       - Downtime Analysis: Pareto chart of top downtime reasons, MTBF/MTTR trending
       - Buffer Management: Buffer level time series, Starvation/blocking frequency
       - Quality Dashboard: Defect rate trending, Thickness/density distribution, Root cause analysis
       - Maintenance Dashboard: Bad actor prioritization, PM compliance, Breakdown vs PM ratio
     - Chart type recommendations: Time series for trending, Pareto for prioritization, Heatmaps for shift/equipment matrices, Waterfall for OEE loss analysis
     - Color coding recommendations: Green (>85% OEE), Yellow (70-85%), Red (<70%)
  6. Add troubleshooting section:
     - Common data quality issues: Missing state transitions, Zero-duration states, Orphaned production records
     - How to validate seed data: Expected event counts, State transition validation
     - Performance optimization tips: Indexing strategies, Incremental refresh patterns
  7. Add references:
     - OEE Foundation (www.oee.com)
     - ISA-95 standards for manufacturing operations management
     - Theory of Constraints resources
     - OSB manufacturing process references
  8. Run tests and verify all documentation is complete and accurate

---

## Success Criteria

1. **Schema completeness**: All dimension, staging, and fact tables defined with proper constraints and relationships
2. **Realistic data generation**: Seed configuration produces 90 days of OSB plant operations with realistic failure patterns, buffer dynamics, and quality variations
3. **Accurate OEE calculations**: OEE metrics match manual calculations for sample data
4. **Actionable insights**: Analytics clearly identify bad actors, constraints, and improvement opportunities with quantified economic impact
5. **Downtime propagation modeled**: Demonstrates how upstream failures cause downstream starvation through buffer depletion
6. **Complete documentation**: README, data dictionary, example queries, and visualization guidance enable independent use
7. **All tests passing**: 100% test coverage on calculations (OEE, MTBF, MTTR, buffer logic, constraint identification)

## Notes

- Focus on educational value: explain OEE methodology, buffer dynamics, and constraint theory
- Ensure examples are transferable to other continuous/batch process industries
- Keep seed data realistic but simplified for clarity (don't over-engineer complexity)
- Emphasize maintenance and operations collaboration (reliability data drives maintenance strategy)
- Consider adding "stretch goal" phase for predictive maintenance integration (predict failures based on operating patterns)
