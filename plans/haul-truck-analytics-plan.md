## Plan: Haul Truck Data Transformation and Analysis

Building a comprehensive open pit mining analytics example that transforms raw heavy-vehicle telemetry (GPS, payload sensors, engine data) into structured haul cycle analytics, tracking productivity from Shovel (loading) to Crusher (dumping) with focus on cycle time analysis, payload utilization, queue bottlenecks, and fleet efficiency metrics.

**Key Domain Constraints:**
- **Haul cycle stages**: Queue at shovel → Loading → Travel loaded → Queue at crusher → Dumping → Travel empty
- **State detection logic**: Use GPS zones + payload thresholds + speed + suspension pressure to identify operational states
- **Multiple fleet sizes**: 100-ton, 200-ton, 400-ton trucks with different performance characteristics
- **Shovel-truck matching**: 3-6 passes to fill truck (match factor optimization)
- **Crusher capacity constraints**: Single crusher bottleneck with queue management
- **Shift operations**: 12-hour shifts with pre-start inspections and handover delays
- **Payload rules**: 85-105% utilization target, overload/underload detection
- **Speed/distance variations**: Different speeds loaded vs empty, baseline road conditions
- **Manned operations**: Operator-driven trucks with performance variance
- **Refueling**: Modeled as spot delays after set engine hours

**Phases: 8**

### Phase 1: Schema Design and DDL Generation
- **Objective:** Define star schema for haul truck analytics including dimensions (truck, shovel, crusher, operator, shift, date), staging tables for raw telemetry, state-detection logic, and fact tables for completed cycles
- **Files/Functions to Modify/Create:**
  - `examples/haul_truck_analytics/schema.yml`
  - `test/haul_truck_schema_test.go`
  - Generated DDL files in `examples/haul_truck_analytics/schema/`
- **Tests to Write:**
  - `TestHaulTruckSchemaValidation` - validates schema YAML structure
  - `TestHaulTruckSchemaParsing` - ensures schema parses correctly
  - `TestHaulTruckDimensionTables` - verifies all required dimensions exist (truck, shovel, crusher, operator, shift, date)
  - `TestHaulTruckStagingTables` - validates raw telemetry staging structure
  - `TestHaulTruckFactTables` - verifies haul cycle fact table and metrics structure
  - `TestPayloadThresholdLogic` - validates loaded/empty state thresholds
- **Steps:**
  1. Write tests for schema validation and structure
  2. Create schema.yml defining dimensions: dim_truck (truck_id, model, payload_capacity_tons, fleet_class), dim_shovel (shovel_id, bucket_size_m3, pit_zone), dim_crusher (crusher_id, capacity_tph), dim_operator (operator_id, experience_level), dim_shift (shift_id, shift_name, start_time, end_time), dim_date
  3. Define staging table: stg_telemetry_events (truck_id, timestamp, gps_lat, gps_lon, speed_kmh, payload_tons, suspension_pressure_psi, engine_rpm, fuel_level_liters, engine_hours, geofence_zone)
  4. Define state detection logic table: stg_truck_states (truck_id, state_start, state_end, operational_state, location_zone, payload_at_start, payload_at_end) where operational_state IN ('queued_at_shovel', 'loading', 'hauling_loaded', 'queued_at_crusher', 'dumping', 'returning_empty', 'spot_delay', 'idle')
  5. Define fact table: fact_haul_cycle (cycle_id, truck_id, shovel_id, crusher_id, operator_id, shift_id, date_id, cycle_start, cycle_end, payload_tons, distance_loaded_km, distance_empty_km, duration_loading_min, duration_hauling_loaded_min, duration_queue_crusher_min, duration_dumping_min, duration_returning_min, duration_queue_shovel_min, duration_spot_delays_min, fuel_consumed_liters, speed_avg_loaded_kmh, speed_avg_empty_kmh)
  6. Run tests and verify DDL generation works correctly

### Phase 2: Seed Configuration for Telemetry Generation
- **Objective:** Create seed configuration that generates realistic CSV telemetry streams for a mixed fleet (12 trucks across 3 size classes) operating with 3 shovels and 1 crusher over 30 days of operations with realistic cycle patterns, queuing behaviors, payload variations, shift handovers, and refueling spot delays
- **Files/Functions to Modify/Create:**
  - `examples/haul_truck_analytics/seed.yml`
  - `internal/domain/telemetry_generator.go` (if haul-specific patterns needed)
  - `test/haul_truck_seed_test.go`
- **Tests to Write:**
  - `TestHaulTruckSeedConfiguration` - validates seed YAML parses
  - `TestFleetComposition` - ensures 12 trucks: 4×100-ton, 6×200-ton, 2×400-ton
  - `TestShovelCapacityMatching` - validates shovel bucket sizes match truck capacities (3-6 passes rule)
  - `TestCrusherSingleBottleneck` - ensures only 1 crusher receiving all trucks
  - `TestPayloadDistribution` - validates payloads range 85-110% of truck capacity with realistic variance
  - `TestCycleTimeRealism` - ensures cycle times 30-90 minutes based on distance and truck class
  - `TestShiftBoundaries` - validates 12-hour shift patterns with handover delays
  - `TestQueueBehavior` - ensures crusher queue forms when arrival rate > dump rate
  - `TestStateTransitionSequence` - validates state progression follows haul cycle logic
  - `TestSpeedByState` - ensures loaded speed < empty speed, queued speed ≈ 0
  - `TestTelemetryFrequency` - validates events generated at 5-10 second intervals
  - `TestRefuelingSpotDelays` - validates refueling events occur after 10-12 engine hours with 15-30 min duration
- **Steps:**
  1. Write tests for seed configuration validation
  2. Define fleet: 4 trucks at 100-ton capacity, 6 at 200-ton, 2 at 400-ton
  3. Define shovels: Shovel A (20m³ bucket → matches 100-ton trucks), Shovel B (35m³ bucket → matches 200-ton trucks), Shovel C (60m³ bucket → matches 400-ton trucks)
  4. Define single crusher with 3000 TPH capacity (creates bottleneck scenarios)
  5. Configure haul routes: 5-8 km loaded haul, 5-8 km empty return (baseline road conditions)
  6. Define shift structure: Day shift (07:00-19:00), Night shift (19:00-07:00) with 30-min handover delays
  7. Configure cycle timing: Loading 4-8 min, Hauling loaded 12-25 min at 20-35 km/h, Dumping 1-2 min, Returning empty 8-18 min at 30-50 km/h
  8. Add queue logic: Shovel queues when >2 trucks arrive within loading time window, Crusher queues when arrival rate exceeds dump rate (3-15 min typical)
  9. Configure payload variance: Normal distribution centered at 95-100% capacity with 5% std dev, occasional underloads (85-90%) and overloads (105-110%)
  10. Add realistic variations: 10% of cycles have spot delays (1-5 min), shift changes reduce productivity 15% for first/last hour
  11. Add refueling logic: Trucks require refueling after 10-12 engine hours, refueling takes 15-30 minutes, modeled as spot_delay state
  12. Add operator variance: Different operators have slightly different performance patterns (cycle times vary ±10%)
  13. Run tests and verify telemetry generation produces valid CSV data

### Phase 3: Cycle Identification Logic (Staging & State Detection)
- **Objective:** Implement SQL transformations that process raw telemetry into identified operational states using geofence zones, payload thresholds, speed patterns, and time-based rules to create state transition records
- **Files/Functions to Modify/Create:**
  - `examples/haul_truck_analytics/models/staging/stg_truck_states.sql`
  - `test/haul_truck_state_detection_test.go`
- **Tests to Write:**
  - `TestLoadingStateDetection` - validates loading identified when in shovel zone + speed <5 km/h + payload increasing
  - `TestHaulingLoadedDetection` - validates hauling state when payload >80% capacity + speed >5 km/h + moving toward crusher
  - `TestQueueAtCrusherDetection` - validates queue when in crusher zone + payload >80% + speed <3 km/h + duration >30 seconds
  - `TestDumpingDetection` - validates dumping when payload drops from >80% to <20% within crusher zone
  - `TestReturningEmptyDetection` - validates empty return when payload <20% + speed >5 km/h + moving toward shovel
  - `TestSpotDelayDetection` - validates spot delays when stopped >2 min outside loading/dumping zones
  - `TestRefuelingDetection` - validates refueling spot delays identified by engine_hours threshold + duration 15-30 min
  - `TestStateTransitionCompleteness` - ensures every telemetry point assigned to exactly one state
  - `TestStateDurationCalculation` - validates duration calculated correctly using window functions
  - `TestAnomalousPatternDetection` - identifies invalid transitions (e.g., loaded→loaded without dump)
- **Steps:**
  1. Write tests for state detection logic
  2. Implement payload threshold rules: Loaded threshold = 80% of truck capacity, Empty threshold = 20% of capacity
  3. Define geofence zones: Shovel zones (3), Crusher zone (1), Haul roads, Other
  4. Create state detection SQL using CASE expressions and window functions (LAG/LEAD) to identify state boundaries
  5. Implement spot delay detection: Stops >2 minutes outside primary work zones
  6. Implement refueling detection: Spot delays occurring after 10-12 engine hours with 15-30 min duration
  7. Implement state transition validation: Enforce valid sequence (queued_at_shovel → loading → hauling_loaded → queued_at_crusher → dumping → returning_empty → cycle repeats, with spot_delay possible at any point)
  8. Calculate state durations using timestamp differences
  9. Run tests and verify state detection accuracy

### Phase 4: Haul Cycle Facts & Duration Calculation
- **Objective:** Aggregate detected states into complete haul cycles, calculating all duration metrics, distances, speeds, payload utilization, and fuel consumption for each trip from shovel to crusher and back
- **Files/Functions to Modify/Create:**
  - `examples/haul_truck_analytics/models/facts/fact_haul_cycle.sql`
  - `test/haul_truck_cycle_facts_test.go`
- **Tests to Write:**
  - `TestCycleCompletenessValidation` - ensures cycles have all required states
  - `TestCycleBoundaryDetection` - validates cycle starts at loading and ends at next loading
  - `TestDurationAggregation` - validates sum of component durations equals total cycle time
  - `TestDistanceCalculation` - validates GPS-based distance calculations for loaded/empty segments
  - `TestSpeedAverageCalculation` - ensures weighted average speeds calculated correctly
  - `TestPayloadUtilizationMetrics` - validates payload % of rated capacity calculated correctly
  - `TestFuelConsumptionAggregation` - ensures fuel consumed summed correctly per cycle
  - `TestSpotDelayAggregation` - validates spot delay durations aggregated correctly per cycle
  - `TestPartialCycleHandling` - validates cycles spanning shift boundaries handled appropriately
- **Steps:**
  1. Write tests for cycle aggregation logic
  2. Implement cycle boundary detection: Identify complete cycles from loading_start to next loading_start
  3. Join state durations to assemble cycle: GROUP BY cycle_id and aggregate state-specific durations
  4. Calculate distances: Use GPS coordinates with haversine formula for loaded/empty segments
  5. Calculate average speeds: Distance / duration for loaded and empty segments separately
  6. Calculate payload metrics: payload_tons, payload_utilization_pct = (payload / truck_capacity) * 100
  7. Calculate fuel consumption: Aggregate fuel level changes across cycle (handle refueling events by excluding fuel increases)
  8. Aggregate spot delay durations: Sum all spot_delay states within cycle
  9. Calculate cycle_time_total_min and component breakdowns
  10. Run tests and verify fact table population

### Phase 5: Metrics Aggregation Tables
- **Objective:** Create aggregated summary tables for truck productivity (daily/shift), shovel utilization, crusher throughput, queue analysis, and fleet performance to support dashboard and reporting requirements
- **Files/Functions to Modify/Create:**
  - `examples/haul_truck_analytics/models/metrics/truck_daily_productivity.sql`
  - `examples/haul_truck_analytics/models/metrics/shovel_utilization.sql`
  - `examples/haul_truck_analytics/models/metrics/crusher_throughput.sql`
  - `examples/haul_truck_analytics/models/metrics/queue_analysis.sql`
  - `examples/haul_truck_analytics/models/metrics/fleet_summary.sql`
  - `test/haul_truck_metrics_test.go`
- **Tests to Write:**
  - `TestTruckDailyProductivityCalculation` - validates tons_moved, cycles_completed, avg_cycle_time per truck per day
  - `TestShovelUtilizationMetrics` - validates loading_hours, idle_hours, utilization_pct per shovel
  - `TestCrusherThroughputCalculation` - validates tons_per_hour, truck_arrivals, avg_queue_time
  - `TestQueueAnalysisMetrics` - validates avg/max queue times, queue_hours_lost per location
  - `TestFleetSummaryRollup` - validates fleet-wide totals and averages
  - `TestPayloadUtilizationDistribution` - validates underload/optimal/overload cycle counts
  - `TestShiftComparisonMetrics` - ensures day vs night shift metrics calculated separately
  - `TestOperatorPerformanceMetrics` - validates operator-level productivity and efficiency metrics
- **Steps:**
  1. Write tests for metrics calculations
  2. Implement truck_daily_productivity: Aggregate cycles by truck_id and date to calculate total tons moved, cycle count, avg cycle time, tons per hour, avg payload utilization, spot delay hours
  3. Implement shovel_utilization: Calculate time shovels spend loading vs idle, truck arrivals, avg loading duration
  4. Implement crusher_throughput: Calculate tons received per hour, truck count, avg dump duration, avg queue time
  5. Implement queue_analysis: Aggregate queue durations by location (shovel/crusher), calculate avg/max queue times, total queue hours, identify peak queue periods
  6. Implement fleet_summary: Roll up fleet-wide metrics by shift and day, calculate overall productivity, utilization, bottleneck indicators
  7. Add payload distribution analysis: Count cycles by utilization bands (<85%, 85-95%, 95-105%, >105%)
  8. Add operator performance metrics: Aggregate by operator_id to compare cycle times, payload utilization, spot delay frequency
  9. Run tests and verify metrics accuracy

### Phase 6: Analytical Queries & Views
- **Objective:** Create business-focused analytical queries and views that answer key operational questions: bottleneck identification, underperforming trucks, payload compliance, shift productivity, fuel efficiency, and operator performance
- **Files/Functions to Modify/Create:**
  - `examples/haul_truck_analytics/models/analytics/worst_performing_trucks.sql`
  - `examples/haul_truck_analytics/models/analytics/bottleneck_analysis.sql`
  - `examples/haul_truck_analytics/models/analytics/payload_compliance.sql`
  - `examples/haul_truck_analytics/models/analytics/shift_performance.sql`
  - `examples/haul_truck_analytics/models/analytics/fuel_efficiency.sql`
  - `examples/haul_truck_analytics/models/analytics/operator_performance.sql`
  - `test/haul_truck_queries_test.go`
- **Tests to Write:**
  - `TestWorstPerformingTrucksQuery` - validates ranking by lowest tons per hour
  - `TestBottleneckAnalysisQuery` - validates identification of constraint (shovel vs crusher queue patterns)
  - `TestPayloadComplianceQuery` - validates underload/overload frequency and patterns
  - `TestShiftPerformanceQuery` - validates productivity comparison day vs night shifts
  - `TestFuelEfficiencyQuery` - validates liters per ton and ton-miles per liter calculations
  - `TestOperatorPerformanceQuery` - validates operator ranking by cycle time and payload utilization
  - `TestQueryResultStructure` - ensures queries return expected columns and data types
- **Steps:**
  1. Write tests for analytical queries
  2. Create worst_performing_trucks: Rank trucks by tons_per_hour, identify trucks with high cycle times or low utilization
  3. Create bottleneck_analysis: Compare avg queue times at shovels vs crusher, calculate utilization rates to identify constraint
  4. Create payload_compliance: Identify trucks/operators with frequent underloading (<85%) or overloading (>105%), calculate compliance percentages
  5. Create shift_performance: Compare day vs night shifts on cycle time, tons moved, queue times to identify shift-specific issues
  6. Create fuel_efficiency: Calculate liters per ton hauled, compare across truck classes and operators
  7. Create operator_performance: Rank operators by cycle time efficiency, payload utilization, spot delay frequency
  8. Add ranking and filtering logic to support operational dashboards
  9. Run tests and verify query outputs

### Phase 7: Data Quality Validation Tests
- **Objective:** Implement comprehensive data quality checks for referential integrity, temporal consistency, business rule violations, state transition validity, and metric reasonableness
- **Files/Functions to Modify/Create:**
  - `examples/haul_truck_analytics/tests/test_referential_integrity.sql`
  - `examples/haul_truck_analytics/tests/test_temporal_consistency.sql`
  - `examples/haul_truck_analytics/tests/test_business_rules.sql`
  - `examples/haul_truck_analytics/tests/test_state_transitions.sql`
  - `test/haul_truck_data_quality_test.go`
- **Tests to Write:**
  - `TestReferentialIntegrity` - validates all foreign keys resolve (truck_id, shovel_id, crusher_id, operator_id exist in dimensions)
  - `TestTemporalConsistency` - validates cycle_end > cycle_start, no overlapping cycles for same truck
  - `TestPayloadBusinessRules` - validates payloads within 0-115% of truck capacity
  - `TestCycleTimeBounds` - validates cycle times within 10-180 minute reasonable range
  - `TestStateTransitionValidity` - validates only valid state sequences occur
  - `TestSpeedReasonableness` - validates speeds <80 km/h, loaded speed < empty speed
  - `TestQueueTimeReasonableness` - validates queue times <120 minutes (catches data errors)
  - `TestFuelConsumptionReasonableness` - validates fuel decreases across cycles (no negative consumption)
  - `TestRefuelingFrequency` - validates refueling occurs at appropriate engine hour intervals
- **Steps:**
  1. Write tests for data quality validations
  2. Implement referential integrity checks: Verify all dimension foreign keys exist
  3. Implement temporal consistency checks: Verify time ordering, no overlaps, no gaps in state coverage
  4. Implement business rule validations: Verify payloads, speeds, durations within expected ranges
  5. Implement state transition validation: Check for invalid sequences, missing states in cycles
  6. Implement metric reasonableness checks: Verify calculated metrics fall within industry benchmarks
  7. Implement refueling validation: Check that refueling events occur at appropriate intervals based on engine hours
  8. Create summary report showing data quality score by check category
  9. Run all quality tests and verify passing

### Phase 8: Documentation & Example Completion
- **Objective:** Create comprehensive README documentation explaining the haul truck domain, data architecture, how to run the example, key metrics, analytical queries, and business insights available from the warehouse
- **Files/Functions to Modify/Create:**
  - `examples/haul_truck_analytics/README.md`
  - `examples/haul_truck_analytics/ARCHITECTURE.md`
  - `examples/haul_truck_analytics/METRICS.md`
  - `FutureExamples.md` (move from Future to Completed)
  - `test/haul_truck_integration_test.go`
- **Tests to Write:**
  - `TestHaulTruckEndToEnd` - runs full pipeline from seed generation through all transformations to final queries
  - `TestDocumentationAccuracy` - validates README instructions work correctly
  - `TestExampleCompleteness` - verifies all expected files exist and are accessible
- **Steps:**
  1. Write end-to-end integration test
  2. Create README.md with: Business context (open pit mining operations), haul cycle explanation with diagram, star schema visualization, how to run the example (go run, scripts), key metrics definitions, analytical query examples, sample insights, troubleshooting tips
  3. Create ARCHITECTURE.md with: Detailed schema description, data flow diagrams (telemetry → staging → states → cycles → metrics), transformation logic explanations, state detection algorithm, cycle aggregation approach, refueling modeling
  4. Create METRICS.md with: Comprehensive KPI definitions (productivity, utilization, queue times, fuel efficiency, payload compliance), calculation formulas, industry benchmarks, interpretation guidance for each metric, operator performance metrics
  5. Update FutureExamples.md: Move "Haul Truck Data Transformation and Analysis" from Future Examples section to Completed Examples section with link to examples/haul_truck_analytics/README.md
  6. Add verification script to run all tests and build example
  7. Run integration test and verify full example works end-to-end
  8. Review documentation for completeness and accuracy
