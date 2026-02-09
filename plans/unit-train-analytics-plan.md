## Plan: Unit Train Analytics Data Warehouse

Building a comprehensive railroad operations data warehouse that transforms raw Car Location Messages (CLM) in CSV format into structured analytics for unit train performance, straggler tracking, corridor efficiency, cycle time analysis, and inferred power transfer patterns.

**Key Constraints:**
- **2 origins** (can only load 1 train at a time, 12-18 hours per train)
- **3 destinations** (can only unload 1 train at a time, 8-12 hours per train)
- **Origin/destination queuing** creates operational bottlenecks
- **Stragglers** travel independently to destination after 6-hour to 3-day delay, then join next returning train
- **Power inference** from arrival/departure timing (1-hour threshold flags different locomotives)
- **Seasonal effects** (1 corridor slower for 1 week, straggler rate doubles for different week)

**Phases: 9**

### Phase 1: Schema Design and DDL Generation
- **Objective:** Define the star schema structure for unit train analytics including dimension tables (trains, cars, locations, corridors, time), fact tables (car_location_events, train_trips, stragglers, inferred_power_transfers), and aggregated metrics tables
- **Files/Functions to Modify/Create:**
  - `examples/unit_train_analytics/schema.yml`
  - `test/unit_train_analytics_test.go`
  - Generated DDL files in `examples/unit_train_analytics/schema/`
- **Tests to Write:**
  - `TestUnitTrainSchemaValidation` - validates schema YAML structure
  - `TestUnitTrainSchemaParsing` - ensures schema parses correctly
  - `TestUnitTrainDimensionTables` - verifies all required dimension tables exist
  - `TestUnitTrainFactTables` - verifies fact table structure including power inference table
  - `TestUnitTrainCorridorLogic` - validates corridor combination logic (origin + destination + stations + transit_time)
- **Steps:**
  1. Write tests for schema validation and structure
  2. Create schema.yml defining: dim_train, dim_car, dim_location (origins, destinations, stations), dim_corridor, dim_date, fact_car_location_event, fact_train_trip, fact_straggler, fact_inferred_power_transfer
  3. Define CLM event types (train_formed, departed_origin, arrived_station, departed_station, arrived_destination, car_set_out, car_picked_up, train_completed)
  4. Model corridor as combination of origin, destination, intermediate stations sequence, and transit time class (2-day, 3-day, 4-day)
  5. Add fields for origin/destination queue wait times and straggler delay periods
  6. Run tests and verify DDL generation works correctly

### Phase 2: Seed Configuration for CLM Message Generation
- **Objective:** Create seed configuration that generates realistic CSV CLM messages for 3 parallel unit trains with 75 cars each across 6 corridors including straggler events with delays, origin/destination queuing, and seasonal effects
- **Files/Functions to Modify/Create:**
  - `examples/unit_train_analytics/seed.yml`
  - `internal/domain/seed_generator.go` (if CLM-specific patterns needed)
  - `test/unit_train_seed_test.go`
- **Tests to Write:**
  - `TestUnitTrainSeedConfiguration` - validates seed YAML parses
  - `TestCarFleetAllocation` - ensures 225+ cars available (3 trains × 75 cars with buffer)
  - `TestTrainFormationLogic` - validates 75 cars per train constraint
  - `TestOriginDestinationPairs` - verifies 2 origins × 3 destinations = 6 corridors
  - `TestOriginQueueLogic` - ensures only 1 train loading at origin at a time (12-18 hours)
  - `TestDestinationQueueLogic` - ensures only 1 train unloading at destination at a time (8-12 hours)
  - `TestTransitTimeDistribution` - ensures 2-4 day variation with 5-10 stations
  - `TestStragglerDelayRange` - validates straggler delay between 6 hours and 3 days before resuming transit
  - `TestStragglerIndependentTravel` - validates stragglers travel to destination alone after delay, then join next returning train
  - `TestStragglerGeneration` - validates 1 car per train per day in transit (both directions), doubles during specific week
  - `TestParallelTrainOperations` - confirms 3 trains can operate simultaneously
  - `TestSeasonalSlowdown` - validates 1 corridor slower for 1 week
  - `TestCSVFormatOutput` - ensures CLM messages output as valid CSV
- **Steps:**
  1. Write failing tests for seed configuration validation
  2. Define fleet configuration: 250 cars total (225 for operations + 25 buffer), unique car IDs, single car type
  3. Configure 2 origins (e.g., COAL_MINE_A, COAL_MINE_B), 3 destinations (e.g., POWER_PLANT_1, POWER_PLANT_2, PORT_TERMINAL)
  4. Define 6 corridors with varying characteristics (transit days, station counts, distances)
  5. Implement origin queue: only 1 train loading at a time, 12-18 hours loading duration, subsequent trains wait
  6. Implement destination queue: only 1 train unloading at a time, 8-12 hours unloading, subsequent trains wait
  7. Implement train formation rules (wait until 75 cars available at origin, behind any loading train)
  8. Add straggler probability distribution (1 car per train per transit day, random selection)
  9. Add straggler delay period: uniformly distributed between 6 hours and 3 days after set_out event before resuming transit
  10. Implement straggler independent travel to destination (after delay), then joins next returning train from same destination
  11. Configure time windows for analysis (e.g., 90 days of operations)
  12. Add seasonal effects: 1 corridor 20% slower transit for 1 week (week 5), straggler rate doubles for different week (week 8)
  13. Configure CSV output format with headers: car_id, event_type, location_id, timestamp, train_id, loaded_flag, commodity, etc.
  14. Run tests until passing

### Phase 3: CLM Event Generation Logic Including Power Inference
- **Objective:** Implement the seed data generation that produces timestamped CSV CLM messages tracking car movements through the unit train lifecycle, including straggler delay periods and logic to infer power transfers
- **Files/Functions to Modify/Create:**
  - `internal/domain/unit_train_events.go`
  - `internal/domain/straggler_logic.go`
  - `internal/domain/queue_management.go`
  - `test/clm_event_generation_test.go`
- **Tests to Write:**
  - `TestCLMEventSequence` - validates proper event order for normal operations
  - `TestTrainLifecycleEvents` - ensures complete lifecycle: form → load → transit → unload → return → repeat
  - `TestStationEventGeneration` - verifies arrival/departure at 5-10 intermediate stations
  - `TestStragglerDelayPeriod` - validates 6-hour to 3-day delay between set_out and resume transit
  - `TestStragglerEventSequence` - validates set_out → delay period → independent travel → destination arrival → join returning train
  - `TestCarExclusivity` - ensures cars only on one train at a time (except stragglers between set_out and rejoin)
  - `TestTimestampLogic` - validates realistic timing between events
  - `TestEmptyReturnTracking` - ensures empty cars tracked separately with stragglers
  - `TestOriginQueueWaitTimes` - validates trains wait for loading slot
  - `TestDestinationQueueWaitTimes` - validates trains wait for unloading slot
  - `TestPowerInferenceMarkers` - validates data sufficient to infer power changes (departure times with <1 hour gap)
  - `TestSeasonalSlowdownEffect` - validates transit time increase during slow week
  - `TestSeasonalStragglerIncrease` - validates doubled straggler rate during specific week
  - `TestCSVRecordValidity` - ensures all CSV records are properly formatted
- **Steps:**
  1. Write failing tests for CLM event generation
  2. Implement train formation event generation with origin queue awareness (aggregate cars at origin, wait behind loading trains)
  3. Generate loaded transit events: departure_origin → station_arrivals → station_departures → arrival_destination
  4. Add station dwell time variation (2-4 hours for inspection/crew change)
  5. Generate unloading events with destination queue awareness and turnaround
  6. Generate empty return events with similar station stops but faster transit (~70% of loaded time)
  7. Implement straggler logic: randomly select cars for set_out, apply delay period (6 hours to 3 days), generate independent movement events to destination, identify next returning train, generate rejoin event
  8. Add origin turnaround time (empty arrival → loading queue wait → loading → loaded departure)
  9. Implement seasonal slowdown: increase transit time 20% for specific corridor during week 5
  10. Implement seasonal straggler increase: double straggler rate during week 8
  11. Add power inference markers: generate timestamps that allow inference (quick turnarounds <1 hour suggest same power, longer gaps >1 hour suggest different locomotives)
  12. Run tests until all pass
  13. Generate seed data CSV file and verify record counts

### Phase 4: Staging and Dimension Loading Transformations
- **Objective:** Create SQL transformations that load CLM raw CSV events into staging tables and populate dimension tables (trains, cars, locations, corridors, time)
- **Files/Functions to Modify/Create:**
  - `examples/unit_train_analytics/transform_staging.sql`
  - `examples/unit_train_analytics/transform_dimensions.sql`
  - `test/transform_staging_test.go`
  - `test/transform_dimensions_test.go`
- **Tests to Write:**
  - `TestStagingTableLoad` - validates raw CLM CSV data loads correctly
  - `TestDimCarGeneration` - ensures all 250 cars in dimension with single car type
  - `TestDimTrainGeneration` - validates train records created from CLM events with trip-specific IDs
  - `TestDimLocationHierarchy` - ensures origin/station/destination hierarchy
  - `TestDimCorridorCreation` - validates 6 corridor records with proper attributes
  - `TestDimDatePopulation` - ensures date dimension covers analysis period (90 days)
  - `TestCSVParsingLogic` - validates CSV parsing handles all expected formats
- **Steps:**
  1. Write failing tests for staging and dimension loading
  2. Create staging table load script from raw CLM CSV with proper column mapping
  3. Implement dim_car population from distinct car IDs with metadata (single car type, single commodity)
  4. Implement dim_train population with trip-specific IDs (train_id format: ORIGIN_DEST_YYYYMMDD_SEQNUM)
  5. Implement dim_location with hierarchy: location_type (origin/station/destination), parent_location, queue_capacity (1 for origins/destinations)
  6. Implement dim_corridor with origin_id, destination_id, typical_transit_days, station_count, distance_miles
  7. Implement dim_date with calendar attributes, week numbers, and business day flags
  8. Add data quality checks (referential integrity, no nulls in required fields)
  9. Run tests until passing
  10. Execute transformations on generated seed data

### Phase 5: Fact Table Transformations Including Power Inference
- **Objective:** Transform staged CLM events into fact tables for car location events, train trips, straggler tracking with delay analysis, and inferred power transfers
- **Files/Functions to Modify/Create:**
  - `examples/unit_train_analytics/transform_facts.sql`
  - `examples/unit_train_analytics/transform_power_inference.sql`
  - `test/transform_facts_test.go`
- **Tests to Write:**
  - `TestFactCarLocationEvents` - validates event fact grain and foreign keys
  - `TestFactTrainTrips` - ensures one record per complete train trip with derived metrics
  - `TestFactStragglerTracking` - validates straggler identification, delay period (6 hours to 3 days), and impact calculation
  - `TestFactInferredPowerTransfer` - validates power transfer inference logic (1-hour threshold)
  - `TestEventSequenceIntegrity` - ensures logical event sequences per car
  - `TestTransitTimeCalculations` - validates time calculations between events
  - `TestCarTrainAssociation` - ensures cars correctly associated with trains
  - `TestStragglerDelayCalculation` - validates delay period calculation between set_out and resume transit
  - `TestStragglerRejoinLogic` - validates stragglers correctly join returning trains
  - `TestQueueWaitTimeCalculations` - validates origin/destination wait times calculated
  - `TestEmptyVsLoadedSegmentation` - validates direction tracking
- **Steps:**
  1. Write failing tests for fact table transformations
  2. Implement fact_car_location_event transformation: one row per CLM message with dimension foreign keys, derived fields (dwell_minutes, transit_speed, queue_wait_flag)
  3. Implement fact_train_trip transformation using window functions to identify complete trip boundaries (origin departure → destination arrival → origin return)
  4. Calculate trip-level metrics: total_transit_time, loaded_transit_time, empty_return_time, cars_at_formation, cars_at_destination, straggler_count, origin_queue_wait, destination_queue_wait
  5. Implement fact_straggler transformation: identify set_out events, calculate delay period (time between set_out and resume transit, expecting 6 hours to 3 days), track independent travel to destination, identify rejoin event on returning train, calculate total_delay_days
  6. Implement fact_inferred_power_transfer: compare consecutive train departures from same location, flag <1 hour gaps as likely same power, >1 hour as likely different locomotives
  7. Add turnaround time calculations (destination turnaround including queue, origin turnaround including queue)
  8. Implement train velocity calculations (miles per hour excluding dwell and queue time)
  9. Run tests until passing
  10. Execute fact transformations and validate row counts

### Phase 6: Analytical Metrics and Aggregations
- **Objective:** Create pre-aggregated metrics tables and analytical queries for corridor performance, turnaround times, cycle times, straggler impact with delay analysis, queue analysis, power efficiency, and seasonal effects
- **Files/Functions to Modify/Create:**
  - `examples/unit_train_analytics/metrics_corridor.sql`
  - `examples/unit_train_analytics/metrics_turnaround.sql`
  - `examples/unit_train_analytics/metrics_cycle_time.sql`
  - `examples/unit_train_analytics/metrics_straggler_impact.sql`
  - `examples/unit_train_analytics/metrics_queue_analysis.sql`
  - `examples/unit_train_analytics/metrics_power_efficiency.sql`
  - `examples/unit_train_analytics/metrics_seasonal.sql`
  - `test/metrics_test.go`
- **Tests to Write:**
  - `TestCorridorMetricsByMonth` - validates monthly corridor aggregations
  - `TestOriginTurnaroundMetrics` - ensures origin turnaround time calculations including queue wait
  - `TestDestinationTurnaroundMetrics` - ensures destination turnaround time calculations including queue wait
  - `TestCycleTimeMetricsByCorridorMonth` - validates complete cycle time per corridor per month
  - `TestStragglerImpactMetrics` - calculates straggler rate, delay distribution (6 hours to 3 days), and impact
  - `TestQueueBottleneckMetrics` - identifies queue wait time patterns at origins/destinations
  - `TestPowerEfficiencyMetrics` - calculates inferred power transfer frequency and patterns
  - `TestTrainUtilizationMetrics` - calculates cars per trip, trips per month
  - `TestSeasonalEffectMetrics` - validates week 5 slowdown and week 8 straggler increase detected
  - `TestSeasonalityAnalysis` - month-over-month and week-over-week comparisons
- **Steps:**
  1. Write failing tests for analytical metrics
  2. Create agg_corridor_monthly: trips, avg_transit_time, avg_loaded_time, avg_empty_return_time, avg_cycle_time, straggler_rate, avg_queue_wait per corridor per month
  3. Create agg_origin_turnaround: avg/min/max turnaround times (including queue wait) by origin and month
  4. Create agg_destination_turnaround: avg/min/max turnaround times (including queue wait) by destination and month
  5. Create agg_straggler_impact: straggler count, cars affected, delay distribution histogram (6-12 hrs, 12-24 hrs, 1-2 days, 2-3 days), average delay, rejoined trains by corridor and month
  6. Create agg_queue_analysis: queue wait frequency, avg wait time, max wait time by location (origin/destination) and week
  7. Create agg_power_efficiency: inferred power transfers, same-power consecutive trips, different-power consecutive trips by corridor
  8. Create agg_train_velocity: average speeds by corridor and direction (loaded vs empty)
  9. Create agg_cycle_time_details: breakdown of cycle time components (loading, loaded transit, unloading, empty return, origin turnaround, destination turnaround, queue waits)
  10. Create agg_seasonal_effects: week-over-week comparison highlighting week 5 (slow corridor) and week 8 (high straggler rate)
  11. Run tests until passing
  12. Execute metric calculations and verify reasonableness

### Phase 7: Analytical Queries and Reporting Examples
- **Objective:** Provide example analytical queries demonstrating key business questions and insights from the data warehouse including queue bottleneck analysis, straggler delay patterns, and power efficiency
- **Files/Functions to Modify/Create:**
  - `examples/unit_train_analytics/queries/corridor_comparison.sql`
  - `examples/unit_train_analytics/queries/bottleneck_analysis.sql`
  - `examples/unit_train_analytics/queries/straggler_trends.sql`
  - `examples/unit_train_analytics/queries/cycle_time_optimization.sql`
  - `examples/unit_train_analytics/queries/queue_impact.sql`
  - `examples/unit_train_analytics/queries/power_efficiency.sql`
  - `examples/unit_train_analytics/queries/seasonal_patterns.sql`
  - `test/queries_test.go`
- **Tests to Write:**
  - `TestCorridorComparisonQuery` - validates comparative analysis across corridors
  - `TestBottleneckIdentificationQuery` - identifies where delays accumulate (including queues)
  - `TestStragglerTrendAnalysis` - shows straggler patterns over time including week 8 spike and delay distribution
  - `TestCycleTimeDecomposition` - breaks down cycle time by component
  - `TestQueueImpactQuery` - analyzes queue wait times and throughput constraints
  - `TestPowerEfficiencyQuery` - evaluates inferred power utilization patterns
  - `TestFleetUtilizationQuery` - calculates car utilization percentage
  - `TestSeasonalPatternQuery` - identifies week 5 slowdown and week 8 straggler increase
- **Steps:**
  1. Write failing tests that validate query results against expected patterns
  2. Create corridor_comparison.sql: side-by-side metrics for all 6 corridors
  3. Create bottleneck_analysis.sql: identify longest dwell times by location type, queue wait analysis
  4. Create straggler_trends.sql: straggler rate over time with week 8 spike clearly visible, delay period distribution showing 6-hour to 3-day range
  5. Create cycle_time_optimization.sql: decompose cycle time and identify improvement opportunities (queue reduction, faster unloading, straggler delay reduction, etc.)
  6. Create queue_impact.sql: analyze origin/destination queue wait times, identify capacity constraints, calculate throughput losses
  7. Create power_efficiency.sql: analyze inferred power transfers, identify corridors with frequent power swaps vs consistent power
  8. Create seasonal_patterns.sql: week-over-week analysis showing week 5 transit slowdown and week 8 straggler increase
  9. Create fleet_utilization.sql: calculate time in transit vs time idle vs time in queue vs time delayed (stragglers)
  10. Add query documentation explaining business context and expected usage
  11. Run tests until passing
  12. Validate queries return reasonable results against seed data

### Phase 8: Validation and Data Quality Checks
- **Objective:** Implement comprehensive data quality checks to validate car accounting, train integrity, cycle completeness, straggler delay ranges, and operational constraints
- **Files/Functions to Modify/Create:**
  - `examples/unit_train_analytics/validation/car_accounting.sql`
  - `examples/unit_train_analytics/validation/train_integrity.sql`
  - `examples/unit_train_analytics/validation/operational_constraints.sql`
  - `examples/unit_train_analytics/validation/straggler_validation.sql`
  - `test/validation_test.go`
- **Tests to Write:**
  - `TestCarAccountingBalance` - ensures all 250 cars accounted for at all times
  - `TestCarExclusivityValidation` - validates no car on multiple trains simultaneously
  - `TestTrainSizeValidation` - ensures trains form with exactly 75 cars (except stragglers)
  - `TestQueueConstraintValidation` - validates only 1 train loading/unloading at origin/destination at a time
  - `TestStragglerDelayRangeValidation` - ensures all straggler delays between 6 hours and 3 days
  - `TestStragglerRejoinValidation` - ensures all stragglers eventually rejoin a returning train
  - `TestCycleCompletenessValidation` - ensures all trips have complete cycle data
  - `TestTimestampSequenceValidation` - ensures logical timestamp ordering
  - `TestSeasonalEffectValidation` - confirms week 5 and week 8 effects present in data
- **Steps:**
  1. Write failing tests for validation logic
  2. Create car_accounting.sql: verify all cars accounted for at every point in time, no duplicates
  3. Create train_integrity.sql: validate train formation (75 cars), straggler reconciliation, no overlapping assignments
  4. Create operational_constraints.sql: verify origin queue (max 1 loading), destination queue (max 1 unloading), formation thresholds
  5. Create straggler_validation.sql: verify all straggler delay periods fall within 6 hours to 3 days range, all stragglers eventually rejoin
  6. Create cycle_completeness.sql: verify all trips have origin departure, destination arrival, destination departure, origin return
  7. Create timestamp_validation.sql: check for logical event sequences, no backwards time travel, reasonable durations
  8. Create seasonal_validation.sql: confirm week 5 has slower transit for designated corridor, week 8 has 2x straggler rate
  9. Run all validation queries against generated data
  10. Document expected vs actual results
  11. Run tests until passing

### Phase 9: Documentation and Example README
- **Objective:** Create comprehensive documentation explaining the unit train analytics example, schema, metrics, operational constraints, straggler delay analysis, and how to run it
- **Files/Functions to Modify/Create:**
  - `examples/unit_train_analytics/README.md`
  - `examples/unit_train_analytics/METRICS.md`
  - `examples/unit_train_analytics/ARCHITECTURE.md`
  - `FutureExamples.md` (mark as complete)
- **Tests to Write:**
  - `TestReadmeCompleteness` - validates README contains all required sections
  - `TestMetricsDocumentation` - ensures all metrics are documented
  - `TestArchitectureDocumentation` - validates architecture decisions documented
  - `TestExampleRunInstructions` - validates commands in README work
- **Steps:**
  1. Write failing tests for documentation completeness
  2. Create README.md with: business context, schema overview, how to generate data, how to run transformations, example queries, key insights
  3. Document unit train lifecycle and operational concepts (formation, transit, queuing, turnaround)
  4. Explain corridor concept and why it matters
  5. Document straggler tracking (set_out → 6-hour to 3-day delay → independent travel → rejoin logic) and impact on operations
  6. Document queue constraints (1 train at origin/destination) and bottleneck implications
  7. Document power inference methodology (1-hour threshold) and limitations
  8. Document seasonal effects (week 5 slowdown, week 8 straggler spike) and analytical value
  9. Create METRICS.md defining each calculated metric, formula, business meaning, typical ranges
  10. Create ARCHITECTURE.md documenting design decisions: CSV format, queuing logic, straggler reconciliation with delay periods, power inference approach
  11. Add visual schema diagram (text-based ERD or reference to tools)
  12. Document how to extend the example (add more corridors, different train sizes, different queue capacities, different delay ranges, etc.)
  13. Update FutureExamples.md to mark "Unit Train Analytics Data Warehouse" as complete
  14. Run tests until passing
