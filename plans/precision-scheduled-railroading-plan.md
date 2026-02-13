## Plan: Precision Scheduled Railroading Performance Monitoring

Comprehensive data warehouse implementation for PSR performance analytics, transforming 10 years of CLM event data into actionable insights through state-interval models, velocity analysis, dwell classification, and network fluidity metrics. This example demonstrates advanced railroad operations analytics with shadow yard detection, slot adherence scoring, and directional asymmetry analysis.

**Configuration:**
- **Time granularity**: Minute-level CLM events for maximum operational realism
- **Fleet size**: 12,000 railcars across 200 locations
- **PSR adoption**: Gradual implementation 2018-2020 showing strategy evolution
- **Shadow yards**: 5-7 subtle cases requiring analytical detection
- **Seasonal effects**: 25% performance variation (moderate intensity)

**Phases: 10**

### Phase 1: Project Setup & Seed Data Generation
- **Objective:** Establish project structure and generate realistic CLM event data covering 10 years (2016-2025) with seasonal variations, gradual PSR strategy shifts, and operational anomalies
- **Files/Functions to Modify/Create:**
  - `examples/precision_railroading/README.md` (initial overview)
  - `examples/precision_railroading/gorchata_project.yml` (project configuration)
  - `examples/precision_railroading/profiles.yml` (database profiles)
  - `examples/precision_railroading/generate_clm_data.go` (CLM data generator)
  - `examples/precision_railroading/seeds/raw_clm_events.csv` (raw events)
  - `examples/precision_railroading/seeds/clm_generation_config.yml` (generator config)
  - `examples/precision_railroading/seeds/seed.yml` (seed configuration)
- **Tests to Write:**
  - `TestCLMDataGenerator` (validates event generation logic)
  - `TestEventTypeDistribution` (verifies DEPA, ARRI, PULL, PLAC ratios)
  - `TestTemporalConsistency` (ensures chronological ordering with minute precision)
  - `TestSeasonalVariation` (confirms 25% winter slowdowns, summer peaks)
  - `TestPSRGradualAdoption` (validates 2018-2020 transition metrics)
- **Steps:**
  1. Write tests for CLM data generation (event types, temporal ordering, seasonal patterns, PSR evolution)
  2. Run tests to confirm failures
  3. Create project directory structure (models/, seeds/, tests/, docs/)
  4. Implement gorchata_project.yml and profiles.yml
  5. Implement generate_clm_data.go with minute-level CLM event generation logic (DEPA, ARRI, PULL, PLAC events)
  6. Generate 10 years of CLM data with 12,000 railcars, 200 locations, 25% seasonal effects
  7. Model gradual PSR adoption (2016-2017 pre-PSR baseline, 2018-2020 transition, 2021-2025 mature PSR)
  8. Include 5-7 subtle shadow yard patterns in location dwell characteristics
  9. Run tests to confirm generation logic passes
  10. Execute generator to create raw_clm_events.csv
  11. Build and verify seed data loads successfully

### Phase 2: Dimension Tables (Location, Railcar, Train, Corridor, Date)
- **Objective:** Create dimensional foundation with SPLC-enriched locations, railcar fleet (12K cars), train consists, corridor definitions, and full date calendar
- **Files/Functions to Modify/Create:**
  - `examples/precision_railroading/models/dimensions/dim_location.sql`
  - `examples/precision_railroading/models/dimensions/dim_railcar.sql`
  - `examples/precision_railroading/models/dimensions/dim_train.sql`
  - `examples/precision_railroading/models/dimensions/dim_corridor.sql`
  - `examples/precision_railroading/models/dimensions/dim_date.sql`
  - `examples/precision_railroading/models/dimensions/schema.yml`
- **Tests to Write:**
  - `test_dim_location_unique` (no duplicate SPLC codes)
  - `test_dim_location_gis_valid` (latitude/longitude ranges)
  - `test_dim_location_shadow_yard_flags` (5-7 locations flagged as potential shadow yards)
  - `test_dim_railcar_fleet_coverage` (exactly 12,000 cars, all car types represented)
  - `test_dim_corridor_distance_valid` (positive distances)
  - `test_dim_date_continuity` (no gaps in 10-year calendar)
- **Steps:**
  1. Write tests for dimension table constraints and business rules
  2. Run tests to confirm failures
  3. Create dim_location with SPLC codes, GIS coordinates, facility types (terminal, interchange, yard, customer site, siding)
  4. Add shadow_yard_risk_score column to dim_location for subtle pattern identification
  5. Create dim_railcar with exactly 12,000 car numbers, types (hopper, tank, box, gondola, intermodal), capacities, owners
  6. Create dim_train with train IDs, types (manifest, intermodal, unit), priority levels, PSR-era flags
  7. Create dim_corridor with route IDs, endpoints, distances, lane types (mainline, branch), congestion levels
  8. Create dim_date with 10-year calendar (2016-2025), week numbers, quarters, seasons, PSR-era period flags
  9. Define schema.yml with dimension table documentation and tests
  10. Run tests to confirm dimensions pass all constraints
  11. Build and verify dimensions materialize correctly

### Phase 3: Staging Layer (CLM Event Processing & SPLC Enrichment)
- **Objective:** Stage raw CLM events with SPLC lookups, temporal deduplication, and event type classification at minute-level granularity
- **Files/Functions to Modify/Create:**
  - `examples/precision_railroading/models/staging/stg_clm_events.sql`
  - `examples/precision_railroading/models/staging/stg_clm_enriched.sql`
  - `examples/precision_railroading/models/staging/schema.yml`
- **Tests to Write:**
  - `test_stg_clm_no_duplicates` (unique event_id)
  - `test_stg_clm_temporal_order` (events chronological per car at minute precision)
  - `test_stg_clm_splc_exists` (all SPLC codes resolve)
  - `test_stg_clm_event_types_valid` (only DEPA, ARRI, PULL, PLAC, HOLD, RELE)
  - `test_stg_clm_timestamp_precision` (minute-level granularity preserved)
- **Steps:**
  1. Write tests for staging layer data quality rules
  2. Run tests to confirm failures
  3. Create stg_clm_events to parse raw CSV with TIMESTAMP data type (minute precision)
  4. Create stg_clm_enriched joining dim_location for SPLC metadata (GIS, facility type, shadow_yard_risk_score)
  5. Implement deduplication logic using ROW_NUMBER() on event_id and timestamp
  6. Add event type validation and classification flags
  7. Preserve minute-level timestamp precision throughout staging
  8. Run tests to confirm staging layer passes
  9. Build and verify staging models execute cleanly

### Phase 4: Intermediate Layer - State Intervals (Trip Segmentation & Cycle Classification)
- **Objective:** Transform discrete CLM events into continuous state-interval model, partitioning into loaded trips and empty return cycles with minute-precision durations
- **Files/Functions to Modify/Create:**
  - `examples/precision_railroading/models/intermediate/int_state_intervals.sql`
  - `examples/precision_railroading/models/intermediate/int_trip_segments.sql`
  - `examples/precision_railroading/models/intermediate/int_cycle_classification.sql`
  - `examples/precision_railroading/models/intermediate/schema.yml`
- **Tests to Write:**
  - `test_int_state_intervals_no_overlap` (no time overlaps per car)
  - `test_int_state_intervals_complete_coverage` (no gaps in timeline)
  - `test_int_state_intervals_minute_precision` (durations calculated to minute)
  - `test_int_trip_load_status` (loaded vs empty logic)
  - `test_int_cycle_duration_reasonable` (cycles 2-30 days)
- **Steps:**
  1. Write tests for state-interval model constraints
  2. Run tests to confirm failures
  3. Create int_state_intervals using LEAD() window function to pair events into intervals
  4. Calculate interval duration in minutes: TIMESTAMPDIFF(MINUTE, start_time, end_time)
  5. Create int_trip_segments identifying origin/destination pairs
  6. Create int_cycle_classification marking loaded trips (PLAC to PULL) vs empty returns (PULL to PLAC)
  7. Add cycle_id to group related trip segments
  8. Add PSR-era flag to correlate intervals with operational period
  9. Run tests to confirm state-interval model passes
  10. Build and verify intermediate models execute correctly

### Phase 5: Intermediate Layer - Velocity & Dwell (Point-to-Point Metrics & Nodal Dwell)
- **Objective:** Calculate velocity vectors between locations and classify dwell events by operational signature, leveraging minute-precision data
- **Files/Functions to Modify/Create:**
  - `examples/precision_railroading/models/intermediate/int_velocity_vectors.sql`
  - `examples/precision_railroading/models/intermediate/int_nodal_dwell.sql`
  - `examples/precision_railroading/models/intermediate/int_dwell_classification.sql`
  - `examples/precision_railroading/models/intermediate/schema.yml`
- **Tests to Write:**
  - `test_int_velocity_positive` (velocity >= 0)
  - `test_int_velocity_reasonable` (< 80 mph)
  - `test_int_dwell_positive` (dwell_duration_minutes > 0)
  - `test_int_dwell_types_valid` (terminal, crew_change, mainline, maintenance, shadow_yard_hold)
  - `test_int_dwell_shadow_yard_detection` (flags 5-7 subtle shadow yard patterns)
- **Steps:**
  1. Write tests for velocity and dwell calculations
  2. Run tests to confirm failures
  3. Create int_velocity_vectors calculating miles/hour between sequential locations
  4. Join dim_corridor for origin-destination distance lookup
  5. Use minute-precision timestamps for accurate velocity calculations
  6. Create int_nodal_dwell capturing stop duration at each location (in minutes)
  7. Create int_dwell_classification using duration patterns and facility types to classify stops:
     - Terminal operations (480-2880 minutes / 8-48 hours at terminals)
     - Crew changes (60-240 minutes / 1-4 hours at crew bases)
     - Mainline holds (30-360 minutes / 0.5-6 hours at sidings)
     - Maintenance (>360 minutes / >6 hours at repair facilities)
     - Shadow yard holds (identify by location risk score + unusual dwell patterns)
  8. Run tests to confirm velocity and dwell logic passes
  9. Build and verify intermediate models complete successfully

### Phase 6: Fact Tables & Stop Classification
- **Objective:** Build grain-level fact tables for trips, dwell events, and classified stops with shadow yard indicators
- **Files/Functions to Modify/Create:**
  - `examples/precision_railroading/models/facts/fact_trip.sql`
  - `examples/precision_railroading/models/facts/fact_dwell.sql`
  - `examples/precision_railroading/models/facts/fact_stop_classification.sql`
  - `examples/precision_railroading/models/facts/fact_corridor_transit.sql`
  - `examples/precision_railroading/models/facts/schema.yml`
- **Tests to Write:**
  - `test_fact_trip_fk_valid` (all FKs exist in dimensions)
  - `test_fact_trip_duration_positive` (trip_duration_minutes > 0)
  - `test_fact_dwell_fk_valid` (all FKs exist)
  - `test_fact_dwell_shadow_yard_flags` (shadow yard events flagged)
  - `test_fact_stop_classification_exclusive` (each stop has one classification)
- **Steps:**
  1. Write tests for fact table referential integrity and business rules
  2. Run tests to confirm failures
  3. Create fact_trip with grain of one row per car trip (loaded or empty)
  4. Join to dimensions (railcar, train, corridor, origin, destination, date)
  5. Include measures: distance_miles, duration_minutes, average_velocity_mph, dwell_count, stop_count
  6. Add PSR-era flag and performance delta vs pre-PSR baseline
  7. Create fact_dwell with grain of one row per stop event
  8. Include measures: dwell_duration_minutes, facility_type, dwell_classification, shadow_yard_flag
  9. Create fact_stop_classification aggregating stop types per trip
  10. Create fact_corridor_transit for origin-destination performance by time period
  11. Run tests to confirm facts pass integrity checks
  12. Build and verify fact tables materialize correctly

### Phase 7: Metrics & Aggregations (Network Fluidity, Slot Adherence, Shadow Yards)
- **Objective:** Compute PSR-specific KPIs including network fluidity index, slot adherence scores, buffer consumption, shadow yard detection, and directional asymmetry
- **Files/Functions to Modify/Create:**
  - `examples/precision_railroading/models/metrics/agg_network_fluidity.sql`
  - `examples/precision_railroading/models/metrics/agg_slot_adherence.sql`
  - `examples/precision_railroading/models/metrics/agg_shadow_yards.sql`
  - `examples/precision_railroading/models/metrics/agg_buffer_consumption.sql`
  - `examples/precision_railroading/models/metrics/agg_directional_asymmetry.sql`
  - `examples/precision_railroading/models/metrics/agg_corridor_weekly_performance.sql`
  - `examples/precision_railroading/models/metrics/agg_psr_evolution.sql`
  - `examples/precision_railroading/models/metrics/schema.yml`
- **Tests to Write:**
  - `test_agg_network_fluidity_range` (fluidity_index 0-100)
  - `test_agg_slot_adherence_percentage` (adherence 0-100)
  - `test_agg_shadow_yards_threshold` (identifies 5-7 locations with dwell_std_dev > threshold)
  - `test_agg_shadow_yards_subtle_detection` (catches non-obvious patterns)
  - `test_agg_directional_symmetry_logic` (asymmetry_ratio calculated)
  - `test_agg_psr_evolution_periods` (pre-PSR, transition, mature phases)
- **Steps:**
  1. Write tests for aggregation logic and metric ranges
  2. Run tests to confirm failures
  3. Create agg_network_fluidity calculating weighted average car velocity by corridor and week
  4. Formula: (sum(distance_miles) / sum(duration_minutes) * 60) weighted by car_count
  5. Create agg_slot_adherence measuring on-time performance vs scheduled slots
  6. Use temporal variance analysis (STDDEV on arrival times at minute precision)
  7. Create agg_shadow_yards detecting 5-7 subtle locations with artificially managed dwell
  8. Logic: Identify locations where terminal dwell < corridor_avg BUT next-location dwell > 1.5x normal
  9. Consider multiple indicators: variance in arrival patterns, time-of-day clustering, downstream impacts
  10. Create agg_buffer_consumption measuring schedule buffer usage patterns
  11. Calculate scheduled vs actual transit times, buffer erosion over corridors
  12. Create agg_directional_asymmetry comparing loaded vs empty trip performance
  13. Identify corridors where railroad prioritizes one direction (line-haul velocity optimization)
  14. Create agg_corridor_weekly_performance for time-series analysis with 25% seasonal variation
  15. Create agg_psr_evolution tracking KPI changes across three periods (2016-2017, 2018-2020, 2021-2025)
  16. Run tests to confirm aggregations pass validation
  17. Build and verify all metrics materialize correctly

### Phase 8: Analytics Queries & Documentation
- **Objective:** Create analytical queries demonstrating data warehouse capabilities and document in comprehensive METRICS.md and ARCHITECTURE.md
- **Files/Functions to Modify/Create:**
  - `examples/precision_railroading/models/analytics/worst_performing_corridors.sql`
  - `examples/precision_railroading/models/analytics/shadow_yard_identification.sql`
  - `examples/precision_railroading/models/analytics/seasonal_performance_trends.sql`
  - `examples/precision_railroading/models/analytics/psr_strategy_shifts.sql`
  - `examples/precision_railroading/models/analytics/network_congestion_hotspots.sql`
  - `examples/precision_railroading/models/analytics/directional_efficiency_analysis.sql`
  - `examples/precision_railroading/models/analytics/schema.yml`
  - `examples/precision_railroading/docs/METRICS.md`
  - `examples/precision_railroading/docs/ARCHITECTURE.md`
- **Tests to Write:**
  - `test_analytics_worst_corridors_returns_results` (query executes)
  - `test_analytics_shadow_yards_threshold_logic` (identifies anomalies)
  - `test_analytics_seasonal_trends_grouping` (25% variance observed)
  - `test_analytics_psr_evolution_phases` (pre/transition/mature periods)
- **Steps:**
  1. Write tests for analytics query execution and result validity
  2. Run tests to confirm failures
  3. Create worst_performing_corridors ranking by fluidity index and dwell time
  4. Show performance degradation from pre-PSR baseline where applicable
  5. Create shadow_yard_identification with sophisticated detection logic for subtle patterns
  6. Rank locations by composite score: dwell variance + downstream impact + time clustering
  7. Create seasonal_performance_trends showing 25% year-over-year changes
  8. Highlight winter slowdowns and summer capacity peaks
  9. Create psr_strategy_shifts detecting operational model changes across three periods
  10. Compare velocity, dwell, asset utilization pre-PSR vs mature PSR (2016-2017 vs 2021-2025)
  11. Create network_congestion_hotspots identifying bottlenecks
  12. Create directional_efficiency_analysis showing loaded vs empty asymmetry
  13. Write METRICS.md documenting all KPIs with formulas and business context
  14. Include shadow yard detection methodology and PSR evolution framework
  15. Write ARCHITECTURE.md documenting data model, transformations, and data lineage
  16. Run tests to confirm all analytics execute successfully
  17. Build and verify analytics produce meaningful insights

### Phase 9: Data Quality Tests & Validation
- **Objective:** Implement comprehensive data quality framework validating referential integrity, temporal consistency, and business rules
- **Files/Functions to Modify/Create:**
  - `examples/precision_railroading/tests/test_referential_integrity.sql`
  - `examples/precision_railroading/tests/test_temporal_consistency.sql`
  - `examples/precision_railroading/tests/test_business_rules.sql`
  - `examples/precision_railroading/tests/test_exclusivity_constraints.sql`
  - `examples/precision_railroading/tests/test_minute_precision.sql`
  - `examples/precision_railroading/tests/schema.yml`
- **Tests to Write:**
  - `test_fact_trip_railcar_fk` (all railcar_id exist in dim_railcar, 12K car coverage)
  - `test_fact_trip_temporal_order` (depart < arrive timestamps at minute precision)
  - `test_fact_dwell_duration_positive` (dwell_minutes > 0)
  - `test_trip_cycle_exclusivity` (trip is loaded XOR empty)
  - `test_velocity_physical_limits` (velocity < 80 mph)
  - `test_timestamp_minute_precision` (no second or millisecond components)
  - `test_shadow_yard_detection_count` (5-7 locations flagged)
  - `test_seasonal_variance_range` (performance varies ~25%)
- **Steps:**
  1. Create test_referential_integrity validating all foreign keys
  2. Verify 12,000 railcars fully represented in fact tables
  3. Create test_temporal_consistency ensuring chronological ordering at minute precision
  4. Create test_business_rules checking:
     - Velocity within physical limits (0-80 mph)
     - Dwell durations reasonable (1 minute - 7 days)
     - Trip distances match corridor distances (±5%)
     - Load status consistency throughout cycle
  5. Create test_exclusivity_constraints ensuring mutually exclusive classifications
  6. Create test_minute_precision validating timestamp granularity
  7. Verify no seconds/milliseconds in timestamps, all durations in whole minutes
  8. Document all tests in schema.yml
  9. Run all tests via gorchata test command
  10. Verify 100% test pass rate
  11. Document test coverage in README

### Phase 10: Final Integration & README Documentation
- **Objective:** Complete comprehensive README with business context, architecture diagrams, setup instructions, and example outputs; update FutureExamples.md
- **Files/Functions to Modify/Create:**
  - `examples/precision_railroading/README.md` (final version)
  - `examples/precision_railroading/docs/BUSINESS_CONTEXT.md`
  - `examples/precision_railroading/docs/SETUP.md`
  - `examples/precision_railroading/docs/QUERIES.md`
  - `examples/precision_railroading/docs/PSR_EVOLUTION.md`
  - `FutureExamples.md` (mark PSR example as complete)
- **Tests to Write:**
  - `test_integration_full_pipeline` (end-to-end test from seed to analytics)
  - `test_integration_sample_queries` (validate example query outputs)
  - `test_integration_shadow_yard_detection` (verify 5-7 locations identified)
  - `test_integration_psr_periods` (confirm three-period evolution visible)
- **Steps:**
  1. Write integration tests for full pipeline execution
  2. Run tests to confirm end-to-end flow
  3. Complete README.md with:
     - Business overview of PSR and its industry impact
     - Key operational concepts (nodal dwell, velocity, fluidity, shadow yards)
     - Data specifications (12K cars, 10 years, minute precision, 25% seasonal effects)
     - Architecture diagram showing star schema
     - Table catalog (7 dimensions, 4 facts, 7 metrics, 6 analytics)
     - Setup instructions
     - Example queries and outputs
  4. Write BUSINESS_CONTEXT.md explaining PSR evolution 2016-2025
  5. Detail pre-PSR operations, gradual adoption challenges, mature PSR characteristics
  6. Write SETUP.md with step-by-step installation and execution
  7. Write QUERIES.md with analytical query cookbook
  8. Write PSR_EVOLUTION.md documenting the three-period framework and KPI shifts
  9. Run integration tests to confirm complete pipeline
  10. Execute sample analytics queries and capture outputs for documentation
  11. Verify shadow yard detection identifies 5-7 subtle locations
  12. Update FutureExamples.md marking PSR example as complete ✅
  13. Build and verify entire project executes cleanly from fresh state
