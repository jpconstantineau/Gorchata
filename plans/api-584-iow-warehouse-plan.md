## Plan: API 584 IOW Data Warehouse

A Risk-Based Integrity Operating Window (IOW) monitoring system built on API 584 principles to track refinery static equipment performance and identify degradation before failures occur. The project models 5-minute interval sensor telemetry from 100 assets over 5 years, detecting excursions from safe operating limits, calculating cumulative damage metrics using Area Under Curve methodology, and prioritizing inspection schedules based on asset integrity health indices. Includes automated alert models for critical excursions and approaching inspection thresholds.

**Phases: 8**

### 1. **Phase 1: Core Dimension Tables**
- **Objective:** Establish fundamental reference data for temporal, asset, and limit hierarchies with 100 assets across 4 refinery units
- **Files/Functions to Modify/Create:**
  - [examples/api_584_iow_warehouse/schema.yml](examples/api_584_iow_warehouse/schema.yml) - Define dim_date, dim_asset, dim_iow_limit, dim_parameter_type, dim_criticality_level
  - [examples/api_584_iow_warehouse/seeds/dim_date.csv](examples/api_584_iow_warehouse/seeds/dim_date.csv)
  - [examples/api_584_iow_warehouse/seeds/dim_asset.csv](examples/api_584_iow_warehouse/seeds/dim_asset.csv) 
  - [examples/api_584_iow_warehouse/seeds/dim_iow_limit.csv](examples/api_584_iow_warehouse/seeds/dim_iow_limit.csv)
  - [examples/api_584_iow_warehouse/seeds/dim_parameter_type.csv](examples/api_584_iow_warehouse/seeds/dim_parameter_type.csv)
  - [examples/api_584_iow_warehouse/seeds/dim_criticality_level.csv](examples/api_584_iow_warehouse/seeds/dim_criticality_level.csv)
- **Tests to Write:**
  - `TestDimDateSchema` - Validates date dimension has required columns and data types
  - `TestDimAssetSchema` - Validates asset hierarchy (unit → system → equipment → tag)
  - `TestDimIOWLimitSchema` - Validates three-tier limits (Critical/Standard/Informational)
  - `TestDimParameterTypeSchema` - Validates sensor parameter types (Pressure/Temperature/pH/Flow)
  - `TestDimCriticalitySchema` - Validates IOW consequence levels
  - `TestAssetCount` - Validates exactly 100 assets defined
  - `TestDateRange` - Validates 5 years of date dimension (1826+ dates)
- **Steps:**
  1. Write schema tests for dim_date expecting columns: date_key, full_date, year, quarter, month, is_turnaround, is_summer_spec, is_winter_spec
  2. Run tests, confirm failure (schema not defined)
  3. Define dim_date in schema.yml with temporal attributes and refinery calendar markers (turnarounds, seasonal specifications)
  4. Create seed file with 5 years of dates (2021-2025) including quarterly turnaround periods
  5. Write schema tests for dim_asset expecting hierarchical asset registry with 100 rows
  6. Define dim_asset with columns: asset_key, tag_id, equipment_name, system_name, unit_name, design_life_years, install_date, material_grade, damage_mechanism_primary, damage_mechanism_secondary
  7. Create seed file with 100 static equipment assets distributed across CDU (30 assets), VDU (20 assets), FCC (30 assets), HCU (20 assets) units
  8. Include diverse damage mechanisms: Sulfidation, Naphthenic Acid Corrosion, Hydrogen Attack, Corrosion Under Insulation (CUI), Creep, Thermal Fatigue, Stress Corrosion Cracking, High Temperature Hydrogen Attack, Caustic Stress Corrosion Cracking
  9. Write schema tests for dim_iow_limit expecting hierarchical limits per parameter type
  10. Define dim_iow_limit with columns: limit_key, parameter_type, criticality_level, lower_limit, upper_limit, consequence_description
  11. Create seed file with IOW limits for each parameter type (Pressure, Temperature, pH, Flow) at three criticality levels
  12. Write tests for dim_parameter_type (4 types) and dim_criticality_level (3 levels)
  13. Define remaining dimension schemas and create seed data
  14. Run all tests to confirm schema creation and row counts

### 2. **Phase 2: Staging Layer - Raw Sensor Telemetry**
- **Objective:** Ingest 5-minute interval sensor measurements with timestamps spanning 5 years (~5.2M readings per asset = 520M+ total readings)
- **Files/Functions to Modify/Create:**
  - [examples/api_584_iow_warehouse/models/staging/stg_sensor_readings.sql](examples/api_584_iow_warehouse/models/staging/stg_sensor_readings.sql)
  - [examples/api_584_iow_warehouse/seeds/raw_sensor_readings.csv](examples/api_584_iow_warehouse/seeds/raw_sensor_readings.csv)
  - [examples/api_584_iow_warehouse/schema.yml](examples/api_584_iow_warehouse/schema.yml) - Add stg_sensor_readings schema
  - [examples/api_584_iow_warehouse/transformations/generate_sensor_data.go](examples/api_584_iow_warehouse/transformations/generate_sensor_data.go) - Synthetic data generator
- **Tests to Write:**
  - `TestStagingSensorReadings` - Validates raw sensor data ingestion
  - `TestSensorTimestampSequence` - Validates 5-minute intervals
  - `TestSensorValueRanges` - Validates physically realistic values (pressure: 0-3000 psig, temp: 32-1400°F, pH: 0-14, flow: 0-50000 bbl/day)
  - `TestAssetJoin` - Validates all sensor tags match asset registry
  - `TestFiveYearCoverage` - Validates data spans 2021-2025
  - `TestDataVolumeScale` - Validates appropriate sampling for testing (representative subset)
- **Steps:**
  1. Write test expecting stg_sensor_readings with columns: reading_id, timestamp, tag_id, parameter_type, measured_value, data_quality_flag
  2. Run test, confirm failure
  3. Create generate_sensor_data.go helper to produce synthetic telemetry at 5-minute intervals
  4. Generate realistic sensor patterns: normal operation (75%), minor drift (15%), IOW excursions (8%), sensor errors (2%)
  5. Model operational events: unit startups, shutdowns, upset conditions, feedstock changes
  6. Create raw_sensor_readings.csv seed with representative subset for testing (1 month of data per asset = ~8.6M readings)
  7. Implement stg_sensor_readings.sql selecting from seed and joining to dim_asset
  8. Add data quality validation (sensor range checks, duplicate detection, gap detection)
  9. Add flags for data_quality: 'Good', 'Questionable', 'Bad', 'Substituted'
  10. Write referential integrity test ensuring all tag_id values exist in dim_asset
  11. Run tests to confirm staging layer processes 5-minute data correctly

### 3. **Phase 3: Intermediate Layer - IOW Excursion Detection**
- **Objective:** Identify when sensor readings breach IOW limits and group consecutive excursions into events
- **Files/Functions to Modify/Create:**
  - [examples/api_584_iow_warehouse/models/intermediate/int_iow_excursions.sql](examples/api_584_iow_warehouse/models/intermediate/int_iow_excursions.sql)
  - [examples/api_584_iow_warehouse/models/intermediate/int_excursion_windows.sql](examples/api_584_iow_warehouse/models/intermediate/int_excursion_windows.sql)
  - [examples/api_584_iow_warehouse/models/intermediate/int_excursion_severity.sql](examples/api_584_iow_warehouse/models/intermediate/int_excursion_severity.sql)
  - [examples/api_584_iow_warehouse/schema.yml](examples/api_584_iow_warehouse/schema.yml) - Add intermediate model schemas
- **Tests to Write:**
  - `TestExcursionDetection` - Validates excursions identified correctly at all three criticality levels
  - `TestExcursionCriticality` - Validates correct criticality level assignment (highest breached limit)
  - `TestExcursionWindowing` - Validates consecutive excursions grouped into events (gap tolerance: 15 minutes)
  - `TestNoFalsePositives` - Validates in-limit readings don't trigger excursions
  - `TestSeverityCalculation` - Validates severity scoring based on magnitude and duration
- **Steps:**
  1. Write test expecting int_iow_excursions to flag readings outside limits at each criticality level
  2. Run test, confirm failure
  3. Implement int_iow_excursions.sql joining stg_sensor_readings to dim_iow_limit
  4. Add CASE logic to determine if measured_value < lower_limit OR > upper_limit
  5. Calculate excursion_magnitude as absolute deviation from breached limit
  6. Assign criticality_level based on most restrictive limit breached
  7. Write test for excursion windowing (consecutive excursions within 15 minutes = single event)
  8. Implement int_excursion_windows.sql using window functions (LAG/LEAD) to detect gaps
  9. Group consecutive excursions with < 15-minute gap into excursion_event_id
  10. Add excursion_start_time, excursion_end_time, duration_minutes, reading_count
  11. Write test for severity calculation
  12. Implement int_excursion_severity.sql calculating severity_score = f(magnitude, duration, criticality)
  13. Run tests to confirm excursion detection and windowing

### 4. **Phase 4: Fact Table - Excursion Events with Damage Metrics**
- **Objective:** Create core fact table with cumulative damage calculations using Area Under Curve methodology
- **Files/Functions to Modify/Create:**
  - [examples/api_584_iow_warehouse/models/marts/fact_excursion_events.sql](examples/api_584_iow_warehouse/models/marts/fact_excursion_events.sql)
  - [examples/api_584_iow_warehouse/models/marts/fact_asset_damage_accumulation.sql](examples/api_584_iow_warehouse/models/marts/fact_asset_damage_accumulation.sql)
  - [examples/api_584_iow_warehouse/schema.yml](examples/api_584_iow_warehouse/schema.yml) - Add fact table schemas
- **Tests to Write:**
  - `TestFactExcursionEvents` - Validates fact table structure and grain
  - `TestDamageCalculationAUC` - Validates Area Under Curve calculation: SUM(excursion_magnitude * duration_minutes)
  - `TestGrainUniqueness` - Validates one row per excursion event
  - `TestReferentialIntegrity` - Validates all foreign keys resolve to dimension tables
  - `TestDamageAccumulation` - Validates cumulative damage aggregates correctly per asset
  - `TestDamageNonNegative` - Validates damage metrics are always >= 0
- **Steps:**
  1. Write test expecting fact_excursion_events with grain = one row per IOW excursion event
  2. Run test, confirm failure
  3. Implement fact_excursion_events.sql selecting from int_excursion_windows and int_excursion_severity
  4. Add foreign keys: asset_key, date_key (for excursion_start_time), limit_key, criticality_key, parameter_type_key
  5. Calculate cumulative_damage_index using Area Under Curve: SUM(excursion_magnitude * duration_minutes)
  6. Add measures: peak_magnitude, average_magnitude, duration_minutes, reading_count, severity_score
  7. Add event classification: excursion_type ('Overpressure', 'Underpressure', 'Overtemp', 'Undertemp', 'pH_High', 'pH_Low', 'Flow_High', 'Flow_Low')
  8. Write referential integrity tests for all dimension joins
  9. Write test for fact_asset_damage_accumulation expecting rolling damage totals
  10. Implement fact_asset_damage_accumulation.sql aggregating excursion events by asset and time period
  11. Calculate cumulative_damage_to_date, damage_last_30_days, damage_last_90_days, damage_last_365_days
  12. Add excursion_count_total, critical_excursion_count, days_since_last_critical_excursion
  13. Run tests to confirm fact tables and damage accumulation

### 5. **Phase 5: Metrics Layer - Asset Integrity Health Indices & Bad Actors**
- **Objective:** Aggregate excursions to calculate asset-level health scores and identify worst performers
- **Files/Functions to Modify/Create:**
  - [examples/api_584_iow_warehouse/models/marts/metrics_asset_integrity_index.sql](examples/api_584_iow_warehouse/models/marts/metrics_asset_integrity_index.sql)
  - [examples/api_584_iow_warehouse/models/marts/metrics_bad_actors.sql](examples/api_584_iow_warehouse/models/marts/metrics_bad_actors.sql)
  - [examples/api_584_iow_warehouse/models/marts/metrics_unit_health_summary.sql](examples/api_584_iow_warehouse/models/marts/metrics_unit_health_summary.sql)
  - [examples/api_584_iow_warehouse/schema.yml](examples/api_584_iow_warehouse/schema.yml) - Add metrics schemas
- **Tests to Write:**
  - `TestIntegrityIndexCalculation` - Validates health score formula (0-100 scale)
  - `TestBadActorRanking` - Validates worst performers identified (bottom 10% = 10 assets)
  - `TestRollingWindow` - Validates 90-day rolling calculations
  - `TestCriticalExcursionWeighting` - Validates critical excursions weighted 3x, standard 2x, informational 1x
  - `TestUnitAggregation` - Validates unit-level rollups across assets
  - `TestHealthTrendDetection` - Validates improving vs degrading health trends
- **Steps:**
  1. Write test expecting metrics_asset_integrity_index with rolling 90-day health scores (0-100 scale)
  2. Run test, confirm failure
  3. Implement metrics_asset_integrity_index.sql aggregating fact_excursion_events by asset
  4. Calculate Integrity_Health_Index = 100 - (weighted_excursion_score / theoretical_max * 100)
  5. Apply weighting: critical excursions 3x, standard 2x, informational 1x
  6. Add measures: total_excursion_count, critical_excursion_count, cumulative_damage_total, days_since_last_critical, health_trend_30d
  7. Calculate health_trend_30d comparing current 30-day window vs previous 30-day window
  8. Write test for bad actor identification expecting bottom 10 assets
  9. Implement metrics_bad_actors.sql ranking assets by composite score
  10. Calculate bad_actor_score = f(excursion_frequency, damage_accumulation, critical_event_count, health_index)
  11. Add reason codes: 'High_Excursion_Frequency', 'Severe_Damage_Accumulation', 'Repeated_Critical_Events', 'Degrading_Health_Trend'
  12. Write test for unit-level aggregation
  13. Implement metrics_unit_health_summary.sql aggregating across assets within each unit (CDU, VDU, FCC, HCU)
  14. Calculate unit_avg_health_index, unit_critical_excursion_count, unit_damage_total, worst_asset_per_unit
  15. Run tests to confirm metrics calculation

### 6. **Phase 6: Alert Models - Automated Notifications**
- **Objective:** Create alert models that trigger on critical excursions and inspection schedule thresholds
- **Files/Functions to Modify/Create:**
  - [examples/api_584_iow_warehouse/models/marts/alerts_critical_excursions.sql](examples/api_584_iow_warehouse/models/marts/alerts_critical_excursions.sql)
  - [examples/api_584_iow_warehouse/models/marts/alerts_inspection_due.sql](examples/api_584_iow_warehouse/models/marts/alerts_inspection_due.sql)
  - [examples/api_584_iow_warehouse/models/marts/alerts_health_degradation.sql](examples/api_584_iow_warehouse/models/marts/alerts_health_degradation.sql)
  - [examples/api_584_iow_warehouse/models/marts/alerts_damage_threshold.sql](examples/api_584_iow_warehouse/models/marts/alerts_damage_threshold.sql)
  - [examples/api_584_iow_warehouse/schema.yml](examples/api_584_iow_warehouse/schema.yml) - Add alert model schemas
- **Tests to Write:**
  - `TestCriticalExcursionAlert` - Validates alerts generated for all critical IOW breaches
  - `TestInspectionDueAlert` - Validates alerts when cumulative damage exceeds threshold (e.g., 80% of design margin)
  - `TestHealthDegradationAlert` - Validates alerts when health index drops >20 points in 30 days
  - `TestDamageThresholdAlert` - Validates alerts when cumulative damage exceeds asset-specific limits
  - `TestAlertPriority` - Validates priority assignment (Critical, High, Medium, Low)
  - `TestAlertDeduplication` - Validates no duplicate alerts for same event within 24 hours
- **Steps:**
  1. Write test expecting alerts_critical_excursions to flag all critical-level IOW breaches
  2. Run test, confirm failure
  3. Implement alerts_critical_excursions.sql selecting from fact_excursion_events WHERE criticality_level = 'Critical'
  4. Add alert fields: alert_id, alert_timestamp, asset_key, alert_type, priority, message, acknowledged_flag
  5. Generate alert message: "CRITICAL IOW EXCURSION: {asset_name} {parameter_type} exceeded {limit_value} by {magnitude}"
  6. Write test for inspection due alerts expecting triggers at 80% damage threshold
  7. Implement alerts_inspection_due.sql monitoring fact_asset_damage_accumulation
  8. Trigger alert when (cumulative_damage / design_margin) > 0.80 OR days_since_last_inspection > scheduled_interval
  9. Write test for health degradation alerts expecting >20 point drops
  10. Implement alerts_health_degradation.sql comparing current vs 30-day-ago health_index from metrics_asset_integrity_index
  11. Trigger alert when health_index_change < -20 within 30-day window
  12. Write test for damage threshold alerts
  13. Implement alerts_damage_threshold.sql with asset-specific damage limits based on damage_mechanism and material_grade
  14. Add deduplication logic using window functions to suppress repeat alerts within 24 hours
  15. Add priority assignment: Critical (immediate action), High (<24hr), Medium (<7 days), Low (next turnaround)
  16. Run tests to confirm alert generation and prioritization

### 7. **Phase 7: Analytical Queries - Inspection Prioritization & Root Cause**
- **Objective:** Create business-facing queries for inspection scheduling, trending, and root cause analysis
- **Files/Functions to Modify/Create:**
  - [examples/api_584_iow_warehouse/queries/inspection_priority_queue.sql](examples/api_584_iow_warehouse/queries/inspection_priority_queue.sql)
  - [examples/api_584_iow_warehouse/queries/parameter_trending.sql](examples/api_584_iow_warehouse/queries/parameter_trending.sql)
  - [examples/api_584_iow_warehouse/queries/damage_mechanism_correlation.sql](examples/api_584_iow_warehouse/queries/damage_mechanism_correlation.sql)
  - [examples/api_584_iow_warehouse/queries/excursion_root_cause_analysis.sql](examples/api_584_iow_warehouse/queries/excursion_root_cause_analysis.sql)
  - [examples/api_584_iow_warehouse/queries/asset_lifecycle_analysis.sql](examples/api_584_iow_warehouse/queries/asset_lifecycle_analysis.sql)
  - [examples/api_584_iow_warehouse/queries/unit_performance_comparison.sql](examples/api_584_iow_warehouse/queries/unit_performance_comparison.sql)
- **Tests to Write:**
  - `TestInspectionPriorityLogic` - Validates inspection schedule recommendations ordered correctly
  - `TestParameterTrendDetection` - Validates drift detection algorithms using moving averages
  - `TestDamageMechanismCorrelation` - Validates corrosion patterns linked to IOW excursion types
  - `TestRootCauseGrouping` - Validates excursions grouped by operational events (upsets, startups, feedstock changes)
  - `TestLifecycleAnalysis` - Validates design life consumption vs chronological age
  - `TestUnitComparison` - Validates fair comparison across units with different asset counts
- **Steps:**
  1. Write test expecting inspection_priority_queue to return ranked list of assets needing inspection
  2. Run test, confirm failure
  3. Implement inspection_priority_queue.sql combining health_index, damage_accumulation, design_life_remaining
  4. Calculate priority_score = f(consequence_of_failure, cumulative_damage_index, health_index, excursion_frequency)
  5. Add columns: asset_name, unit, priority_score, days_until_recommended_inspection, consequence_category, estimated_inspection_cost
  6. Order by priority_score DESC
  7. Write test for parameter trending expecting 30-day moving averages and standard deviations
  8. Implement parameter_trending.sql using window functions for trend detection
  9. Calculate 30-day moving average, 3-sigma control limits, drift from baseline
  10. Flag parameters approaching IOW limits (within 10% of limit)
  11. Write test for damage mechanism correlation
  12. Implement damage_mechanism_correlation.sql linking IOW excursions to specific corrosion types
  13. Correlate high-temp excursions → creep/thermal fatigue, pH excursions → naphthenic acid corrosion, etc.
  14. Write test for root cause grouping
  15. Implement excursion_root_cause_analysis.sql joining to operational events
  16. Group excursions by: 'Unit_Startup', 'Feedstock_Change', 'Upset_Condition', 'Control_Failure', 'Seasonal_Effect', 'Unknown'
  17. Write test for asset lifecycle analysis
  18. Implement asset_lifecycle_analysis.sql calculating design_life_consumed_pct based on damage vs age
  19. Identify assets aging faster than design basis (damage accumulation rate > theoretical rate)
  20. Write test for unit performance comparison
  21. Implement unit_performance_comparison.sql normalizing metrics across CDU, VDU, FCC, HCU
  22. Calculate per-asset averages within each unit for fair comparison
  23. Run all query tests to confirm functionality

### 8. **Phase 8: Data Validation, Testing & Documentation**
- **Objective:** Implement comprehensive data quality checks and complete documentation
- **Files/Functions to Modify/Create:**
  - [examples/api_584_iow_warehouse/tests/test_iow_validation.go](examples/api_584_iow_warehouse/tests/test_iow_validation.go)
  - [examples/api_584_iow_warehouse/tests/test_damage_calculation.go](examples/api_584_iow_warehouse/tests/test_damage_calculation.go)
  - [examples/api_584_iow_warehouse/tests/test_alert_logic.go](examples/api_584_iow_warehouse/tests/test_alert_logic.go)
  - [examples/api_584_iow_warehouse/schema.yml](examples/api_584_iow_warehouse/schema.yml) - Add data_tests throughout
  - [examples/api_584_iow_warehouse/README.md](examples/api_584_iow_warehouse/README.md)
  - [examples/api_584_iow_warehouse/ARCHITECTURE.md](examples/api_584_iow_warehouse/ARCHITECTURE.md)
  - [examples/api_584_iow_warehouse/METRICS.md](examples/api_584_iow_warehouse/METRICS.md)
  - [FutureExamples.md](FutureExamples.md) - Update completion status
- **Tests to Write:**
  - `TestIOWLimitHierarchy` - Validates three-tier limit hierarchy (Critical > Standard > Informational)
  - `TestDamageMonotonicity` - Validates cumulative damage never decreases per asset
  - `TestAssetLifecycleConsistency` - Validates install_date < excursion_date < retirement_date
  - `TestExcursionClosure` - Validates all excursions have end timestamps (no open-ended events > 24hr)
  - `TestAlertThresholds` - Validates automated alerts triggered at correct thresholds
  - `TestReferentialIntegrity` - Validates all foreign keys resolve across entire warehouse
  - `TestDataQualityFlags` - Validates data quality flags applied correctly
  - `TestExampleCompleteness` - Validates all models have documentation
  - `TestReadmeAccuracy` - Validates README instructions execute successfully
  - `TestSchemaDocumentation` - Validates all columns have descriptions
- **Steps:**
  1. Write test validating critical limits are stricter than standard limits which are stricter than informational
  2. Run test, confirm current limit definitions pass
  3. Write test validating damage accumulation monotonically increases per asset over time
  4. Add schema-level data_tests for limit hierarchy in schema.yml
  5. Write test validating asset lifecycle timestamps (install_date < first_reading < last_reading, exclude retired assets from active analysis)
  6. Add not_null and relationship tests throughout schema.yml for all foreign keys
  7. Write test for excursion closure (confirm no open-ended excursions older than 24 hours in fact_excursion_events)
  8. Add accepted_values tests for parameter_type ('Pressure', 'Temperature', 'pH', 'Flow') and criticality_level ('Critical', 'Standard', 'Informational')
  9. Write alert threshold validation test confirming triggers fire at documented thresholds
  10. Add unique tests for surrogate keys and composite grain tests for fact tables
  11. Write data quality flag test confirming 'Bad' readings excluded from damage calculations
  12. Run full test suite to confirm data quality across all layers
  13. Write README.md following haul_truck_analytics and oil_refinery_warehousing patterns
  14. Document IOW concepts, API 584 background, Risk-Based Inspection methodology
  15. Add business context section explaining refinery integrity management and damage mechanisms
  16. Document star schema design with ASCII diagrams showing fact and dimension relationships
  17. Add sections: Overview, Business Context, Architecture, Key Performance Indicators, How to Run, Queries, Testing
  18. Create ARCHITECTURE.md explaining layered transformation flow
  19. Document staging → intermediate → marts → metrics → alerts pipeline
  20. Explain design decisions: 5-minute sampling rationale, 100-asset scale, 5-year history, alert priority logic
  21. Create METRICS.md defining all KPIs
  22. Document formulas: Integrity Health Index, Bad Actor Score, Cumulative Damage Index (Area Under Curve), Priority Score
  23. Add example calculations and interpretation guidance
  24. Add "How to Run" instructions for gorchata CLI with specific commands
  25. Write test validating README instructions execute successfully end-to-end
  26. Update [FutureExamples.md](FutureExamples.md) moving API 584 IOW from "Future Examples" to "Completed Examples" section
  27. Run documentation tests to confirm completeness
  28. Run full test suite: `go test ./examples/api_584_iow_warehouse/tests/...` and confirm all tests pass
