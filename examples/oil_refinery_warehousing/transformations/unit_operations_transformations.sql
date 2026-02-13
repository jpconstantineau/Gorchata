-- ==============================================================================
-- Unit Operations Transformations - Phase 3
-- ==============================================================================
--
-- This SQL file documents the transformation logic for unit operations fact tables
-- including capacity utilization, downtime aggregation, energy intensity, and
-- unit hierarchy rollup calculations.
--
-- Data Flow: stg_unit_operations -> fact_unit_operations
--           stg_unit_feed -> fact_unit_feed
--
-- ==============================================================================

-- ==============================================================================
-- FACT_UNIT_OPERATIONS Transformations
-- ==============================================================================

-- -----------------------------------------------------------------------------
-- 1. Capacity Utilization Calculation
-- -----------------------------------------------------------------------------
-- Business Rule: Capacity utilization measures how effectively a unit is being
-- used relative to its nominal capacity. This is a key performance indicator
-- for refinery operations.
--
-- Formula: capacity_utilization_pct = (throughput_bbl / capacity_bbl_day) × 100
--
-- Example:
--   CDU processes 142,500 bbl/day with capacity of 150,000 bbl/day
--   = (142,500 / 150,000) × 100 = 95.0%
--
-- Typical ranges:
--   - CDU/VDU: 90-96% (continuous operation)
--   - FCC/HCU: 85-93% (catalytic units have more variability)
--   - Hydrotreaters: 91-94% (stable operation)
--   - Alkylation: 80-87% (acid catalyst units vary more)

INSERT INTO fact_unit_operations (
    operation_id,
    date_key,
    unit_id,
    capacity_utilization_pct
)
SELECT
    operation_id,
    date_key,
    unit_id,
    (throughput_bbl / NULLIF(capacity_bbl_day, 0)) * 100 AS capacity_utilization_pct
FROM stg_unit_operations
WHERE capacity_bbl_day > 0;

-- -----------------------------------------------------------------------------
-- 2. Downtime Aggregation (Planned + Unplanned)
-- -----------------------------------------------------------------------------
-- Business Rule: Total downtime is the sum of planned maintenance/turnaround
-- downtime and unplanned trips/upsets. These are tracked separately to:
--   - Measure maintenance effectiveness (planned vs unplanned ratio)
--   - Calculate availability and reliability KPIs
--   - Support root cause analysis of unplanned events
--
-- Formulas:
--   total_downtime_hours = planned_downtime_hours + unplanned_downtime_hours
--   operating_hours = 24 - total_downtime_hours
--
-- Example 1 - Planned Maintenance:
--   FCC turnaround: planned_downtime = 24 hrs, unplanned_downtime = 0 hrs
--   total_downtime = 24 hrs, operating_hours = 0 hrs
--
-- Example 2 - Unplanned Trip:
--   Reformer catalyst issue: planned_downtime = 0 hrs, unplanned_downtime = 4.5 hrs
--   total_downtime = 4.5 hrs, operating_hours = 19.5 hrs
--
-- Example 3 - Both Types:
--   Unit with partial maintenance + brief trip
--   planned_downtime = 8 hrs, unplanned_downtime = 3 hrs
--   total_downtime = 11 hrs, operating_hours = 13 hrs

SELECT
    operation_id,
    date_key,
    unit_id,
    planned_downtime_hours,
    unplanned_downtime_hours,
    (planned_downtime_hours + unplanned_downtime_hours) AS total_downtime_hours,
    (24 - (planned_downtime_hours + unplanned_downtime_hours)) AS calculated_operating_hours
FROM stg_unit_operations;

-- Data Quality Check: Downtime must not exceed 24 hours
SELECT
    operation_id,
    planned_downtime_hours,
    unplanned_downtime_hours,
    (planned_downtime_hours + unplanned_downtime_hours) AS total_downtime
FROM stg_unit_operations
WHERE (planned_downtime_hours + unplanned_downtime_hours) > 24;
-- Expected: 0 rows (constraint violation if any rows returned)

-- -----------------------------------------------------------------------------
-- 3. Operating Hours Validation
-- -----------------------------------------------------------------------------
-- Business Rule: Operating hours should equal 24 minus total downtime
-- This is a critical consistency check for operational data integrity

SELECT
    operation_id,
    operating_hours AS reported_operating_hours,
    (24 - (planned_downtime_hours + unplanned_downtime_hours)) AS calculated_operating_hours,
    ABS(operating_hours - (24 - (planned_downtime_hours + unplanned_downtime_hours))) AS variance
FROM stg_unit_operations
WHERE ABS(operating_hours - (24 - (planned_downtime_hours + unplanned_downtime_hours))) > 0.01;
-- Expected: 0 rows (if operating_hours is correctly calculated)

-- -----------------------------------------------------------------------------
-- 4. Energy Intensity Calculation
-- -----------------------------------------------------------------------------
-- Business Rule: Energy intensity measures energy efficiency of unit operations
-- Lower values indicate better energy efficiency
--
-- Formula: energy_intensity = energy_consumed_mmbtu / throughput_bbl
--
-- Typical energy intensities by unit type:
--   - CDU: 0.15 MMBtu/bbl
--   - VDU: 0.20 MMBtu/bbl
--   - FCC: 0.25 MMBtu/bbl
--   - Hydrocracker: 0.35 MMBtu/bbl (highest due to high pressure H2)
--   - Reformer: 0.22 MMBtu/bbl
--   - Hydrotreater: 0.12-0.14 MMBtu/bbl
--   - Alkylation: 0.18 MMBtu/bbl

SELECT
    operation_id,
    date_key,
    unit_id,
    energy_consumed_mmbtu,
    throughput_bbl,
    (energy_consumed_mmbtu / NULLIF(throughput_bbl, 0)) AS energy_intensity_mmbtu_per_bbl
FROM stg_unit_operations
WHERE throughput_bbl > 0;

-- Data Quality Check: Energy intensity should be within expected ranges
SELECT
    u.unit_type,
    AVG(o.energy_consumed_mmbtu / NULLIF(o.throughput_bbl, 0)) AS avg_energy_intensity,
    MIN(o.energy_consumed_mmbtu / NULLIF(o.throughput_bbl, 0)) AS min_energy_intensity,
    MAX(o.energy_consumed_mmbtu / NULLIF(o.throughput_bbl, 0)) AS max_energy_intensity
FROM stg_unit_operations o
JOIN dim_unit u ON o.unit_id = u.unit_id
WHERE o.throughput_bbl > 0
GROUP BY u.unit_type;

-- -----------------------------------------------------------------------------
-- 5. Unit Complex Hierarchy Rollup
-- -----------------------------------------------------------------------------
-- Business Rule: Process units are organized into complexes for high-level
-- performance tracking and management reporting
--
-- Unit Hierarchy:
--   Crude Unit Complex
--     ├─ CDU-1 (Crude Distillation Unit)
--     └─ VDU-1 (Vacuum Distillation Unit)
--   Conversion Complex
--     ├─ FCC-1 (Fluid Catalytic Cracker)
--     └─ HCU-1 (Hydrocracker)
--   Clean Fuels Complex
--     ├─ NHT-1 (Naphtha Hydrotreater)
--     └─ DHT-1 (Diesel Hydrotreater)
--   Gasoline Production Complex
--     ├─ REF-1 (Catalytic Reformer)
--     └─ ALK-1 (Alkylation Unit)
--
-- Formula: complex_throughput = SUM(unit_throughput) WHERE unit.complex = 'Complex Name'

SELECT
    u.complex_name,
    o.date_key,
    SUM(o.throughput_bbl) AS complex_throughput_bbl,
    SUM(o.energy_consumed_mmbtu) AS complex_energy_mmbtu,
    SUM(o.planned_downtime_hours) AS complex_planned_downtime_hrs,
    SUM(o.unplanned_downtime_hours) AS complex_unplanned_downtime_hrs,
    AVG(o.capacity_utilization_pct) AS avg_complex_utilization_pct
FROM stg_unit_operations o
JOIN dim_unit u ON o.unit_id = u.unit_id
GROUP BY u.complex_name, o.date_key
ORDER BY u.complex_name, o.date_key;

-- Example rollup for 2026-02-01:
--   Crude Unit Complex: 142,500 (CDU) + 54,000 (VDU) = 196,500 bbl
--   Conversion Complex: 41,400 (FCC) + 26,400 (HCU) = 67,800 bbl
--   Clean Fuels Complex: 32,550 (NHT) + 36,400 (DHT) = 68,950 bbl
--   Gasoline Production Complex: 22,500 (REF) + 12,750 (ALK) = 35,250 bbl

-- -----------------------------------------------------------------------------
-- 6. Catalyst Cycle Performance Correlation
-- -----------------------------------------------------------------------------
-- Business Rule: Catalyst efficiency degrades over time. Track performance
-- metrics correlated with catalyst cycle stage to optimize regeneration timing.
--
-- Units with catalyst tracking: FCC, Hydrocracker, Reformer

SELECT
    c.catalyst_cycle_id,
    c.cycle_stage,
    c.typical_efficiency_pct,
    AVG(o.throughput_bbl) AS avg_throughput,
    AVG(o.capacity_utilization_pct) AS avg_utilization,
    AVG(o.conversion_pct) AS avg_conversion,
    AVG(o.energy_consumed_mmbtu / NULLIF(o.throughput_bbl, 0)) AS avg_energy_intensity
FROM stg_unit_operations o
JOIN dim_catalyst_cycle c ON o.catalyst_cycle_id = c.catalyst_cycle_id
WHERE o.catalyst_cycle_id IS NOT NULL
GROUP BY c.catalyst_cycle_id, c.cycle_stage, c.typical_efficiency_pct
ORDER BY c.cycle_stage;

-- Expected insights:
--   - Fresh catalyst: Higher conversion, lower energy intensity
--   - Mid-cycle: Optimal balance of efficiency and stability
--   - End-of-run: Lower conversion, higher energy use, more downtime risk

-- -----------------------------------------------------------------------------
-- 7. Availability and Reliability Metrics
-- -----------------------------------------------------------------------------
-- Business Rule: Calculate operational availability and reliability KPIs
--
-- Availability = (Operating Hours / 24) × 100
-- Reliability = (Operating Hours / (Operating Hours + Unplanned Downtime)) × 100
-- MTBF (Mean Time Between Failures) = Total Operating Hours / Number of Failures

SELECT
    operation_id,
    date_key,
    unit_id,
    -- Availability (includes planned downtime impact)
    (operating_hours / 24.0) * 100 AS availability_pct,
    -- Reliability (excludes planned downtime)
    CASE
        WHEN (operating_hours + unplanned_downtime_hours) > 0
        THEN (operating_hours / (operating_hours + unplanned_downtime_hours)) * 100
        ELSE 100.0
    END AS reliability_pct,
    -- Classify downtime events
    CASE
        WHEN unplanned_downtime_hours > 0 THEN 'Unplanned Event'
        WHEN planned_downtime_hours > 0 THEN 'Planned Maintenance'
        ELSE 'Normal Operation'
    END AS operation_status
FROM stg_unit_operations;

-- -----------------------------------------------------------------------------
-- 8. Crude Mix Impact on Throughput
-- -----------------------------------------------------------------------------
-- Business Rule: Different crude grades have varying processing characteristics
-- Heavy sour crudes typically reduce throughput by 5-10% vs light sweet crudes
--
-- This requires joining to crude receipts to identify crude slate composition

SELECT
    o.date_key,
    o.unit_id,
    o.throughput_bbl,
    o.capacity_utilization_pct,
    -- Calculate weighted average crude API gravity from receipts
    AVG(cr.api_gravity_60f) AS avg_crude_api,
    -- Classify crude slate
    CASE
        WHEN AVG(cr.api_gravity_60f) > 35 THEN 'Light Sweet'
        WHEN AVG(cr.api_gravity_60f) BETWEEN 25 AND 35 THEN 'Medium'
        ELSE 'Heavy Sour'
    END AS crude_slate_classification
FROM stg_unit_operations o
JOIN fact_crude_receipts cr ON o.date_key = cr.date_key
WHERE o.unit_id IN ('CDU-1', 'VDU-1')  -- Crude processing units
GROUP BY o.date_key, o.unit_id, o.throughput_bbl, o.capacity_utilization_pct;

-- ==============================================================================
-- FACT_UNIT_FEED Transformations
-- ==============================================================================

-- -----------------------------------------------------------------------------
-- 9. Unit Feed Quality Tracking
-- -----------------------------------------------------------------------------
-- Business Rule: Track feed quality characteristics that impact unit performance
-- Key metrics: API gravity, sulfur content, temperature

SELECT
    feed_id,
    date_key,
    unit_id,
    feed_stream_id,
    feed_volume_bbl,
    feed_api_gravity,
    feed_sulfur_ppm,
    -- Classify feed quality
    CASE
        WHEN feed_api_gravity > 35 THEN 'Light'
        WHEN feed_api_gravity BETWEEN 25 AND 35 THEN 'Medium'
        ELSE 'Heavy'
    END AS feed_gravity_class,
    CASE
        WHEN feed_sulfur_ppm < 500 THEN 'Sweet'
        WHEN feed_sulfur_ppm BETWEEN 500 AND 5000 THEN 'Medium Sour'
        ELSE 'High Sour'
    END AS feed_sulfur_class
FROM stg_unit_feed;

-- ==============================================================================
-- Data Quality Summary Report
-- ==============================================================================

-- Comprehensive data quality metrics for unit operations
SELECT
    'Total Operations' AS metric,
    COUNT(*) AS value
FROM stg_unit_operations

UNION ALL

SELECT
    'Operations with Planned Downtime',
    COUNT(*)
FROM stg_unit_operations
WHERE planned_downtime_hours > 0

UNION ALL

SELECT
    'Operations with Unplanned Downtime',
    COUNT(*)
FROM stg_unit_operations
WHERE unplanned_downtime_hours > 0

UNION ALL

SELECT
    'Full Day Shutdowns',
    COUNT(*)
FROM stg_unit_operations
WHERE operating_hours = 0

UNION ALL

SELECT
    'High Utilization (>90%)',
    COUNT(*)
FROM stg_unit_operations
WHERE capacity_utilization_pct > 90

UNION ALL

SELECT
    'Catalytic Units Tracked',
    COUNT(DISTINCT unit_id)
FROM stg_unit_operations
WHERE catalyst_cycle_id IS NOT NULL;

-- ==============================================================================
-- End of Unit Operations Transformations
-- ==============================================================================
