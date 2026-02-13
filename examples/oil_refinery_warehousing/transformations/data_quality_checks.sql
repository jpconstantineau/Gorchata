-- ==============================================================================
-- Data Quality Checks and Anomaly Detection
-- ==============================================================================
--
-- This file contains SQL transformations for comprehensive data quality
-- validation and anomaly detection including:
-- - Range validations (industry-standard bounds)
-- - Z-score outlier detection (3-sigma rule)
-- - Cross-attribute consistency checks
-- - Seasonal specification compliance (RVP)
-- - Moving average trend deviation
-- - Yield sum reasonableness
--
-- References:
-- - docs/DATA_QUALITY_RULES.md
-- - Statistical Process Control (SPC) principles
-- - EPA/State seasonal gasoline specifications
-- ==============================================================================

-- ==============================================================================
-- A. RANGE VALIDATIONS
-- ==============================================================================
--
-- These checks validate that measured values fall within industry-standard
-- acceptable ranges. Violations indicate measurement errors, equipment
-- malfunction, or data entry issues.
-- ==============================================================================

-- API Gravity Range Check (5-50° typical for crude oil)
-- Light crude: 35-45°, Medium: 25-35°, Heavy: 10-25°
INSERT INTO stg_data_quality_checks (
    check_id,
    date_key,
    rule_id,
    entity_type,
    entity_id,
    check_timestamp,
    measured_value,
    expected_value,
    deviation,
    z_score,
    pass_fail,
    notes
)
SELECT
    'CHK_API_' || receipt_id AS check_id,
    date_key,
    'RULE_RANGE_API' AS rule_id,
    'Crude' AS entity_type,
    receipt_id AS entity_id,
    CURRENT_TIMESTAMP AS check_timestamp,
    api_gravity_60f AS measured_value,
    NULL AS expected_value,
    CASE 
        WHEN api_gravity_60f < 5.0 THEN 5.0 - api_gravity_60f
        WHEN api_gravity_60f > 50.0 THEN api_gravity_60f - 50.0
        ELSE 0.0
    END AS deviation,
    NULL AS z_score,
    CASE 
        WHEN api_gravity_60f BETWEEN 5.0 AND 50.0 THEN 'Pass'
        ELSE 'Fail'
    END AS pass_fail,
    CASE 
        WHEN api_gravity_60f < 5.0 THEN 'API gravity below minimum (5°) - verify measurement'
        WHEN api_gravity_60f > 50.0 THEN 'API gravity above maximum (50°) - verify measurement'
        ELSE NULL
    END AS notes
FROM fact_crude_receipts
WHERE api_gravity_60f IS NOT NULL;

-- Sulfur Content Range Check (0.01-7% by weight)
-- Sweet crude: <0.5%, Sour crude: >0.5%, High sour: >2%
INSERT INTO stg_data_quality_checks (
    check_id,
    date_key,
    rule_id,
    entity_type,
    entity_id,
    check_timestamp,
    measured_value,
    expected_value,
    deviation,
    z_score,
    pass_fail,
    notes
)
SELECT
    'CHK_SULFUR_' || receipt_id AS check_id,
    date_key,
    'RULE_RANGE_SULFUR' AS rule_id,
    'Crude' AS entity_type,
    receipt_id AS entity_id,
    CURRENT_TIMESTAMP AS check_timestamp,
    sulfur_wt_pct AS measured_value,
    NULL AS expected_value,
    CASE 
        WHEN sulfur_wt_pct < 0.01 THEN 0.01 - sulfur_wt_pct
        WHEN sulfur_wt_pct > 7.0 THEN sulfur_wt_pct - 7.0
        ELSE 0.0
    END AS deviation,
    NULL AS z_score,
    CASE 
        WHEN sulfur_wt_pct BETWEEN 0.01 AND 7.0 THEN 'Pass'
        ELSE 'Fail'
    END AS pass_fail,
    CASE 
        WHEN sulfur_wt_pct < 0.01 THEN 'Sulfur content below detection limit'
        WHEN sulfur_wt_pct > 7.0 THEN 'Sulfur content exceeds typical range - verify analysis'
        ELSE NULL
    END AS notes
FROM fact_crude_receipts
WHERE sulfur_wt_pct IS NOT NULL;

-- Temperature Range Check (30-150°F for storage/transfer)
INSERT INTO stg_data_quality_checks (
    check_id,
    date_key,
    rule_id,
    entity_type,
    entity_id,
    check_timestamp,
    measured_value,
    expected_value,
    deviation,
    z_score,
    pass_fail,
    notes
)
SELECT
    'CHK_TEMP_' || receipt_id AS check_id,
    date_key,
    'RULE_RANGE_TEMPERATURE' AS rule_id,
    'Crude' AS entity_type,
    receipt_id AS entity_id,
    CURRENT_TIMESTAMP AS check_timestamp,
    observed_temperature_f AS measured_value,
    NULL AS expected_value,
    CASE 
        WHEN observed_temperature_f < 30.0 THEN 30.0 - observed_temperature_f
        WHEN observed_temperature_f > 150.0 THEN observed_temperature_f - 150.0
        ELSE 0.0
    END AS deviation,
    NULL AS z_score,
    CASE 
        WHEN observed_temperature_f BETWEEN 30.0 AND 150.0 THEN 'Pass'
        ELSE 'Fail'
    END AS pass_fail,
    CASE 
        WHEN observed_temperature_f < 30.0 THEN 'Temperature below operational minimum'
        WHEN observed_temperature_f > 150.0 THEN 'Temperature exceeds safe handling limit'
        ELSE NULL
    END AS notes
FROM fact_crude_receipts
WHERE observed_temperature_f IS NOT NULL;

-- Capacity Utilization Range Check (0-105%)
-- Can slightly exceed 100% for short periods
INSERT INTO stg_data_quality_checks (
    check_id,
    date_key,
    rule_id,
    entity_type,
    entity_id,
    check_timestamp,
    measured_value,
    expected_value,
    deviation,
    z_score,
    pass_fail,
    notes
)
SELECT
    'CHK_CAPACITY_' || operation_id AS check_id,
    date_key,
    'RULE_RANGE_CAPACITY' AS rule_id,
    'Unit' AS entity_type,
    unit_id AS entity_id,
    CURRENT_TIMESTAMP AS check_timestamp,
    capacity_utilization_pct AS measured_value,
    NULL AS expected_value,
    CASE 
        WHEN capacity_utilization_pct < 0.0 THEN 0.0 - capacity_utilization_pct
        WHEN capacity_utilization_pct > 105.0 THEN capacity_utilization_pct - 105.0
        ELSE 0.0
    END AS deviation,
    NULL AS z_score,
    CASE 
        WHEN capacity_utilization_pct BETWEEN 0.0 AND 105.0 THEN 'Pass'
        ELSE 'Fail'
    END AS pass_fail,
    CASE 
        WHEN capacity_utilization_pct < 0.0 THEN 'Negative capacity utilization - data error'
        WHEN capacity_utilization_pct > 105.0 THEN 'Capacity exceeded - verify throughput calculation'
        ELSE NULL
    END AS notes
FROM fact_unit_operations
WHERE capacity_utilization_pct IS NOT NULL;

-- BS&W Range Check (0-2% for Bottom Sediment & Water)
INSERT INTO stg_data_quality_checks (
    check_id,
    date_key,
    rule_id,
    entity_type,
    entity_id,
    check_timestamp,
    measured_value,
    expected_value,
    deviation,
    z_score,
    pass_fail,
    notes
)
SELECT
    'CHK_BSW_' || receipt_id AS check_id,
    date_key,
    'RULE_RANGE_BSW' AS rule_id,
    'Crude' AS entity_type,
    receipt_id AS entity_id,
    CURRENT_TIMESTAMP AS check_timestamp,
    bsw_pct AS measured_value,
    NULL AS expected_value,
    CASE 
        WHEN bsw_pct < 0.0 THEN 0.0 - bsw_pct
        WHEN bsw_pct > 2.0 THEN bsw_pct - 2.0
        ELSE 0.0
    END AS deviation,
    NULL AS z_score,
    CASE 
        WHEN bsw_pct BETWEEN 0.0 AND 2.0 THEN 'Pass'
        ELSE 'Fail'
    END AS pass_fail,
    CASE 
        WHEN bsw_pct < 0.0 THEN 'Negative BS&W - data error'
        WHEN bsw_pct > 2.0 THEN 'Excessive BS&W - quality control issue'
        ELSE NULL
    END AS notes
FROM fact_crude_receipts
WHERE bsw_pct IS NOT NULL;

-- ==============================================================================
-- B. Z-SCORE OUTLIER DETECTION (3-Sigma Rule)
-- ==============================================================================
--
-- Statistical outlier detection using the Z-score method:
-- Z-score = (Value - Mean) / StandardDeviation
-- Flag if |Z-score| > 3.0 (99.7% confidence interval)
--
-- Applied to time-series data over rolling 30-day windows.
-- ==============================================================================

-- Crude Receipt Volume Outlier Detection
INSERT INTO stg_data_quality_checks (
    check_id,
    date_key,
    rule_id,
    entity_type,
    entity_id,
    check_timestamp,
    measured_value,
    expected_value,
    deviation,
    z_score,
    pass_fail,
    notes
)
WITH receipt_stats AS (
    SELECT 
        crude_grade_id,
        AVG(net_volume_bbl) AS mean_volume,
        STDDEV(net_volume_bbl) AS stddev_volume,
        COUNT(*) AS sample_count
    FROM fact_crude_receipts
    WHERE date_key >= CAST(FORMAT(DATEADD(day, -30, GETDATE()), 'yyyyMMdd') AS INT)
    GROUP BY crude_grade_id
    HAVING COUNT(*) >= 10  -- Require minimum sample size
)
SELECT
    'CHK_ZSCORE_VOL_' || cr.receipt_id AS check_id,
    cr.date_key,
    'RULE_OUTLIER_VOLUME' AS rule_id,
    'Crude' AS entity_type,
    cr.receipt_id AS entity_id,
    CURRENT_TIMESTAMP AS check_timestamp,
    cr.net_volume_bbl AS measured_value,
    rs.mean_volume AS expected_value,
    cr.net_volume_bbl - rs.mean_volume AS deviation,
    (cr.net_volume_bbl - rs.mean_volume) / NULLIF(rs.stddev_volume, 0) AS z_score,
    CASE 
        WHEN ABS((cr.net_volume_bbl - rs.mean_volume) / NULLIF(rs.stddev_volume, 0)) > 3.0 
        THEN 'Fail'
        WHEN ABS((cr.net_volume_bbl - rs.mean_volume) / NULLIF(rs.stddev_volume, 0)) > 2.0 
        THEN 'Warning'
        ELSE 'Pass'
    END AS pass_fail,
    CASE 
        WHEN ABS((cr.net_volume_bbl - rs.mean_volume) / NULLIF(rs.stddev_volume, 0)) > 3.0 
        THEN 'Statistical outlier detected - investigate receipt volume'
        WHEN ABS((cr.net_volume_bbl - rs.mean_volume) / NULLIF(rs.stddev_volume, 0)) > 2.0 
        THEN 'Volume approaching 3-sigma limit'
        ELSE NULL
    END AS notes
FROM fact_crude_receipts cr
INNER JOIN receipt_stats rs ON cr.crude_grade_id = rs.crude_grade_id
WHERE rs.stddev_volume > 0;

-- Unit Throughput Outlier Detection
INSERT INTO stg_data_quality_checks (
    check_id,
    date_key,
    rule_id,
    entity_type,
    entity_id,
    check_timestamp,
    measured_value,
    expected_value,
    deviation,
    z_score,
    pass_fail,
    notes
)
WITH throughput_stats AS (
    SELECT 
        unit_id,
        AVG(throughput_bbl) AS mean_throughput,
        STDDEV(throughput_bbl) AS stddev_throughput,
        COUNT(*) AS sample_count
    FROM fact_unit_operations
    WHERE date_key >= CAST(FORMAT(DATEADD(day, -30, GETDATE()), 'yyyyMMdd') AS INT)
    GROUP BY unit_id
    HAVING COUNT(*) >= 10
)
SELECT
    'CHK_ZSCORE_THRU_' || uo.operation_id AS check_id,
    uo.date_key,
    'RULE_OUTLIER_THROUGHPUT' AS rule_id,
    'Unit' AS entity_type,
    uo.operation_id AS entity_id,
    CURRENT_TIMESTAMP AS check_timestamp,
    uo.throughput_bbl AS measured_value,
    ts.mean_throughput AS expected_value,
    uo.throughput_bbl - ts.mean_throughput AS deviation,
    (uo.throughput_bbl - ts.mean_throughput) / NULLIF(ts.stddev_throughput, 0) AS z_score,
    CASE 
        WHEN ABS((uo.throughput_bbl - ts.mean_throughput) / NULLIF(ts.stddev_throughput, 0)) > 3.0 
        THEN 'Fail'
        WHEN ABS((uo.throughput_bbl - ts.mean_throughput) / NULLIF(ts.stddev_throughput, 0)) > 2.0 
        THEN 'Warning'
        ELSE 'Pass'
    END AS pass_fail,
    CASE 
        WHEN ABS((uo.throughput_bbl - ts.mean_throughput) / NULLIF(ts.stddev_throughput, 0)) > 3.0 
        THEN 'Statistical outlier - verify unit operations data'
        WHEN ABS((uo.throughput_bbl - ts.mean_throughput) / NULLIF(ts.stddev_throughput, 0)) > 2.0 
        THEN 'Throughput deviating from typical range'
        ELSE NULL
    END AS notes
FROM fact_unit_operations uo
INNER JOIN throughput_stats ts ON uo.unit_id = ts.unit_id
WHERE ts.stddev_throughput > 0;

-- ==============================================================================
-- C. CROSS-ATTRIBUTE CONSISTENCY CHECKS
-- ==============================================================================
--
-- These checks validate consistency between related measurements:
-- - API gravity vs. Specific gravity (calculated relationship)
-- - Volume vs. Weight (with known specific gravity)
-- ==============================================================================

-- API Gravity vs Specific Gravity Consistency Check
-- Formula: SG = 141.5 / (API + 131.5)
-- Tolerance: ±0.5% deviation acceptable (measurement precision)
INSERT INTO stg_data_quality_checks (
    check_id,
    date_key,
    rule_id,
    entity_type,
    entity_id,
    check_timestamp,
    measured_value,
    expected_value,
    deviation,
    z_score,
    pass_fail,
    notes
)
SELECT
    'CHK_CONSIST_API_SG_' || receipt_id AS check_id,
    date_key,
    'RULE_CONSISTENCY_API_SG' AS rule_id,
    'Crude' AS entity_type,
    receipt_id AS entity_id,
    CURRENT_TIMESTAMP AS check_timestamp,
    specific_gravity_60f AS measured_value,
    141.5 / (api_gravity_60f + 131.5) AS expected_value,
    ABS(specific_gravity_60f - (141.5 / (api_gravity_60f + 131.5))) AS deviation,
    NULL AS z_score,
    CASE 
        WHEN ABS(specific_gravity_60f - (141.5 / (api_gravity_60f + 131.5))) 
             / (141.5 / (api_gravity_60f + 131.5)) * 100 <= 0.5 
        THEN 'Pass'
        ELSE 'Fail'
    END AS pass_fail,
    CASE 
        WHEN ABS(specific_gravity_60f - (141.5 / (api_gravity_60f + 131.5))) 
             / (141.5 / (api_gravity_60f + 131.5)) * 100 > 0.5 
        THEN 'API gravity and specific gravity inconsistent - deviation: ' 
             || CAST(ROUND(ABS(specific_gravity_60f - (141.5 / (api_gravity_60f + 131.5))) 
                     / (141.5 / (api_gravity_60f + 131.5)) * 100, 3) AS VARCHAR) || '%'
        ELSE NULL
    END AS notes
FROM fact_crude_receipts
WHERE api_gravity_60f IS NOT NULL 
  AND specific_gravity_60f IS NOT NULL
  AND api_gravity_60f > 0;

-- Volume-Weight Consistency Check
-- Formula: Weight (tons) = Volume (bbl) × 0.1756 × SG
-- Tolerance: ±1.0% deviation acceptable
INSERT INTO stg_data_quality_checks (
    check_id,
    date_key,
    rule_id,
    entity_type,
    entity_id,
    check_timestamp,
    measured_value,
    expected_value,
    deviation,
    z_score,
    pass_fail,
    notes
)
SELECT
    'CHK_CONSIST_VOL_WT_' || receipt_id AS check_id,
    date_key,
    'RULE_CONSISTENCY_VOL_WEIGHT' AS rule_id,
    'Crude' AS entity_type,
    receipt_id AS entity_id,
    CURRENT_TIMESTAMP AS check_timestamp,
    weight_short_tons AS measured_value,
    net_volume_bbl * 0.1756 * specific_gravity_60f AS expected_value,
    ABS(weight_short_tons - (net_volume_bbl * 0.1756 * specific_gravity_60f)) AS deviation,
    NULL AS z_score,
    CASE 
        WHEN ABS(weight_short_tons - (net_volume_bbl * 0.1756 * specific_gravity_60f)) 
             / (net_volume_bbl * 0.1756 * specific_gravity_60f) * 100 <= 1.0 
        THEN 'Pass'
        ELSE 'Fail'
    END AS pass_fail,
    CASE 
        WHEN ABS(weight_short_tons - (net_volume_bbl * 0.1756 * specific_gravity_60f)) 
             / (net_volume_bbl * 0.1756 * specific_gravity_60f) * 100 > 1.0 
        THEN 'Volume and weight measurements inconsistent - deviation: ' 
             || CAST(ROUND(ABS(weight_short_tons - (net_volume_bbl * 0.1756 * specific_gravity_60f)) 
                     / (net_volume_bbl * 0.1756 * specific_gravity_60f) * 100, 3) AS VARCHAR) || '%'
        ELSE NULL
    END AS notes
FROM fact_crude_receipts
WHERE net_volume_bbl IS NOT NULL 
  AND weight_short_tons IS NOT NULL
  AND specific_gravity_60f IS NOT NULL
  AND specific_gravity_60f > 0
  AND net_volume_bbl > 0;

-- ==============================================================================
-- D. SEASONAL SPECIFICATION COMPLIANCE
-- ==============================================================================
--
-- Gasoline Reid Vapor Pressure (RVP) Seasonal Limits
-- Summer (June 1 - September 15): ≤ 7.8 psi (volatility control)
-- Winter (September 16 - May 31): ≤ 13.5 psi (cold-start performance)
--
-- These specifications prevent excessive evaporative emissions in hot weather
-- while ensuring adequate volatility for cold weather starting.
-- ==============================================================================

INSERT INTO stg_data_quality_checks (
    check_id,
    date_key,
    rule_id,
    entity_type,
    entity_id,
    check_timestamp,
    measured_value,
    expected_value,
    deviation,
    z_score,
    pass_fail,
    notes
)
WITH seasonal_limits AS (
    SELECT 
        ps.shipment_id,
        ps.date_key,
        ps.product_id,
        ps.rvp_psi,
        d.month,
        d.day_of_month,
        CASE 
            WHEN d.month >= 6 AND d.month <= 9 
                AND (d.month < 9 OR d.day_of_month <= 15)
            THEN 7.8   -- Summer spec: June 1 - September 15
            ELSE 13.5  -- Winter spec: September 16 - May 31
        END AS max_rvp_psi,
        CASE 
            WHEN d.month >= 6 AND d.month <= 9 
                AND (d.month < 9 OR d.day_of_month <= 15)
            THEN 'Summer'
            ELSE 'Winter'
        END AS season
    FROM fact_product_shipments ps
    INNER JOIN dim_date d ON ps.date_key = d.date_key
    INNER JOIN dim_product p ON ps.product_id = p.product_id
    WHERE p.product_name LIKE '%Gasoline%'
      AND ps.rvp_psi IS NOT NULL
)
SELECT
    'CHK_RVP_' || shipment_id AS check_id,
    date_key,
    'RULE_SEASONAL_RVP' AS rule_id,
    'Product' AS entity_type,
    shipment_id AS entity_id,
    CURRENT_TIMESTAMP AS check_timestamp,
    rvp_psi AS measured_value,
    max_rvp_psi AS expected_value,
    CASE 
        WHEN rvp_psi > max_rvp_psi THEN rvp_psi - max_rvp_psi
        ELSE 0.0
    END AS deviation,
    NULL AS z_score,
    CASE 
        WHEN rvp_psi <= max_rvp_psi THEN 'Pass'
        ELSE 'Fail'
    END AS pass_fail,
    CASE 
        WHEN rvp_psi > max_rvp_psi 
        THEN season || ' RVP limit exceeded: ' || CAST(rvp_psi AS VARCHAR) 
             || ' psi > ' || CAST(max_rvp_psi AS VARCHAR) || ' psi'
        ELSE NULL
    END AS notes
FROM seasonal_limits;

-- ==============================================================================
-- E. MOVING AVERAGE DEVIATION (Trend Analysis)
-- ==============================================================================
--
-- Flags values that deviate significantly from recent trends.
-- Uses 7-day moving average with ±15% tolerance.
-- Helps identify process upsets, equipment degradation, or data quality issues.
-- ==============================================================================

-- Unit Throughput Moving Average Deviation Check
INSERT INTO stg_data_quality_checks (
    check_id,
    date_key,
    rule_id,
    entity_type,
    entity_id,
    check_timestamp,
    measured_value,
    expected_value,
    deviation,
    z_score,
    pass_fail,
    notes
)
WITH moving_avg AS (
    SELECT 
        operation_id,
        date_key,
        unit_id,
        throughput_bbl,
        AVG(throughput_bbl) OVER (
            PARTITION BY unit_id 
            ORDER BY date_key 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS avg_7day,
        COUNT(*) OVER (
            PARTITION BY unit_id 
            ORDER BY date_key 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS day_count
    FROM fact_unit_operations
)
SELECT
    'CHK_MAVG_' || operation_id AS check_id,
    date_key,
    'RULE_TREND_MOVING_AVG' AS rule_id,
    'Unit' AS entity_type,
    operation_id AS entity_id,
    CURRENT_TIMESTAMP AS check_timestamp,
    throughput_bbl AS measured_value,
    avg_7day AS expected_value,
    (throughput_bbl - avg_7day) / NULLIF(avg_7day, 0) * 100 AS deviation,
    NULL AS z_score,
    CASE 
        WHEN ABS((throughput_bbl - avg_7day) / NULLIF(avg_7day, 0)) * 100 > 15.0 
        THEN 'Warning'
        ELSE 'Pass'
    END AS pass_fail,
    CASE 
        WHEN ABS((throughput_bbl - avg_7day) / NULLIF(avg_7day, 0)) * 100 > 15.0 
        THEN 'Throughput deviates ' 
             || CAST(ROUND((throughput_bbl - avg_7day) / NULLIF(avg_7day, 0) * 100, 1) AS VARCHAR) 
             || '% from 7-day average'
        ELSE NULL
    END AS notes
FROM moving_avg
WHERE day_count >= 7  -- Require full 7-day window
  AND avg_7day > 0;

-- ==============================================================================
-- F. YIELD SUM REASONABLENESS CHECK
-- ==============================================================================
--
-- Validates that total product yields from process units are within reasonable
-- bounds accounting for:
-- - Volume expansion from cracking (FCC): 95-110% acceptable
-- - Weight losses from conversion: 95-99% acceptable (coke, fuel, losses)
--
-- Violations indicate yield calculation errors or data quality issues.
-- ==============================================================================

INSERT INTO stg_data_quality_checks (
    check_id,
    date_key,
    rule_id,
    entity_type,
    entity_id,
    check_timestamp,
    measured_value,
    expected_value,
    deviation,
    z_score,
    pass_fail,
    notes
)
WITH yield_totals AS (
    SELECT 
        date_key,
        unit_id,
        SUM(yield_pct_volume) AS total_volume_yield,
        SUM(yield_pct_weight) AS total_weight_yield,
        COUNT(*) AS product_count
    FROM fact_unit_production
    GROUP BY date_key, unit_id
)
SELECT
    'CHK_YIELD_' || CAST(date_key AS VARCHAR) || '_' || unit_id AS check_id,
    date_key,
    'RULE_CONSISTENCY_YIELD_SUM' AS rule_id,
    'Unit' AS entity_type,
    unit_id AS entity_id,
    CURRENT_TIMESTAMP AS check_timestamp,
    total_volume_yield AS measured_value,
    100.0 AS expected_value,
    ABS(total_volume_yield - 100.0) AS deviation,
    NULL AS z_score,
    CASE 
        WHEN total_volume_yield BETWEEN 95.0 AND 110.0 
             AND total_weight_yield BETWEEN 95.0 AND 99.0 
        THEN 'Pass'
        ELSE 'Fail'
    END AS pass_fail,
    CASE 
        WHEN total_volume_yield < 95.0 OR total_volume_yield > 110.0 
        THEN 'Volume yield sum unreasonable: ' || CAST(ROUND(total_volume_yield, 1) AS VARCHAR) || '% (expected 95-110%)'
        WHEN total_weight_yield < 95.0 OR total_weight_yield > 99.0 
        THEN 'Weight yield sum unreasonable: ' || CAST(ROUND(total_weight_yield, 1) AS VARCHAR) || '% (expected 95-99%)'
        ELSE NULL
    END AS notes
FROM yield_totals
WHERE product_count >= 3;  -- Require minimum product count

-- ==============================================================================
-- TRANSFORMATION: stg_data_quality_checks → fact_data_quality_checks
-- ==============================================================================
--
-- Promote validated quality check results from staging to the fact table.
-- ==============================================================================

INSERT INTO fact_data_quality_checks (
    check_id,
    date_key,
    rule_id,
    entity_type,
    entity_id,
    check_timestamp,
    measured_value,
    expected_value,
    deviation,
    z_score,
    pass_fail,
    notes
)
SELECT
    check_id,
    date_key,
    rule_id,
    entity_type,
    entity_id,
    check_timestamp,
    measured_value,
    expected_value,
    deviation,
    z_score,
    pass_fail,
    notes
FROM stg_data_quality_checks;

-- ==============================================================================
-- DATA QUALITY DASHBOARD QUERIES
-- ==============================================================================

-- Summary by Rule Category
-- SELECT 
--     qr.rule_category,
--     COUNT(*) AS total_checks,
--     SUM(CASE WHEN dq.pass_fail = 'Pass' THEN 1 ELSE 0 END) AS passed,
--     SUM(CASE WHEN dq.pass_fail = 'Fail' THEN 1 ELSE 0 END) AS failed,
--     SUM(CASE WHEN dq.pass_fail = 'Warning' THEN 1 ELSE 0 END) AS warnings,
--     ROUND(SUM(CASE WHEN dq.pass_fail = 'Pass' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pass_rate_pct
-- FROM fact_data_quality_checks dq
-- INNER JOIN dim_quality_rule qr ON dq.rule_id = qr.rule_id
-- GROUP BY qr.rule_category
-- ORDER BY pass_rate_pct ASC;

-- Recent Failures by Severity
-- SELECT TOP 20
--     dq.date_key,
--     qr.rule_name,
--     qr.severity,
--     dq.entity_type,
--     dq.entity_id,
--     dq.measured_value,
--     dq.expected_value,
--     dq.deviation,
--     dq.notes
-- FROM fact_data_quality_checks dq
-- INNER JOIN dim_quality_rule qr ON dq.rule_id = qr.rule_id
-- WHERE dq.pass_fail = 'Fail'
--   AND qr.severity = 'Critical'
-- ORDER BY dq.date_key DESC;

-- Trend Analysis - Daily Pass Rate
-- SELECT 
--     date_key,
--     COUNT(*) AS total_checks,
--     SUM(CASE WHEN pass_fail = 'Pass' THEN 1 ELSE 0 END) AS passed,
--     ROUND(SUM(CASE WHEN pass_fail = 'Pass' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pass_rate_pct
-- FROM fact_data_quality_checks
-- GROUP BY date_key
-- ORDER BY date_key DESC;
