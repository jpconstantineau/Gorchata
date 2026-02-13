-- ===================================================================
-- KPI AGGREGATIONS - Oil Refinery Data Warehousing
-- ===================================================================
-- Purpose: Calculate daily and monthly KPI metrics for yield optimization,
--          energy efficiency, and operational performance monitoring
-- Phase: 8 (Final Phase)
-- Dependencies: fact_crude_receipts, fact_unit_operations, fact_unit_production,
--               fact_mass_balance, fact_data_quality_checks
-- ===================================================================

-- ===================================================================
-- SECTION 1: DAILY KPI AGGREGATIONS
-- ===================================================================

-- -------------------------------------------------------------------
-- STEP 1.1: Calculate Crude Input Aggregates
-- -------------------------------------------------------------------
-- Purpose: Sum daily crude oil receipts as basis for yield calculations
-- Business Rule: All crude entering the refinery must be tracked
-- -------------------------------------------------------------------

WITH crude_daily AS (
  SELECT 
    date_key,
    SUM(receipt_volume_bbl) AS crude_input_bbl,
    SUM(receipt_volume_tons) AS crude_input_tons
  FROM fact_crude_receipts
  GROUP BY date_key
),

-- -------------------------------------------------------------------
-- STEP 1.2: Calculate Throughput Aggregates
-- -------------------------------------------------------------------
-- Purpose: Sum throughput across all process units
-- Business Rule: Throughput represents total refinery processing capacity utilization
-- -------------------------------------------------------------------

throughput_daily AS (
  SELECT 
    date_key,
    SUM(throughput_bbl) AS total_throughput_bbl,
    SUM(refinery_fuel_consumed_tons) AS refinery_fuel_tons
  FROM fact_unit_operations
  GROUP BY date_key
),

-- -------------------------------------------------------------------
-- STEP 1.3: Calculate Product Slate
-- -------------------------------------------------------------------
-- Purpose: Aggregate production by major product categories
-- Business Rule: Gasoline and distillates are high-value products
--                Gasoline = Regular, Premium, Midgrade gasoline blends
--                Distillates = Diesel, Jet Fuel
-- -------------------------------------------------------------------

production_daily AS (
  SELECT 
    date_key,
    SUM(CASE WHEN product_type IN ('Gasoline', 'Premium Gasoline', 'Regular Gasoline') 
             THEN product_volume_bbl ELSE 0 END) AS gasoline_production_bbl,
    SUM(CASE WHEN product_type IN ('Diesel', 'Jet Fuel', 'Kerosene') 
             THEN product_volume_bbl ELSE 0 END) AS distillate_production_bbl,
    SUM(CASE WHEN product_type IN ('Gasoline', 'Premium Gasoline', 'Regular Gasoline', 
                                    'Diesel', 'Jet Fuel', 'Kerosene') 
             THEN product_volume_bbl ELSE 0 END) AS high_value_products_bbl
  FROM fact_unit_production
  GROUP BY date_key
),

-- -------------------------------------------------------------------
-- STEP 1.4: Calculate Energy Intensity Index (EII)
-- -------------------------------------------------------------------
-- Purpose: Measure energy efficiency across the refinery
-- Formula: EII = Energy Consumed (MMBtu) / Throughput (bbl)
-- Business Rule: Lower EII indicates better energy efficiency
--                Industry benchmark for complex refineries: 0.6-0.8 MMBtu/bbl
--                Target: < 0.7 MMBtu/bbl
-- Assumptions: Refinery fuel gas has heating value of ~18 MMBtu/ton
-- -------------------------------------------------------------------

energy_daily AS (
  SELECT 
    date_key,
    refinery_fuel_tons * 18.0 AS energy_consumed_mmbtu,
    -- Refinery fuel gas typically has ~18 MMBtu/ton heating value
    total_throughput_bbl,
    (refinery_fuel_tons * 18.0) / NULLIF(total_throughput_bbl, 0) AS energy_intensity_index
  FROM throughput_daily
),

-- -------------------------------------------------------------------
-- STEP 1.5: Calculate Capacity Utilization
-- -------------------------------------------------------------------
-- Purpose: Measure how effectively refinery capacity is being used
-- Formula: Weighted average by unit capacity
--          WeightedAvg = Sum(Throughput × Capacity) / Sum(Capacity²) × 100
-- Business Rule: High utilization (>90%) = efficient operations
--                Low utilization (<70%) = investigate constraints
-- -------------------------------------------------------------------

capacity_daily AS (
  SELECT 
    o.date_key,
    SUM(o.throughput_bbl * u.capacity_bbl_day) / 
      NULLIF(SUM(u.capacity_bbl_day * u.capacity_bbl_day), 0) * 100 AS avg_capacity_utilization_pct
  FROM fact_unit_operations o
  JOIN dim_unit u ON o.unit_id = u.unit_id
  GROUP BY o.date_key
),

-- -------------------------------------------------------------------
-- STEP 1.6: Calculate Downtime and Reliability
-- -------------------------------------------------------------------
-- Purpose: Track planned vs unplanned downtime for reliability analysis
-- Formula: Reliability Factor = (Available Hours - Downtime) / Available Hours × 100
-- Business Rule: Planned downtime expected during turnarounds (every 3-5 years)
--                Unplanned downtime should be minimized (target < 2% annually)
--                Available hours = 24 hours/day × number of units
-- -------------------------------------------------------------------

downtime_daily AS (
  SELECT 
    date_key,
    SUM(CASE WHEN downtime_type = 'Planned' THEN downtime_hours ELSE 0 END) AS planned_downtime_hours,
    SUM(CASE WHEN downtime_type = 'Unplanned' THEN downtime_hours ELSE 0 END) AS unplanned_downtime_hours,
    COUNT(DISTINCT unit_id) * 24 AS available_hours
  FROM fact_unit_operations
  GROUP BY date_key
),

reliability_daily AS (
  SELECT 
    date_key,
    planned_downtime_hours,
    unplanned_downtime_hours,
    ((available_hours - planned_downtime_hours - unplanned_downtime_hours) / 
      NULLIF(available_hours, 0)) * 100 AS reliability_factor_pct
  FROM downtime_daily
),

-- -------------------------------------------------------------------
-- STEP 1.7: Calculate Mass Balance Metrics
-- -------------------------------------------------------------------
-- Purpose: Track unaccounted for loss (UFL) from mass balance
-- Business Rule: UFL should be < 0.5% for accurate inventory management
--                UFL > 1.0% triggers investigation
-- -------------------------------------------------------------------

balance_daily AS (
  SELECT 
    date_key,
    AVG(unaccounted_pct) AS ufl_pct
  FROM fact_mass_balance
  GROUP BY date_key
),

-- -------------------------------------------------------------------
-- STEP 1.8: Calculate Data Quality Metrics
-- -------------------------------------------------------------------
-- Purpose: Monitor data quality for decision-making confidence
-- Formula: Pass Rate = (Checks Passed / Total Checks) × 100
-- Business Rule: Target data quality pass rate > 95%
--                Pass rate < 90% indicates data integrity issues
-- -------------------------------------------------------------------

quality_daily AS (
  SELECT 
    date_key,
    SUM(CASE WHEN pass_fail = 'Pass' THEN 1 ELSE 0 END) * 100.0 / 
      NULLIF(COUNT(*), 0) AS data_quality_pass_rate_pct
  FROM fact_data_quality_checks
  GROUP BY date_key
)

-- -------------------------------------------------------------------
-- STEP 1.9: Assemble Daily KPI Fact Table
-- -------------------------------------------------------------------
-- Purpose: Combine all daily KPI metrics into single aggregate table
-- Target: stg_daily_kpi staging table, then fact_daily_kpi
-- -------------------------------------------------------------------

INSERT INTO stg_daily_kpi (
  kpi_id,
  date_key,
  crude_input_bbl,
  crude_input_tons,
  total_throughput_bbl,
  gasoline_production_bbl,
  distillate_production_bbl,
  gasoline_yield_pct,
  distillate_yield_pct,
  high_value_product_pct,
  energy_consumed_mmbtu,
  energy_intensity_index,
  avg_capacity_utilization_pct,
  planned_downtime_hours,
  unplanned_downtime_hours,
  reliability_factor_pct,
  ufl_pct,
  data_quality_pass_rate_pct
)
SELECT 
  'KPI-' || c.date_key AS kpi_id,
  c.date_key,
  c.crude_input_bbl,
  c.crude_input_tons,
  t.total_throughput_bbl,
  p.gasoline_production_bbl,
  p.distillate_production_bbl,
  (p.gasoline_production_bbl / NULLIF(c.crude_input_bbl, 0)) * 100 AS gasoline_yield_pct,
  (p.distillate_production_bbl / NULLIF(c.crude_input_bbl, 0)) * 100 AS distillate_yield_pct,
  (p.high_value_products_bbl / NULLIF(c.crude_input_bbl, 0)) * 100 AS high_value_product_pct,
  e.energy_consumed_mmbtu,
  e.energy_intensity_index,
  cap.avg_capacity_utilization_pct,
  r.planned_downtime_hours,
  r.unplanned_downtime_hours,
  r.reliability_factor_pct,
  b.ufl_pct,
  q.data_quality_pass_rate_pct
FROM crude_daily c
LEFT JOIN throughput_daily t ON c.date_key = t.date_key
LEFT JOIN production_daily p ON c.date_key = p.date_key
LEFT JOIN energy_daily e ON c.date_key = e.date_key
LEFT JOIN capacity_daily cap ON c.date_key = cap.date_key
LEFT JOIN reliability_daily r ON c.date_key = r.date_key
LEFT JOIN balance_daily b ON c.date_key = b.date_key
LEFT JOIN quality_daily q ON c.date_key = q.date_key;

-- ===================================================================
-- SECTION 2: MONTHLY KPI AGGREGATIONS
-- ===================================================================

-- -------------------------------------------------------------------
-- STEP 2.1: Calculate Monthly KPI Aggregates with Trending
-- -------------------------------------------------------------------
-- Purpose: Roll up daily KPIs to monthly level for executive dashboards
-- Additional Metrics: 
--   - Avg/Max/Min daily throughput for volatility analysis
--   - Operating days count
--   - Month-over-month change percentage for trending
-- Business Rule: Monthly aggregates enable long-term trend analysis
--                and strategic decision-making
-- -------------------------------------------------------------------

WITH monthly_aggregates AS (
  SELECT 
    strftime('%Y%m', substr(date_key, 1, 4) || '-' || substr(date_key, 5, 2) || '-01') || '01' AS month_date_key,
    strftime('%Y-%m', substr(date_key, 1, 4) || '-' || substr(date_key, 5, 2) || '-01') AS month_year,
    SUM(crude_input_bbl) AS crude_input_bbl,
    SUM(crude_input_tons) AS crude_input_tons,
    SUM(total_throughput_bbl) AS total_throughput_bbl,
    SUM(gasoline_production_bbl) AS gasoline_production_bbl,
    SUM(distillate_production_bbl) AS distillate_production_bbl,
    AVG(gasoline_yield_pct) AS gasoline_yield_pct,
    AVG(distillate_yield_pct) AS distillate_yield_pct,
    AVG(high_value_product_pct) AS high_value_product_pct,
    SUM(energy_consumed_mmbtu) AS energy_consumed_mmbtu,
    AVG(energy_intensity_index) AS energy_intensity_index,
    AVG(avg_capacity_utilization_pct) AS avg_capacity_utilization_pct,
    SUM(planned_downtime_hours) AS planned_downtime_hours,
    SUM(unplanned_downtime_hours) AS unplanned_downtime_hours,
    AVG(reliability_factor_pct) AS reliability_factor_pct,
    AVG(total_throughput_bbl) AS avg_daily_throughput_bbl,
    MAX(total_throughput_bbl) AS max_daily_throughput_bbl,
    MIN(total_throughput_bbl) AS min_daily_throughput_bbl,
    -- Standard deviation for volatility
    CASE 
      WHEN COUNT(*) > 1 THEN
        SQRT(SUM((total_throughput_bbl - AVG(total_throughput_bbl)) * 
                 (total_throughput_bbl - AVG(total_throughput_bbl))) / 
             (COUNT(*) - 1))
      ELSE 0
    END AS throughput_volatility,
    COUNT(*) AS operating_days,
    AVG(ufl_pct) AS ufl_pct,
    AVG(data_quality_pass_rate_pct) AS data_quality_pass_rate_pct
  FROM stg_daily_kpi
  GROUP BY month_date_key, month_year
),

-- -------------------------------------------------------------------
-- STEP 2.2: Calculate Month-over-Month Change
-- -------------------------------------------------------------------
-- Purpose: Enable trending analysis by comparing to previous month
-- Formula: MoM Change% = ((Current - Previous) / Previous) × 100
-- Business Rule: Significant MoM changes (>10%) warrant investigation
-- -------------------------------------------------------------------

monthly_with_change AS (
  SELECT 
    m.*,
    LAG(m.total_throughput_bbl) OVER (ORDER BY m.month_date_key) AS prev_month_throughput,
    ((m.total_throughput_bbl - LAG(m.total_throughput_bbl) OVER (ORDER BY m.month_date_key)) /
      NULLIF(LAG(m.total_throughput_bbl) OVER (ORDER BY m.month_date_key), 0)) * 100 
      AS month_over_month_change_pct
  FROM monthly_aggregates m
)

-- -------------------------------------------------------------------
-- STEP 2.3: Assemble Monthly KPI Fact Table
-- -------------------------------------------------------------------
-- Purpose: Populate monthly KPI aggregate table for executive dashboards
-- Target: stg_monthly_kpi staging table, then fact_monthly_kpi
-- -------------------------------------------------------------------

INSERT INTO stg_monthly_kpi (
  kpi_id,
  date_key,
  month_year,
  crude_input_bbl,
  crude_input_tons,
  total_throughput_bbl,
  gasoline_production_bbl,
  distillate_production_bbl,
  gasoline_yield_pct,
  distillate_yield_pct,
  high_value_product_pct,
  energy_consumed_mmbtu,
  energy_intensity_index,
  avg_capacity_utilization_pct,
  planned_downtime_hours,
  unplanned_downtime_hours,
  reliability_factor_pct,
  avg_daily_throughput_bbl,
  max_daily_throughput_bbl,
  min_daily_throughput_bbl,
  throughput_volatility,
  operating_days,
  month_over_month_change_pct,
  ufl_pct,
  data_quality_pass_rate_pct
)
SELECT 
  'MKPI-' || month_date_key AS kpi_id,
  month_date_key AS date_key,
  month_year,
  crude_input_bbl,
  crude_input_tons,
  total_throughput_bbl,
  gasoline_production_bbl,
  distillate_production_bbl,
  gasoline_yield_pct,
  distillate_yield_pct,
  high_value_product_pct,
  energy_consumed_mmbtu,
  energy_intensity_index,
  avg_capacity_utilization_pct,
  planned_downtime_hours,
  unplanned_downtime_hours,
  reliability_factor_pct,
  avg_daily_throughput_bbl,
  max_daily_throughput_bbl,
  min_daily_throughput_bbl,
  throughput_volatility,
  operating_days,
  month_over_month_change_pct,
  ufl_pct,
  data_quality_pass_rate_pct
FROM monthly_with_change;

-- ===================================================================
-- SECTION 3: UNIT-LEVEL KPI CALCULATIONS
-- ===================================================================

-- -------------------------------------------------------------------
-- STEP 3.1: FCC Conversion Efficiency
-- -------------------------------------------------------------------
-- Purpose: Measure FCC unit performance by conversion rate
-- Formula: Conversion% = (Light Products / Feed) × 100
--          Light Products = Dry Gas + LPG + Gasoline
-- Business Rule: Target conversion: 72-78% for typical FCC
--                < 72% indicates catalyst deactivation
--                > 78% may indicate over-cracking (coke yield concern)
-- -------------------------------------------------------------------

CREATE VIEW IF NOT EXISTS vw_fcc_conversion_efficiency AS
WITH fcc_products AS (
  SELECT 
    p.date_key,
    p.unit_id,
    u.unit_name,
    p.feed_volume_bbl,
    SUM(CASE WHEN p.product_type IN ('Dry Gas', 'LPG', 'Gasoline', 'Premium Gasoline') 
             THEN p.product_volume_bbl ELSE 0 END) AS light_products_bbl,
    SUM(CASE WHEN p.product_type IN ('Light Cycle Oil', 'Heavy Cycle Oil') 
             THEN p.product_volume_bbl ELSE 0 END) AS cycle_oils_bbl,
    SUM(CASE WHEN p.product_type = 'Coke' 
             THEN p.product_volume_bbl ELSE 0 END) AS coke_bbl
  FROM fact_unit_production p
  JOIN dim_unit u ON p.unit_id = u.unit_id
  WHERE u.unit_type = 'FCC'
  GROUP BY p.date_key, p.unit_id, u.unit_name, p.feed_volume_bbl
)
SELECT 
  date_key,
  unit_id,
  unit_name,
  feed_volume_bbl,
  light_products_bbl,
  cycle_oils_bbl,
  coke_bbl,
  (light_products_bbl / NULLIF(feed_volume_bbl, 0)) * 100 AS conversion_pct,
  CASE 
    WHEN (light_products_bbl / NULLIF(feed_volume_bbl, 0)) * 100 BETWEEN 72 AND 78 THEN 'On Target'
    WHEN (light_products_bbl / NULLIF(feed_volume_bbl, 0)) * 100 < 72 THEN 'Below Target'
    ELSE 'Above Target'
  END AS performance_status
FROM fcc_products;

-- -------------------------------------------------------------------
-- STEP 3.2: Yield Gap Analysis
-- -------------------------------------------------------------------
-- Purpose: Compare actual yields to theoretical yields from crude assays
-- Formula: Yield Gap% = ((Theoretical - Actual) / Theoretical) × 100
-- Business Rule: Yield gap > 5% indicates yield loss, requires investigation
--                Possible causes: Operating conditions, catalyst activity,
--                                feedstock quality variation
-- Note: Theoretical yields would typically come from laboratory assay data
--       For this example, using industry-typical values
-- -------------------------------------------------------------------

CREATE VIEW IF NOT EXISTS vw_yield_gap_analysis AS
WITH theoretical_yields AS (
  -- In production system, join to crude assay table
  -- For example purposes, using typical gasoline yields by crude grade
  SELECT 
    'WTI' AS crude_grade, 
    52.0 AS theoretical_gasoline_pct,
    30.0 AS theoretical_distillate_pct
  UNION ALL
  SELECT 'Brent', 50.0, 32.0
  UNION ALL
  SELECT 'Maya', 42.0, 28.0
  UNION ALL
  SELECT 'Dubai', 48.0, 31.0
  UNION ALL
  SELECT 'Mars', 47.0, 30.0
),
actual_yields AS (
  SELECT 
    date_key,
    gasoline_yield_pct AS actual_gasoline_pct,
    distillate_yield_pct AS actual_distillate_pct
  FROM stg_daily_kpi
)
SELECT 
  a.date_key,
  t.crude_grade,
  t.theoretical_gasoline_pct,
  a.actual_gasoline_pct,
  ((t.theoretical_gasoline_pct - a.actual_gasoline_pct) / t.theoretical_gasoline_pct) * 100 
    AS gasoline_yield_gap_pct,
  t.theoretical_distillate_pct,
  a.actual_distillate_pct,
  ((t.theoretical_distillate_pct - a.actual_distillate_pct) / t.theoretical_distillate_pct) * 100 
    AS distillate_yield_gap_pct,
  CASE 
    WHEN ((t.theoretical_gasoline_pct - a.actual_gasoline_pct) / t.theoretical_gasoline_pct) * 100 > 5.0
      THEN 'Investigate'
    WHEN ((t.theoretical_gasoline_pct - a.actual_gasoline_pct) / t.theoretical_gasoline_pct) * 100 < 2.0
      THEN 'Excellent'
    ELSE 'Acceptable'
  END AS gasoline_status
FROM actual_yields a
CROSS JOIN theoretical_yields t
-- In production, would join on actual crude slate for the day
ORDER BY a.date_key;

-- ===================================================================
-- SECTION 4: TRENDING AND MOVING AVERAGES
-- ===================================================================

-- -------------------------------------------------------------------
-- STEP 4.1: 7-Day and 30-Day Moving Averages
-- -------------------------------------------------------------------
-- Purpose: Smooth daily volatility and identify trends in key metrics
-- Business Rule: Moving averages reveal directional trends
--                7-day avg shows short-term operational patterns
--                30-day avg shows longer-term performance trends
-- Use Case: Identify gradual efficiency degradation or improvement
-- -------------------------------------------------------------------

CREATE VIEW IF NOT EXISTS vw_kpi_trends AS
SELECT 
  date_key,
  gasoline_yield_pct,
  -- 7-Day Moving Average
  AVG(gasoline_yield_pct) OVER (
    ORDER BY date_key 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS gasoline_yield_7day_avg,
  -- 30-Day Moving Average
  AVG(gasoline_yield_pct) OVER (
    ORDER BY date_key 
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS gasoline_yield_30day_avg,
  energy_intensity_index,
  AVG(energy_intensity_index) OVER (
    ORDER BY date_key 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS eii_7day_avg,
  AVG(energy_intensity_index) OVER (
    ORDER BY date_key 
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS eii_30day_avg,
  avg_capacity_utilization_pct,
  AVG(avg_capacity_utilization_pct) OVER (
    ORDER BY date_key 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS utilization_7day_avg,
  AVG(avg_capacity_utilization_pct) OVER (
    ORDER BY date_key 
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS utilization_30day_avg
FROM stg_daily_kpi
ORDER BY date_key;

-- ===================================================================
-- END OF KPI AGGREGATIONS
-- ===================================================================
