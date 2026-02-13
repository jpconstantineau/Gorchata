-- ===================================================================
-- OPERATIONAL DASHBOARDS - Oil Refinery Data Warehousing
-- ===================================================================
-- Purpose: Analytical queries for executive dashboards, operational excellence
--          monitoring, and performance management
-- Phase: 8 (Final Phase)
-- Target Users: Refinery Manager, Operations Manager, Engineering Manager,
--               Optimization Team, Executive Leadership
-- ===================================================================

-- ===================================================================
-- DASHBOARD 1: EXECUTIVE SUMMARY
-- ===================================================================
-- Purpose: High-level KPIs for executive leadership
-- Refresh: Daily
-- Audience: VP Operations, Refinery General Manager
-- ===================================================================

CREATE VIEW IF NOT EXISTS vw_executive_dashboard AS
WITH latest_month AS (
  SELECT MAX(date_key) AS max_date_key
  FROM fact_monthly_kpi
),
current_month AS (
  SELECT 
    month_year,
    crude_input_bbl,
    total_throughput_bbl,
    gasoline_yield_pct,
    distillate_yield_pct,
    high_value_product_pct,
    energy_intensity_index,
    avg_capacity_utilization_pct,
    reliability_factor_pct,
    month_over_month_change_pct,
    throughput_volatility,
    operating_days,
    ufl_pct,
    data_quality_pass_rate_pct
  FROM fact_monthly_kpi
  WHERE date_key = (SELECT max_date_key FROM latest_month)
),
previous_month AS (
  SELECT 
    gasoline_yield_pct AS prev_gasoline_yield_pct,
    energy_intensity_index AS prev_eii,
    avg_capacity_utilization_pct AS prev_utilization_pct
  FROM fact_monthly_kpi
  WHERE date_key = (
    SELECT MAX(date_key) 
    FROM fact_monthly_kpi 
    WHERE date_key < (SELECT max_date_key FROM latest_month)
  )
)
SELECT 
  c.month_year AS "Month",
  ROUND(c.crude_input_bbl, 0) AS "Crude Input (bbl)",
  ROUND(c.total_throughput_bbl, 0) AS "Total Throughput (bbl)",
  ROUND(c.gasoline_yield_pct, 1) AS "Gasoline Yield %",
  ROUND(c.gasoline_yield_pct - p.prev_gasoline_yield_pct, 1) AS "Gasoline Yield Change",
  ROUND(c.distillate_yield_pct, 1) AS "Distillate Yield %",
  ROUND(c.high_value_product_pct, 1) AS "High-Value Products %",
  ROUND(c.energy_intensity_index, 2) AS "Energy Intensity (MMBtu/bbl)",
  ROUND(c.energy_intensity_index - p.prev_eii, 2) AS "EII Change",
  CASE 
    WHEN c.energy_intensity_index <= 0.70 THEN '✓ On Target'
    WHEN c.energy_intensity_index <= 0.85 THEN '⚠ Review'
    ELSE '✗ Action Required'
  END AS "EII Status",
  ROUND(c.avg_capacity_utilization_pct, 1) AS "Capacity Utilization %",
  ROUND(c.avg_capacity_utilization_pct - p.prev_utilization_pct, 1) AS "Utilization Change",
  ROUND(c.reliability_factor_pct, 1) AS "Reliability %",
  ROUND(c.month_over_month_change_pct, 1) AS "MoM Throughput Change %",
  c.operating_days AS "Operating Days",
  ROUND(c.ufl_pct, 2) AS "UFL %",
  CASE 
    WHEN c.ufl_pct < 0.5 THEN '✓ Excellent'
    WHEN c.ufl_pct < 1.0 THEN '⚠ Acceptable'
    ELSE '✗ Investigate'
  END AS "UFL Status",
  ROUND(c.data_quality_pass_rate_pct, 1) AS "Data Quality %",
  CASE 
    WHEN c.data_quality_pass_rate_pct >= 95 THEN '✓ Excellent'
    WHEN c.data_quality_pass_rate_pct >= 90 THEN '⚠ Good'
    ELSE '✗ Action Required'
  END AS "Data Quality Status"
FROM current_month c
CROSS JOIN previous_month p;

-- ===================================================================
-- DASHBOARD 2: UNIT PERFORMANCE REPORT
-- ===================================================================
-- Purpose: Detailed performance metrics by refinery unit
-- Refresh: Daily
-- Audience: Unit Supervisors, Operations Engineers
-- ===================================================================

CREATE VIEW IF NOT EXISTS vw_unit_performance_dashboard AS
WITH latest_date AS (
  SELECT MAX(date_key) AS max_date_key
  FROM fact_unit_operations
),
unit_metrics AS (
  SELECT 
    o.date_key,
    u.unit_name,
    u.unit_type,
    u.complex_name,
    u.capacity_bbl_day,
    o.throughput_bbl,
    (o.throughput_bbl / u.capacity_bbl_day) * 100 AS utilization_pct,
    o.operating_hours,
    o.downtime_hours,
    o.downtime_type,
    o.refinery_fuel_consumed_tons,
    (o.refinery_fuel_consumed_tons * 18.0) / NULLIF(o.throughput_bbl, 0) AS unit_eii
  FROM fact_unit_operations o
  JOIN dim_unit u ON o.unit_id = u.unit_id
  WHERE o.date_key = (SELECT max_date_key FROM latest_date)
)
SELECT 
  complex_name AS "Complex",
  unit_name AS "Unit",
  unit_type AS "Unit Type",
  ROUND(capacity_bbl_day, 0) AS "Capacity (bbl/day)",
  ROUND(throughput_bbl, 0) AS "Actual Throughput (bbl)",
  ROUND(utilization_pct, 1) AS "Utilization %",
  CASE 
    WHEN utilization_pct >= 90 THEN '✓ Excellent'
    WHEN utilization_pct >= 80 THEN '✓ Good'
    WHEN utilization_pct >= 70 THEN '⚠ Low'
    ELSE '✗ Very Low'
  END AS "Utilization Status",
  ROUND(operating_hours, 1) AS "Operating Hours",
  ROUND(downtime_hours, 1) AS "Downtime Hours",
  downtime_type AS "Downtime Type",
  ROUND(refinery_fuel_consumed_tons, 1) AS "Fuel Consumed (tons)",
  ROUND(unit_eii, 3) AS "Unit EII (MMBtu/bbl)"
FROM unit_metrics
ORDER BY complex_name, unit_name;

-- ===================================================================
-- DASHBOARD 3: YIELD OPTIMIZATION VIEW
-- ===================================================================
-- Purpose: Track gasoline and distillate yields with trending
-- Refresh: Daily
-- Audience: Optimization Engineers, Refinery Planning Team
-- ===================================================================

CREATE VIEW IF NOT EXISTS vw_yield_optimization_dashboard AS
WITH daily_yields AS (
  SELECT 
    d.full_date,
    k.date_key,
    k.crude_input_bbl,
    k.gasoline_production_bbl,
    k.distillate_production_bbl,
    k.gasoline_yield_pct,
    k.distillate_yield_pct,
    k.high_value_product_pct,
    -- 7-day moving averages
    AVG(k.gasoline_yield_pct) OVER (
      ORDER BY k.date_key 
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS gasoline_7day_avg,
    AVG(k.distillate_yield_pct) OVER (
      ORDER BY k.date_key 
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS distillate_7day_avg
  FROM fact_daily_kpi k
  JOIN dim_date d ON k.date_key = d.date_key
  ORDER BY k.date_key DESC
  LIMIT 30  -- Last 30 days
)
SELECT 
  full_date AS "Date",
  ROUND(crude_input_bbl, 0) AS "Crude Input (bbl)",
  ROUND(gasoline_production_bbl, 0) AS "Gasoline (bbl)",
  ROUND(distillate_production_bbl, 0) AS "Distillate (bbl)",
  ROUND(gasoline_yield_pct, 1) AS "Gasoline Yield %",
  ROUND(gasoline_7day_avg, 1) AS "Gasoline 7-Day Avg %",
  CASE 
    WHEN gasoline_yield_pct BETWEEN 45.0 AND 55.0 THEN '✓ Industry Typical'
    WHEN gasoline_yield_pct < 45.0 THEN '⚠ Below Typical'
    ELSE '✓ Above Typical'
  END AS "Gasoline Status",
  ROUND(distillate_yield_pct, 1) AS "Distillate Yield %",
  ROUND(distillate_7day_avg, 1) AS "Distillate 7-Day Avg %",
  CASE 
    WHEN distillate_yield_pct BETWEEN 25.0 AND 35.0 THEN '✓ Industry Typical'
    WHEN distillate_yield_pct < 25.0 THEN '⚠ Below Typical'
    ELSE '✓ Above Typical'
  END AS "Distillate Status",
  ROUND(high_value_product_pct, 1) AS "High-Value %",
  CASE 
    WHEN high_value_product_pct >= 75.0 THEN '✓ Excellent'
    WHEN high_value_product_pct >= 70.0 THEN '✓ Good'
    ELSE '⚠ Improve'
  END AS "Product Mix Status"
FROM daily_yields
ORDER BY full_date DESC;

-- ===================================================================
-- DASHBOARD 4: ENERGY EFFICIENCY DASHBOARD
-- ===================================================================
-- Purpose: Monitor energy consumption and efficiency trends
-- Refresh: Daily
-- Audience: Energy Manager, Utilities Manager, Sustainability Team
-- ===================================================================

CREATE VIEW IF NOT EXISTS vw_energy_efficiency_dashboard AS
WITH energy_trends AS (
  SELECT 
    d.full_date,
    d.month_name,
    k.energy_consumed_mmbtu,
    k.total_throughput_bbl,
    k.energy_intensity_index,
    -- 7-day and 30-day moving averages for EII
    AVG(k.energy_intensity_index) OVER (
      ORDER BY k.date_key 
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS eii_7day_avg,
    AVG(k.energy_intensity_index) OVER (
      ORDER BY k.date_key 
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS eii_30day_avg,
    -- Rolling 30-day total energy
    SUM(k.energy_consumed_mmbtu) OVER (
      ORDER BY k.date_key 
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS energy_30day_total
  FROM fact_daily_kpi k
  JOIN dim_date d ON k.date_key = d.date_key
  ORDER BY k.date_key DESC
  LIMIT 90  -- Last 90 days
)
SELECT 
  full_date AS "Date",
  month_name AS "Month",
  ROUND(energy_consumed_mmbtu, 0) AS "Energy Consumed (MMBtu)",
  ROUND(total_throughput_bbl, 0) AS "Throughput (bbl)",
  ROUND(energy_intensity_index, 3) AS "EII (MMBtu/bbl)",
  ROUND(eii_7day_avg, 3) AS "EII 7-Day Avg",
  ROUND(eii_30day_avg, 3) AS "EII 30-Day Avg",
  CASE 
    WHEN eii_30day_avg < 0.70 THEN '✓ Excellent (< 0.70)'
    WHEN eii_30day_avg < 0.80 THEN '✓ Good (0.70-0.80)'
    WHEN eii_30day_avg < 0.90 THEN '⚠ Fair (0.80-0.90)'
    ELSE '✗ Poor (> 0.90)'
  END AS "Performance Level",
  ROUND(energy_30day_total, 0) AS "30-Day Energy Total (MMBtu)",
  -- Calculate potential savings if at target of 0.70
  ROUND((energy_intensity_index - 0.70) * total_throughput_bbl, 0) AS "Potential Savings (MMBtu)",
  CASE 
    WHEN energy_intensity_index <= 0.70 THEN 'N/A - At Target'
    WHEN (energy_intensity_index - 0.70) * total_throughput_bbl > 5000 THEN 'High Savings Opportunity'
    WHEN (energy_intensity_index - 0.70) * total_throughput_bbl > 2000 THEN 'Moderate Savings Opportunity'
    ELSE 'Low Savings Opportunity'
  END AS "Improvement Opportunity"
FROM energy_trends
ORDER BY full_date DESC;

-- ===================================================================
-- DASHBOARD 5: RELIABILITY DASHBOARD
-- ===================================================================
-- Purpose: Track uptime, downtime, and reliability metrics
-- Refresh: Daily
-- Audience: Maintenance Manager, Reliability Engineer
-- ===================================================================

CREATE VIEW IF NOT EXISTS vw_reliability_dashboard AS
WITH reliability_metrics AS (
  SELECT 
    d.full_date,
    d.year,
    d.month_name,
    k.planned_downtime_hours,
    k.unplanned_downtime_hours,
    k.planned_downtime_hours + k.unplanned_downtime_hours AS total_downtime_hours,
    k.reliability_factor_pct,
    -- Year-to-date totals
    SUM(k.unplanned_downtime_hours) OVER (
      PARTITION BY d.year 
      ORDER BY k.date_key
    ) AS ytd_unplanned_downtime,
    -- Rolling 30-day totals
    SUM(k.planned_downtime_hours) OVER (
      ORDER BY k.date_key 
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS planned_30day,
    SUM(k.unplanned_downtime_hours) OVER (
      ORDER BY k.date_key 
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS unplanned_30day
  FROM fact_daily_kpi k
  JOIN dim_date d ON k.date_key = d.date_key
  ORDER BY k.date_key DESC
  LIMIT 90  -- Last 90 days
)
SELECT 
  full_date AS "Date",
  month_name AS "Month",
  ROUND(planned_downtime_hours, 1) AS "Planned Downtime (hrs)",
  ROUND(unplanned_downtime_hours, 1) AS "Unplanned Downtime (hrs)",
  ROUND(total_downtime_hours, 1) AS "Total Downtime (hrs)",
  ROUND(reliability_factor_pct, 1) AS "Reliability Factor %",
  CASE 
    WHEN reliability_factor_pct >= 95.0 THEN '✓ Excellent'
    WHEN reliability_factor_pct >= 90.0 THEN '✓ Good'
    WHEN reliability_factor_pct >= 85.0 THEN '⚠ Fair'
    ELSE '✗ Poor'
  END AS "Reliability Status",
  ROUND(ytd_unplanned_downtime, 1) AS "YTD Unplanned (hrs)",
  ROUND(planned_30day, 1) AS "30-Day Planned Total (hrs)",
  ROUND(unplanned_30day, 1) AS "30-Day Unplanned Total (hrs)",
  CASE 
    WHEN unplanned_downtime_hours = 0 THEN '✓ Perfect Day'
    WHEN unplanned_downtime_hours < 4 THEN '✓ Minor Issue'
    WHEN unplanned_downtime_hours < 12 THEN '⚠ Significant Issue'
    ELSE '✗ Major Event'
  END AS "Daily Status"
FROM reliability_metrics
ORDER BY full_date DESC;

-- ===================================================================
-- DASHBOARD 6: DATA QUALITY DASHBOARD
-- ===================================================================
-- Purpose: Monitor data quality check pass rates and anomalies
-- Refresh: Daily
-- Audience: Data Analysts, IT Team, Operations Support
-- ===================================================================

CREATE VIEW IF NOT EXISTS vw_data_quality_dashboard AS
WITH quality_summary AS (
  SELECT 
    d.full_date,
    d.month_name,
    k.data_quality_pass_rate_pct,
    -- Calculate 7-day moving average
    AVG(k.data_quality_pass_rate_pct) OVER (
      ORDER BY k.date_key 
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS quality_7day_avg,
    -- Count of issues by entity type from fact_data_quality_checks
    (SELECT COUNT(*) 
     FROM fact_data_quality_checks dqc 
     WHERE dqc.date_key = k.date_key 
       AND dqc.pass_fail = 'Fail') AS failed_checks_count,
    (SELECT COUNT(*) 
     FROM fact_data_quality_checks dqc 
     WHERE dqc.date_key = k.date_key 
       AND dqc.pass_fail = 'Warning') AS warning_checks_count
  FROM fact_daily_kpi k
  JOIN dim_date d ON k.date_key = d.date_key
  ORDER BY k.date_key DESC
  LIMIT 30  -- Last 30 days
)
SELECT 
  full_date AS "Date",
  month_name AS "Month",
  ROUND(data_quality_pass_rate_pct, 1) AS "Pass Rate %",
  ROUND(quality_7day_avg, 1) AS "7-Day Avg %",
  CASE 
    WHEN data_quality_pass_rate_pct >= 95.0 THEN '✓ Excellent'
    WHEN data_quality_pass_rate_pct >= 90.0 THEN '✓ Good'
    WHEN data_quality_pass_rate_pct >= 85.0 THEN '⚠ Fair'
    ELSE '✗ Action Required'
  END AS "Quality Status",
  failed_checks_count AS "Failed Checks",
  warning_checks_count AS "Warning Checks",
  (failed_checks_count + warning_checks_count) AS "Total Issues",
  CASE 
    WHEN failed_checks_count = 0 THEN '✓ No Failures'
    WHEN failed_checks_count <= 2 THEN '⚠ Minor Issues'
    WHEN failed_checks_count <= 5 THEN '⚠ Multiple Issues'
    ELSE '✗ Significant Issues'
  END AS "Issue Severity"
FROM quality_summary
ORDER BY full_date DESC;

-- ===================================================================
-- DASHBOARD 7: COMPLEX-LEVEL PERFORMANCE COMPARISON
-- ===================================================================
-- Purpose: Compare performance across refinery complexes
-- Refresh: Monthly
-- Audience: Executive Leadership, Operations Management
-- ===================================================================

CREATE VIEW IF NOT EXISTS vw_complex_performance_comparison AS
WITH complex_metrics AS (
  SELECT 
    u.complex_name,
    o.date_key,
    SUM(o.throughput_bbl) AS complex_throughput,
    SUM(u.capacity_bbl_day) AS complex_capacity,
    (SUM(o.throughput_bbl) / NULLIF(SUM(u.capacity_bbl_day), 0)) * 100 AS complex_utilization_pct,
    SUM(o.refinery_fuel_consumed_tons) * 18.0 AS complex_energy_mmbtu,
    (SUM(o.refinery_fuel_consumed_tons) * 18.0) / NULLIF(SUM(o.throughput_bbl), 0) AS complex_eii,
    AVG(o.downtime_hours) AS avg_downtime_hours
  FROM fact_unit_operations o
  JOIN dim_unit u ON o.unit_id = u.unit_id
  WHERE o.date_key >= (
    SELECT strftime('%Y%m%d', date(MAX(date_key)/10000 || '-' || 
                                    (MAX(date_key)/100)%100 || '-' || 
                                    MAX(date_key)%100, '-30 days'))
    FROM fact_unit_operations
  )
  GROUP BY u.complex_name, o.date_key
),
complex_summary AS (
  SELECT 
    complex_name,
    COUNT(DISTINCT date_key) AS days_operated,
    ROUND(AVG(complex_throughput), 0) AS avg_daily_throughput,
    ROUND(AVG(complex_capacity), 0) AS total_capacity,
    ROUND(AVG(complex_utilization_pct), 1) AS avg_utilization_pct,
    ROUND(AVG(complex_eii), 3) AS avg_eii,
    ROUND(SUM(avg_downtime_hours), 1) AS total_downtime_hours
  FROM complex_metrics
  GROUP BY complex_name
)
SELECT 
  complex_name AS "Complex",
  days_operated AS "Days Operated",
  avg_daily_throughput AS "Avg Daily Throughput (bbl)",
  total_capacity AS "Total Capacity (bbl/day)",
  avg_utilization_pct AS "Avg Utilization %",
  CASE 
    WHEN avg_utilization_pct >= 90 THEN '✓ Excellent'
    WHEN avg_utilization_pct >= 80 THEN '✓ Good'
    WHEN avg_utilization_pct >= 70 THEN '⚠ Fair'
    ELSE '✗ Low'
  END AS "Utilization Rating",
  avg_eii AS "Avg EII (MMBtu/bbl)",
  CASE 
    WHEN avg_eii < 0.70 THEN '✓ Excellent'
    WHEN avg_eii < 0.80 THEN '✓ Good'
    WHEN avg_eii < 0.90 THEN '⚠ Fair'
    ELSE '✗ Poor'
  END AS "Energy Rating",
  total_downtime_hours AS "Total Downtime (hrs)"
FROM complex_summary
ORDER BY avg_utilization_pct DESC;

-- ===================================================================
-- DASHBOARD 8: PRODUCT SLATE OPTIMIZATION
-- ===================================================================
-- Purpose: Analyze product mix and identify optimization opportunities
-- Refresh: Weekly
-- Audience: Refinery Planning Team, Commercial Team
-- ===================================================================

CREATE VIEW IF NOT EXISTS vw_product_slate_optimization AS
WITH product_analysis AS (
  SELECT 
    d.full_date,
    d.week,
    k.gasoline_production_bbl,
    k.distillate_production_bbl,
    k.crude_input_bbl,
    k.gasoline_yield_pct,
    k.distillate_yield_pct,
    k.high_value_product_pct,
    -- Calculate gasoline-to-distillate ratio
    k.gasoline_production_bbl / NULLIF(k.distillate_production_bbl, 0) AS gas_dist_ratio
  FROM fact_daily_kpi k
  JOIN dim_date d ON k.date_key = d.date_key
  WHERE k.date_key >= (
    SELECT strftime('%Y%m%d', date(MAX(date_key)/10000 || '-' || 
                                    (MAX(date_key)/100)%100 || '-' || 
                                    MAX(date_key)%100, '-90 days'))
    FROM fact_daily_kpi
  )
),
weekly_summary AS (
  SELECT 
    week AS "Week",
    ROUND(AVG(gasoline_yield_pct), 1) AS "Avg Gasoline Yield %",
    ROUND(AVG(distillate_yield_pct), 1) AS "Avg Distillate Yield %",
    ROUND(AVG(high_value_product_pct), 1) AS "Avg High-Value %",
    ROUND(AVG(gas_dist_ratio), 2) AS "Gas/Dist Ratio",
    CASE 
      WHEN AVG(gas_dist_ratio) > 2.0 THEN 'Gasoline-Heavy'
      WHEN AVG(gas_dist_ratio) > 1.5 THEN 'Gasoline-Optimized'
      WHEN AVG(gas_dist_ratio) > 1.0 THEN 'Balanced'
      ELSE 'Distillate-Optimized'
    END AS "Product Mix"
  FROM product_analysis
  GROUP BY week
  ORDER BY week DESC
  LIMIT 12  -- Last 12 weeks
)
SELECT * FROM weekly_summary;

-- ===================================================================
-- END OF OPERATIONAL DASHBOARDS
-- ===================================================================

-- Usage Notes:
-- 1. All dashboard views are read-only analytical queries
-- 2. Refresh frequencies are recommendations; adjust based on business needs
-- 3. Status indicators use symbols: ✓ (good), ⚠ (warning), ✗ (action required)
-- 4. Moving averages smooth volatility and reveal trends
-- 5. Benchmarks based on industry-typical ranges; customize for specific refinery
-- 6. Color coding suggestions:
--    - Green for ✓ statuses
--    - Yellow/Orange for ⚠ statuses
--    - Red for ✗ statuses
