{{ config "materialized" "view" }}

-- Worst Performing Trucks Analysis
-- Business Question: Which trucks are underperforming and why?
-- 
-- This query identifies trucks with the lowest productivity (tons per hour) and
-- analyzes root causes including low utilization, high cycle times, and excessive delays.
-- Use this to:
-- - Target maintenance and repair priorities
-- - Identify operator training needs
-- - Plan equipment replacement
-- - Optimize truck-shovel assignments

WITH truck_performance AS (
  -- Aggregate truck performance metrics
  SELECT
    tp.truck_id,
    t.model,
    t.fleet_class,
    t.payload_capacity_tons,
    
    -- Productivity metrics
    SUM(tp.total_tons_moved) AS total_tons_moved,
    AVG(tp.avg_cycle_time_min) AS avg_cycle_time_min,
    AVG(tp.tons_per_hour) AS tons_per_hour,
    AVG(tp.avg_payload_utilization_pct) AS avg_payload_utilization_pct,
    SUM(tp.total_spot_delay_min) / 60.0 AS total_spot_delay_hours,
    COUNT(DISTINCT tp.date_id) AS days_operated,
    
    -- Calculate cycles per day
    SUM(tp.cycles_completed) / NULLIF(COUNT(DISTINCT tp.date_id), 0) AS avg_cycles_per_day
    
  FROM {{ ref "truck_daily_productivity" }} tp
  INNER JOIN {{ ref "dim_truck" }} t ON tp.truck_id = t.truck_id
  GROUP BY tp.truck_id, t.model, t.fleet_class, t.payload_capacity_tons
),

fleet_averages AS (
  -- Calculate fleet-wide averages for comparison
  SELECT
    fleet_class,
    AVG(tons_per_hour) AS fleet_avg_tons_per_hour,
    AVG(avg_cycle_time_min) AS fleet_avg_cycle_time,
    AVG(avg_payload_utilization_pct) AS fleet_avg_utilization
  FROM truck_performance
  GROUP BY fleet_class
),

ranked_performance AS (
  -- Rank trucks by performance within their fleet class
  SELECT
    tp.truck_id,
    tp.model,
    tp.fleet_class,
    tp.total_tons_moved,
    ROUND(tp.avg_cycle_time_min, 2) AS avg_cycle_time_min,
    ROUND(tp.tons_per_hour, 2) AS tons_per_hour,
    ROUND(tp.avg_payload_utilization_pct, 2) AS avg_payload_utilization_pct,
    ROUND(tp.total_spot_delay_hours, 2) AS total_spot_delay_hours,
    
    -- Performance ranking (1 = worst within fleet class)
    ROW_NUMBER() OVER (
      PARTITION BY tp.fleet_class 
      ORDER BY tp.tons_per_hour ASC
    ) AS performance_rank,
    
    -- Performance score (composite metric: 0-100, higher is better)
    ROUND(
      (tp.tons_per_hour / NULLIF(fa.fleet_avg_tons_per_hour, 0) * 50) +
      (tp.avg_payload_utilization_pct / 100.0 * 30) +
      ((100 - (tp.avg_cycle_time_min / NULLIF(fa.fleet_avg_cycle_time, 0) * 100)) / 100 * 20),
      2
    ) AS performance_score,
    
    -- Performance vs fleet average
    ROUND((tp.tons_per_hour / NULLIF(fa.fleet_avg_tons_per_hour, 0) - 1) * 100, 2) AS tons_per_hour_vs_fleet_pct,
    ROUND((tp.avg_payload_utilization_pct / NULLIF(fa.fleet_avg_utilization, 0) - 1) * 100, 2) AS utilization_vs_fleet_pct,
    ROUND((tp.avg_cycle_time_min / NULLIF(fa.fleet_avg_cycle_time, 0) - 1) * 100, 2) AS cycle_time_vs_fleet_pct,
    
    tp.days_operated,
    tp.avg_cycles_per_day
    
  FROM truck_performance tp
  INNER JOIN fleet_averages fa ON tp.fleet_class = fa.fleet_class
)

-- Final output with issue identification
SELECT
  truck_id,
  model,
  fleet_class,
  total_tons_moved,
  avg_cycle_time_min,
  tons_per_hour,
  avg_payload_utilization_pct,
  total_spot_delay_hours,
  performance_rank,
  performance_score,
  
  -- Identify specific issues
  CASE
    WHEN avg_payload_utilization_pct < 85 AND avg_cycle_time_min > cycle_time_vs_fleet_pct + 10 AND total_spot_delay_hours > 5 THEN
      'Multiple issues: Low utilization, Slow cycles, Excessive delays'
    WHEN avg_payload_utilization_pct < 85 AND avg_cycle_time_min > cycle_time_vs_fleet_pct + 10 THEN
      'Low utilization and Slow cycle times - Check loading process'
    WHEN avg_payload_utilization_pct < 85 AND total_spot_delay_hours > 5 THEN
      'Low utilization and Excessive delays - Check maintenance'
    WHEN avg_cycle_time_min > cycle_time_vs_fleet_pct + 15 AND total_spot_delay_hours > 5 THEN
      'Slow cycles and Excessive delays - Possible mechanical issues'
    WHEN avg_payload_utilization_pct < 85 THEN
      'Low payload utilization - Review loading procedures'
    WHEN avg_cycle_time_min > cycle_time_vs_fleet_pct + 15 THEN
      'High cycle times vs fleet average - Check operator performance'
    WHEN total_spot_delay_hours > 5 THEN
      'Excessive spot delays - Maintenance required'
    WHEN tons_per_hour < 0.8 * (SELECT AVG(tons_per_hour) FROM ranked_performance WHERE fleet_class = ranked_performance.fleet_class) THEN
      'Overall low productivity - Investigate root cause'
    ELSE
      'Monitor closely'
  END AS issues_identified,
  
  -- Comparison metrics
  tons_per_hour_vs_fleet_pct,
  utilization_vs_fleet_pct,
  cycle_time_vs_fleet_pct,
  
  days_operated,
  ROUND(avg_cycles_per_day, 1) AS avg_cycles_per_day

FROM ranked_performance
ORDER BY fleet_class, performance_rank ASC
