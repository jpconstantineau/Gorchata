{{ config "materialized" "view" }}

-- Shift Performance Comparison
-- Business Question: How does productivity differ between day and night shifts?
-- 
-- This query compares day vs night shift performance to identify operational
-- differences and optimization opportunities. Use this to:
-- - Identify staffing optimization needs
-- - Plan equipment allocation by shift
-- - Address shift-specific training gaps
-- - Optimize maintenance scheduling

WITH shift_aggregation AS (
  -- Aggregate metrics by shift
  SELECT
    s.shift_name,
    s.start_time,
    s.end_time,
    
    -- Production metrics
    SUM(f.payload_tons) AS total_tons_moved,
    COUNT(f.cycle_id) AS total_cycles,
    COUNT(DISTINCT f.truck_id) AS trucks_used,
    COUNT(DISTINCT f.operator_id) AS operators_active,
    COUNT(DISTINCT f.date_id) AS days_observed,
    
    -- Time metrics
    AVG((julianday(f.cycle_end) - julianday(f.cycle_start)) * 24 * 60) AS avg_cycle_time_min,
    AVG(f.duration_queue_crusher_min + f.duration_queue_shovel_min) AS avg_queue_time_min,
    SUM(f.duration_spot_delays_min) / 60.0 AS total_spot_delay_hours,
    
    -- Efficiency metrics
    AVG((julianday(f.cycle_end) - julianday(f.cycle_start)) * 24) AS avg_cycle_duration_hours
    
  FROM {{ ref "fact_haul_cycle" }} f
  INNER JOIN {{ ref "dim_shift" }} s ON f.shift_id = s.shift_id
  GROUP BY s.shift_name, s.start_time, s.end_time
),

productivity_calc AS (
  -- Calculate productivity and comparative metrics
  SELECT
    shift_name,
    total_tons_moved,
    total_cycles,
    trucks_used,
    operators_active,
    days_observed,
    ROUND(avg_cycle_time_min, 2) AS avg_cycle_time_min,
    ROUND(avg_queue_time_min, 2) AS avg_queue_time_min,
    ROUND(total_spot_delay_hours, 2) AS total_spot_delay_hours,
    
    -- Tons per hour calculation
    ROUND(
      total_tons_moved / NULLIF(total_cycles * avg_cycle_duration_hours, 0),
      2
    ) AS tons_per_hour,
    
    -- Cycles per truck per day
    ROUND(
      total_cycles / NULLIF(trucks_used * days_observed, 0),
      2
    ) AS avg_cycles_per_truck_per_day,
    
    -- Tons per truck per day
    ROUND(
      total_tons_moved / NULLIF(trucks_used * days_observed, 0),
      2
    ) AS tons_per_truck_per_day
    
  FROM shift_aggregation
),

shift_comparison AS (
  -- Compare shifts and calculate differences
  SELECT
    pc.*,
    
    -- Calculate shift performance vs system average
    ROUND(
      (pc.tons_per_hour / (SELECT AVG(tons_per_hour) FROM productivity_calc) - 1) * 100,
      2
    ) AS productivity_vs_avg_pct,
    
    -- Calculate difference from best shift
    ROUND(
      (pc.tons_per_hour / (SELECT MAX(tons_per_hour) FROM productivity_calc) - 1) * 100,
      2
    ) AS productivity_vs_best_pct
    
  FROM productivity_calc pc
)

-- Final output with variance analysis
SELECT
  shift_name,
  total_tons_moved,
  total_cycles,
  avg_cycle_time_min,
  tons_per_hour,
  avg_queue_time_min,
  total_spot_delay_hours,
  
  -- Productivity difference percentage
  productivity_vs_avg_pct AS productivity_difference_pct,
  
  -- Performance variance reasons
  CASE
    WHEN shift_name = 'Night' AND productivity_vs_avg_pct < -10 THEN
      'Night shift underperforming: Longer cycle times (' || CAST(avg_cycle_time_min AS TEXT) || ' min), Higher queue times (' || CAST(avg_queue_time_min AS TEXT) || ' min) - Review visibility, staffing, and equipment maintenance'
    WHEN shift_name = 'Night' AND productivity_vs_avg_pct < -5 THEN
      'Night shift slightly lower: Possible lighting or operator fatigue factors'
    WHEN shift_name = 'Day' AND productivity_vs_avg_pct > 10 THEN
      'Day shift excelling: Better visibility, optimal staffing, efficient operations'
    WHEN shift_name = 'Day' AND productivity_vs_avg_pct > 5 THEN
      'Day shift performing well: Maintain current practices'
    WHEN avg_queue_time_min > 5 THEN
      'High queue times (' || CAST(avg_queue_time_min AS TEXT) || ' min) impacting productivity - Review dispatch and scheduling'
    WHEN total_spot_delay_hours > 50 THEN
      'Excessive spot delays (' || CAST(total_spot_delay_hours AS TEXT) || ' hrs) - Investigate maintenance or operator issues'
    WHEN avg_cycle_time_min > 60 THEN
      'Long cycle times (' || CAST(avg_cycle_time_min AS TEXT) || ' min) - Check haul routes and equipment performance'
    ELSE
      'Performance within expected range - Monitor for trends'
  END AS performance_variance_reasons,
  
  -- Additional metrics
  trucks_used,
  operators_active,
  days_observed,
  avg_cycles_per_truck_per_day,
  tons_per_truck_per_day,
  productivity_vs_best_pct,
  
  -- Recommendations
  CASE
    WHEN productivity_vs_avg_pct < -15 THEN
      'CRITICAL: Immediate investigation required - Shift performance significantly below standard'
    WHEN productivity_vs_avg_pct < -10 THEN
      'Priority: Review shift operations, equipment allocation, and operator performance'
    WHEN productivity_vs_avg_pct < -5 THEN
      'Action: Identify and address minor operational differences between shifts'
    WHEN productivity_vs_avg_pct > 10 THEN
      'Excellence: Document and replicate best practices from this shift'
    ELSE
      'Normal: Continue monitoring shift performance trends'
  END AS shift_recommendation

FROM shift_comparison
ORDER BY tons_per_hour DESC
