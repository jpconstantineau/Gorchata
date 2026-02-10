-- Seasonal Pattern Detection Analysis
-- Business Question: Are there predictable seasonal effects in our operations?
-- 
-- This query detects seasonal patterns, specifically the known Week 5 slowdown
-- (20% longer transit) and Week 8 straggler spike (2x rate). Use this to:
-- - Plan for seasonal capacity needs
-- - Adjust maintenance schedules
-- - Set realistic performance targets by season

WITH weekly_baseline AS (
  -- Calculate baseline (average) metrics across all weeks
  SELECT
    corridor_id,
    AVG(avg_transit_hours) AS baseline_transit_hours,
    AVG(avg_total_trip_hours) AS baseline_total_trip_hours,
    AVG(straggler_rate) AS baseline_straggler_rate,
    ROUND(STDEV(avg_transit_hours), 4) AS transit_stddev,
    ROUND(STDEV(straggler_rate), 4) AS straggler_stddev
  FROM {{ ref "agg_corridor_weekly_metrics" }}
  GROUP BY corridor_id
),

weekly_metrics_with_comparison AS (
  -- Compare each week to baseline
  SELECT
    m.corridor_id,
    c.corridor_name,
    m.year,
    m.week,
    
    -- Actual metrics
    m.avg_transit_hours,
    m.avg_total_trip_hours,
    m.straggler_rate,
    m.total_trips,
    m.total_stragglers,
    
    -- Baseline comparison
    b.baseline_transit_hours,
    b.baseline_total_trip_hours,
    b.baseline_straggler_rate,
    
    -- Percentage differences
    ROUND((m.avg_transit_hours - b.baseline_transit_hours) / 
          NULLIF(b.baseline_transit_hours, 0) * 100, 2) AS transit_vs_baseline_pct,
    
    ROUND((m.straggler_rate - b.baseline_straggler_rate) / 
          NULLIF(b.baseline_straggler_rate, 0) * 100, 2) AS straggler_vs_baseline_pct,
    
    -- Standard deviation distance (z-score)
    ROUND((m.avg_transit_hours - b.baseline_transit_hours) / 
          NULLIF(b.transit_stddev, 0), 2) AS transit_z_score,
    
    ROUND((m.straggler_rate - b.baseline_straggler_rate) / 
          NULLIF(b.straggler_stddev, 0), 2) AS straggler_z_score
    
  FROM {{ ref "agg_corridor_weekly_metrics" }} m
  INNER JOIN {{ ref "dim_corridor" }} c ON m.corridor_id = c.corridor_id
  INNER JOIN weekly_baseline b ON m.corridor_id = b.corridor_id
),

seasonal_anomalies AS (
  -- Identify weeks with significant deviations
  SELECT
    *,
    
    -- Flag significant transit slowdown (> 15% slower)
    CASE 
      WHEN transit_vs_baseline_pct > 15 THEN 1
      ELSE 0
    END AS transit_slowdown_flag,
    
    -- Flag significant straggler spike (> 50% increase)
    CASE 
      WHEN straggler_vs_baseline_pct > 50 THEN 1
      ELSE 0
    END AS straggler_spike_flag,
    
    -- Overall anomaly severity
    CASE 
      WHEN ABS(transit_z_score) > 2 OR ABS(straggler_z_score) > 2 THEN 'High Anomaly'
      WHEN ABS(transit_z_score) > 1 OR ABS(straggler_z_score) > 1 THEN 'Moderate Anomaly'
      ELSE 'Normal'
    END AS anomaly_severity
    
  FROM weekly_metrics_with_comparison
),

week_summary AS (
  -- Aggregate patterns across all corridors by week
  SELECT
    week,
    
    -- Count of corridors affected
    COUNT(*) AS corridors_analyzed,
    SUM(transit_slowdown_flag) AS corridors_with_slowdown,
    SUM(straggler_spike_flag) AS corridors_with_spike,
    
    -- Average deviations across all corridors
    AVG(transit_vs_baseline_pct) AS avg_transit_deviation_pct,
    AVG(straggler_vs_baseline_pct) AS avg_straggler_deviation_pct,
    
    -- Max deviations (worst corridor)
    MAX(transit_vs_baseline_pct) AS max_transit_deviation_pct,
    MAX(straggler_vs_baseline_pct) AS max_straggler_deviation_pct,
    
    -- Total trips analyzed
    SUM(total_trips) AS total_trips
    
  FROM seasonal_anomalies
  GROUP BY week
)

-- Final results highlighting seasonal patterns
SELECT
  week,
  corridors_analyzed,
  total_trips,
  
  -- Deviation metrics
  ROUND(avg_transit_deviation_pct, 2) AS avg_transit_deviation_pct,
  ROUND(max_transit_deviation_pct, 2) AS max_transit_deviation_pct,
  ROUND(avg_straggler_deviation_pct, 2) AS avg_straggler_deviation_pct,
  ROUND(max_straggler_deviation_pct, 2) AS max_straggler_deviation_pct,
  
  -- Impact flags
  corridors_with_slowdown,
  corridors_with_spike,
  
  -- Pattern identification
  CASE 
    WHEN week = 5 AND avg_transit_deviation_pct > 15 THEN 'CONFIRMED: Week 5 Slowdown (20% target)'
    WHEN week = 8 AND avg_straggler_deviation_pct > 80 THEN 'CONFIRMED: Week 8 Straggler Spike (2x target)'
    WHEN avg_transit_deviation_pct > 15 THEN 'Significant Transit Slowdown'
    WHEN avg_straggler_deviation_pct > 50 THEN 'Significant Straggler Increase'
    WHEN corridors_with_slowdown >= 3 THEN 'Widespread Slowdown'
    WHEN corridors_with_spike >= 3 THEN 'Widespread Straggler Issue'
    ELSE 'Normal Operations'
  END AS seasonal_pattern,
  
  -- Recommendations
  CASE 
    WHEN week = 5 THEN 'Expect 20% longer transit; add buffer to schedules'
    WHEN week = 8 THEN 'Expect 2x straggler rate; increase maintenance readiness'
    WHEN avg_transit_deviation_pct > 15 THEN 'Investigate cause of slowdown'
    WHEN avg_straggler_deviation_pct > 50 THEN 'Investigate car mechanical issues'
    ELSE 'Continue normal operations'
  END AS recommendation
  
FROM week_summary
ORDER BY week ASC;
