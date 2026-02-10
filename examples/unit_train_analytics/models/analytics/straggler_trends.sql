-- Straggler Trends Over Time Analysis
-- Business Question: How are straggler rates and delays changing over time?
-- 
-- This query analyzes straggler patterns by week and corridor to identify
-- trends, seasonal effects, and problem areas. Use this to:
-- - Detect deteriorating car performance
-- - Plan maintenance schedules
-- - Identify seasonal patterns (e.g., week 8 spike)

WITH weekly_straggler_metrics AS (
  -- Get weekly straggler rates and delays by corridor
  SELECT
    s.corridor_id,
    c.corridor_name,
    s.year,
    s.week,
    
    -- Straggler counts and rates
    s.total_trips,
    s.total_stragglers,
    s.straggler_rate,
    
    -- Delay impact
    s.avg_delay_hours,
    s.total_delay_hours,
    
    -- Calculate week-over-week change
    LAG(s.straggler_rate) OVER (
      PARTITION BY s.corridor_id 
      ORDER BY s.year, s.week
    ) AS prev_week_straggler_rate,
    
    LAG(s.avg_delay_hours) OVER (
      PARTITION BY s.corridor_id 
      ORDER BY s.year, s.week
    ) AS prev_week_avg_delay
    
  FROM {{ ref "agg_straggler_impact" }} s
  INNER JOIN {{ ref "dim_corridor" }} c ON s.corridor_id = c.corridor_id
),

straggler_trends AS (
  -- Calculate weekly changes and trends
  SELECT
    corridor_id,
    corridor_name,
    year,
    week,
    total_trips,
    total_stragglers,
    straggler_rate,
    avg_delay_hours,
    total_delay_hours,
    
    -- Week-over-week changes
    CASE 
      WHEN prev_week_straggler_rate IS NOT NULL THEN
        ROUND((straggler_rate - prev_week_straggler_rate) * 100, 2)
      ELSE NULL
    END AS straggler_rate_change_pct,
    
    CASE 
      WHEN prev_week_avg_delay IS NOT NULL THEN
        ROUND(avg_delay_hours - prev_week_avg_delay, 2)
      ELSE NULL
    END AS avg_delay_change_hours,
    
    -- Flag significant increases
    CASE 
      WHEN prev_week_straggler_rate IS NOT NULL 
           AND straggler_rate > prev_week_straggler_rate * 1.5 THEN 1
      ELSE 0
    END AS significant_increase_flag
    
  FROM weekly_straggler_metrics
),

corridor_baseline AS (
  -- Calculate baseline (average) straggler rate per corridor
  SELECT
    corridor_id,
    AVG(straggler_rate) AS baseline_straggler_rate,
    ROUND(STDEV(straggler_rate), 4) AS straggler_rate_stddev
  FROM {{ ref "agg_straggler_impact" }}
  GROUP BY corridor_id
)

-- Final results with trend indicators
SELECT
  t.week,
  t.year,
  t.corridor_id,
  t.corridor_name,
  t.total_trips,
  t.total_stragglers,
  ROUND(t.straggler_rate * 100, 2) AS straggler_rate_pct,
  ROUND(t.avg_delay_hours, 2) AS avg_delay_hours,
  ROUND(t.total_delay_hours, 2) AS total_delay_hours,
  
  -- Trend indicators
  t.straggler_rate_change_pct AS wow_rate_change_pct,
  t.avg_delay_change_hours AS wow_delay_change_hours,
  
  -- Comparison to baseline
  ROUND(b.baseline_straggler_rate * 100, 2) AS baseline_rate_pct,
  ROUND((t.straggler_rate - b.baseline_straggler_rate) * 100, 2) AS vs_baseline_pct,
  
  -- Alert flags
  CASE 
    WHEN t.straggler_rate > b.baseline_straggler_rate + (2 * b.straggler_rate_stddev) THEN 'High Alert'
    WHEN t.straggler_rate > b.baseline_straggler_rate + b.straggler_rate_stddev THEN 'Warning'
    ELSE 'Normal'
  END AS trend_status,
  
  t.significant_increase_flag AS spike_flag
  
FROM straggler_trends t
INNER JOIN corridor_baseline b ON t.corridor_id = b.corridor_id
ORDER BY t.year ASC, t.week ASC, t.corridor_id ASC;
