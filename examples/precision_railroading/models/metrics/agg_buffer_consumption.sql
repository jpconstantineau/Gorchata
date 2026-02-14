-- Buffer Consumption Metrics
-- Measure schedule buffer usage patterns
-- Uses pre-PSR period as baseline "scheduled" time (target performance)
{{ config "materialized" "table" }}

WITH baseline_times AS (
  -- Calculate baseline (scheduled) transit times from pre-PSR period
  SELECT 
    corridor_id,
    AVG(duration_minutes) as baseline_duration_minutes
  FROM {{ ref "fact_trip" }}
  WHERE psr_period = 'pre-PSR' AND corridor_id IS NOT NULL
  GROUP BY corridor_id
),
time_period_actuals AS (
  -- Calculate actual transit times by corridor and time period
  SELECT 
    corridor_id,
    STRFTIME('%Y-%m', trip_start_timestamp) as time_period,
    AVG(duration_minutes) as avg_actual_duration_minutes,
    COUNT(*) as trip_count
  FROM {{ ref "fact_trip" }}
  WHERE corridor_id IS NOT NULL
  GROUP BY corridor_id, STRFTIME('%Y-%m', trip_start_timestamp)
)
SELECT 
  tpa.corridor_id,
  tpa.time_period,
  COALESCE(bt.baseline_duration_minutes, tpa.avg_actual_duration_minutes) as avg_scheduled_duration_minutes,
  tpa.avg_actual_duration_minutes,
  tpa.avg_actual_duration_minutes - COALESCE(bt.baseline_duration_minutes, tpa.avg_actual_duration_minutes) as avg_buffer_consumed_minutes,
  -- Buffer consumption percentage: (actual - scheduled) / scheduled * 100
  CASE 
    WHEN COALESCE(bt.baseline_duration_minutes, tpa.avg_actual_duration_minutes) > 0 
    THEN ((tpa.avg_actual_duration_minutes - COALESCE(bt.baseline_duration_minutes, tpa.avg_actual_duration_minutes)) / 
          COALESCE(bt.baseline_duration_minutes, tpa.avg_actual_duration_minutes)) * 100.0
    ELSE 0
  END as buffer_consumption_percentage
FROM time_period_actuals tpa
LEFT JOIN baseline_times bt ON tpa.corridor_id = bt.corridor_id
WHERE tpa.trip_count >= 2  -- Need sufficient data for meaningful comparison
ORDER BY tpa.corridor_id, tpa.time_period
