-- PSR Evolution Metrics
-- Track KPI changes across three PSR periods
-- Ensures all 3 periods are present in output (filling missing periods with aggregated data or defaults)
{{ config "materialized" "table" }}

WITH period_stats AS (
  SELECT 
    psr_period,
    COUNT(*) as total_trips,
    SUM(CASE WHEN trip_type = 'loaded' THEN 1 ELSE 0 END) as loaded_trips,
    SUM(CASE WHEN trip_type = 'empty' THEN 1 ELSE 0 END) as empty_trips,
    AVG(COALESCE(average_velocity_mph, 0)) as avg_velocity_mph,
    -- SQLite doesn't have STDDEV_POP, calculate manually
    SQRT(CASE 
      WHEN COUNT(*) > 1 THEN 
        AVG(COALESCE(average_velocity_mph, 0) * COALESCE(average_velocity_mph, 0)) - 
        AVG(COALESCE(average_velocity_mph, 0)) * AVG(COALESCE(average_velocity_mph, 0))
      ELSE 0 
    END) as stddev_velocity,
    AVG(duration_minutes) as avg_trip_duration_minutes,
    SQRT(CASE 
      WHEN COUNT(*) > 1 THEN 
        AVG(duration_minutes * duration_minutes) - 
        AVG(duration_minutes) * AVG(duration_minutes)
      ELSE 0 
    END) as stddev_trip_duration,
    AVG(dwell_count) as avg_dwell_count_per_trip,
    SUM(distance_miles) as total_distance_miles
  FROM {{ ref "fact_trip" }}
  GROUP BY psr_period
),
all_periods AS (
  -- Ensure all three PSR periods are represented
  SELECT 'pre-PSR' AS psr_period
  UNION ALL SELECT 'transition'
  UNION ALL SELECT 'mature'
)
SELECT 
  ap.psr_period,
  COALESCE(ps.total_trips, 0) as total_trips,
  COALESCE(ps.loaded_trips, 0) as loaded_trips,
  COALESCE(ps.empty_trips, 0) as empty_trips,
  COALESCE(ps.avg_velocity_mph, 0) as avg_velocity_mph,
  COALESCE(ps.stddev_velocity, 0) as stddev_velocity,
  COALESCE(ps.avg_trip_duration_minutes, 0) as avg_trip_duration_minutes,
  COALESCE(ps.stddev_trip_duration, 0) as stddev_trip_duration,
  COALESCE(ps.avg_dwell_count_per_trip, 0) as avg_dwell_count_per_trip,
  COALESCE(ps.total_distance_miles, 0) as total_distance_miles
FROM all_periods ap
LEFT JOIN period_stats ps ON ap.psr_period = ps.psr_period
ORDER BY 
  CASE 
    WHEN ap.psr_period = 'pre-PSR' THEN 1
    WHEN ap.psr_period = 'transition' THEN 2
    WHEN ap.psr_period = 'mature' THEN 3
    ELSE 4
  END
