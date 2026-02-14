-- Directional Asymmetry Analysis
-- Compare loaded vs empty trip performance to identify prioritization
{{ config "materialized" "table" }}

WITH trip_aggregations AS (
  SELECT 
    corridor_id,
    trip_type,
    COUNT(*) as trip_count,
    AVG(COALESCE(average_velocity_mph, 0)) as avg_velocity_mph,
    AVG(duration_minutes) as avg_duration_minutes
  FROM {{ ref "fact_trip" }}
  GROUP BY corridor_id, trip_type
),
corridor_summary AS (
  SELECT 
    corridor_id,
    MAX(CASE WHEN trip_type = 'loaded' THEN trip_count ELSE 0 END) as loaded_trip_count,
    MAX(CASE WHEN trip_type = 'loaded' THEN avg_velocity_mph ELSE 0 END) as loaded_avg_velocity_mph,
    MAX(CASE WHEN trip_type = 'loaded' THEN avg_duration_minutes ELSE 0 END) as loaded_avg_duration_minutes,
    MAX(CASE WHEN trip_type = 'empty' THEN trip_count ELSE 0 END) as empty_trip_count,
    MAX(CASE WHEN trip_type = 'empty' THEN avg_velocity_mph ELSE 0 END) as empty_avg_velocity_mph,
    MAX(CASE WHEN trip_type = 'empty' THEN avg_duration_minutes ELSE 0 END) as empty_avg_duration_minutes
  FROM trip_aggregations
  GROUP BY corridor_id
)
SELECT 
  corridor_id,
  loaded_trip_count,
  loaded_avg_velocity_mph,
  loaded_avg_duration_minutes,
  empty_trip_count,
  empty_avg_velocity_mph,
  empty_avg_duration_minutes,
  -- Asymmetry ratio: ratio of loaded to empty velocity
  -- Handle edge cases where velocities are 0 or very small
  CASE 
    WHEN empty_avg_velocity_mph > 0.01 AND loaded_avg_velocity_mph > 0.01 THEN 
      loaded_avg_velocity_mph / empty_avg_velocity_mph
    WHEN empty_avg_velocity_mph <= 0.01 AND loaded_avg_velocity_mph > 0.01 THEN 
      10.0  -- Loaded has velocity, empty doesn't - strong loaded preference
    WHEN loaded_avg_velocity_mph <= 0.01 AND empty_avg_velocity_mph > 0.01 THEN 
      0.1  -- Empty has velocity, loaded doesn't - strong empty preference
    ELSE 1.0  -- Both near zero, treat as balanced
  END as asymmetry_ratio,
  -- Determine priority direction
  CASE 
    WHEN empty_avg_velocity_mph > 0.01 AND loaded_avg_velocity_mph > 0.01 THEN
      CASE
        WHEN (loaded_avg_velocity_mph / empty_avg_velocity_mph) > 1.2 THEN 'loaded'
        WHEN (loaded_avg_velocity_mph / empty_avg_velocity_mph) < 0.8 THEN 'empty'
        ELSE 'balanced'
      END
    WHEN empty_avg_velocity_mph <= 0.01 AND loaded_avg_velocity_mph > 0.01 THEN 'loaded'
    WHEN loaded_avg_velocity_mph <= 0.01 AND empty_avg_velocity_mph > 0.01 THEN 'empty'
    ELSE 'balanced'
  END as priority_direction
FROM corridor_summary
WHERE loaded_trip_count > 0 AND empty_trip_count > 0
ORDER BY asymmetry_ratio DESC
