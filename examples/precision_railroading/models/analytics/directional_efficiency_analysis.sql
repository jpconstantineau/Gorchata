-- Directional Efficiency Analysis
-- Shows loaded vs empty trip asymmetry
-- Identifies corridors where railroad prioritizes one direction

{{ config "materialized" "table" }}

WITH trip_directional_stats AS (
  SELECT
    t.corridor_id,
    c.corridor_code || ' (' || c.origin_splc || ' → ' || c.destination_splc || ')' AS corridor_name,
    t.trip_type,
    AVG((t.distance_miles / NULLIF((julianday(t.trip_end_timestamp) - julianday(t.trip_start_timestamp)) * 24 * 60, 0)) * 60) AS avg_velocity_mph,
    AVG((julianday(t.trip_end_timestamp) - julianday(t.trip_start_timestamp)) * 24 * 60) AS avg_duration_minutes,
    COUNT(*) AS trip_count,
    SUM(t.distance_miles) AS total_distance_miles
  FROM {{ ref "fact_trip" }} t
  LEFT JOIN {{ ref "dim_corridor" }} c ON t.corridor_id = c.corridor_id
  WHERE t.trip_type IN ('loaded', 'empty')
    AND t.corridor_id IS NOT NULL
  GROUP BY t.corridor_id, c.corridor_code, c.origin_splc, c.destination_splc, t.trip_type
),

pivoted_stats AS (
  SELECT
    corridor_id,
    corridor_name,
    MAX(CASE WHEN trip_type = 'loaded' THEN avg_velocity_mph ELSE 0 END) AS loaded_velocity_mph,
    MAX(CASE WHEN trip_type = 'empty' THEN avg_velocity_mph ELSE 0 END) AS empty_velocity_mph,
    MAX(CASE WHEN trip_type = 'loaded' THEN avg_duration_minutes ELSE 0 END) AS loaded_duration_minutes,
    MAX(CASE WHEN trip_type = 'empty' THEN avg_duration_minutes ELSE 0 END) AS empty_duration_minutes,
    MAX(CASE WHEN trip_type = 'loaded' THEN trip_count ELSE 0 END) AS loaded_trip_count,
    MAX(CASE WHEN trip_type = 'empty' THEN trip_count ELSE 0 END) AS empty_trip_count,
    MAX(CASE WHEN trip_type = 'loaded' THEN total_distance_miles ELSE 0 END) AS loaded_distance_miles,
    MAX(CASE WHEN trip_type = 'empty' THEN total_distance_miles ELSE 0 END) AS empty_distance_miles
  FROM trip_directional_stats
  GROUP BY corridor_id, corridor_name
  -- Require both loaded and empty trips for meaningful comparison
  HAVING MAX(CASE WHEN trip_type = 'loaded' THEN 1 ELSE 0 END) = 1
     AND MAX(CASE WHEN trip_type = 'empty' THEN 1 ELSE 0 END) = 1
)

SELECT
  corridor_id,
  corridor_name,
  ROUND(loaded_velocity_mph, 2) AS loaded_velocity_mph,
  ROUND(empty_velocity_mph, 2) AS empty_velocity_mph,
  ROUND(loaded_duration_minutes, 2) AS loaded_duration_minutes,
  ROUND(empty_duration_minutes, 2) AS empty_duration_minutes,
  loaded_trip_count,
  empty_trip_count,
  ROUND(loaded_distance_miles, 2) AS loaded_distance_miles,
  ROUND(empty_distance_miles, 2) AS empty_distance_miles,
  -- Calculate asymmetry ratio (loaded / empty velocity)
  ROUND(
    loaded_velocity_mph / NULLIF(empty_velocity_mph, 0),
    3
  ) AS asymmetry_ratio,
  -- Determine priority direction
  CASE
    WHEN loaded_velocity_mph / NULLIF(empty_velocity_mph, 0) > 1.2 THEN 'Loaded Prioritized'
    WHEN loaded_velocity_mph / NULLIF(empty_velocity_mph, 0) < 0.8 THEN 'Empty Prioritized'
    ELSE 'Balanced'
  END AS priority_direction,
  -- Calculate velocity delta (absolute mph difference)
  ROUND(ABS(loaded_velocity_mph - empty_velocity_mph), 2) AS velocity_delta_mph,
  -- Calculate balance score (100 = perfectly balanced, 0 = very imbalanced)
  ROUND(
    100 - ABS(
      ((loaded_velocity_mph - empty_velocity_mph) / 
       NULLIF((loaded_velocity_mph + empty_velocity_mph) / 2, 0)) * 100
    ),
    2
  ) AS balance_score
FROM pivoted_stats
WHERE loaded_velocity_mph > 0 
  AND empty_velocity_mph > 0  -- Valid velocities only
ORDER BY ABS(asymmetry_ratio - 1.0) DESC  -- Most imbalanced first
