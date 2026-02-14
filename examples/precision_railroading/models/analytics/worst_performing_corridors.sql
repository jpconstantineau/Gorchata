-- Worst Performing Corridors Analysis
-- Ranks corridors by lowest fluidity index and highest dwell time
-- Highlights network segments requiring operational improvement

{{ config "materialized" "table" }}

WITH corridor_performance AS (
  SELECT
    t.corridor_id,
    c.corridor_code,
    c.origin_splc || ' → ' || c.destination_splc AS corridor_name,
    AVG((t.distance_miles / NULLIF((julianday(t.trip_end_timestamp) - julianday(t.trip_start_timestamp)) * 24 * 60, 0)) * 60) AS avg_velocity_mph,
    AVG((julianday(t.trip_end_timestamp) - julianday(t.trip_start_timestamp)) * 24 * 60) AS avg_duration_minutes,
    COUNT(*) AS trip_count,
    -- Calculate average dwell time by joining to fact_dwell via railcar
    COALESCE(AVG(dwell_stats.avg_dwell_minutes), 0) AS avg_dwell_minutes
  FROM {{ ref "fact_trip" }} t
  LEFT JOIN {{ ref "dim_corridor" }} c ON t.corridor_id = c.corridor_id
  LEFT JOIN (
    SELECT 
      railcar_id,
      AVG(dwell_duration_minutes) AS avg_dwell_minutes
    FROM {{ ref "fact_dwell" }}
    GROUP BY railcar_id
  ) dwell_stats ON t.railcar_id = dwell_stats.railcar_id
  WHERE t.corridor_id IS NOT NULL  -- Only include trips with corridor assignment
  GROUP BY t.corridor_id, c.corridor_code, c.origin_splc, c.destination_splc
  HAVING COUNT(*) >= 2  -- Require at least 2 trips for meaningful statistics
)

SELECT
  corridor_id,
  corridor_name,
  ROUND(avg_velocity_mph, 2) AS avg_velocity_mph,
  ROUND(avg_dwell_minutes, 2) AS avg_dwell_minutes,
  ROUND(avg_duration_minutes, 2) AS avg_duration_minutes,
  trip_count,
  ROW_NUMBER() OVER (ORDER BY avg_velocity_mph ASC, avg_dwell_minutes DESC) AS fluidity_rank
FROM corridor_performance
ORDER BY fluidity_rank
LIMIT 10  -- Top 10 worst performing corridors
