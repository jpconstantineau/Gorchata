-- Corridor Weekly Performance
-- Time-series analysis showing seasonal variation (25% variation pattern)
{{ config "materialized" "table" }}

SELECT 
  corridor_id,
  STRFTIME('%Y-W%W', trip_start_timestamp) as week_period,
  -- Derive year, quarter, month for seasonal analysis
  CAST(STRFTIME('%Y', trip_start_timestamp) AS INTEGER) as year,
  CAST((CAST(STRFTIME('%m', trip_start_timestamp) AS INTEGER) + 2) / 3 AS INTEGER) as quarter,
  CAST(STRFTIME('%m', trip_start_timestamp) AS INTEGER) as month,
  COUNT(*) as trip_count,
  COUNT(*) as car_count,  -- Each trip is one car
  AVG(COALESCE(average_velocity_mph, 0)) as avg_velocity_mph,
  AVG(duration_minutes) as avg_duration_minutes,
  SUM(distance_miles) as total_distance_miles
FROM {{ ref "fact_trip" }}
WHERE corridor_id IS NOT NULL
GROUP BY corridor_id, STRFTIME('%Y-W%W', trip_start_timestamp)
HAVING COUNT(*) > 0
ORDER BY week_period, corridor_id
