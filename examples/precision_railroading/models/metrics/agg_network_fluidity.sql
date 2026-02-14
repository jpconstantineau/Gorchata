-- Network Fluidity Index Aggregation
-- Calculate weighted average car velocity by corridor and time period
{{ config "materialized" "table" }}

SELECT 
  corridor_id,
  STRFTIME('%Y-W%W', trip_start_timestamp) as week_period,
  SUM(distance_miles) as total_distance_miles,
  SUM(duration_minutes) as total_duration_minutes,
  -- Fluidity index: network-wide velocity (distance / duration * 60)
  CASE 
    WHEN SUM(duration_minutes) > 0 THEN (SUM(distance_miles) / SUM(duration_minutes)) * 60.0
    ELSE 0
  END as fluidity_index_mph,
  COUNT(*) as car_count,  -- Each row represents a car trip
  COUNT(*) as trip_count
FROM {{ ref "fact_trip" }}
WHERE corridor_id IS NOT NULL  -- Only include trips with assigned corridors
GROUP BY corridor_id, STRFTIME('%Y-W%W', trip_start_timestamp)
HAVING COUNT(*) > 0
ORDER BY week_period, corridor_id
