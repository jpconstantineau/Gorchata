{{ config "materialized" "table" }}

-- Fact: Corridor Transit
-- Purpose: Aggregated corridor metrics by time period
-- Grain: One row per corridor per time period (weekly)
-- Source: fact_trip aggregated by corridor and week

WITH trip_data AS (
  SELECT
    corridor_id,
    railcar_id,
    trip_segment_id,
    trip_start_timestamp,
    trip_type,
    distance_miles,
    duration_minutes,
    dwell_count,
    psr_period
  FROM {{ ref "fact_trip" }}
),

-- Add time period grouping (weekly)
trips_with_period AS (
  SELECT
    corridor_id,
    STRFTIME('%Y-W%W', trip_start_timestamp) AS time_period,
    CAST(STRFTIME('%Y', trip_start_timestamp) AS INTEGER) AS year,
    CAST(STRFTIME('%W', trip_start_timestamp) AS INTEGER) AS week_number,
    railcar_id,
    trip_segment_id,
    trip_type,
    distance_miles,
    duration_minutes,
    dwell_count,
    psr_period
  FROM trip_data
  WHERE corridor_id IS NOT NULL  -- Exclude trips without corridor assignment
)

-- Aggregate by corridor and time period
SELECT
  corridor_id,
  time_period,
  year,
  week_number,
  COUNT(DISTINCT railcar_id) AS car_count,
  COUNT(trip_segment_id) AS trip_count,
  SUM(CASE WHEN trip_type = 'loaded' THEN 1 ELSE 0 END) AS loaded_trip_count,
  SUM(CASE WHEN trip_type = 'empty' THEN 1 ELSE 0 END) AS empty_trip_count,
  COALESCE(SUM(distance_miles), 0) AS total_distance_miles,
  SUM(duration_minutes) AS total_duration_minutes,
  -- Calculate average velocity: total distance / total time * 60 (convert hours to mph)
  CASE 
    WHEN SUM(duration_minutes) > 0 
    THEN (COALESCE(SUM(distance_miles), 0) / SUM(duration_minutes)) * 60.0
    ELSE NULL
  END AS average_velocity_mph,
  AVG(duration_minutes) AS average_trip_duration_minutes,
  AVG(dwell_count) AS average_dwell_count,
  -- Use the most common psr_period in the group (simplified: take MAX)
  MAX(psr_period) AS psr_period
FROM trips_with_period
GROUP BY 
  corridor_id,
  time_period,
  year,
  week_number
