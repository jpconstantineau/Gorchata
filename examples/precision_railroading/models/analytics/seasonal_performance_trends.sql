-- Seasonal Performance Trends Analysis
-- Shows quarter-over-quarter and year-over-year performance changes
-- Highlights winter slowdowns and summer performance peaks

{{ config "materialized" "table" }}

WITH trip_with_temporal AS (
  SELECT
    t.trip_segment_id,
    t.trip_start_timestamp,
    t.trip_end_timestamp,
    t.distance_miles,
    (julianday(t.trip_end_timestamp) - julianday(t.trip_start_timestamp)) * 24 * 60 AS duration_minutes,
    (t.distance_miles / NULLIF((julianday(t.trip_end_timestamp) - julianday(t.trip_start_timestamp)) * 24 * 60, 0)) * 60 AS velocity_mph,
    -- Extract temporal components  
    CAST(strftime('%Y', t.trip_start_timestamp) AS INTEGER) AS year,
    CAST(strftime('%m', t.trip_start_timestamp) AS INTEGER) AS month,
    CASE
      WHEN CAST(strftime('%m', t.trip_start_timestamp) AS INTEGER) IN (1, 2, 3) THEN 1
      WHEN CAST(strftime('%m', t.trip_start_timestamp) AS INTEGER) IN (4, 5, 6) THEN 2
      WHEN CAST(strftime('%m', t.trip_start_timestamp) AS INTEGER) IN (7, 8, 9) THEN 3
      ELSE 4
    END AS quarter
  FROM {{ ref "fact_trip" }} t
  WHERE t.trip_start_timestamp IS NOT NULL
    AND t.trip_end_timestamp IS NOT NULL
),

quarterly_performance AS (
  SELECT
    year,
    quarter,
    year || '-Q' || quarter AS time_period,
    AVG(velocity_mph) AS avg_velocity_mph,
    AVG(duration_minutes) AS avg_duration_minutes,
    COUNT(*) AS trip_count,
    SUM(distance_miles) AS total_distance_miles
  FROM trip_with_temporal
  WHERE velocity_mph > 0  -- Exclude invalid velocities
  GROUP BY year, quarter
),

performance_with_yoy AS (
  SELECT
    time_period,
    quarter,
    year,
    ROUND(avg_velocity_mph, 2) AS avg_velocity_mph,
    ROUND(avg_duration_minutes, 2) AS avg_duration_minutes,
    trip_count,
    ROUND(total_distance_miles, 2) AS total_distance_miles,
    -- Calculate year-over-year velocity change
    ROUND(
      ((avg_velocity_mph - LAG(avg_velocity_mph) OVER (PARTITION BY quarter ORDER BY year)) / 
       NULLIF(LAG(avg_velocity_mph) OVER (PARTITION BY quarter ORDER BY year), 0)) * 100,
      2
    ) AS yoy_velocity_change_pct,
    -- Calculate quarter-over-quarter velocity change
    ROUND(
      ((avg_velocity_mph - LAG(avg_velocity_mph) OVER (ORDER BY year, quarter)) / 
       NULLIF(LAG(avg_velocity_mph) OVER (ORDER BY year, quarter), 0)) * 100,
      2
    ) AS qoq_velocity_change_pct
  FROM quarterly_performance
)

SELECT
  time_period,
  quarter,
  year,
  avg_velocity_mph,
  avg_duration_minutes,
  trip_count,
  total_distance_miles,
  COALESCE(yoy_velocity_change_pct, 0) AS yoy_velocity_change_pct,
  COALESCE(qoq_velocity_change_pct, 0) AS qoq_velocity_change_pct,
  -- Flag seasonal patterns
  CASE
    WHEN quarter IN (1, 4) THEN 'Winter (Slower)'
    WHEN quarter IN (2, 3) THEN 'Summer (Faster)'
  END AS seasonal_pattern
FROM performance_with_yoy
ORDER BY year, quarter
