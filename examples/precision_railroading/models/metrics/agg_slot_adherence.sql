-- Slot Adherence Metrics
-- Measure on-time performance using temporal variance
-- Lower variance in arrival times indicates better slot adherence
{{ config "materialized" "table" }}

WITH arrival_times AS (
  SELECT 
    location_id,
    STRFTIME('%Y-%m', dwell_start_timestamp) as time_period,
    -- Convert timestamp to decimal hours (0-24)
    CAST(STRFTIME('%H', dwell_start_timestamp) AS REAL) + 
    (CAST(STRFTIME('%M', dwell_start_timestamp) AS REAL) / 60.0) as arrival_hour_decimal
  FROM {{ ref "fact_dwell" }}
  WHERE dwell_start_timestamp IS NOT NULL
),
variance_calculations AS (
  SELECT 
    location_id,
    time_period,
    COUNT(*) as arrival_count,
    AVG(arrival_hour_decimal) as avg_hour,
    -- Calculate standard deviation manually
    SQRT(AVG(arrival_hour_decimal * arrival_hour_decimal) - 
         AVG(arrival_hour_decimal) * AVG(arrival_hour_decimal)) as stddev_hours
  FROM arrival_times
  GROUP BY location_id, time_period
  HAVING COUNT(*) >= 2  -- Need at least 2 arrivals to calculate variance
)
SELECT 
  location_id,
  time_period,
  arrival_count,
  stddev_hours,
  -- Transform variance to 0-100 adherence score (lower variance = higher score)
  -- Using exponential decay: score = 100 * exp(-stddev)
  -- Simplified: score = MAX(0, 100 - (stddev_hours * 15))
  CASE 
    WHEN stddev_hours IS NULL THEN 100
    ELSE MAX(0, MIN(100, 100 - (stddev_hours * 15)))
  END as adherence_score
FROM variance_calculations
ORDER BY location_id, time_period
