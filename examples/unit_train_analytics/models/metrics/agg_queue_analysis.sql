{{ config "materialized" "table" }}

-- Aggregated Queue Analysis Metrics
-- Identifies queue wait time patterns and bottlenecks at origins and destinations
-- Grain: one row per location per week
-- Analyzes both origin (loading) and destination (unloading) queue times

WITH origin_queue_times AS (
  -- Extract origin queue times with location context
  SELECT
    c.origin_location_id AS location_id,
    'ORIGIN' AS location_type,
    d.year,
    d.week,
    t.trip_id,
    t.origin_queue_hours AS queue_hours
  FROM {{ ref "fact_train_trip" }} t
  INNER JOIN {{ ref "dim_corridor" }} c ON t.corridor_id = c.corridor_id
  INNER JOIN {{ ref "dim_date" }} d ON t.departure_date_key = d.date_key
  WHERE t.origin_queue_hours IS NOT NULL
),

destination_queue_times AS (
  -- Extract destination queue times with location context
  SELECT
    c.destination_location_id AS location_id,
    'DESTINATION' AS location_type,
    d.year,
    d.week,
    t.trip_id,
    t.destination_queue_hours AS queue_hours
  FROM {{ ref "fact_train_trip" }} t
  INNER JOIN {{ ref "dim_corridor" }} c ON t.corridor_id = c.corridor_id
  INNER JOIN {{ ref "dim_date" }} d ON t.departure_date_key = d.date_key
  WHERE t.destination_queue_hours IS NOT NULL
),

-- Union origin and destination queue times
all_queue_times AS (
  SELECT * FROM origin_queue_times
  UNION ALL
  SELECT * FROM destination_queue_times
)

-- Aggregate queue metrics by location and week
SELECT
  location_id,
  location_type,
  year,
  week,
  
  -- Queue frequency (number of trips)
  COUNT(trip_id) AS queue_frequency,
  
  -- Average queue wait time
  AVG(queue_hours) AS avg_queue_hours,
  
  -- Minimum queue wait time
  MIN(queue_hours) AS min_queue_hours,
  
  -- Maximum queue wait time (identify bottlenecks)
  MAX(queue_hours) AS max_queue_hours,
  
  -- Standard deviation to measure variability
  SQRT(
    AVG(queue_hours * queue_hours) - 
    (AVG(queue_hours) * AVG(queue_hours))
  ) AS stddev_queue_hours,
  
  -- Queue time percentiles for distribution analysis
  -- 75th percentile (approximate)
  (
    SELECT queue_hours
    FROM (
      SELECT 
        queue_hours,
        ROW_NUMBER() OVER (ORDER BY queue_hours) AS rn,
        COUNT(*) OVER () AS cnt
      FROM all_queue_times aqt2
      WHERE aqt2.location_id = aqt.location_id
        AND aqt2.year = aqt.year
        AND aqt2.week = aqt.week
    )
    WHERE rn = (cnt * 3) / 4
  ) AS p75_queue_hours,
  
  -- 95th percentile (approximate)
  (
    SELECT queue_hours
    FROM (
      SELECT 
        queue_hours,
        ROW_NUMBER() OVER (ORDER BY queue_hours) AS rn,
        COUNT(*) OVER () AS cnt
      FROM all_queue_times aqt3
      WHERE aqt3.location_id = aqt.location_id
        AND aqt3.year = aqt.year
        AND aqt3.week = aqt.week
    )
    WHERE rn = (cnt * 95) / 100
  ) AS p95_queue_hours

FROM all_queue_times aqt
GROUP BY location_id, location_type, year, week
ORDER BY location_id, year, week
