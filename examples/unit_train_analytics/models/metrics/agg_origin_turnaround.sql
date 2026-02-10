{{ config "materialized" "table" }}

-- Aggregated Origin Turnaround Metrics
-- Turnaround time analysis at origin locations (including queue wait)
-- Grain: one row per origin location per week
-- Turnaround = queue/loading time at origin (origin_queue_hours)

WITH origin_turnarounds AS (
  -- Get turnaround metrics for each trip at origin
  SELECT
    c.origin_location_id,
    d.year,
    d.week,
    t.trip_id,
    t.origin_queue_hours AS turnaround_hours
  FROM {{ ref "fact_train_trip" }} t
  INNER JOIN {{ ref "dim_corridor" }} c ON t.corridor_id = c.corridor_id
  INNER JOIN {{ ref "dim_date" }} d ON t.departure_date_key = d.date_key
  WHERE t.origin_queue_hours IS NOT NULL
)

-- Aggregate turnaround metrics by origin and week
SELECT
  origin_location_id AS location_id,
  year,
  week,
  
  -- Trip count
  COUNT(trip_id) AS trip_count,
  
  -- Average turnaround time
  AVG(turnaround_hours) AS avg_turnaround_hours,
  
  -- Minimum turnaround time (fastest loading)
  MIN(turnaround_hours) AS min_turnaround_hours,
  
  -- Maximum turnaround time (slowest loading)
  MAX(turnaround_hours) AS max_turnaround_hours,
  
  -- Standard deviation to measure variability
  -- Note: SQLite doesn't have STDDEV, so we calculate it manually
  SQRT(
    AVG(turnaround_hours * turnaround_hours) - 
    (AVG(turnaround_hours) * AVG(turnaround_hours))
  ) AS stddev_turnaround_hours

FROM origin_turnarounds
GROUP BY origin_location_id, year, week
ORDER BY origin_location_id, year, week
