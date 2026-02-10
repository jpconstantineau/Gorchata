{{ config "materialized" "table" }}

-- Aggregated Destination Turnaround Metrics
-- Turnaround time analysis at destination locations (including queue wait)
-- Grain: one row per destination location per week
-- Turnaround = queue/unloading time at destination (destination_queue_hours)

WITH destination_turnarounds AS (
  -- Get turnaround metrics for each trip at destination
  SELECT
    c.destination_location_id,
    d.year,
    d.week,
    t.trip_id,
    t.destination_queue_hours AS turnaround_hours
  FROM {{ ref "fact_train_trip" }} t
  INNER JOIN {{ ref "dim_corridor" }} c ON t.corridor_id = c.corridor_id
  INNER JOIN {{ ref "dim_date" }} d ON t.departure_date_key = d.date_key
  WHERE t.destination_queue_hours IS NOT NULL
)

-- Aggregate turnaround metrics by destination and week
SELECT
  destination_location_id AS location_id,
  year,
  week,
  
  -- Trip count
  COUNT(trip_id) AS trip_count,
  
  -- Average turnaround time
  AVG(turnaround_hours) AS avg_turnaround_hours,
  
  -- Minimum turnaround time (fastest unloading)
  MIN(turnaround_hours) AS min_turnaround_hours,
  
  -- Maximum turnaround time (slowest unloading)
  MAX(turnaround_hours) AS max_turnaround_hours,
  
  -- Standard deviation to measure variability
  -- Note: SQLite doesn't have STDDEV, so we calculate it manually
  SQRT(
    AVG(turnaround_hours * turnaround_hours) - 
    (AVG(turnaround_hours) * AVG(turnaround_hours))
  ) AS stddev_turnaround_hours

FROM destination_turnarounds
GROUP BY destination_location_id, year, week
ORDER BY destination_location_id, year, week
