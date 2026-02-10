{{ config "materialized" "table" }}

-- Aggregated Corridor Weekly Metrics
-- Pre-aggregated weekly KPIs by corridor for performance analysis
-- Grain: one row per corridor per week
-- Supports seasonal analysis (week 5 slowdown, week 8 straggler spike)

WITH trip_metrics AS (
  -- Get all trip metrics with date/week information
  SELECT
    t.corridor_id,
    d.year,
    d.week,
    t.trip_id,
    t.transit_hours,
    t.origin_queue_hours,
    t.destination_queue_hours,
    t.total_trip_hours,
    t.num_stragglers,
    -- Get straggler delay information
    COALESCE(s.avg_delay, 0) AS avg_straggler_delay_hours
  FROM {{ ref "fact_train_trip" }} t
  INNER JOIN {{ ref "dim_corridor" }} c ON t.corridor_id = c.corridor_id
  INNER JOIN {{ ref "dim_date" }} d 
    ON t.departure_date_key = d.date_key
  LEFT JOIN (
    -- Calculate average straggler delay per trip
    SELECT
      train_id,
      set_out_date_key,
      AVG(delay_hours) AS avg_delay
    FROM {{ ref "fact_straggler" }}
    WHERE delay_hours IS NOT NULL
    GROUP BY train_id, set_out_date_key
  ) s
    ON t.train_id = s.train_id
    AND t.departure_date_key = s.set_out_date_key
)

-- Aggregate by corridor and week
SELECT
  corridor_id,
  year,
  week,
  
  -- Trip counts
  COUNT(trip_id) AS total_trips,
  
  -- Average transit time (origin to destination loaded)
  AVG(transit_hours) AS avg_transit_hours,
  
  -- Average queue wait times at origin (loading)
  AVG(origin_queue_hours) AS avg_origin_queue_hours,
  
  -- Average queue wait times at destination (unloading)
  AVG(destination_queue_hours) AS avg_destination_queue_hours,
  
  -- Total straggler occurrences
  SUM(num_stragglers) AS total_stragglers,
  
  -- Average straggler delay (only for trips with stragglers)
  -- Use NULLIF to avoid division by zero
  AVG(
    CASE 
      WHEN num_stragglers > 0 THEN avg_straggler_delay_hours
      ELSE NULL
    END
  ) AS avg_straggler_delay_hours,
  
  -- Average cycle time (complete round trip)
  AVG(total_trip_hours) AS avg_cycle_hours,
  
  -- Calculate straggler rate (stragglers per trip)
  CAST(SUM(num_stragglers) AS REAL) / NULLIF(COUNT(trip_id), 0) AS straggler_rate

FROM trip_metrics
GROUP BY corridor_id, year, week
ORDER BY corridor_id, year, week
