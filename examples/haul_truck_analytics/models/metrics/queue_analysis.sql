{{ config "materialized" "table" }}

-- Queue Analysis Metrics
-- Analyzes queue wait times at both crusher and shovel locations
-- Grain: one row per location per date
-- Metrics: average/max queue times, queue hours, trucks affected, peak hours

WITH crusher_queues AS (
  -- Extract crusher queue metrics from cycles
  SELECT
    crusher_id AS location_id,
    'CRUSHER' AS location_type,
    date_id,
    truck_id,
    duration_queue_crusher_min AS queue_time_min,
    cycle_start,
    CAST(strftime('%H', cycle_start) AS INTEGER) AS queue_hour
  FROM {{ ref "fact_haul_cycle" }}
  WHERE duration_queue_crusher_min > 0
),

shovel_queues AS (
  -- Extract shovel queue metrics from cycles
  SELECT
    shovel_id AS location_id,
    'SHOVEL' AS location_type,
    date_id,
    truck_id,
    duration_queue_shovel_min AS queue_time_min,
    cycle_start,
    CAST(strftime('%H', cycle_start) AS INTEGER) AS queue_hour
  FROM {{ ref "fact_haul_cycle" }}
  WHERE duration_queue_shovel_min > 0
),

all_queues AS (
  -- Union both queue sources
  SELECT * FROM crusher_queues
  UNION ALL
  SELECT * FROM shovel_queues
),

queue_aggregation AS (
  -- Aggregate queue metrics by location and date
  SELECT
    location_id,
    location_type,
    date_id,
    -- Average queue time
    AVG(queue_time_min) AS avg_queue_time_min,
    -- Maximum queue time (worst case)
    MAX(queue_time_min) AS max_queue_time_min,
    -- Total queue hours
    SUM(queue_time_min) / 60.0 AS total_queue_hours,
    -- Number of queue events
    COUNT(*) AS queue_events_count,
    -- Number of unique trucks affected
    COUNT(DISTINCT truck_id) AS trucks_affected
  FROM all_queues
  GROUP BY location_id, location_type, date_id
),

peak_hours AS (
  -- Identify peak queue hour for each location/date
  SELECT
    location_id,
    location_type,
    date_id,
    queue_hour,
    AVG(queue_time_min) AS avg_queue_for_hour,
    ROW_NUMBER() OVER (
      PARTITION BY location_id, location_type, date_id
      ORDER BY AVG(queue_time_min) DESC
    ) AS hour_rank
  FROM all_queues
  GROUP BY location_id, location_type, date_id, queue_hour
)

-- Final queue analysis output
SELECT
  qa.location_id,
  qa.location_type,
  qa.date_id,
  qa.avg_queue_time_min,
  qa.max_queue_time_min,
  qa.total_queue_hours,
  qa.queue_events_count,
  qa.trucks_affected,
  -- Peak queue hour (hour with longest average queues)
  COALESCE(ph.queue_hour, 0) AS peak_queue_hour
FROM queue_aggregation qa
LEFT JOIN peak_hours ph
  ON qa.location_id = ph.location_id
 AND qa.location_type = ph.location_type
 AND qa.date_id = ph.date_id
 AND ph.hour_rank = 1
ORDER BY qa.location_type, qa.location_id, qa.date_id
