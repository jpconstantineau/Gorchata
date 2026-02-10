-- Operational Bottleneck Analysis
-- Business Question: Where are the biggest bottlenecks in our operations?
-- 
-- This query identifies locations and trains with the longest delays and queue times
-- to pinpoint operational constraints. Use this to:
-- - Target capacity improvements
-- - Optimize scheduling
-- - Reduce wait times at congested locations

WITH location_queue_bottlenecks AS (
  -- Identify locations with longest average queue times
  SELECT
    q.corridor_id,
    c.corridor_name,
    c.origin_location,
    c.destination_location,
    
    -- Origin queue metrics
    AVG(q.avg_origin_queue_hours) AS avg_origin_queue_hours,
    MAX(q.max_origin_queue_hours) AS max_origin_queue_hours,
    SUM(q.total_origin_queue_hours) AS total_origin_queue_hours,
    
    -- Destination queue metrics
    AVG(q.avg_destination_queue_hours) AS avg_dest_queue_hours,
    MAX(q.max_destination_queue_hours) AS max_dest_queue_hours,
    SUM(q.total_destination_queue_hours) AS total_dest_queue_hours,
    
    -- Total queue impact
    SUM(q.total_origin_queue_hours + q.total_destination_queue_hours) AS total_queue_hours,
    AVG(q.avg_origin_queue_hours + q.avg_destination_queue_hours) AS avg_total_queue_hours,
    
    COUNT(*) AS weeks_observed
    
  FROM {{ ref "agg_queue_analysis" }} q
  INNER JOIN {{ ref "dim_corridor" }} c ON q.corridor_id = c.corridor_id
  GROUP BY q.corridor_id, c.corridor_name, c.origin_location, c.destination_location
),

train_delay_bottlenecks AS (
  -- Identify trains with most delays
  SELECT
    t.train_id,
    tr.train_number,
    COUNT(t.trip_id) AS total_trips,
    
    -- Total delay hours accumulated
    SUM(t.origin_queue_hours + t.destination_queue_hours) AS total_queue_delay_hours,
    AVG(t.origin_queue_hours + t.destination_queue_hours) AS avg_queue_delay_hours,
    
    -- Straggler impact
    SUM(t.num_stragglers) AS total_stragglers,
    AVG(t.num_stragglers) AS avg_stragglers_per_trip,
    
    -- Trip duration variance (high variance indicates inconsistent performance)
    AVG(t.total_trip_hours) AS avg_trip_hours,
    MAX(t.total_trip_hours) - MIN(t.total_trip_hours) AS trip_duration_range
    
  FROM {{ ref "fact_train_trip" }} t
  INNER JOIN {{ ref "dim_train" }} tr ON t.train_id = tr.train_id
  GROUP BY t.train_id, tr.train_number
),

ranked_location_bottlenecks AS (
  -- Rank location bottlenecks by total queue hours
  SELECT
    *,
    RANK() OVER (ORDER BY total_queue_hours DESC) AS bottleneck_rank
  FROM location_queue_bottlenecks
),

ranked_train_bottlenecks AS (
  -- Rank train bottlenecks by total delay hours
  SELECT
    *,
    RANK() OVER (ORDER BY total_queue_delay_hours DESC) AS delay_rank
  FROM train_delay_bottlenecks
)

-- Combine results showing top bottlenecks
SELECT
  'Location Queue' AS bottleneck_type,
  bottleneck_rank AS rank,
  corridor_name AS entity_name,
  origin_location || ' → ' || destination_location AS route,
  ROUND(total_queue_hours, 2) AS total_hours_lost,
  ROUND(avg_total_queue_hours, 2) AS avg_hours_per_week,
  ROUND(avg_origin_queue_hours, 2) AS avg_origin_queue,
  ROUND(avg_dest_queue_hours, 2) AS avg_dest_queue,
  weeks_observed AS observations
FROM ranked_location_bottlenecks
WHERE bottleneck_rank <= 5

UNION ALL

SELECT
  'Train Delays' AS bottleneck_type,
  delay_rank AS rank,
  train_number AS entity_name,
  'All Corridors' AS route,
  ROUND(total_queue_delay_hours, 2) AS total_hours_lost,
  ROUND(avg_queue_delay_hours, 2) AS avg_hours_per_trip,
  ROUND(avg_stragglers_per_trip, 2) AS avg_stragglers,
  ROUND(trip_duration_range, 2) AS duration_variance,
  total_trips AS observations
FROM ranked_train_bottlenecks
WHERE delay_rank <= 5

ORDER BY bottleneck_type ASC, rank ASC;
