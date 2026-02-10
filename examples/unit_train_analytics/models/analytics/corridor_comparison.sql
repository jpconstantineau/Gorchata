-- Corridor Performance Comparison Analysis
-- Business Question: Which corridors have the best/worst performance across key metrics?
-- 
-- This query ranks corridors by transit time, straggler rates, and queue times
-- to identify high-performing and underperforming routes. Use this to:
-- - Prioritize operational improvements
-- - Benchmark corridor performance
-- - Identify best practices from top performers

WITH corridor_avg_metrics AS (
  -- Calculate average metrics across all weeks for each corridor
  SELECT
    c.corridor_id,
    c.corridor_name,
    c.origin_location,
    c.destination_location,
    
    -- Average trip metrics
    AVG(m.avg_transit_hours) AS avg_transit_hours,
    AVG(m.avg_origin_queue_hours) AS avg_origin_queue_hours,
    AVG(m.avg_destination_queue_hours) AS avg_destination_queue_hours,
    AVG(m.avg_total_trip_hours) AS avg_total_trip_hours,
    
    -- Straggler metrics
    AVG(m.straggler_rate) AS avg_straggler_rate,
    AVG(m.avg_straggler_delay_hours) AS avg_straggler_delay_hours,
    
    -- Total metrics for context
    SUM(m.total_trips) AS total_trips,
    SUM(m.total_stragglers) AS total_stragglers
    
  FROM {{ ref "agg_corridor_weekly_metrics" }} m
  INNER JOIN {{ ref "dim_corridor" }} c ON m.corridor_id = c.corridor_id
  GROUP BY c.corridor_id, c.corridor_name, c.origin_location, c.destination_location
),

corridor_rankings AS (
  -- Rank corridors by key performance metrics (lower is better for times/rates)
  SELECT
    corridor_id,
    corridor_name,
    origin_location,
    destination_location,
    
    -- Metrics
    avg_transit_hours,
    avg_origin_queue_hours,
    avg_destination_queue_hours,
    avg_total_trip_hours,
    avg_straggler_rate,
    avg_straggler_delay_hours,
    total_trips,
    total_stragglers,
    
    -- Rankings (1 = best performance)
    RANK() OVER (ORDER BY avg_transit_hours ASC) AS transit_time_rank,
    RANK() OVER (ORDER BY avg_origin_queue_hours ASC) AS origin_queue_rank,
    RANK() OVER (ORDER BY avg_destination_queue_hours ASC) AS dest_queue_rank,
    RANK() OVER (ORDER BY avg_straggler_rate ASC) AS straggler_rate_rank,
    RANK() OVER (ORDER BY avg_total_trip_hours ASC) AS total_trip_time_rank
    
  FROM corridor_avg_metrics
)

-- Final ranking with overall performance score
SELECT
  corridor_id,
  corridor_name,
  origin_location || ' → ' || destination_location AS route,
  
  -- Rounded metrics for readability
  ROUND(avg_transit_hours, 2) AS avg_transit_hours,
  ROUND(avg_origin_queue_hours, 2) AS avg_origin_queue_hours,
  ROUND(avg_destination_queue_hours, 2) AS avg_dest_queue_hours,
  ROUND(avg_total_trip_hours, 2) AS avg_total_trip_hours,
  ROUND(avg_straggler_rate * 100, 2) AS avg_straggler_rate_pct,
  ROUND(avg_straggler_delay_hours, 2) AS avg_straggler_delay_hours,
  
  -- Rankings
  transit_time_rank,
  origin_queue_rank,
  dest_queue_rank,
  straggler_rate_rank,
  total_trip_time_rank,
  
  -- Overall performance score (sum of ranks, lower is better)
  (transit_time_rank + origin_queue_rank + dest_queue_rank + 
   straggler_rate_rank + total_trip_time_rank) AS overall_rank_score,
  
  -- Context
  total_trips,
  total_stragglers
  
FROM corridor_rankings
ORDER BY overall_rank_score ASC, avg_total_trip_hours ASC;
