-- Cycle Time Optimization Analysis
-- Business Question: Where can we reduce total cycle time?
-- 
-- This query breaks down cycle time into components (transit, queue, turnaround)
-- to identify the biggest opportunities for time savings. Use this to:
-- - Prioritize improvement initiatives
-- - Quantify potential savings
-- - Balance investments across different cycle components

WITH corridor_cycle_components AS (
  -- Get average cycle time components by corridor
  SELECT
    m.corridor_id,
    c.corridor_name,
    c.origin_location,
    c.destination_location,
    
    -- Transit time (origin to destination loaded)
    AVG(m.avg_transit_hours) AS avg_transit_hours,
    
    -- Queue time at both ends
    AVG(m.avg_origin_queue_hours) AS avg_origin_queue_hours,
    AVG(m.avg_destination_queue_hours) AS avg_dest_queue_hours,
    AVG(m.avg_origin_queue_hours + m.avg_destination_queue_hours) AS avg_total_queue_hours,
    
    -- Get turnaround times (from separate aggregations)
    o.avg_turnaround_hours AS avg_origin_turnaround_hours,
    d.avg_turnaround_hours AS avg_dest_turnaround_hours,
    
    -- Total trip time
    AVG(m.avg_total_trip_hours) AS avg_total_trip_hours,
    
    -- Context
    SUM(m.total_trips) AS total_trips
    
  FROM {{ ref "agg_corridor_weekly_metrics" }} m
  INNER JOIN {{ ref "dim_corridor" }} c ON m.corridor_id = c.corridor_id
  LEFT JOIN {{ ref "agg_origin_turnaround" }} o 
    ON m.corridor_id = o.corridor_id AND m.year = o.year AND m.week = o.week
  LEFT JOIN {{ ref "agg_destination_turnaround" }} d 
    ON m.corridor_id = d.corridor_id AND m.year = d.year AND m.week = d.week
  GROUP BY 
    m.corridor_id, c.corridor_name, c.origin_location, c.destination_location,
    o.avg_turnaround_hours, d.avg_turnaround_hours
),

cycle_time_breakdown AS (
  -- Calculate full cycle time and component percentages
  SELECT
    corridor_id,
    corridor_name,
    origin_location,
    destination_location,
    
    -- Component times
    avg_transit_hours,
    avg_total_queue_hours,
    COALESCE(avg_origin_turnaround_hours, 0) AS avg_origin_turnaround_hours,
    COALESCE(avg_dest_turnaround_hours, 0) AS avg_dest_turnaround_hours,
    
    -- Full cycle approximation (round trip)
    (avg_transit_hours * 2) + 
    avg_total_queue_hours + 
    COALESCE(avg_origin_turnaround_hours, 0) + 
    COALESCE(avg_dest_turnaround_hours, 0) AS estimated_full_cycle_hours,
    
    total_trips
    
  FROM corridor_cycle_components
),

component_percentages AS (
  -- Calculate what percentage each component is of total cycle
  SELECT
    *,
    
    -- Component percentages
    ROUND((avg_transit_hours * 2) / NULLIF(estimated_full_cycle_hours, 0) * 100, 1) AS transit_pct,
    ROUND(avg_total_queue_hours / NULLIF(estimated_full_cycle_hours, 0) * 100, 1) AS queue_pct,
    ROUND((COALESCE(avg_origin_turnaround_hours, 0) + COALESCE(avg_dest_turnaround_hours, 0)) / 
          NULLIF(estimated_full_cycle_hours, 0) * 100, 1) AS turnaround_pct,
    
    -- Rank by longest delays in each category
    RANK() OVER (ORDER BY avg_transit_hours DESC) AS transit_delay_rank,
    RANK() OVER (ORDER BY avg_total_queue_hours DESC) AS queue_delay_rank,
    RANK() OVER (ORDER BY (COALESCE(avg_origin_turnaround_hours, 0) + 
                            COALESCE(avg_dest_turnaround_hours, 0)) DESC) AS turnaround_delay_rank
    
  FROM cycle_time_breakdown
)

-- Final results with improvement opportunities
SELECT
  corridor_id,
  corridor_name,
  origin_location || ' → ' || destination_location AS route,
  
  -- Cycle time components (hours)
  ROUND(avg_transit_hours, 2) AS avg_transit_hours,
  ROUND(avg_total_queue_hours, 2) AS avg_queue_hours,
  ROUND(avg_origin_turnaround_hours, 2) AS avg_origin_turnaround_hours,
  ROUND(avg_dest_turnaround_hours, 2) AS avg_dest_turnaround_hours,
  ROUND(estimated_full_cycle_hours, 2) AS estimated_full_cycle_hours,
  
  -- Component percentages
  transit_pct || '%' AS transit_pct_of_cycle,
  queue_pct || '%' AS queue_pct_of_cycle,
  turnaround_pct || '%' AS turnaround_pct_of_cycle,
  
  -- Improvement priority (which component to focus on)
  CASE 
    WHEN queue_pct >= transit_pct AND queue_pct >= turnaround_pct THEN 'Queue Reduction'
    WHEN transit_pct >= queue_pct AND transit_pct >= turnaround_pct THEN 'Transit Speed'
    ELSE 'Turnaround Efficiency'
  END AS top_opportunity,
  
  -- Rankings
  transit_delay_rank,
  queue_delay_rank,
  turnaround_delay_rank,
  
  total_trips AS trips_analyzed
  
FROM component_percentages
ORDER BY estimated_full_cycle_hours DESC;
