-- Queue Waiting Impact Analysis
-- Business Question: How much time and money are we losing to queuing?
-- 
-- This query quantifies the total hours lost to queue waiting by location
-- and calculates what percentage of cycle time is spent waiting. Use this to:
-- - Justify capacity investments
-- - Calculate operational cost of congestion
-- - Prioritize queue reduction initiatives

WITH queue_totals_by_location AS (
  -- Calculate total queue hours by location (origin vs destination)
  SELECT
    q.corridor_id,
    c.corridor_name,
    c.origin_location,
    c.destination_location,
    
    -- Origin queue impact
    SUM(q.total_origin_queue_hours) AS total_origin_queue_hours,
    AVG(q.avg_origin_queue_hours) AS avg_origin_queue_hours,
    MAX(q.max_origin_queue_hours) AS max_origin_queue_hours,
    COUNT(DISTINCT q.week) AS weeks_with_origin_queue,
    
    -- Destination queue impact
    SUM(q.total_destination_queue_hours) AS total_dest_queue_hours,
    AVG(q.avg_destination_queue_hours) AS avg_dest_queue_hours,
    MAX(q.max_destination_queue_hours) AS max_dest_queue_hours,
    COUNT(DISTINCT q.week) AS weeks_with_dest_queue,
    
    -- Combined queue impact
    SUM(q.total_origin_queue_hours + q.total_destination_queue_hours) AS total_queue_hours,
    AVG(q.avg_origin_queue_hours + q.avg_destination_queue_hours) AS avg_queue_hours_per_week
    
  FROM {{ ref "agg_queue_analysis" }} q
  INNER JOIN {{ ref "dim_corridor" }} c ON q.corridor_id = c.corridor_id
  GROUP BY q.corridor_id, c.corridor_name, c.origin_location, c.destination_location
),

corridor_total_time AS (
  -- Get total trip time for percentage calculation
  SELECT
    corridor_id,
    SUM(m.total_trips * m.avg_total_trip_hours) AS total_trip_hours,
    AVG(m.avg_total_trip_hours) AS avg_trip_hours
  FROM {{ ref "agg_corridor_weekly_metrics" }} m
  GROUP BY corridor_id
),

queue_impact_with_percentages AS (
  -- Calculate queue percentage of total cycle time
  SELECT
    q.corridor_id,
    q.corridor_name,
    q.origin_location,
    q.destination_location,
    
    -- Queue hours
    q.total_origin_queue_hours,
    q.avg_origin_queue_hours,
    q.max_origin_queue_hours,
    q.total_dest_queue_hours,
    q.avg_dest_queue_hours,
    q.max_dest_queue_hours,
    q.total_queue_hours,
    q.avg_queue_hours_per_week,
    
    -- Total time context
    t.total_trip_hours,
    t.avg_trip_hours,
    
    -- Queue as percentage of total time
    ROUND(q.total_queue_hours / NULLIF(t.total_trip_hours, 0) * 100, 2) AS queue_pct_of_total_time,
    
    -- Origin vs destination queue split
    ROUND(q.total_origin_queue_hours / NULLIF(q.total_queue_hours, 0) * 100, 1) AS origin_queue_pct,
    ROUND(q.total_dest_queue_hours / NULLIF(q.total_queue_hours, 0) * 100, 1) AS dest_queue_pct,
    
    -- Weeks observed
    q.weeks_with_origin_queue,
    q.weeks_with_dest_queue
    
  FROM queue_totals_by_location q
  INNER JOIN corridor_total_time t ON q.corridor_id = t.corridor_id
),

ranked_queue_impact AS (
  -- Rank corridors by queue impact severity
  SELECT
    *,
    RANK() OVER (ORDER BY total_queue_hours DESC) AS queue_impact_rank,
    RANK() OVER (ORDER BY queue_pct_of_total_time DESC) AS queue_pct_rank
  FROM queue_impact_with_percentages
)

-- Final results with cost estimates (assuming $500/hour for idle train)
SELECT
  queue_impact_rank AS rank,
  corridor_id,
  corridor_name,
  origin_location,
  destination_location,
  
  -- Total queue hours lost
  ROUND(total_queue_hours, 2) AS total_queue_hours,
  
  -- Breakdown by location
  ROUND(total_origin_queue_hours, 2) AS total_origin_queue_hours,
  ROUND(total_dest_queue_hours, 2) AS total_dest_queue_hours,
  
  -- Average and max queue times
  ROUND(avg_queue_hours_per_week, 2) AS avg_queue_hours_per_week,
  ROUND(GREATEST(max_origin_queue_hours, max_dest_queue_hours), 2) AS max_single_queue_hours,
  
  -- Queue percentage of cycle time
  queue_pct_of_total_time || '%' AS queue_pct_of_cycle,
  
  -- Origin vs destination split
  origin_queue_pct || '%' AS origin_pct_of_queue,
  dest_queue_pct || '%' AS dest_pct_of_queue,
  
  -- Estimated cost impact (assuming $500/hour for idle train)
  '$' || CAST(ROUND(total_queue_hours * 500, 0) AS TEXT) AS estimated_cost_at_500_per_hour,
  
  -- Improvement opportunity
  CASE 
    WHEN queue_pct_of_total_time > 30 THEN 'Critical - Reduce Queue'
    WHEN queue_pct_of_total_time > 20 THEN 'High - Improve Flow'
    WHEN queue_pct_of_total_time > 10 THEN 'Moderate - Monitor'
    ELSE 'Low - Acceptable'
  END AS priority_level,
  
  -- Which location has bigger queue problem
  CASE 
    WHEN origin_queue_pct > dest_queue_pct + 10 THEN 'Focus on Origin'
    WHEN dest_queue_pct > origin_queue_pct + 10 THEN 'Focus on Destination'
    ELSE 'Balance Both Ends'
  END AS improvement_focus
  
FROM ranked_queue_impact
ORDER BY queue_impact_rank ASC;
