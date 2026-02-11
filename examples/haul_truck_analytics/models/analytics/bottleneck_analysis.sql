{{ config "materialized" "view" }}

-- Bottleneck Analysis
-- Business Question: Where are the operational constraints in the haul system?
-- 
-- This query analyzes queue patterns at shovels and crushers to identify
-- system bottlenecks. Use this to:
-- - Prioritize capacity expansion investments
-- - Optimize fleet allocation
-- - Plan maintenance windows
-- - Improve scheduling and dispatch

WITH location_queue_metrics AS (
  -- Aggregate queue metrics by location
  SELECT
    q.location_id,
    q.location_type,
    
    -- Queue time metrics
    AVG(q.avg_queue_time_min) AS avg_queue_time_min,
    MAX(q.max_queue_time_min) AS max_queue_time_min,
    SUM(q.total_queue_hours) AS total_queue_hours,
    SUM(q.queue_events_count) AS total_queue_events,
    AVG(q.trucks_affected) AS avg_trucks_affected,
    
    -- Days observed
    COUNT(DISTINCT q.date_id) AS days_observed
    
  FROM {{ ref "queue_analysis" }} q
  GROUP BY q.location_id, q.location_type
),

location_details AS (
  -- Enrich with location capacity information
  SELECT
    lqm.*,
    
    -- Add shovel details
    s.bucket_size_m3,
    s.pit_zone,
    
    -- Add crusher details
    c.capacity_tph
    
  FROM location_queue_metrics lqm
  LEFT JOIN {{ ref "dim_shovel" }} s ON lqm.location_id = s.shovel_id AND lqm.location_type = 'SHOVEL'
  LEFT JOIN {{ ref "dim_crusher" }} c ON lqm.location_id = c.crusher_id AND lqm.location_type = 'CRUSHER'
),

utilization_calc AS (
  -- Calculate utilization based on actual throughput
  SELECT
    ld.*,
    
    -- Calculate utilization percentage (simplified: based on queue time as proxy)
    -- High queue time = high utilization approaching or exceeding capacity
    CASE
      WHEN ld.location_type = 'CRUSHER' THEN
        -- Crusher utilization estimated from queue presence
        ROUND(
          CASE
            WHEN ld.avg_queue_time_min > 5 THEN 95.0  -- Frequent queues indicate near-capacity
            WHEN ld.avg_queue_time_min > 2 THEN 85.0
            WHEN ld.avg_queue_time_min > 0.5 THEN 70.0
            ELSE 50.0
          END,
          2
        )
      WHEN ld.location_type = 'SHOVEL' THEN
        -- Shovel utilization estimated from queue patterns
        ROUND(
          CASE
            WHEN ld.avg_queue_time_min > 3 THEN 90.0
            WHEN ld.avg_queue_time_min > 1 THEN 75.0
            WHEN ld.avg_queue_time_min > 0.3 THEN 60.0
            ELSE 45.0
          END,
          2
        )
      ELSE 50.0
    END AS utilization_pct
    
  FROM location_details ld
),

bottleneck_identification AS (
  -- Identify bottlenecks using multiple criteria
  SELECT
    uc.*,
    
    -- Bottleneck score (0-100, higher = bigger bottleneck)
    ROUND(
      (uc.avg_queue_time_min / (SELECT MAX(avg_queue_time_min) FROM utilization_calc) * 40) +
      (uc.total_queue_hours / (SELECT MAX(total_queue_hours) FROM utilization_calc) * 30) +
      (uc.utilization_pct * 0.3),
      2
    ) AS bottleneck_score,
    
    -- Compare to system average
    ROUND(
      (uc.avg_queue_time_min / (SELECT AVG(avg_queue_time_min) FROM utilization_calc WHERE avg_queue_time_min > 0) - 1) * 100,
      2
    ) AS queue_time_vs_avg_pct
    
  FROM utilization_calc uc
)

-- Final output with recommendations
SELECT
  location_type,
  location_id,
  ROUND(avg_queue_time_min, 2) AS avg_queue_time_min,
  ROUND(max_queue_time_min, 2) AS max_queue_time_min,
  ROUND(total_queue_hours, 2) AS total_queue_hours,
  utilization_pct,
  
  -- Bottleneck indicator
  CASE
    WHEN bottleneck_score > 70 THEN 'CRITICAL BOTTLENECK'
    WHEN bottleneck_score > 50 THEN 'BOTTLENECK'
    WHEN bottleneck_score > 30 THEN 'CONSTRAINT'
    ELSE 'NORMAL'
  END AS constraint_indicator,
  
  -- Actionable recommendations
  CASE
    WHEN location_type = 'CRUSHER' AND avg_queue_time_min > 5 THEN
      'URGENT: Add crusher capacity or optimize dump cycle times'
    WHEN location_type = 'CRUSHER' AND avg_queue_time_min > 2 THEN
      'Consider crusher capacity expansion or truck fleet rebalancing'
    WHEN location_type = 'SHOVEL' AND avg_queue_time_min > 3 THEN
      'Add shovel or redistribute truck assignments'
    WHEN location_type = 'SHOVEL' AND avg_queue_time_min > 1 THEN
      'Monitor shovel utilization - approaching capacity'
    WHEN utilization_pct > 85 THEN
      'High utilization - plan maintenance windows carefully'
    WHEN total_queue_hours > 100 THEN
      'Significant queue time impact - Investigate scheduling improvements'
    ELSE
      'Normal operations - Continue monitoring'
  END AS recommendation,
  
  bottleneck_score,
  queue_time_vs_avg_pct,
  total_queue_events,
  ROUND(avg_trucks_affected, 1) AS avg_trucks_affected,
  days_observed,
  
  -- Additional context fields
  bucket_size_m3,
  pit_zone,
  capacity_tph

FROM bottleneck_identification
ORDER BY bottleneck_score DESC, total_queue_hours DESC
