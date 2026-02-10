-- Locomotive Power Transfer Efficiency Analysis
-- Business Question: How efficient are our locomotive power transfer operations?
-- 
-- This query analyzes locomotive repower patterns and power consistency
-- to optimize power management. Use this to:
-- - Improve locomotive utilization
-- - Reduce repower frequency where possible
-- - Identify corridors with power constraints

WITH power_transfer_metrics AS (
  -- Get power efficiency metrics by corridor
  SELECT
    p.corridor_id,
    c.corridor_name,
    c.origin_location,
    c.destination_location,
    p.year,
    p.week,
    
    -- Trip counts by power pattern
    p.total_trips,
    p.trips_with_repower,
    p.trips_same_power_both_ways,
    p.trips_different_power,
    
    -- Repower rates
    p.repower_rate,
    p.same_power_rate,
    p.different_power_rate,
    
    -- Power change frequency
    p.avg_power_changes_per_trip
    
  FROM {{ ref "agg_power_efficiency" }} p
  INNER JOIN {{ ref "dim_corridor" }} c ON p.corridor_id = c.corridor_id
),

corridor_power_summary AS (
  -- Aggregate power metrics across all weeks per corridor
  SELECT
    corridor_id,
    corridor_name,
    origin_location,
    destination_location,
    
    -- Overall trip counts
    SUM(total_trips) AS total_trips,
    SUM(trips_with_repower) AS total_trips_with_repower,
    SUM(trips_same_power_both_ways) AS total_trips_same_power,
    SUM(trips_different_power) AS total_trips_different_power,
    
    -- Average rates
    AVG(repower_rate) AS avg_repower_rate,
    AVG(same_power_rate) AS avg_same_power_rate,
    AVG(different_power_rate) AS avg_different_power_rate,
    
    -- Average power changes per trip
    AVG(avg_power_changes_per_trip) AS avg_power_changes_per_trip,
    
    -- Variability (standard deviation indicates consistency)
    ROUND(STDEV(repower_rate), 4) AS repower_rate_stddev,
    
    COUNT(DISTINCT week) AS weeks_observed
    
  FROM power_transfer_metrics
  GROUP BY 
    corridor_id, corridor_name, origin_location, destination_location
),

power_efficiency_rankings AS (
  -- Rank corridors by power efficiency
  SELECT
    *,
    
    -- Lower repower rate is better (more efficient)
    RANK() OVER (ORDER BY avg_repower_rate ASC) AS efficiency_rank,
    
    -- Higher same-power rate is better (more consistent)
    RANK() OVER (ORDER BY avg_same_power_rate DESC) AS consistency_rank,
    
    -- Categorize power management pattern
    CASE 
      WHEN avg_same_power_rate >= 0.7 THEN 'High Consistency'
      WHEN avg_same_power_rate >= 0.4 THEN 'Moderate Consistency'
      ELSE 'Low Consistency'
    END AS power_pattern,
    
    CASE 
      WHEN avg_repower_rate >= 0.3 THEN 'Frequent Repower'
      WHEN avg_repower_rate >= 0.1 THEN 'Moderate Repower'
      ELSE 'Minimal Repower'
    END AS repower_pattern
    
  FROM corridor_power_summary
)

-- Final results with actionable insights
SELECT
  efficiency_rank AS rank,
  corridor_id,
  corridor_name,
  origin_location || ' → ' || destination_location AS route,
  
  -- Trip counts
  total_trips,
  total_trips_with_repower,
  total_trips_same_power,
  total_trips_different_power,
  
  -- Power efficiency metrics (as percentages)
  ROUND(avg_repower_rate * 100, 2) AS repower_rate_pct,
  ROUND(avg_same_power_rate * 100, 2) AS same_power_both_ways_pct,
  ROUND(avg_different_power_rate * 100, 2) AS different_power_pct,
  
  -- Power changes per trip
  ROUND(avg_power_changes_per_trip, 2) AS avg_power_changes_per_trip,
  
  -- Consistency indicator
  ROUND(repower_rate_stddev * 100, 2) AS repower_rate_stddev_pct,
  
  -- Pattern classification
  power_pattern,
  repower_pattern,
  
  -- Rankings
  efficiency_rank,
  consistency_rank,
  
  -- Actionable recommendations
  CASE 
    WHEN avg_repower_rate > 0.25 AND avg_same_power_rate < 0.5 THEN 
      'High - Investigate frequent repowers; optimize power allocation'
    WHEN avg_repower_rate > 0.15 THEN 
      'Medium - Consider reducing repower frequency'
    WHEN repower_rate_stddev > 0.1 THEN
      'Medium - High variability; standardize power operations'
    ELSE 
      'Low - Power operations are efficient'
  END AS optimization_priority,
  
  -- Potential savings estimate (each repower takes ~2 hours @ $500/hour)
  CASE 
    WHEN avg_repower_rate > 0.25 THEN
      'Save ' || CAST(ROUND(total_trips_with_repower * 2 * 500 * 0.3, 0) AS TEXT) || 
      ' if reduce repower by 30%'
    ELSE 'Already efficient'
  END AS potential_savings,
  
  weeks_observed
  
FROM power_efficiency_rankings
ORDER BY efficiency_rank ASC;
