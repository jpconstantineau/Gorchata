{{ config "materialized" "view" }}

-- Operator Performance Analysis
-- Business Question: How do different operators perform and who needs training?
-- 
-- This query ranks operators by efficiency metrics including cycle time,
-- payload utilization, and delay management. Use this to:
-- - Identify training needs
-- - Recognize top performers
-- - Optimize operator-truck assignments
-- - Improve safety and productivity

WITH operator_cycle_metrics AS (
  -- Aggregate cycle metrics by operator
  SELECT
    f.operator_id,
    o.experience_level,
    
    -- Cycle counts
    COUNT(f.cycle_id) AS total_cycles,
    COUNT(DISTINCT f.truck_id) AS trucks_operated,
    COUNT(DISTINCT f.date_id) AS days_worked,
    
    -- Time efficiency
    AVG((julianday(f.cycle_end) - julianday(f.cycle_start)) * 24 * 60) AS avg_cycle_time_min,
    
    -- Payload utilization
    AVG((f.payload_tons / t.payload_capacity_tons) * 100) AS avg_payload_utilization_pct,
    
    -- Queue behavior (indicator of operator anticipation/planning)
    AVG(f.duration_queue_crusher_min + f.duration_queue_shovel_min) AS avg_queue_time_min,
    
    -- Spot delays (indicator of operational issues or behavior)
    SUM(f.duration_spot_delays_min) / 60.0 AS total_spot_delay_hours,
    SUM(CASE WHEN f.duration_spot_delays_min > 0 THEN 1 ELSE 0 END) AS cycles_with_delays,
    
    -- Production
    SUM(f.payload_tons) AS total_tons_moved,
    
    -- Operating time
    SUM((julianday(f.cycle_end) - julianday(f.cycle_start)) * 24) AS total_operating_hours
    
  FROM {{ ref "fact_haul_cycle" }} f
  INNER JOIN {{ ref "dim_operator" }} o ON f.operator_id = o.operator_id
  INNER JOIN {{ ref "dim_truck" }} t ON f.truck_id = t.truck_id
  GROUP BY f.operator_id, o.experience_level
),

operator_efficiency_calc AS (
  -- Calculate efficiency metrics
  SELECT
    operator_id,
    experience_level,
    total_cycles,
    trucks_operated,
    days_worked,
    ROUND(avg_cycle_time_min, 2) AS avg_cycle_time_min,
    ROUND(avg_payload_utilization_pct, 2) AS avg_payload_utilization_pct,
    ROUND(avg_queue_time_min, 2) AS avg_queue_time_min,
    ROUND(total_spot_delay_hours, 2) AS total_spot_delay_hours,
    
    -- Spot delay frequency (% of cycles with delays)
    ROUND(
      (cycles_with_delays * 100.0) / NULLIF(total_cycles, 0),
      2
    ) AS spot_delay_frequency_pct,
    
    -- Tons per hour (productivity metric)
    ROUND(
      total_tons_moved / NULLIF(total_operating_hours, 0),
      2
    ) AS tons_per_hour,
    
    -- Cycles per day
    ROUND(
      total_cycles / NULLIF(days_worked, 0),
      2
    ) AS avg_cycles_per_day,
    
    total_tons_moved
    
  FROM operator_cycle_metrics
),

experience_benchmarks AS (
  -- Calculate benchmarks by experience level
  SELECT
    experience_level,
    AVG(avg_cycle_time_min) AS exp_level_avg_cycle_time,
    AVG(avg_payload_utilization_pct) AS exp_level_avg_utilization,
    AVG(tons_per_hour) AS exp_level_avg_tons_per_hour
  FROM operator_efficiency_calc
  GROUP BY experience_level
),

operator_ranking AS (
  -- Rank operators and compare to peers
  SELECT
    oec.*,
    
    -- Performance rank (1 = best overall)
    ROW_NUMBER() OVER (ORDER BY oec.tons_per_hour DESC, oec.avg_cycle_time_min ASC) AS performance_rank,
    
    -- Rank within experience level
    ROW_NUMBER() OVER (
      PARTITION BY oec.experience_level
      ORDER BY oec.tons_per_hour DESC, oec.avg_cycle_time_min ASC
    ) AS rank_within_experience_level,
    
    -- Efficiency score (0-100, higher is better)
    ROUND(
      -- Productivity component (40%)
      ((oec.tons_per_hour / (SELECT MAX(tons_per_hour) FROM operator_efficiency_calc)) * 40) +
      -- Payload utilization component (30%)
      ((oec.avg_payload_utilization_pct / 105.0) * 30) +
      -- Cycle time efficiency component (20%)
      ((1 - (oec.avg_cycle_time_min / (SELECT MAX(avg_cycle_time_min) FROM operator_efficiency_calc))) * 20) +
      -- Delay management component (10%)
      ((1 - (oec.spot_delay_frequency_pct / 100.0)) * 10),
      2
    ) AS efficiency_score,
    
    -- Compare to experience level peers
    ROUND(
      (oec.avg_cycle_time_min / NULLIF(eb.exp_level_avg_cycle_time, 0) - 1) * 100,
      2
    ) AS cycle_time_vs_peers_pct,
    
    ROUND(
      (oec.avg_payload_utilization_pct / NULLIF(eb.exp_level_avg_utilization, 0) - 1) * 100,
      2
    ) AS utilization_vs_peers_pct
    
  FROM operator_efficiency_calc oec
  INNER JOIN experience_benchmarks eb ON oec.experience_level = eb.experience_level
)

-- Final output with performance assessment
SELECT
  operator_id,
  experience_level,
  total_cycles,
  avg_cycle_time_min,
  avg_payload_utilization_pct,
  avg_queue_time_min,
  total_spot_delay_hours,
  spot_delay_frequency_pct,
  performance_rank,
  efficiency_score,
  tons_per_hour,
  
  -- Performance assessment
  CASE
    -- Top performers
    WHEN performance_rank <= 3 AND efficiency_score >= 75 THEN
      'TOP PERFORMER - Excellent productivity (' || CAST(tons_per_hour AS TEXT) || ' tph) and efficiency. ' ||
      'Use as training benchmark and mentor for junior operators'
      
    -- Good performers
    WHEN efficiency_score >= 70 AND avg_payload_utilization_pct >= 92 THEN
      'HIGH PERFORMER - Strong productivity and good payload management. ' ||
      'Maintain current performance standards'
      
    -- Experience level concerns
    WHEN experience_level = 'Senior' AND efficiency_score < 60 THEN
      'BELOW EXPECTATIONS - Senior operator underperforming. ' ||
      'Review for equipment issues, health concerns, or retraining needs. ' ||
      'Cycle time: ' || CAST(avg_cycle_time_min AS TEXT) || ' min (vs peers: ' || CAST(cycle_time_vs_peers_pct AS TEXT) || '%)'
      
    WHEN experience_level = 'Junior' AND efficiency_score >= 65 THEN
      'PROMISING JUNIOR - Performing well for experience level. ' ||
      'Continue development and mentoring'
      
    -- Specific improvement areas
    WHEN avg_payload_utilization_pct < 88 THEN
      'TRAINING NEEDED - Low payload utilization (' || CAST(avg_payload_utilization_pct AS TEXT) || '%). ' ||
      'Focus on loading coordination and bucket pass counting'
      
    WHEN spot_delay_frequency_pct > 25 THEN
      'RELIABILITY CONCERN - High delay frequency (' || CAST(spot_delay_frequency_pct AS TEXT) || '%). ' ||
      'Investigate causes: equipment issues, work practices, or safety behaviors'
      
    WHEN avg_cycle_time_min > 65 THEN
      'EFFICIENCY OPPORTUNITY - Long cycle times (' || CAST(avg_cycle_time_min AS TEXT) || ' min). ' ||
      'Review route planning, speed management, and loading/dumping procedures'
      
    -- Average performers
    WHEN efficiency_score >= 55 AND efficiency_score < 70 THEN
      'AVERAGE PERFORMANCE - Meeting basic standards. ' ||
      'Identify specific improvement opportunities through coaching'
      
    -- Needs improvement
    ELSE
      'NEEDS IMPROVEMENT - Performance below standards. ' ||
      'Immediate training intervention recommended. ' ||
      'Focus areas: cycle time (' || CAST(avg_cycle_time_min AS TEXT) || ' min), ' ||
      'utilization (' || CAST(avg_payload_utilization_pct AS TEXT) || '%)'
  END AS performance_assessment,
  
  -- Additional context
  trucks_operated,
  days_worked,
  avg_cycles_per_day,
  rank_within_experience_level,
  cycle_time_vs_peers_pct,
  utilization_vs_peers_pct,
  ROUND(total_tons_moved, 2) AS total_tons_moved

FROM operator_ranking
ORDER BY performance_rank ASC, efficiency_score DESC
