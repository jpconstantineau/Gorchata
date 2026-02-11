{{ config "materialized" "view" }}

-- Payload Compliance Analysis
-- Business Question: Are trucks being loaded properly?
-- 
-- This query analyzes payload compliance tracking underload (<85%), optimal (95-105%),
-- and overload (>105%) patterns by truck and operator. Use this to:
-- - Identify operator training needs
-- - Optimize truck-shovel matching
-- - Reduce tire wear and fuel waste
-- - Improve safety and equipment longevity

WITH cycle_payload_analysis AS (
  -- Classify each cycle by payload compliance
  SELECT
    f.truck_id,
    f.operator_id,
    t.payload_capacity_tons,
    f.payload_tons,
    
    -- Calculate payload utilization percentage
    (f.payload_tons / t.payload_capacity_tons * 100) AS payload_utilization_pct,
    
    -- Classify payload compliance
    CASE
      WHEN (f.payload_tons / t.payload_capacity_tons * 100) < 85 THEN 'UNDERLOAD'
      WHEN (f.payload_tons / t.payload_capacity_tons * 100) >= 95 AND (f.payload_tons / t.payload_capacity_tons * 100) <= 105 THEN 'OPTIMAL'
      WHEN (f.payload_tons / t.payload_capacity_tons * 100) > 105 THEN 'OVERLOAD'
      ELSE 'ACCEPTABLE'
    END AS compliance_category
    
  FROM {{ ref "fact_haul_cycle" }} f
  INNER JOIN {{ ref "dim_truck" }} t ON f.truck_id = t.truck_id
),

compliance_summary AS (
  -- Aggregate compliance by truck and operator
  SELECT
    truck_id,
    operator_id,
    
    -- Cycle counts
    COUNT(*) AS total_cycles,
    SUM(CASE WHEN compliance_category = 'UNDERLOAD' THEN 1 ELSE 0 END) AS underload_cycles,
    SUM(CASE WHEN compliance_category = 'OPTIMAL' THEN 1 ELSE 0 END) AS optimal_cycles,
    SUM(CASE WHEN compliance_category = 'OVERLOAD' THEN 1 ELSE 0 END) AS overload_cycles,
    SUM(CASE WHEN compliance_category = 'ACCEPTABLE' THEN 1 ELSE 0 END) AS acceptable_cycles,
    
    -- Percentage calculations
    ROUND(SUM(CASE WHEN compliance_category = 'UNDERLOAD' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS underload_pct,
    ROUND(SUM(CASE WHEN compliance_category = 'OPTIMAL' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS optimal_pct,
    ROUND(SUM(CASE WHEN compliance_category = 'OVERLOAD' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS overload_pct,
    
    -- Average utilization
    ROUND(AVG(payload_utilization_pct), 2) AS avg_payload_utilization_pct,
    ROUND(MIN(payload_utilization_pct), 2) AS min_payload_utilization_pct,
    ROUND(MAX(payload_utilization_pct), 2) AS max_payload_utilization_pct
    
  FROM cycle_payload_analysis
  GROUP BY truck_id, operator_id
),

compliance_scoring AS (
  -- Calculate compliance scores and trends
  SELECT
    cs.*,
    o.experience_level,
    
    -- Compliance score (0-100, higher is better)
    -- Optimal cycles weighted highest, acceptable acceptable, violations penalized
    ROUND(
      (optimal_pct * 1.0) +
      ((100 - underload_pct - overload_pct) * 0.5) -
      (overload_pct * 2.0) -  -- Overloads penalized heavily (safety/equipment)
      (underload_pct * 1.0),  -- Underloads penalized (productivity)
      2
    ) AS compliance_score
    
  FROM compliance_summary cs
  INNER JOIN {{ ref "dim_operator" }} o ON cs.operator_id = o.operator_id
)

-- Final output with violation patterns and trends
SELECT
  truck_id,
  operator_id,
  experience_level,
  total_cycles,
  underload_cycles,
  optimal_cycles,
  overload_cycles,
  acceptable_cycles,
  underload_pct,
  optimal_pct,
  overload_pct,
  compliance_score,
  avg_payload_utilization_pct,
  min_payload_utilization_pct,
  max_payload_utilization_pct,
  
  -- Violation trend assessment
  CASE
    WHEN overload_pct > 20 THEN 'SAFETY RISK - Frequent overloading'
    WHEN overload_pct > 10 THEN 'CONCERN - Overloading pattern detected'
    WHEN underload_pct > 30 THEN 'PRODUCTIVITY LOSS - Frequent underloading'
    WHEN underload_pct > 15 THEN 'INEFFICIENT - Review loading procedures'
    WHEN optimal_pct > 70 THEN 'EXCELLENT - Consistent optimal loading'
    WHEN optimal_pct > 50 THEN 'GOOD - Mostly optimal loading'
    WHEN compliance_score > 60 THEN 'ACCEPTABLE - Minor improvements needed'
    ELSE 'NEEDS IMPROVEMENT - Review operator training'
  END AS violation_trend,
  
  -- Actionable recommendations
  CASE
    WHEN overload_pct > 15 AND experience_level = 'Junior' THEN
      'Priority: Operator training on safe loading limits'
    WHEN overload_pct > 15 THEN
      'Check shovel operator coordination and bucket pass count'
    WHEN underload_pct > 25 AND experience_level = 'Senior' THEN
      'Investigate truck-shovel matching or equipment issues'
    WHEN underload_pct > 25 THEN
      'Operator training: Maximize payload per pass'
    WHEN optimal_pct > 80 THEN
      'Excellent performance - Use as training benchmark'
    WHEN avg_payload_utilization_pct < 85 THEN
      'Review loading procedures and shovel assignment'
    WHEN avg_payload_utilization_pct > 108 THEN
      'Reduce overload risk - Reinforce safety protocols'
    ELSE
      'Monitor and maintain current performance'
  END AS recommended_action

FROM compliance_scoring
ORDER BY compliance_score ASC, overload_pct DESC, underload_pct DESC
