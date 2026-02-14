-- Shadow Yards Detection Aggregation
-- Detect subtle shadow yard patterns by aggregating dwell characteristics
{{ config "materialized" "table" }}

SELECT 
  location_id,
  COUNT(*) as total_dwell_events,
  SUM(CASE WHEN shadow_yard_flag = 1 THEN 1 ELSE 0 END) as shadow_yard_dwell_events,
  (CAST(SUM(CASE WHEN shadow_yard_flag = 1 THEN 1 ELSE 0 END) AS REAL) / COUNT(*)) * 100.0 as shadow_yard_percentage,
  AVG(dwell_duration_minutes) as avg_dwell_duration_minutes,
  -- Additional variance metrics for detection
  SQRT(AVG(dwell_duration_minutes * dwell_duration_minutes) - 
       AVG(dwell_duration_minutes) * AVG(dwell_duration_minutes)) as stddev_dwell_duration,
  MIN(dwell_duration_minutes) as min_dwell_duration_minutes,
  MAX(dwell_duration_minutes) as max_dwell_duration_minutes
FROM {{ ref "fact_dwell" }}
GROUP BY location_id
HAVING COUNT(*) > 0
ORDER BY shadow_yard_percentage DESC
