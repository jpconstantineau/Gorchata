-- Test: Standing Alarm Detection per ISA 18.2
-- Purpose: Detect alarms that remain active for more than 10 minutes
--          ISA 18.2 defines standing alarms as those active >10 minutes
--          These indicate potential issues with alarm rationalization
--          or equipment problems requiring attention

SELECT
  dt.tag_id,
  dt.tag_name,
  dt.alarm_type,
  de.equipment_id,
  de.equipment_type,
  dp.priority_code,
  f.activation_timestamp,
  f.inactive_timestamp,
  f.duration_seconds,
  ROUND(f.duration_seconds / 60.0, 2) AS duration_minutes,
  CASE 
    WHEN f.duration_seconds > 1800 THEN 'CRITICAL (>30 min)'
    WHEN f.duration_seconds > 600 THEN 'WARNING (>10 min)'
    ELSE 'OK'
  END AS isa_18_2_severity
FROM fct_alarm_occurrence f
INNER JOIN dim_alarm_tag dt ON f.tag_key = dt.tag_key
INNER JOIN dim_equipment de ON f.equipment_key = de.equipment_key
INNER JOIN dim_priority dp ON f.priority_key = dp.priority_key
WHERE 
  -- Standing alarm threshold: 10 minutes (600 seconds)
  f.duration_seconds > 600
  -- Still active alarms (no inactive timestamp)
  OR f.inactive_timestamp IS NULL
ORDER BY f.duration_seconds DESC
