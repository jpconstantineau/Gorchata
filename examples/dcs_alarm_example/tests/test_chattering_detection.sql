-- Test: Chattering Alarm Detection per ISA 18.2
-- Purpose: Detect alarms with rapid state changes (chattering/fleeting alarms)
--          ISA 18.2 defines chattering as alarms activating multiple times
--          in a short period (typically >5 activations per 10 minutes)
--          These indicate nuisance alarms requiring rationalization

WITH alarm_activations AS (
  SELECT
    tag_id,
    event_timestamp,
    -- Count activations in 10-minute rolling window
    COUNT(*) OVER (
      PARTITION BY tag_id
      ORDER BY event_timestamp
      RANGE BETWEEN INTERVAL '10 minutes' PRECEDING AND CURRENT ROW
    ) AS activations_in_10_min,
    -- Get next activation time to calculate cycling frequency
    LEAD(event_timestamp) OVER (PARTITION BY tag_id ORDER BY event_timestamp) AS next_activation
  FROM fct_alarm_state_change
  WHERE event_type = 'ACTIVE'
),

chattering_alarms AS (
  SELECT
    tag_id,
    event_timestamp,
    activations_in_10_min,
    -- Calculate seconds between activations
    CASE 
      WHEN next_activation IS NOT NULL THEN
        CAST((julianday(next_activation) - julianday(event_timestamp)) * 86400 AS INTEGER)
      ELSE NULL
    END AS seconds_to_next_activation
  FROM alarm_activations
  WHERE 
    -- ISA 18.2 chattering threshold: >5 activations per 10 minutes
    activations_in_10_min > 5
)

SELECT
  dt.tag_id,
  dt.tag_name,
  dt.alarm_type,
  de.equipment_id,
  de.equipment_type,
  dp.priority_code,
  ca.event_timestamp,
  ca.activations_in_10_min,
  ca.seconds_to_next_activation,
  CASE 
    WHEN ca.activations_in_10_min > 10 THEN 'CRITICAL (>10 activations/10min)'
    WHEN ca.activations_in_10_min > 5 THEN 'WARNING (>5 activations/10min)'
    ELSE 'OK'
  END AS isa_18_2_severity,
  'Chattering alarm - requires rationalization' AS recommendation
FROM chattering_alarms ca
INNER JOIN dim_alarm_tag dt ON ca.tag_id = dt.tag_id
INNER JOIN fct_alarm_occurrence f ON ca.tag_id = dt.tag_id
INNER JOIN dim_equipment de ON f.equipment_key = de.equipment_key
INNER JOIN dim_priority dp ON f.priority_key = dp.priority_key
GROUP BY dt.tag_id, dt.tag_name, dt.alarm_type, de.equipment_id, de.equipment_type, 
         dp.priority_code, ca.event_timestamp, ca.activations_in_10_min, 
         ca.seconds_to_next_activation
ORDER BY ca.activations_in_10_min DESC, ca.event_timestamp
