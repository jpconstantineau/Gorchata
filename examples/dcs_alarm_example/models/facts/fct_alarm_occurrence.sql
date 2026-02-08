{{ config "materialized" "table" }}

-- Primary Fact Table: Alarm Occurrences
-- Grain: One row per alarm activation/lifecycle
-- Captures alarm lifecycle events with timestamps, durations, operator actions, and ISA 18.2 metrics

WITH active_events AS (
  SELECT
    event_id,
    tag_id,
    event_timestamp AS activation_timestamp,
    priority_code,
    alarm_value,
    setpoint_value,
    area_code
  FROM {{ ref "raw_alarm_events" }}
  WHERE event_type = 'ACTIVE'
),

acknowledged_events AS (
  SELECT
    tag_id,
    event_timestamp AS acknowledged_timestamp,
    operator_id,
    ROW_NUMBER() OVER (PARTITION BY tag_id ORDER BY event_timestamp) AS ack_seq
  FROM {{ ref "raw_alarm_events" }}
  WHERE event_type = 'ACKNOWLEDGED'
),

inactive_events AS (
  SELECT
    tag_id,
    event_timestamp AS inactive_timestamp,
    ROW_NUMBER() OVER (PARTITION BY tag_id ORDER BY event_timestamp) AS inactive_seq
  FROM {{ ref "raw_alarm_events" }}
  WHERE event_type = 'INACTIVE'
),

alarm_lifecycle AS (
  SELECT
    a.event_id,
    a.tag_id || '_' || a.activation_timestamp AS alarm_id,
    a.tag_id,
    a.activation_timestamp,
    ack.acknowledged_timestamp,
    ack.operator_id,
    inact.inactive_timestamp,
    a.priority_code,
    a.alarm_value,
    a.setpoint_value,
    a.area_code,
    ROW_NUMBER() OVER (PARTITION BY a.tag_id ORDER BY a.activation_timestamp) AS activation_seq
  FROM active_events a
  LEFT JOIN acknowledged_events ack 
    ON a.tag_id = ack.tag_id 
    AND ack.acknowledged_timestamp > a.activation_timestamp
    AND ack.ack_seq = (
      SELECT MIN(ack2.ack_seq)
      FROM acknowledged_events ack2
      WHERE ack2.tag_id = a.tag_id
        AND ack2.acknowledged_timestamp > a.activation_timestamp
    )
  LEFT JOIN inactive_events inact
    ON a.tag_id = inact.tag_id
    AND inact.inactive_timestamp > a.activation_timestamp
    AND inact.inactive_seq = (
      SELECT MIN(inact2.inactive_seq)
      FROM inactive_events inact2
      WHERE inact2.tag_id = a.tag_id
        AND inact2.inactive_timestamp > a.activation_timestamp
    )
)

SELECT
  lc.event_id AS occurrence_key,
  lc.alarm_id,
  dt.tag_key,
  de.equipment_key,
  da.area_key,
  dp.priority_key,
  CASE 
    WHEN lc.operator_id IS NOT NULL THEN dop.operator_key 
    ELSE NULL 
  END AS operator_key_ack,
  
  -- Timestamps
  lc.activation_timestamp,
  lc.acknowledged_timestamp,
  lc.inactive_timestamp,
  
  -- Date key for dimensional analysis
  CAST(strftime('%Y%m%d', lc.activation_timestamp) AS INTEGER) AS activation_date_key,
  
  -- Metrics
  lc.alarm_value,
  lc.setpoint_value,
  
  -- Duration calculations (in seconds)
  CASE 
    WHEN lc.acknowledged_timestamp IS NOT NULL THEN
      CAST((JULIANDAY(lc.acknowledged_timestamp) - JULIANDAY(lc.activation_timestamp)) * 86400 AS INTEGER)
    ELSE NULL
  END AS duration_to_ack_sec,
  
  CASE 
    WHEN lc.inactive_timestamp IS NOT NULL THEN
      CAST((JULIANDAY(lc.inactive_timestamp) - JULIANDAY(lc.activation_timestamp)) * 86400 AS INTEGER)
    ELSE NULL
  END AS duration_to_resolve_sec,
  
  -- ISA 18.2 flags (derived)
  CASE 
    WHEN lc.acknowledged_timestamp IS NULL THEN 1  -- Unacknowledged alarms are standing
    WHEN CAST((JULIANDAY(lc.acknowledged_timestamp) - JULIANDAY(lc.activation_timestamp)) * 86400 AS INTEGER) > 600 THEN 1
    ELSE 0 
  END AS is_standing_10min,
  
  CASE 
    WHEN lc.acknowledged_timestamp IS NOT NULL 
      AND CAST((JULIANDAY(lc.acknowledged_timestamp) - JULIANDAY(lc.activation_timestamp)) * 86400 AS INTEGER) > 86400 THEN 1
    ELSE 0
  END AS is_standing_24hr,
  
  CASE 
    WHEN lc.inactive_timestamp IS NOT NULL 
      AND CAST((JULIANDAY(lc.inactive_timestamp) - JULIANDAY(lc.activation_timestamp)) * 86400 AS INTEGER) < 2 THEN 1
    ELSE 0
  END AS is_fleeting,
  
  CASE 
    WHEN lc.acknowledged_timestamp IS NOT NULL THEN 1 
    ELSE 0 
  END AS is_acknowledged,
  
  CASE 
    WHEN lc.inactive_timestamp IS NOT NULL THEN 1 
    ELSE 0 
  END AS is_resolved

FROM alarm_lifecycle lc

-- Point-in-time join to alarm tag dimension (SCD Type 2)
INNER JOIN {{ ref "dim_alarm_tag" }} dt 
  ON lc.tag_id = dt.tag_id 
  AND dt.is_current = 1  -- Simplified: always use current version

-- Join to equipment dimension
INNER JOIN {{ ref "dim_equipment" }} de 
  ON dt.equipment_id = de.equipment_id

-- Join to process area dimension
INNER JOIN {{ ref "dim_process_area" }} da 
  ON lc.area_code = da.area_code

-- Join to priority dimension
INNER JOIN {{ ref "dim_priority" }} dp 
  ON lc.priority_code = dp.priority_code

-- Left join to operator dimension (nullable)
LEFT JOIN {{ ref "dim_operator" }} dop 
  ON lc.operator_id = dop.operator_id

ORDER BY lc.activation_timestamp
