{{ config(materialized='table') }}

-- Secondary Fact Table: Alarm State Changes
-- Grain: One row per state transition
-- Captures all state changes for chattering detection and lifecycle analysis

WITH all_events AS (
  SELECT
    event_id,
    tag_id,
    event_timestamp,
    event_type AS to_state,
    LAG(event_type) OVER (PARTITION BY tag_id ORDER BY event_timestamp) AS from_state,
    LAG(event_timestamp) OVER (PARTITION BY tag_id ORDER BY event_timestamp) AS prev_timestamp,
    ROW_NUMBER() OVER (PARTITION BY tag_id ORDER BY event_timestamp) AS sequence_number
  FROM {{ ref "raw_alarm_events" }}
),

state_changes AS (
  SELECT
    ae.event_id AS state_change_key,
    ae.tag_id,
    ae.from_state,
    ae.to_state,
    ae.event_timestamp AS change_timestamp,
    ae.sequence_number,
    ae.prev_timestamp,
    
    -- Calculate time since last change
    CASE 
      WHEN ae.prev_timestamp IS NOT NULL THEN
        CAST((JULIANDAY(ae.event_timestamp) - JULIANDAY(ae.prev_timestamp)) * 86400 AS INTEGER)
      ELSE NULL
    END AS time_since_last_change_sec
  FROM all_events ae
)

SELECT
  sc.state_change_key,
  
  -- Link to parent occurrence (only for ACTIVE starts or subsequent events)
  CASE 
    WHEN sc.to_state = 'ACTIVE' THEN 
      (SELECT f.occurrence_key 
       FROM {{ ref "fct_alarm_occurrence" }} f 
       WHERE f.occurrence_key = sc.state_change_key)
    WHEN sc.from_state IS NOT NULL THEN
      (SELECT f.occurrence_key 
       FROM {{ ref "fct_alarm_occurrence" }} f 
       WHERE f.alarm_id LIKE sc.tag_id || '_%'
         AND f.activation_timestamp <= sc.change_timestamp
       ORDER BY f.activation_timestamp DESC
       LIMIT 1)
    ELSE NULL
  END AS occurrence_key,
  
  dt.tag_key,
  sc.from_state,
  sc.to_state,
  sc.change_timestamp,
  sc.sequence_number,
  sc.time_since_last_change_sec

FROM state_changes sc

-- Join to alarm tag dimension
INNER JOIN {{ ref "dim_alarm_tag" }} dt 
  ON sc.tag_id = dt.tag_id 
  AND dt.is_current = 1

ORDER BY sc.tag_id, sc.sequence_number
