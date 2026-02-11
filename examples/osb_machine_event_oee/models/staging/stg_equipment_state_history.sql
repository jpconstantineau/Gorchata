{{ config "materialized" "view" }}

-- Staging: Equipment State History
-- Transform discrete machine state events into continuous state periods
-- with calculated durations using window functions

WITH state_periods AS (
  SELECT 
    equipment_id,
    event_timestamp AS state_start_timestamp,
    -- Use LEAD to get the next event's timestamp as this state's end time
    LEAD(event_timestamp) OVER (PARTITION BY equipment_id ORDER BY event_timestamp) AS state_end_timestamp,
    state AS machine_state,
    reason_code_id
  FROM stg_machine_events
),

state_with_duration AS (
  SELECT 
    equipment_id,
    state_start_timestamp,
    -- For last event per equipment, use CURRENT_TIMESTAMP as end time
    COALESCE(state_end_timestamp, CURRENT_TIMESTAMP) AS state_end_timestamp,
    machine_state,
    reason_code_id,
    -- Calculate duration in minutes using julianday (round to nearest minute)
    ROUND((julianday(COALESCE(state_end_timestamp, CURRENT_TIMESTAMP)) - julianday(state_start_timestamp)) * 24 * 60) AS state_duration_min
  FROM state_periods
),

state_with_shift AS (
  SELECT 
    s.*,
    -- Assign shift based on time of day
    CASE
      WHEN CAST(strftime('%H:%M', s.state_start_timestamp) AS TEXT) >= '06:00' 
       AND CAST(strftime('%H:%M', s.state_start_timestamp) AS TEXT) < '14:00' THEN 'DAY'
      WHEN CAST(strftime('%H:%M', s.state_start_timestamp) AS TEXT) >= '14:00' 
       AND CAST(strftime('%H:%M', s.state_start_timestamp) AS TEXT) < '22:00' THEN 'SWING'
      ELSE 'NIGHT'
    END AS shift_id
  FROM state_with_duration s
),

state_with_date AS (
  SELECT 
    s.*,
    -- Assign date_id based on calendar date (format: YYYYMMDD)
    strftime('%Y%m%d', s.state_start_timestamp) AS date_id
  FROM state_with_shift s
)

SELECT
  equipment_id,
  state_start_timestamp,
  state_end_timestamp,
  state_duration_min,
  machine_state,
  reason_code_id,
  shift_id,
  date_id
FROM state_with_date
ORDER BY equipment_id, state_start_timestamp
