-- Test: Alarm Lifecycle State Transitions
-- Purpose: Verify alarm state transitions follow valid sequences per ISA 18.2
--          Valid sequences: ACTIVE -> ACKNOWLEDGED -> INACTIVE
--                          ACTIVE -> INACTIVE (unacknowledged clear)
--          Invalid: Any state appearing before ACTIVE
--                  Multiple ACTIVE states without INACTIVE in between

-- Find invalid state transitions
WITH alarm_states AS (
  SELECT
    tag_id,
    event_timestamp,
    event_type,
    LAG(event_type) OVER (PARTITION BY tag_id ORDER BY event_timestamp) AS prev_state,
    LAG(event_timestamp) OVER (PARTITION BY tag_id ORDER BY event_timestamp) AS prev_timestamp
  FROM fct_alarm_state_change
),

invalid_transitions AS (
  SELECT
    tag_id,
    event_timestamp,
    event_type AS current_state,
    prev_state,
    'Invalid transition: ' || COALESCE(prev_state, 'NULL') || ' -> ' || event_type AS violation_reason
  FROM alarm_states
  WHERE 
    -- ACKNOWLEDGED without preceding ACTIVE
    (event_type = 'ACKNOWLEDGED' AND prev_state != 'ACTIVE')
    -- INACTIVE without preceding ACTIVE or ACKNOWLEDGED
    OR (event_type = 'INACTIVE' AND prev_state NOT IN ('ACTIVE', 'ACKNOWLEDGED'))
    -- Duplicate ACTIVE without INACTIVE
    OR (event_type = 'ACTIVE' AND prev_state = 'ACTIVE')
    -- Any event without a previous ACTIVE
    OR (event_type IN ('ACKNOWLEDGED', 'INACTIVE') AND prev_state IS NULL)
)

SELECT
  tag_id,
  event_timestamp,
  current_state,
  prev_state,
  violation_reason
FROM invalid_transitions
ORDER BY tag_id, event_timestamp
