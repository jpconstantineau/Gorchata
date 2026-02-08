{{ config(materialized='table') }}

-- Chattering Alarms Detection
-- Identifies tags exhibiting rapid state cycling behavior (≥5 activations within 10 minutes)
-- Grain: One row per tag exhibiting chattering behavior

WITH state_transitions AS (
  -- Get all state changes with time since previous change
  SELECT 
    sc.tag_key,
    dt.area_code,
    sc.change_timestamp,
    sc.time_since_last_change_sec,
    ROW_NUMBER() OVER (PARTITION BY sc.tag_key ORDER BY sc.change_timestamp) AS seq_num
  FROM {{ ref "fct_alarm_state_change" }} sc
  INNER JOIN {{ ref "dim_alarm_tag" }} dt ON sc.tag_key = dt.tag_key AND dt.is_current = 1
  WHERE sc.time_since_last_change_sec IS NOT NULL
),

state_with_area AS (
  -- Add area_key from dim_process_area
  SELECT 
    st.tag_key,
    da.area_key,
    st.change_timestamp,
    st.time_since_last_change_sec,
    st.seq_num
  FROM state_transitions st
  INNER JOIN {{ ref "dim_process_area" }} da ON st.area_code = da.area_code
),

chattering_windows AS (
  -- Use window functions to identify chattering patterns
  -- Chattering = ≥5 state changes within 600 seconds (10 minutes)
  SELECT
    tag_key,
    area_key,
    change_timestamp,
    time_since_last_change_sec,
    seq_num,
    -- Count recent transitions in sliding window (5 transitions = current + 4 preceding)
    -- Note: Chattering is defined by rapid state cycling, not just activations
    COUNT(*) OVER (
      PARTITION BY tag_key 
      ORDER BY change_timestamp 
      ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS recent_5_changes,
    -- Sum time span of sliding window (time span of 5 transitions)
    SUM(time_since_last_change_sec) OVER (
      PARTITION BY tag_key 
      ORDER BY change_timestamp 
      ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS time_window_sec
  FROM state_with_area
),

chattering_flags AS (
  -- Flag transitions that are part of chattering pattern
  SELECT
    tag_key,
    area_key,
    change_timestamp,
    time_since_last_change_sec,
    seq_num,
    recent_5_changes,
    time_window_sec,
    CASE 
      WHEN recent_5_changes >= 5 AND time_window_sec <= 600 THEN 1
      ELSE 0
    END AS is_chattering,
    -- Detect start of new episode (previous row was not chattering, current is)
    LAG(CASE WHEN recent_5_changes >= 5 AND time_window_sec <= 600 THEN 1 ELSE 0 END, 1, 0) 
      OVER (PARTITION BY tag_key ORDER BY seq_num) AS prev_chattering
  FROM chattering_windows
),

episode_starts AS (
  -- Identify the start of each distinct chattering episode
  SELECT
    tag_key,
    area_key,
    change_timestamp,
    time_since_last_change_sec,
    is_chattering,
    CASE 
      WHEN is_chattering = 1 AND prev_chattering = 0 THEN 1
      ELSE 0
    END AS is_episode_start
  FROM chattering_flags
),

tag_metrics AS (
  -- Aggregate metrics per tag
  SELECT
    tag_key,
    area_key,
    SUM(is_episode_start) AS chattering_episode_count,  -- Count episode starts
    COUNT(*) AS total_state_changes,  -- Total state changes for this tag
    MIN(time_since_last_change_sec) AS min_cycle_time_sec,
    AVG(time_since_last_change_sec) AS avg_cycle_time_sec
  FROM episode_starts
  WHERE is_chattering = 1 OR is_episode_start = 1  -- Only include rows that were part of chattering
  GROUP BY tag_key, area_key
  HAVING chattering_episode_count > 0
),

hourly_rates AS (
  -- Calculate peak hourly activation rates for chattering tags
  SELECT 
    tag_key,
    MAX(activations_per_hour) AS max_activations_per_hour
  FROM (
    SELECT
      es.tag_key,
      CAST(strftime('%Y-%m-%d %H', es.change_timestamp) AS TEXT) AS hour_bucket,
      COUNT(*) AS activations_per_hour
    FROM episode_starts es
    INNER JOIN tag_metrics tm ON es.tag_key = tm.tag_key
    WHERE es.is_chattering = 1
    GROUP BY es.tag_key, hour_bucket
  )
  GROUP BY tag_key
)

SELECT
  ROW_NUMBER() OVER (ORDER BY tm.chattering_episode_count DESC) AS chattering_key,
  tm.tag_key,
  tm.area_key,
  tm.chattering_episode_count,
  tm.total_state_changes,
  CAST(COALESCE(hr.max_activations_per_hour, 0) AS REAL) AS max_activations_per_hour,
  CAST(tm.min_cycle_time_sec AS INTEGER) AS min_cycle_time_sec,
  CAST(tm.avg_cycle_time_sec AS INTEGER) AS avg_cycle_time_sec
FROM tag_metrics tm
LEFT JOIN hourly_rates hr ON tm.tag_key = hr.tag_key
ORDER BY tm.chattering_episode_count DESC
