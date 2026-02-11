{{ config "materialized" "table" }}

-- Shovel Utilization Metrics
-- Aggregates loading state data to calculate shovel utilization and productivity
-- Grain: one row per shovel per shift per date
-- Metrics: loading time, idle time, utilization percentage, loads completed, tons loaded

WITH loading_states AS (
  -- Extract loading states for each shovel
  SELECT
    CASE
      WHEN location_zone = 'Shovel_A' THEN 'SHOVEL_A'
      WHEN location_zone = 'Shovel_B' THEN 'SHOVEL_B'
      WHEN location_zone = 'Shovel_C' THEN 'SHOVEL_C'
      ELSE 'UNKNOWN'
    END AS shovel_id,
    state_start,
    state_end,
    -- Calculate loading duration in minutes
    (julianday(state_end) - julianday(state_start)) * 24 * 60 AS loading_duration_min,
    payload_at_end AS tons_loaded
  FROM {{ ref "stg_truck_states" }}
  WHERE operational_state = 'loading'
    AND location_zone IN ('Shovel_A', 'Shovel_B', 'Shovel_C')
),

loading_with_shift AS (
  -- Assign shift to each loading event
  SELECT
    shovel_id,
    state_start,
    state_end,
    loading_duration_min,
    tons_loaded,
    -- Determine shift based on time of day
    CASE
      WHEN CAST(strftime('%H', state_start) AS INTEGER) >= 7
       AND CAST(strftime('%H', state_start) AS INTEGER) < 19
      THEN 'SHIFT_DAY'
      ELSE 'SHIFT_NIGHT'
    END AS shift_id,
    -- Extract date
    CAST(strftime('%Y%m%d', state_start) AS INTEGER) AS date_id
  FROM loading_states
  WHERE shovel_id != 'UNKNOWN'
),

shift_duration AS (
  -- Define shift duration (12 hours = 720 minutes)
  SELECT 720 AS shift_minutes
)

-- Aggregate shovel metrics by shovel, date, and shift
SELECT
  ls.shovel_id,
  ls.date_id,
  ls.shift_id,
  -- Total loading time in hours
  SUM(ls.loading_duration_min) / 60.0 AS total_loading_time_hours,
  -- Total idle time in hours (shift duration - loading time)
  (sd.shift_minutes - SUM(ls.loading_duration_min)) / 60.0 AS total_idle_time_hours,
  -- Truck loads completed
  COUNT(*) AS truck_loads_completed,
  -- Average loading duration per load
  AVG(ls.loading_duration_min) AS avg_loading_duration_min,
  -- Utilization percentage (loading time / available time * 100)
  (SUM(ls.loading_duration_min) / sd.shift_minutes) * 100 AS utilization_pct,
  -- Total tons loaded
  SUM(ls.tons_loaded) AS tons_loaded
FROM loading_with_shift ls
CROSS JOIN shift_duration sd
GROUP BY ls.shovel_id, ls.date_id, ls.shift_id
ORDER BY ls.shovel_id, ls.date_id, ls.shift_id
