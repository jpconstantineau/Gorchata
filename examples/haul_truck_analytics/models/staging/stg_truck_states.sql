{{ config "materialized" "view" }}

-- Staging: Truck States
-- Detect operational states from raw telemetry using geofence zones, 
-- payload thresholds, speed patterns, and time-based rules

WITH telemetry_with_capacity AS (
  SELECT 
    t.*,
    dt.payload_capacity_tons,
    -- Calculate payload percentages for state detection
    (t.payload_tons / dt.payload_capacity_tons) * 100 AS payload_pct
  FROM stg_telemetry_events t
  INNER JOIN dim_truck dt ON t.truck_id = dt.truck_id
),

telemetry_with_changes AS (
  SELECT 
    *,
    -- Detect payload changes using window functions
    LAG(payload_tons) OVER (PARTITION BY truck_id ORDER BY timestamp) AS prev_payload,
    LAG(geofence_zone) OVER (PARTITION BY truck_id ORDER BY timestamp) AS prev_zone
  FROM telemetry_with_capacity
),

state_classification AS (
  SELECT
    truck_id,
    timestamp,
    geofence_zone,
    speed_kmh,
    payload_tons,
    payload_pct,
    payload_capacity_tons,
    prev_payload,
    -- Classify operational state based on business rules
    CASE
      -- Loading: in shovel zone, low speed, payload increasing
      WHEN geofence_zone IN ('Shovel_A', 'Shovel_B', 'Shovel_C')
        AND speed_kmh < 5
        AND payload_pct < 80
        AND (prev_payload IS NULL OR payload_tons > prev_payload)
        THEN 'loading'
      
      -- Queue at Shovel: in shovel zone, low speed, empty, not increasing
      WHEN geofence_zone IN ('Shovel_A', 'Shovel_B', 'Shovel_C')
        AND speed_kmh < 3
        AND payload_pct < 20
        THEN 'queued_at_shovel'
      
      -- Hauling Loaded: loaded (>80%), moving (>5 km/h), not in work zones
      WHEN payload_pct > 80
        AND speed_kmh > 5
        AND geofence_zone NOT IN ('Shovel_A', 'Shovel_B', 'Shovel_C', 'Crusher')
        THEN 'hauling_loaded'
      
      -- Queue at Crusher: in crusher zone, loaded, slow/stopped
      WHEN geofence_zone = 'Crusher'
        AND payload_pct > 80
        AND speed_kmh < 3
        THEN 'queued_at_crusher'
      
      -- Dumping: in crusher zone, payload dropping rapidly
      WHEN geofence_zone = 'Crusher'
        AND payload_pct < 80
        AND prev_payload IS NOT NULL 
        AND prev_payload > payload_capacity_tons * 0.80
        THEN 'dumping'
      
      -- Returning Empty: empty (<20%), moving (>5 km/h), not in work zones
      WHEN payload_pct < 20
        AND speed_kmh > 5
        AND geofence_zone NOT IN ('Shovel_A', 'Shovel_B', 'Shovel_C', 'Crusher')
        THEN 'returning_empty'
      
      -- Spot Delay: stopped for extended period outside work zones
      WHEN speed_kmh < 3
        AND geofence_zone NOT IN ('Shovel_A', 'Shovel_B', 'Shovel_C', 'Crusher')
        THEN 'spot_delay'
      
      -- Idle: anything else (low activity in work zones or transitions)
      ELSE 'idle'
    END AS operational_state,
    -- Add row number for grouping
    ROW_NUMBER() OVER (PARTITION BY truck_id ORDER BY timestamp) AS rn
  FROM telemetry_with_changes
),

state_with_prev AS (
  SELECT
    *,
    LAG(operational_state) OVER (PARTITION BY truck_id ORDER BY timestamp) AS prev_state
  FROM state_classification
),

state_groups AS (
  SELECT
    *,
    -- Create groups when state changes
    SUM(CASE WHEN prev_state = operational_state OR prev_state IS NULL THEN 0 ELSE 1 END) 
      OVER (PARTITION BY truck_id ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS state_group
  FROM state_with_prev
),

state_periods AS (
  SELECT
    truck_id,
    operational_state,
    geofence_zone AS location_zone,
    MIN(timestamp) AS state_start,
    MAX(timestamp) AS state_end,
    MIN(payload_tons) AS payload_at_start,
    MAX(payload_tons) AS payload_at_end
  FROM state_groups
  GROUP BY truck_id, operational_state, location_zone, state_group
)

SELECT
  truck_id,
  state_start,
  state_end,
  operational_state,
  location_zone,
  payload_at_start,
  payload_at_end
FROM state_periods
ORDER BY truck_id, state_start
