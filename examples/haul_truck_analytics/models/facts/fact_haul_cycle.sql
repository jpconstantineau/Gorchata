{{ config "materialized" "table" }}

-- Fact Haul Cycle
-- Aggregated fact table for haul truck cycles
-- Grain: one row per complete haul cycle (loading → hauling → dumping → returning → next loading)
-- Captures complete cycle metrics including durations, distances, speeds, payload, and fuel consumption
-- Uses window functions (LEAD) for cycle boundary identification

WITH loading_states AS (
  -- Identify all loading states as potential cycle boundaries
  SELECT
    truck_id,
    state_start,
    state_end,
    location_zone,
    payload_at_end
  FROM {{ ref "stg_truck_states" }}
  WHERE operational_state = 'loading'
),

cycle_boundaries AS (
  -- Define cycle boundaries: from loading start to next loading start
  SELECT
    truck_id,
    state_start AS cycle_start,
    LEAD(state_start) OVER (PARTITION BY truck_id ORDER BY state_start) AS cycle_end,
    location_zone,
    payload_at_end AS payload_tons
  FROM loading_states
),

valid_cycles AS (
  -- Only include complete cycles (those that have an end)
  SELECT *
  FROM cycle_boundaries
  WHERE cycle_end IS NOT NULL
),

-- Aggregate all states within each cycle
cycle_states AS (
  SELECT
    vc.truck_id,
    vc.cycle_start,
    vc.cycle_end,
    vc.payload_tons,
    vc.location_zone AS shovel_zone,
    s.operational_state,
    s.state_start,
    s.state_end,
    s.location_zone,
    s.payload_at_start,
    s.payload_at_end,
    -- Calculate duration in minutes for each state
    (julianday(s.state_end) - julianday(s.state_start)) * 24 * 60 AS duration_minutes
  FROM valid_cycles vc
  INNER JOIN {{ ref "stg_truck_states" }} s
    ON vc.truck_id = s.truck_id
    AND s.state_start >= vc.cycle_start
    AND s.state_start < vc.cycle_end
),

-- Aggregate durations by state type within each cycle
cycle_durations AS (
  SELECT
    truck_id,
    cycle_start,
    cycle_end,
    payload_tons,
    shovel_zone,
    -- Sum durations by operational state
    SUM(CASE WHEN operational_state = 'loading' THEN duration_minutes ELSE 0 END) AS duration_loading_min,
    SUM(CASE WHEN operational_state = 'hauling_loaded' THEN duration_minutes ELSE 0 END) AS duration_hauling_loaded_min,
    SUM(CASE WHEN operational_state = 'queued_at_crusher' THEN duration_minutes ELSE 0 END) AS duration_queue_crusher_min,
    SUM(CASE WHEN operational_state = 'dumping' THEN duration_minutes ELSE 0 END) AS duration_dumping_min,
    SUM(CASE WHEN operational_state = 'returning_empty' THEN duration_minutes ELSE 0 END) AS duration_returning_min,
    SUM(CASE WHEN operational_state = 'queued_at_shovel' THEN duration_minutes ELSE 0 END) AS duration_queue_shovel_min,
    SUM(CASE WHEN operational_state = 'spot_delay' THEN duration_minutes ELSE 0 END) AS duration_spot_delays_min,
    -- Capture crusher zone from dumping state
    MAX(CASE WHEN operational_state = 'dumping' THEN location_zone END) AS crusher_zone
  FROM cycle_states
  GROUP BY truck_id, cycle_start, cycle_end, payload_tons, shovel_zone
),

-- Calculate GPS-based distances using haversine formula
telemetry_in_cycles AS (
  SELECT
    vc.truck_id,
    vc.cycle_start,
    vc.cycle_end,
    t.timestamp,
    t.gps_lat,
    t.gps_lon,
    t.speed_kmh,
    t.payload_tons,
    t.fuel_level_liters,
    -- Determine if loaded or empty based on payload (>80% = loaded, <20% = empty)
    CASE 
      WHEN t.payload_tons > (dt.payload_capacity_tons * 0.80) THEN 'loaded'
      WHEN t.payload_tons < (dt.payload_capacity_tons * 0.20) THEN 'empty'
      ELSE 'other'
    END AS load_state,
    -- Get previous telemetry point for distance/speed calculations
    LAG(t.gps_lat) OVER (PARTITION BY vc.truck_id, vc.cycle_start ORDER BY t.timestamp) AS prev_lat,
    LAG(t.gps_lon) OVER (PARTITION BY vc.truck_id, vc.cycle_start ORDER BY t.timestamp) AS prev_lon,
    LAG(t.fuel_level_liters) OVER (PARTITION BY vc.truck_id, vc.cycle_start ORDER BY t.timestamp) AS prev_fuel
  FROM valid_cycles vc
  INNER JOIN {{ ref "stg_telemetry_events" }} t
    ON vc.truck_id = t.truck_id
    AND t.timestamp >= vc.cycle_start
    AND t.timestamp < vc.cycle_end
  INNER JOIN {{ ref "dim_truck" }} dt
    ON t.truck_id = dt.truck_id
),

-- Calculate distances using haversine formula
telemetry_with_distances AS (
  SELECT
    truck_id,
    cycle_start,
    cycle_end,
    timestamp,
    speed_kmh,
    load_state,
    fuel_level_liters,
    prev_fuel,
    -- Haversine formula for distance in kilometers
    CASE
      WHEN prev_lat IS NOT NULL AND prev_lon IS NOT NULL THEN
        2 * 6371 * ASIN(SQRT(
          POWER(SIN(RADIANS(gps_lat - prev_lat) / 2), 2) +
          COS(RADIANS(prev_lat)) * COS(RADIANS(gps_lat)) *
          POWER(SIN(RADIANS(gps_lon - prev_lon) / 2), 2)
        ))
      ELSE 0
    END AS segment_distance_km
  FROM telemetry_in_cycles
),

-- Aggregate distances and speeds by load state
cycle_distances_speeds AS (
  SELECT
    truck_id,
    cycle_start,
    cycle_end,
    -- Sum distances by load state
    SUM(CASE WHEN load_state = 'loaded' THEN segment_distance_km ELSE 0 END) AS distance_loaded_km,
    SUM(CASE WHEN load_state = 'empty' THEN segment_distance_km ELSE 0 END) AS distance_empty_km,
    -- Calculate average speeds (excluding zeros/very low speeds)
    AVG(CASE WHEN load_state = 'loaded' AND speed_kmh > 5 THEN speed_kmh END) AS speed_avg_loaded_kmh,
    AVG(CASE WHEN load_state = 'empty' AND speed_kmh > 5 THEN speed_kmh END) AS speed_avg_empty_kmh,
    -- Calculate fuel consumption (start - end, excluding refueling events)
    MAX(fuel_level_liters) - MIN(fuel_level_liters) AS fuel_consumed_raw,
    -- Count fuel increases (refueling events to exclude)
    SUM(CASE WHEN fuel_level_liters > prev_fuel AND (fuel_level_liters - prev_fuel) > 10 THEN (fuel_level_liters - prev_fuel) ELSE 0 END) AS fuel_added
  FROM telemetry_with_distances
  GROUP BY truck_id, cycle_start, cycle_end
),

-- Calculate final fuel consumption (excluding refueling)
cycle_fuel AS (
  SELECT
    truck_id,
    cycle_start,
    cycle_end,
    distance_loaded_km,
    distance_empty_km,
    speed_avg_loaded_kmh,
    speed_avg_empty_kmh,
    -- Fuel consumed = raw consumption + fuel added (both are negative/positive appropriately)
    CASE 
      WHEN fuel_consumed_raw < 0 THEN ABS(fuel_consumed_raw) - fuel_added
      ELSE fuel_consumed_raw + fuel_added  
    END AS fuel_consumed_liters
  FROM cycle_distances_speeds
),

-- Map shovel zones to shovel IDs
shovel_mapping AS (
  SELECT 
    'Shovel_A' AS zone, 'SHOVEL_A' AS shovel_id UNION ALL
  SELECT 'Shovel_B', 'SHOVEL_B' UNION ALL
  SELECT 'Shovel_C', 'SHOVEL_C' UNION ALL
  SELECT 'North Pit', 'SHOVEL_A' UNION ALL
  SELECT 'South Pit', 'SHOVEL_B' UNION ALL
  SELECT 'East Pit', 'SHOVEL_C'
),

-- Map crusher zone to crusher ID
crusher_mapping AS (
  SELECT 'Crusher' AS zone, 'CRUSHER_1' AS crusher_id
),

-- Combine all metrics
cycle_metrics AS (
  SELECT
    cd.truck_id,
    cd.cycle_start,
    cd.cycle_end,
    cd.payload_tons,
    cd.shovel_zone,
    cd.crusher_zone,
    cd.duration_loading_min,
    cd.duration_hauling_loaded_min,
    cd.duration_queue_crusher_min,
    cd.duration_dumping_min,
    cd.duration_returning_min,
    cd.duration_queue_shovel_min,
    cd.duration_spot_delays_min,
    COALESCE(cf.distance_loaded_km, 0) AS distance_loaded_km,
    COALESCE(cf.distance_empty_km, 0) AS distance_empty_km,
    COALESCE(cf.speed_avg_loaded_kmh, 0) AS speed_avg_loaded_kmh,
    COALESCE(cf.speed_avg_empty_kmh, 0) AS speed_avg_empty_kmh,
    COALESCE(cf.fuel_consumed_liters, 0) AS fuel_consumed_liters,
    sm.shovel_id,
    cm.crusher_id
  FROM cycle_durations cd
  LEFT JOIN cycle_fuel cf
    ON cd.truck_id = cf.truck_id
    AND cd.cycle_start = cf.cycle_start
    AND cd.cycle_end = cf.cycle_end
  LEFT JOIN shovel_mapping sm
    ON cd.shovel_zone = sm.zone
  LEFT JOIN crusher_mapping cm
    ON cd.crusher_zone = cm.zone
)

-- Final fact table with dimension keys
SELECT
  -- Generate cycle_id
  cm.truck_id || '_' || cm.cycle_start AS cycle_id,
  -- Dimension foreign keys
  cm.truck_id,
  COALESCE(cm.shovel_id, 'SHOVEL_A') AS shovel_id,
  COALESCE(cm.crusher_id, 'CRUSHER_1') AS crusher_id,
  'OP_001' AS operator_id,  -- Simplified for testing; would join to operator assignment in production
  CASE 
    WHEN CAST(strftime('%H', cm.cycle_start) AS INTEGER) >= 7 
     AND CAST(strftime('%H', cm.cycle_start) AS INTEGER) < 19 
    THEN 'SHIFT_DAY'
    ELSE 'SHIFT_NIGHT'
  END AS shift_id,
  CAST(strftime('%Y%m%d', cm.cycle_start) AS INTEGER) AS date_id,
  -- Timestamps
  cm.cycle_start,
  cm.cycle_end,
  -- Payload
  cm.payload_tons,
  -- Distances
  cm.distance_loaded_km,
  cm.distance_empty_km,
  -- Durations
  cm.duration_loading_min,
  cm.duration_hauling_loaded_min,
  cm.duration_queue_crusher_min,
  cm.duration_dumping_min,
  cm.duration_returning_min,
  cm.duration_queue_shovel_min,
  cm.duration_spot_delays_min,
  -- Fuel
  cm.fuel_consumed_liters,
  -- Speeds
  cm.speed_avg_loaded_kmh,
  cm.speed_avg_empty_kmh
FROM cycle_metrics cm
ORDER BY cm.truck_id, cm.cycle_start
