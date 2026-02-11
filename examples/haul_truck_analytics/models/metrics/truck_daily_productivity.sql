{{ config "materialized" "table" }}

-- Truck Daily Productivity Metrics
-- Aggregates haul cycle data by truck and date to calculate daily productivity metrics
-- Grain: one row per truck per day
-- Metrics: tons moved, cycles completed, cycle time, payload utilization, spot delays

WITH cycle_metrics AS (
  -- Calculate basic cycle metrics from fact table
  SELECT
    truck_id,
    date_id,
    operator_id,
    payload_tons,
    -- Total cycle duration
    (julianday(cycle_end) - julianday(cycle_start)) * 24 * 60 AS cycle_duration_min,
    duration_spot_delays_min,
    distance_loaded_km,
    distance_empty_km,
    speed_avg_loaded_kmh,
    speed_avg_empty_kmh
  FROM {{ ref "fact_haul_cycle" }}
),

truck_capacity AS (
  -- Get truck capacity for payload utilization calculation
  SELECT
    truck_id,
    payload_capacity_tons
  FROM {{ ref "dim_truck" }}
),

daily_aggregation AS (
  -- Aggregate metrics by truck and day
  SELECT
    cm.truck_id,
    cm.date_id,
    cm.operator_id,
    -- Total tons moved
    SUM(cm.payload_tons) AS total_tons_moved,
    -- Cycles completed
    COUNT(*) AS cycles_completed,
    -- Average cycle time
    AVG(cm.cycle_duration_min) AS avg_cycle_time_min,
    -- Total operating hours (sum of cycle durations)
    SUM(cm.cycle_duration_min) / 60.0 AS total_operating_hours,
    -- Average payload utilization percentage
    AVG((cm.payload_tons / tc.payload_capacity_tons) * 100) AS avg_payload_utilization_pct,
    -- Total spot delay time
    SUM(cm.duration_spot_delays_min) AS total_spot_delay_min,
    -- Average distances
    AVG(cm.distance_loaded_km) AS avg_distance_loaded_km,
    AVG(cm.distance_empty_km) AS avg_distance_empty_km,
    -- Average speeds
    AVG(cm.speed_avg_loaded_kmh) AS avg_speed_loaded_kmh,
    AVG(cm.speed_avg_empty_kmh) AS avg_speed_empty_kmh
  FROM cycle_metrics cm
  INNER JOIN truck_capacity tc
    ON cm.truck_id = tc.truck_id
  GROUP BY cm.truck_id, cm.date_id, cm.operator_id
)

-- Final output with calculated tons per hour
SELECT
  truck_id,
  date_id,
  operator_id,
  total_tons_moved,
  cycles_completed,
  avg_cycle_time_min,
  -- Tons per hour (productivity metric)
  CASE
    WHEN total_operating_hours > 0 THEN total_tons_moved / total_operating_hours
    ELSE 0
  END AS tons_per_hour,
  avg_payload_utilization_pct,
  total_spot_delay_min,
  avg_distance_loaded_km,
  avg_distance_empty_km,
  avg_speed_loaded_kmh,
  avg_speed_empty_kmh
FROM daily_aggregation
ORDER BY truck_id, date_id
