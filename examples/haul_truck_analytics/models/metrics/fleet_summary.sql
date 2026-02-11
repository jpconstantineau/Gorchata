{{ config "materialized" "table" }}

-- Fleet Summary Metrics
-- Fleet-wide aggregated metrics by shift and date
-- Grain: one row per shift per date
-- Metrics: tons moved, cycles, cycle time, fuel, spot delays, payload distribution

WITH cycle_metrics AS (
  -- Extract cycle metrics with payload utilization bands
  SELECT
    fc.date_id,
    fc.shift_id,
    fc.truck_id,
    fc.payload_tons,
    -- Calculate cycle duration
    (julianday(fc.cycle_end) - julianday(fc.cycle_start)) * 24 * 60 AS cycle_duration_min,
    fc.duration_spot_delays_min,
    fc.fuel_consumed_liters,
    -- Get truck capacity for utilization calculation
    dt.payload_capacity_tons,
    -- Calculate payload utilization percentage  
    (fc.payload_tons / dt.payload_capacity_tons) * 100 AS payload_utilization_pct
  FROM {{ ref "fact_haul_cycle" }} fc
  INNER JOIN {{ ref "dim_truck" }} dt
    ON fc.truck_id = dt.truck_id
),

payload_bands AS (
  -- Classify cycles into payload utilization bands
  SELECT
    date_id,
    shift_id,
    truck_id,
    cycle_duration_min,
    duration_spot_delays_min,
    fuel_consumed_liters,
    payload_tons,
    payload_utilization_pct,
    -- Payload utilization bands
    CASE
      WHEN payload_utilization_pct < 85 THEN 'underload'
      WHEN payload_utilization_pct >= 85 AND payload_utilization_pct < 95 THEN 'suboptimal'
      WHEN payload_utilization_pct >= 95 AND payload_utilization_pct <= 105 THEN 'optimal'
      WHEN payload_utilization_pct > 105 THEN 'overload'
      ELSE 'unknown'
    END AS payload_band
  FROM cycle_metrics
),

fleet_aggregation AS (
  -- Aggregate fleet-wide metrics
  SELECT
    date_id,
    shift_id,
    -- Total tons moved
    SUM(payload_tons) AS total_tons_moved,
    -- Total cycles completed
    COUNT(*) AS total_cycles_completed,
    -- Fleet average cycle time
    AVG(cycle_duration_min) AS fleet_avg_cycle_time_min,
    -- Fleet utilization (operating hours vs available)
    -- Assume max 12 trucks * 12 hours = 144 truck-hours available per shift
    (SUM(cycle_duration_min) / 60.0) / 144.0 * 100 AS fleet_utilization_pct,
    -- Total fuel consumed
    SUM(fuel_consumed_liters) AS total_fuel_consumed_liters,
    -- Average payload utilization
    AVG(payload_utilization_pct) AS avg_payload_utilization_pct,
    -- Total spot delay hours
    SUM(duration_spot_delays_min) / 60.0 AS total_spot_delay_hours,
    -- Payload distribution counts
    SUM(CASE WHEN payload_band = 'underload' THEN 1 ELSE 0 END) AS cycles_underload,
    SUM(CASE WHEN payload_band = 'suboptimal' THEN 1 ELSE 0 END) AS cycles_suboptimal,
    SUM(CASE WHEN payload_band = 'optimal' THEN 1 ELSE 0 END) AS cycles_optimal,
    SUM(CASE WHEN payload_band = 'overload' THEN 1 ELSE 0 END) AS cycles_overload
  FROM payload_bands
  GROUP BY date_id, shift_id
),

bottleneck_indicator AS (
  -- Determine bottleneck: shovel vs crusher based on queue times
  SELECT
    fc.date_id,
    fc.shift_id,
    AVG(fc.duration_queue_crusher_min) AS avg_crusher_queue,
    AVG(fc.duration_queue_shovel_min) AS avg_shovel_queue,
    CASE
      WHEN AVG(fc.duration_queue_crusher_min) > AVG(fc.duration_queue_shovel_min) THEN 'CRUSHER'
      WHEN AVG(fc.duration_queue_shovel_min) > AVG(fc.duration_queue_crusher_min) THEN 'SHOVEL'
      ELSE 'BALANCED'
    END AS bottleneck_indicator
  FROM {{ ref "fact_haul_cycle" }} fc
  GROUP BY fc.date_id, fc.shift_id
)

-- Final fleet summary output
SELECT
  fa.date_id,
  fa.shift_id,
  fa.total_tons_moved,
  fa.total_cycles_completed,
  fa.fleet_avg_cycle_time_min,
  fa.fleet_utilization_pct,
  fa.total_fuel_consumed_liters,
  fa.avg_payload_utilization_pct,
  fa.total_spot_delay_hours,
  -- Payload distribution
  fa.cycles_underload,
  fa.cycles_suboptimal,
  fa.cycles_optimal,
  fa.cycles_overload,
  -- Bottleneck identification
  bi.bottleneck_indicator
FROM fleet_aggregation fa
LEFT JOIN bottleneck_indicator bi
  ON fa.date_id = bi.date_id
 AND fa.shift_id = bi.shift_id
ORDER BY fa.date_id, fa.shift_id
