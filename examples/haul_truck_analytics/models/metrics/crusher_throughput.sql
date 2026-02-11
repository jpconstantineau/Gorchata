{{ config "materialized" "table" }}

-- Crusher Throughput Metrics
-- Aggregates dumping activity to calculate crusher throughput and queue performance
-- Grain: one row per crusher per shift per date
-- Metrics: tons received, truck arrivals, throughput rate, dump duration, queue times

WITH crusher_cycles AS (
  -- Extract crusher-related metrics from fact table
  SELECT
    crusher_id,
    date_id,
    -- Determine shift based on cycle start time
    CASE
      WHEN CAST(strftime('%H', cycle_start) AS INTEGER) >= 7
       AND CAST(strftime('%H', cycle_start) AS INTEGER) < 19
      THEN 'SHIFT_DAY'
      ELSE 'SHIFT_NIGHT'
    END AS shift_id,
    payload_tons,
    duration_dumping_min,
    duration_queue_crusher_min
  FROM {{ ref "fact_haul_cycle" }}
),

shift_operating_hours AS (
  -- Calculate operating hours per shift (consider time span of cycles)
  SELECT
    crusher_id,
    date_id,
    shift_id,
    -- Assume 12-hour shift as baseline operating time
    12.0 AS operating_hours
  FROM crusher_cycles
  GROUP BY crusher_id, date_id, shift_id
)

-- Aggregate crusher metrics
SELECT
  cc.crusher_id,
  cc.date_id,
  cc.shift_id,
  -- Total tons received
  SUM(cc.payload_tons) AS tons_received,
  -- Truck arrivals (number of dumps)
  COUNT(*) AS truck_arrivals,
  -- Throughput rate (tons per hour)
  SUM(cc.payload_tons) / soh.operating_hours AS tons_per_hour,
  -- Average dump duration
  AVG(cc.duration_dumping_min) AS avg_dump_duration_min,
  -- Average queue time at crusher
  AVG(cc.duration_queue_crusher_min) AS avg_queue_time_min,
  -- Maximum queue time (peak congestion)
  MAX(cc.duration_queue_crusher_min) AS max_queue_time_min,
  -- Total queue hours lost
  SUM(cc.duration_queue_crusher_min) / 60.0 AS total_queue_hours,
  -- Utilization percentage (operating vs available time)
  (SUM(cc.duration_dumping_min) / (soh.operating_hours * 60)) * 100 AS utilization_pct
FROM crusher_cycles cc
INNER JOIN shift_operating_hours soh
  ON cc.crusher_id = soh.crusher_id
  AND cc.date_id = soh.date_id
  AND cc.shift_id = soh.shift_id
GROUP BY cc.crusher_id, cc.date_id, cc.shift_id, soh.operating_hours
ORDER BY cc.crusher_id, cc.date_id, cc.shift_id
