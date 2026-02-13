{{ config "materialized" "table" }}

-- Fact: Stop Classification
-- Purpose: Aggregated stop types per trip
-- Grain: One row per trip with stop type counts
-- Source: fact_trip joined to fact_dwell

WITH trip_data AS (
  SELECT
    trip_segment_id,
    railcar_id,
    trip_start_timestamp,
    trip_end_timestamp,
    psr_period
  FROM {{ ref "fact_trip" }}
),

-- Match dwells to trips (dwells occurring during trip time window)
trip_dwells AS (
  SELECT
    td.trip_segment_id,
    td.railcar_id,
    td.trip_start_timestamp,
    td.trip_end_timestamp,
    td.psr_period,
    fd.dwell_classification,
    fd.shadow_yard_flag,
    fd.dwell_duration_minutes
  FROM trip_data td
  LEFT JOIN {{ ref "fact_dwell" }} fd
    ON td.railcar_id = fd.railcar_id
    AND fd.dwell_start_timestamp >= td.trip_start_timestamp
    AND fd.dwell_end_timestamp <= td.trip_end_timestamp
)

-- Aggregate stop types per trip
SELECT
  trip_segment_id,
  railcar_id,
  trip_start_timestamp,
  trip_end_timestamp,
  COUNT(dwell_classification) AS total_stops,
  SUM(CASE WHEN shadow_yard_flag = 1 THEN 1 ELSE 0 END) AS shadow_yard_stops,
  SUM(CASE WHEN dwell_classification = 'crew_change' THEN 1 ELSE 0 END) AS crew_change_stops,
  SUM(CASE WHEN dwell_classification = 'terminal' THEN 1 ELSE 0 END) AS terminal_stops,
  SUM(CASE WHEN dwell_classification = 'mainline_hold' THEN 1 ELSE 0 END) AS mainline_stops,
  SUM(CASE WHEN dwell_classification = 'maintenance' THEN 1 ELSE 0 END) AS maintenance_stops,
  SUM(CASE WHEN dwell_classification = 'unclassified' THEN 1 ELSE 0 END) AS unclassified_stops,
  COALESCE(SUM(dwell_duration_minutes), 0) AS total_dwell_minutes,
  -- Normalize PSR period
  CASE
    WHEN psr_period = 'mature_psr' THEN 'mature'
    WHEN psr_period = 'transition_psr' THEN 'transition'
    WHEN psr_period = 'pre_psr' THEN 'pre-PSR'
    ELSE psr_period
  END AS psr_period
FROM trip_dwells
GROUP BY 
  trip_segment_id,
  railcar_id,
  trip_start_timestamp,
  trip_end_timestamp,
  psr_period
