{{ config "materialized" "table" }}

-- Fact: Trip
-- Purpose: Grain-level fact table for car trips (loaded or empty)
-- Grain: One row per trip segment
-- Source: int_trip_segments with velocity and dimension joins

WITH trip_segments AS (
  SELECT
    trip_segment_id,
    railcar_id,
    train_id AS train_number,  -- This is the business key (e.g., 'T-M100')
    origin_location_id,
    origin_splc_code,
    destination_location_id,
    destination_splc_code,
    trip_start_timestamp,
    trip_end_timestamp,
    trip_duration_minutes,
    is_loaded_trip,
    psr_period
  FROM {{ ref "int_trip_segments" }}
),

-- Join to dim_train to get surrogate key
trips_with_train AS (
  SELECT
    ts.*,
    dt.train_id  -- Surrogate key from dimension
  FROM trip_segments ts
  LEFT JOIN dim_train dt ON ts.train_number = dt.train_number
),

-- Join velocity vectors for distance and speed metrics
trips_with_velocity AS (
  SELECT
    twt.*,
    vv.distance_miles,
    vv.velocity_mph
  FROM trips_with_train twt
  LEFT JOIN {{ ref "int_velocity_vectors" }} vv 
    ON twt.trip_segment_id = vv.trip_segment_id
),

-- Count dwells during each trip (dwells that occur within trip time window)
trip_dwell_counts AS (
  SELECT
    twt.trip_segment_id,
    COUNT(DISTINCT nd.dwell_id) AS dwell_count
  FROM trips_with_train twt
  LEFT JOIN {{ ref "int_nodal_dwell" }} nd
    ON twt.railcar_id = nd.railcar_id
    AND nd.dwell_start_timestamp >= twt.trip_start_timestamp
    AND nd.dwell_end_timestamp <= twt.trip_end_timestamp
  GROUP BY twt.trip_segment_id
),

-- Join to corridor dimension for corridor_id
trips_with_corridor AS (
  SELECT
    twv.*,
    dc.corridor_id
  FROM trips_with_velocity twv
  LEFT JOIN dim_corridor dc
    ON twv.origin_splc_code = dc.origin_splc
    AND twv.destination_splc_code = dc.destination_splc
)

-- Final fact table assembly with all dimensions and measures
SELECT
  twc.trip_segment_id,
  twc.railcar_id,
  twc.train_id,  -- Now the surrogate key from dim_train
  twc.corridor_id,
  twc.origin_location_id,
  twc.destination_location_id,
  -- Extract date key from timestamp for dim_date FK
  CAST(STRFTIME('%Y%m%d', twc.trip_start_timestamp) AS INTEGER) AS trip_start_date_id,
  twc.trip_start_timestamp,
  twc.trip_end_timestamp,
  twc.distance_miles,
  twc.trip_duration_minutes AS duration_minutes,
  twc.velocity_mph AS average_velocity_mph,
  CASE WHEN twc.is_loaded_trip = 1 THEN 'loaded' ELSE 'empty' END AS trip_type,
  COALESCE(tdc.dwell_count, 0) AS dwell_count,
  COALESCE(tdc.dwell_count, 0) AS stop_count,  -- Synonym for dwell_count
  -- Normalize PSR period: 'mature_psr' -> 'mature', 'transition_psr' -> 'transition', 'pre_psr' -> 'pre-PSR'
  CASE
    WHEN twc.psr_period = 'mature_psr' THEN 'mature'
    WHEN twc.psr_period = 'transition_psr' THEN 'transition'
    WHEN twc.psr_period = 'pre_psr' THEN 'pre-PSR'
    ELSE twc.psr_period  -- Fallback
  END AS psr_period
FROM trips_with_corridor twc
LEFT JOIN trip_dwell_counts tdc ON twc.trip_segment_id = tdc.trip_segment_id
