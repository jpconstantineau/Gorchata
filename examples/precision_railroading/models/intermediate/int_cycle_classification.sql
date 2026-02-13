{{ config "materialized" "table" }}

-- Intermediate: Cycle Classification
-- Purpose: Pair loaded and empty trips into complete cycles
-- Grain: One row per complete cycle (loaded trip + empty return)
-- Source: int_trip_segments

WITH trip_segments AS (
  SELECT
    trip_segment_id,
    car_number,
    railcar_id,
    trip_start_timestamp,
    trip_end_timestamp,
    trip_duration_minutes,
    origin_location_id,
    origin_splc_code,
    destination_location_id,
    destination_splc_code,
    distance_miles,
    is_loaded_trip,
    train_id,
    psr_period
  FROM {{ ref "int_trip_segments" }}
),

-- Add sequence numbers and get next trip info
trip_sequence AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY car_number ORDER BY trip_start_timestamp) AS trip_seq,
    LEAD(trip_segment_id) OVER (PARTITION BY car_number ORDER BY trip_start_timestamp) AS next_trip_id,
    LEAD(is_loaded_trip) OVER (PARTITION BY car_number ORDER BY trip_start_timestamp) AS next_is_loaded,
    LEAD(origin_splc_code) OVER (PARTITION BY car_number ORDER BY trip_start_timestamp) AS next_origin_splc,
    LEAD(destination_splc_code) OVER (PARTITION BY car_number ORDER BY trip_start_timestamp) AS next_dest_splc,
    LEAD(trip_duration_minutes) OVER (PARTITION BY car_number ORDER BY trip_start_timestamp) AS next_duration_minutes,
    LEAD(trip_end_timestamp) OVER (PARTITION BY car_number ORDER BY trip_start_timestamp) AS next_end_timestamp,
    LEAD(distance_miles) OVER (PARTITION BY car_number ORDER BY trip_start_timestamp) AS next_distance_miles
  FROM trip_segments
),

-- Match loaded trips with their following empty return
-- OR empty trips with their following loaded trip (both patterns valid)
cycle_pairs AS (
  SELECT
    car_number,
    railcar_id,
    
    -- For loaded → empty pattern
    CASE 
      WHEN is_loaded_trip = 1 AND next_is_loaded = 0 THEN trip_start_timestamp
      WHEN is_loaded_trip = 0 AND next_is_loaded = 1 THEN next_trip_id  -- Skip this row, use next
      ELSE NULL
    END AS cycle_start_timestamp,
    
    CASE 
      WHEN is_loaded_trip = 1 AND next_is_loaded = 0 THEN next_end_timestamp
      ELSE NULL
    END AS cycle_end_timestamp,
    
    -- Trip IDs
    CASE 
      WHEN is_loaded_trip = 1 AND next_is_loaded = 0 THEN trip_segment_id
      ELSE NULL
    END AS loaded_trip_segment_id,
    
    CASE 
      WHEN is_loaded_trip = 1 AND next_is_loaded = 0 THEN next_trip_id
      ELSE NULL
    END AS empty_trip_segment_id,
    
    -- Origins and destinations
    CASE 
      WHEN is_loaded_trip = 1 AND next_is_loaded = 0 THEN origin_splc_code
      ELSE NULL
    END AS loaded_origin_splc,
    
    CASE 
      WHEN is_loaded_trip = 1 AND next_is_loaded = 0 THEN destination_splc_code
      ELSE NULL
    END AS loaded_destination_splc,
    
    CASE 
      WHEN is_loaded_trip = 1 AND next_is_loaded = 0 THEN next_origin_splc
      ELSE NULL
    END AS empty_origin_splc,
    
    CASE 
      WHEN is_loaded_trip = 1 AND next_is_loaded = 0 THEN next_dest_splc
      ELSE NULL
    END AS empty_destination_splc,
    
    -- Durations
    CASE 
      WHEN is_loaded_trip = 1 AND next_is_loaded = 0 THEN trip_duration_minutes
      ELSE NULL
    END AS loaded_duration_minutes,
    
    CASE 
      WHEN is_loaded_trip = 1 AND next_is_loaded = 0 THEN next_duration_minutes
      ELSE NULL
    END AS empty_duration_minutes,
    
    -- Distance
    CASE 
      WHEN is_loaded_trip = 1 AND next_is_loaded = 0 THEN 
        COALESCE(distance_miles, 0) + COALESCE(next_distance_miles, 0)
      ELSE NULL
    END AS total_distance_miles,
    
    psr_period,
    trip_seq AS cycle_number
    
  FROM trip_sequence
  WHERE is_loaded_trip = 1 AND next_is_loaded = 0  -- Only keep loaded→empty pairs
),

-- Calculate cycle duration
cycle_metrics AS (
  SELECT
    *,
    CAST((julianday(cycle_end_timestamp) - julianday(cycle_start_timestamp)) AS REAL) AS cycle_duration_days
  FROM cycle_pairs
  WHERE cycle_start_timestamp IS NOT NULL
    AND cycle_end_timestamp IS NOT NULL
),

final AS (
  SELECT
    -- Generate surrogate key
    ROW_NUMBER() OVER (ORDER BY car_number, cycle_start_timestamp) AS cycle_id,
    
    -- Car identification
    car_number,
    railcar_id,
    
    -- Cycle timing
    cycle_start_timestamp,
    cycle_end_timestamp,
    cycle_duration_days,
    
    -- Trip segment references
    loaded_trip_segment_id,
    empty_trip_segment_id,
    
    -- Trip endpoints
    loaded_origin_splc,
    loaded_destination_splc,
    empty_origin_splc,
    empty_destination_splc,
    
    -- Trip durations
    loaded_duration_minutes,
    empty_duration_minutes,
    
    -- Distance
    CASE 
      WHEN total_distance_miles > 0 THEN total_distance_miles
      ELSE NULL
    END AS total_distance_miles,
    
    -- Period and sequence
    psr_period,
    cycle_number
    
  FROM cycle_metrics
  WHERE cycle_duration_days BETWEEN 0.08 AND 30  -- Allow short cycles (>2 hours) but filter unreasonable ones
)

SELECT * FROM final
