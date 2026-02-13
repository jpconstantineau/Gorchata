{{ config "materialized" "table" }}

-- Intermediate: Trip Segments
-- Purpose: Group state intervals into meaningful origin-destination trips
-- Grain: One row per trip segment (loaded or empty)
-- Source: int_state_intervals + stg_clm_enriched

WITH intervals AS (
  SELECT
    interval_id,
    car_number,
    railcar_id,
    start_event_id,
    start_timestamp,
    start_event_type,
    start_location_id,
    start_splc_code,
    end_event_id,
    end_timestamp,
    end_event_type,
    end_location_id,
    end_splc_code,
    duration_minutes,
    train_id,
    psr_period
  FROM {{ ref "int_state_intervals" }}
  WHERE end_timestamp IS NOT NULL  -- Only complete intervals
),

-- Identify trip boundaries (PLAC starts loaded trip, PULL starts empty trip)
trip_boundaries AS (
  SELECT
    *,
    CASE 
      WHEN start_event_type IN ('PLAC', 'PULL') THEN 1 
      ELSE 0 
    END AS is_trip_start,
    -- Create trip grouping by counting cumulative trip starts
    SUM(CASE WHEN start_event_type IN ('PLAC', 'PULL') THEN 1 ELSE 0 END) 
      OVER (PARTITION BY car_number ORDER BY start_timestamp 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS trip_group_id
  FROM intervals
),

-- Aggregate intervals into trips
trip_aggregation AS (
  SELECT
    car_number,
    railcar_id,
    trip_group_id,
    
    -- Trip timing
    MIN(start_timestamp) AS trip_start_timestamp,
    MAX(COALESCE(end_timestamp, start_timestamp)) AS trip_end_timestamp,
    SUM(COALESCE(duration_minutes, 0)) AS trip_duration_minutes,
    
    -- Origin and destination
    MIN(start_location_id) AS origin_location_id,
    MIN(start_splc_code) AS origin_splc_code,
    MAX(end_location_id) AS destination_location_id,
    MAX(end_splc_code) AS destination_splc_code,
    
    -- Trip classification
    MAX(CASE WHEN start_event_type = 'PLAC' THEN 1 ELSE 0 END) AS is_loaded_trip,
    
    -- Associated attributes
    MAX(train_id) AS train_id,
    MAX(psr_period) AS psr_period
    
  FROM trip_boundaries
  GROUP BY car_number, railcar_id, trip_group_id
),

-- Join with dim_corridor for distance calculation (if applicable)
final AS (
  SELECT
    -- Generate surrogate key
    ROW_NUMBER() OVER (ORDER BY car_number, trip_start_timestamp) AS trip_segment_id,
    
    -- Car identification
    car_number,
    railcar_id,
    
    -- Trip timing
    trip_start_timestamp,
    trip_end_timestamp,
    trip_duration_minutes,
    
    -- Origin and destination
    origin_location_id,
    origin_splc_code,
    destination_location_id,
    destination_splc_code,
    
    -- Distance (placeholder - would join with dim_corridor if available)
    NULL AS distance_miles,
    
    -- Trip classification
    CAST(is_loaded_trip AS BOOLEAN) AS is_loaded_trip,
    
    -- Associated attributes
    train_id,
    psr_period
    
  FROM trip_aggregation
  WHERE trip_duration_minutes > 0  -- Filter out zero-duration trips
)

SELECT * FROM final
