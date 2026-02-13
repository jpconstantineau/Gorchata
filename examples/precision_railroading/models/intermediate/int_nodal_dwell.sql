{{ config "materialized" "table" }}

-- Intermediate: Nodal Dwell
-- Purpose: Capture stop duration at each location (minutes)
-- Grain: One row per dwell event at a location
-- Source: int_state_intervals

WITH state_intervals AS (
  SELECT
    interval_id,
    car_number,
    railcar_id,
    start_event_id,
    start_timestamp,
    start_event_type,
    start_location_id,
    end_event_id,
    end_timestamp,
    end_event_type,
    end_location_id,
    duration_minutes,
    psr_period
  FROM {{ ref "int_state_intervals" }}
  WHERE end_timestamp IS NOT NULL  -- Only complete intervals
),

-- Identify dwell events (where railcar stayed at same location)
dwell_events AS (
  SELECT
    interval_id,
    car_number,
    railcar_id,
    start_location_id AS location_id,
    start_timestamp AS dwell_start_timestamp,
    end_timestamp AS dwell_end_timestamp,
    
    -- Calculate dwell duration (minute precision using julianday)
    CAST((julianday(end_timestamp) - julianday(start_timestamp)) * 24 * 60 AS INTEGER) AS dwell_duration_minutes,
    
    start_event_type AS event_type_at_arrival,
    end_event_type AS event_type_at_departure,
    
    -- Determine if loaded based on event types
    -- PLAC = loaded, PULL = empty
    CASE 
      WHEN start_event_type = 'PLAC' THEN 1
      ELSE 0
    END AS is_loaded,
    
    psr_period
  FROM state_intervals
  WHERE start_location_id = end_location_id  -- Same location = dwell
    AND start_location_id IS NOT NULL
    AND end_location_id IS NOT NULL
),

-- Filter to significant dwell events only
final AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY car_number, dwell_start_timestamp) AS dwell_id,
    railcar_id,
    location_id,
    dwell_start_timestamp,
    dwell_end_timestamp,
    dwell_duration_minutes,
    event_type_at_arrival,
    event_type_at_departure,
    is_loaded
  FROM dwell_events
  WHERE dwell_duration_minutes >= 5  -- Filter out insignificant stops (<5 minutes)
)

SELECT * FROM final
