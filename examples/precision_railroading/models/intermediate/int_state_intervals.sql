{{ config "materialized" "table" }}

-- Intermediate: State Intervals
-- Purpose: Transform discrete CLM events into continuous time intervals
-- Grain: One row per state interval (from one event to the next)
-- Source: stg_clm_enriched

WITH enriched_events AS (
  SELECT
    event_key,
    event_id,
    car_number,
    railcar_id,
    timestamp,
    event_type,
    splc_code,
    location_id,
    location_name,
    train_id,
    date_id,
    psr_period,
    event_sequence
  FROM {{ ref "stg_clm_enriched" }}
),

-- Use LEAD window function to pair each event with the next event for the same car
event_pairs AS (
  SELECT
    -- Current event (interval start)
    event_id AS start_event_id,
    car_number,
    railcar_id,
    timestamp AS start_timestamp,
    event_type AS start_event_type,
    splc_code AS start_splc_code,
    location_id AS start_location_id,
    location_name AS start_location_name,
    train_id,
    psr_period,
    
    -- Next event (interval end)
    LEAD(event_id) OVER (PARTITION BY car_number ORDER BY timestamp, event_sequence) AS end_event_id,
    LEAD(timestamp) OVER (PARTITION BY car_number ORDER BY timestamp, event_sequence) AS end_timestamp,
    LEAD(event_type) OVER (PARTITION BY car_number ORDER BY timestamp, event_sequence) AS end_event_type,
    LEAD(splc_code) OVER (PARTITION BY car_number ORDER BY timestamp, event_sequence) AS end_splc_code,
    LEAD(location_id) OVER (PARTITION BY car_number ORDER BY timestamp, event_sequence) AS end_location_id,
    LEAD(location_name) OVER (PARTITION BY car_number ORDER BY timestamp, event_sequence) AS end_location_name
    
  FROM enriched_events
),

-- Calculate duration and assign interval IDs
final AS (
  SELECT
    -- Generate surrogate key
    ROW_NUMBER() OVER (ORDER BY car_number, start_timestamp) AS interval_id,
    
    -- Car identification
    car_number,
    railcar_id,
    
    -- Start event details
    start_event_id,
    start_timestamp,
    start_event_type,
    start_location_id,
    start_splc_code,
    start_location_name,
    
    -- End event details (may be NULL for terminal intervals)
    end_event_id,
    end_timestamp,
    end_event_type,
    end_location_id,
    end_splc_code,
    end_location_name,
    
    -- Duration calculation (minute precision)
    CASE 
      WHEN end_timestamp IS NOT NULL THEN
        CAST((julianday(end_timestamp) - julianday(start_timestamp)) * 24 * 60 AS INTEGER)
      ELSE NULL
    END AS duration_minutes,
    
    -- Associated train and period
    train_id,
    psr_period
    
  FROM event_pairs
)

SELECT * FROM final
