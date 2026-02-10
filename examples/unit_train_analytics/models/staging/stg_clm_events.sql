{{ config "materialized" "view" }}

-- Staging: CLM Events
-- Clean and standardize raw CLM event data from CSV seed
-- Converts string booleans to proper booleans and parses timestamps

WITH source_data AS (
  SELECT
    event_id,
    -- Parse timestamp string to proper timestamp type
    CAST(event_timestamp AS TIMESTAMP) AS event_timestamp,
    car_id,
    train_id,
    location_id,
    event_type,
    -- Convert string boolean to proper boolean
    CASE 
      WHEN loaded_flag = 'true' THEN TRUE
      WHEN loaded_flag = 'false' THEN FALSE
      ELSE NULL
    END AS loaded_flag,
    commodity,
    weight_tons
  FROM {{ seed "raw_clm_events" }}
),

-- Add derived fields for data quality and analysis
enriched AS (
  SELECT
    *,
    -- Extract date for date dimension joins
    CAST(event_timestamp AS DATE) AS event_date,
    -- Flag different event categories
    CASE 
      WHEN event_type = 'FORM_TRAIN' THEN 'FORMATION'
      WHEN event_type = 'LOAD_CARGO' THEN 'LOADING'
      WHEN event_type = 'DEPART_LOCATION' THEN 'MOVEMENT'
      WHEN event_type = 'ARRIVE_LOCATION' THEN 'MOVEMENT'
      WHEN event_type = 'UNLOAD_CARGO' THEN 'UNLOADING'
      WHEN event_type = 'COMPLETE_TRAIN' THEN 'COMPLETION'
      ELSE 'OTHER'
    END AS event_category
  FROM source_data
)

SELECT
  event_id,
  event_timestamp,
  event_date,
  car_id,
  train_id,
  location_id,
  event_type,
  event_category,
  loaded_flag,
  commodity,
  weight_tons
FROM enriched
ORDER BY event_timestamp, event_id
