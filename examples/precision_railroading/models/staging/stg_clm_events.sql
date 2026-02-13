{{ config "materialized" "view" }}

-- Staging: CLM Events
-- Purpose: First-stage processing of raw CLM data with basic cleaning and validation
-- Grain: One row per CLM event
-- Source: raw_clm_events seed (loaded directly into database due to 8GB size)

WITH source_data AS (
  SELECT
    event_id,
    car_number,
    timestamp,
    event_type,
    splc_code,
    train_id,
    location_name
  FROM {{ seed "raw_clm_events" }}
),

-- Deduplicate any potential duplicates using ROW_NUMBER
deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY timestamp) AS rn
  FROM source_data
),

-- Clean and standardize data
cleaned AS (
  SELECT
    -- Generate surrogate key
    ROW_NUMBER() OVER (ORDER BY timestamp, event_id) AS event_key,
    
    -- Original columns with data cleaning
    TRIM(event_id) AS event_id,
    TRIM(car_number) AS car_number,
    
    -- Keep timestamp as TEXT (SQLite stores timestamps as TEXT)
    -- Timestamps are already in 'YYYY-MM-DD HH:MM:SS' format with minute precision
    timestamp AS timestamp,
    
    -- Clean event type
    UPPER(TRIM(event_type)) AS event_type,
    
    -- Clean SPLC code
    TRIM(splc_code) AS splc_code,
    
    -- Clean train ID (may be NULL for some events)
    TRIM(train_id) AS train_id,
    
    -- Clean location name
    TRIM(location_name) AS location_name,
    
    -- Add load timestamp
    CURRENT_TIMESTAMP AS load_timestamp
    
  FROM deduplicated
  WHERE rn = 1  -- Keep only first occurrence of any duplicate event_id
)

SELECT
  event_key,
  event_id,
  car_number,
  timestamp,
  event_type,
  splc_code,
  train_id,
  location_name,
  load_timestamp
FROM cleaned
