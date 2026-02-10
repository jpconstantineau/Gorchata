{{ config "materialized" "table" }}

-- Location Dimension
-- All locations including origins (2), destinations (3), and intermediate stations
-- Type 1 SCD (current state only)

WITH all_locations AS (
  -- Get all unique locations from CLM events
  SELECT DISTINCT
    location_id
  FROM {{ seed "raw_clm_events" }}
),

location_classification AS (
  -- Classify locations by type based on naming patterns
  SELECT
    location_id,
    location_id AS location_name,
    CASE
      -- Origins: COAL_MINE_*
      WHEN location_id LIKE 'COAL_MINE_%' THEN 'ORIGIN'
      -- Destinations: POWER_PLANT_* or PORT_*
      WHEN location_id LIKE 'POWER_PLANT_%' THEN 'DESTINATION'
      WHEN location_id LIKE 'PORT_%' THEN 'DESTINATION'
      -- Everything else is a station
      ELSE 'STATION'
    END AS location_type
  FROM all_locations
),

location_attributes AS (
  -- Add location-specific attributes
  SELECT
    location_id,
    location_name,
    location_type,
    CASE
      -- Origins have 12-18 hour queue/loading times
      WHEN location_type = 'ORIGIN' THEN 15.0
      -- Destinations have 8-12 hour queue/unloading times
      WHEN location_type = 'DESTINATION' THEN 10.0
      -- Stations have minimal dwell time
      ELSE 0.5
    END AS avg_queue_hours
  FROM location_classification
)

SELECT
  location_id,
  location_name,
  location_type,
  avg_queue_hours
FROM location_attributes
ORDER BY 
  CASE location_type
    WHEN 'ORIGIN' THEN 1
    WHEN 'DESTINATION' THEN 2
    WHEN 'STATION' THEN 3
  END,
  location_id
