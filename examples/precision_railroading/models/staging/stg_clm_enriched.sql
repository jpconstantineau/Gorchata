{{ config "materialized" "view" }}

-- Staging: CLM Events Enriched
-- Purpose: Second-stage processing with dimension lookups and derived fields
-- Grain: One row per CLM event with dimension attributes
-- Source: stg_clm_events + all dimension tables

WITH base_events AS (
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
  FROM {{ ref "stg_clm_events" }}
),

-- Join to dimension tables for enrichment
enriched AS (
  SELECT
    -- Base event columns
    e.event_key,
    e.event_id,
    e.car_number,
    e.timestamp,
    e.event_type,
    e.splc_code,
    e.train_id,
    e.location_name,
    e.load_timestamp,
    
    -- From dim_location
    l.location_id,
    l.location_type,
    l.latitude,
    l.longitude,
    l.shadow_yard_risk_score,
    l.region,
    
    -- From dim_railcar
    r.railcar_id,
    r.railroad_owner,
    r.car_type,
    
    -- From dim_train (may be NULL for some events)
    t.train_id AS train_db_id,
    t.train_type,
    t.priority_level,
    
    -- From dim_date
    d.date_id,
    d.psr_period,
    d.season
    
  FROM base_events e
  
  -- LEFT JOIN to handle potential missing dimension records gracefully
  LEFT JOIN {{ ref "dim_location" }} l
    ON e.splc_code = l.splc_code
    
  LEFT JOIN {{ ref "dim_railcar" }} r
    ON e.car_number = r.car_number
    
  LEFT JOIN {{ ref "dim_train" }} t
    ON e.train_id = t.train_id
    
  LEFT JOIN {{ ref "dim_date" }} d
    ON DATE(e.timestamp) = d.date
),

-- Add calculated fields
final AS (
  SELECT
    -- All enriched columns
    *,
    
    -- Calculated: is_loaded_event
    -- TRUE for PLAC (placement/loading), FALSE for PULL (pull/unloading), NULL for movement events
    CASE 
      WHEN event_type = 'PLAC' THEN 1
      WHEN event_type = 'PULL' THEN 0
      ELSE NULL
    END AS is_loaded_event,
    
    -- Calculated: is_movement_event
    -- TRUE for DEPA/ARRI (departures/arrivals), FALSE for PLAC/PULL (load/unload)
    CASE 
      WHEN event_type IN ('DEPA', 'ARRI') THEN 1
      WHEN event_type IN ('PLAC', 'PULL') THEN 0
      ELSE 0
    END AS is_movement_event,
    
    -- Calculated: event_sequence (row number per car ordered by timestamp)
    ROW_NUMBER() OVER (PARTITION BY car_number ORDER BY timestamp, event_id) AS event_sequence
    
  FROM enriched
)

SELECT
  -- Base event identification
  event_key,
  event_id,
  car_number,
  timestamp,
  event_type,
  splc_code,
  train_id,
  location_name,
  load_timestamp,
  
  -- Dimension foreign keys
  location_id,
  railcar_id,
  train_db_id,
  date_id,
  
  -- Location attributes
  location_type,
  latitude,
  longitude,
  shadow_yard_risk_score,
  region,
  
  -- Railcar attributes
  railroad_owner,
  car_type,
  
  -- Train attributes
  train_type,
  priority_level,
  
  -- Date attributes
  psr_period,
  season,
  
  -- Calculated fields
  is_loaded_event,
  is_movement_event,
  event_sequence
  
FROM final
