{{ config "materialized" "table" }}

-- Fact Car Location Event
-- Primary fact table capturing CLM (Car Location Message) events
-- Grain: one row per car location event
-- Source: stg_clm_events with dimension foreign keys

WITH events_with_fks AS (
  -- Join staged events to all dimension tables to validate foreign keys
  SELECT
    s.event_id,
    s.event_timestamp,
    s.car_id,
    s.train_id,
    s.location_id,
    s.event_type,
    s.loaded_flag AS is_loaded,
    s.commodity,
    s.weight_tons,
    -- Get date key from dim_date
    d.date_key
  FROM {{ ref "stg_clm_events" }} s
  INNER JOIN {{ ref "dim_date" }} d
    ON s.event_date = d.full_date
  -- Validate car_id exists in dim_car
  INNER JOIN {{ ref "dim_car" }} c
    ON s.car_id = c.car_id
  -- Validate location_id exists in dim_location
  INNER JOIN {{ ref "dim_location" }} l
    ON s.location_id = l.location_id
  -- Note: train_id can be NULL (stragglers), so LEFT JOIN not needed
),

-- Calculate derived fields for analytics
enriched_events AS (
  SELECT
    e.*,
    -- Calculate dwell time (time since previous event at same location)
    CAST((julianday(e.event_timestamp) - 
          julianday(LAG(e.event_timestamp) OVER (PARTITION BY e.car_id, e.location_id ORDER BY e.event_timestamp))) * 24 AS REAL
    ) AS dwell_hours,
    
    -- Transit speed indicator (for travel events)
    CASE 
      WHEN e.event_type IN ('DEPART_ORIGIN', 'DEPART_STATION', 'DEPART_DESTINATION') THEN 1
      ELSE 0
    END AS is_departure,
    
    -- Queue wait flag (for origin/destination loading/unloading)
    CASE 
      WHEN e.event_type IN ('LOAD_START', 'LOAD_COMPLETE', 'UNLOAD_START', 'UNLOAD_COMPLETE') THEN 1
      ELSE 0
    END AS is_queue_event,
    
    -- Event sequence number per car
    ROW_NUMBER() OVER (PARTITION BY e.car_id ORDER BY e.event_timestamp) AS event_sequence_num
    
  FROM events_with_fks e
)

-- Final select with all fact columns
SELECT
  event_id,
  event_timestamp,
  car_id,
  train_id,
  location_id,
  event_type,
  date_key,
  is_loaded,
  commodity,
  weight_tons,
  dwell_hours,
  is_departure,
  is_queue_event,
  event_sequence_num
FROM enriched_events
ORDER BY event_timestamp, event_id
