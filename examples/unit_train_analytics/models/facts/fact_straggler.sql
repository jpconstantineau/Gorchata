{{ config "materialized" "table" }}

-- Fact Straggler
-- Tracks cars that were set out from trains and traveled independently
-- Grain: one row per straggler occurrence (SET_OUT event)
-- Calculates delay periods between SET_OUT and RESUME_TRANSIT (6 hours to 3 days typical)

WITH set_out_events AS (
  -- Identify all car_set_out events
  SELECT
    event_id AS straggler_id,
    car_id,
    train_id,
    location_id AS set_out_location_id,
    event_timestamp AS set_out_timestamp,
    date_key AS set_out_date_key
  FROM {{ ref "fact_car_location_event" }}
  WHERE event_type = 'car_set_out'
),

-- Find corresponding car_picked_up events for each straggler
resume_events AS (
  SELECT
    f.car_id,
    f.event_timestamp AS picked_up_timestamp,
    f.location_id AS resume_location_id,
    f.train_id AS rejoin_train_id,
    -- Use window function to find next car_picked_up after each car's timeline
    LAG(f.event_timestamp) OVER (PARTITION BY f.car_id ORDER BY f.event_timestamp) AS prev_event_timestamp,
    LAG(f.event_type) OVER (PARTITION BY f.car_id ORDER BY f.event_timestamp) AS prev_event_type
  FROM {{ ref "fact_car_location_event" }} f
  WHERE f.event_type = 'car_picked_up'
),

-- Match car_set_out events with their corresponding car_picked_up
straggler_periods AS (
  SELECT
    s.straggler_id,
    s.car_id,
    s.train_id AS original_train_id,
    s.set_out_location_id,
    s.set_out_timestamp,
    s.set_out_date_key,
    
    -- Find the next car_picked_up event for this car after car_set_out
    (
      SELECT MIN(r.picked_up_timestamp)
      FROM resume_events r
      WHERE r.car_id = s.car_id
        AND r.picked_up_timestamp > s.set_out_timestamp
    ) AS picked_up_timestamp,
    
    (
      SELECT r.resume_location_id
      FROM resume_events r
      WHERE r.car_id = s.car_id
        AND r.picked_up_timestamp > s.set_out_timestamp
      ORDER BY r.picked_up_timestamp
      LIMIT 1
    ) AS resume_location_id,
    
    (
      SELECT r.rejoin_train_id
      FROM resume_events r
      WHERE r.car_id = s.car_id
        AND r.picked_up_timestamp > s.set_out_timestamp
      ORDER BY r.picked_up_timestamp
      LIMIT 1
    ) AS rejoin_train_id
    
  FROM set_out_events s
),

-- Calculate delay metrics
straggler_delays AS (
  SELECT
    straggler_id,
    car_id,
    original_train_id,
    set_out_location_id,
    set_out_timestamp,
    picked_up_timestamp,
    resume_location_id,
    rejoin_train_id,
    set_out_date_key,
    
    -- Calculate delay in hours (6 hours to 3 days typical)
    CAST((julianday(picked_up_timestamp) - julianday(set_out_timestamp)) * 24 AS REAL) AS delay_hours,
    
    -- Categorize delay (filter out NULL picked_up_timestamp)
    CASE 
      WHEN picked_up_timestamp IS NULL THEN NULL  -- No category for incomplete stragglers
      WHEN CAST((julianday(picked_up_timestamp) - julianday(set_out_timestamp)) * 24 AS REAL) < 12 THEN 'short'     -- < 12 hours
      WHEN CAST((julianday(picked_up_timestamp) - julianday(set_out_timestamp)) * 24 AS REAL) < 24 THEN 'medium'    -- 12-24 hours
      WHEN CAST((julianday(picked_up_timestamp) - julianday(set_out_timestamp)) * 24 AS REAL) <= 72 THEN 'long'     -- 1-3 days
      ELSE 'extended'  -- > 3 days
    END AS delay_category
    
  FROM straggler_periods
  WHERE picked_up_timestamp IS NOT NULL  -- Only include stragglers with pickup
),

-- Calculate impact on original train
straggler_impact AS (
  SELECT
    sd.*
  FROM straggler_delays sd
)

-- Final select with all straggler metrics
SELECT
  straggler_id,
  car_id,
  original_train_id AS train_id,  -- Reference to original train
  set_out_location_id,
  set_out_timestamp,
  picked_up_timestamp,
  resume_location_id,
  rejoin_train_id,
  set_out_date_key,
  delay_hours,
  delay_category
FROM straggler_impact
ORDER BY set_out_timestamp
