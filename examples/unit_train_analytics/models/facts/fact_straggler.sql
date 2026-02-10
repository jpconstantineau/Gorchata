{{ config "materialized" "table" }}

-- Fact Straggler
-- Tracks cars that were set out from trains and traveled independently
-- Grain: one row per straggler occurrence (SET_OUT event)
-- Calculates delay periods between SET_OUT and RESUME_TRANSIT (6 hours to 3 days typical)

WITH set_out_events AS (
  -- Identify all SET_OUT events
  SELECT
    event_id AS straggler_id,
    car_id,
    train_id,
    location_id AS set_out_location_id,
    event_timestamp AS set_out_timestamp,
    date_key AS set_out_date_key
  FROM {{ ref "fact_car_location_event" }}
  WHERE event_type = 'SET_OUT'
),

-- Find corresponding RESUME_TRANSIT events for each straggler
resume_events AS (
  SELECT
    f.car_id,
    f.event_timestamp AS resume_travel_timestamp,
    f.location_id AS resume_location_id,
    f.train_id AS rejoin_train_id,
    -- Use window function to find next RESUME_TRANSIT after each car's timeline
    LAG(f.event_timestamp) OVER (PARTITION BY f.car_id ORDER BY f.event_timestamp) AS prev_event_timestamp,
    LAG(f.event_type) OVER (PARTITION BY f.car_id ORDER BY f.event_timestamp) AS prev_event_type
  FROM {{ ref "fact_car_location_event" }} f
  WHERE f.event_type = 'RESUME_TRANSIT'
),

-- Match SET_OUT events with their corresponding RESUME_TRANSIT
straggler_periods AS (
  SELECT
    s.straggler_id,
    s.car_id,
    s.train_id AS original_train_id,
    s.set_out_location_id,
    s.set_out_timestamp,
    s.set_out_date_key,
    
    -- Find the next RESUME_TRANSIT event for this car after SET_OUT
    (
      SELECT MIN(r.resume_travel_timestamp)
      FROM resume_events r
      WHERE r.car_id = s.car_id
        AND r.resume_travel_timestamp > s.set_out_timestamp
    ) AS resume_travel_timestamp,
    
    (
      SELECT r.resume_location_id
      FROM resume_events r
      WHERE r.car_id = s.car_id
        AND r.resume_travel_timestamp > s.set_out_timestamp
      ORDER BY r.resume_travel_timestamp
      LIMIT 1
    ) AS resume_location_id,
    
    (
      SELECT r.rejoin_train_id
      FROM resume_events r
      WHERE r.car_id = s.car_id
        AND r.resume_travel_timestamp > s.set_out_timestamp
      ORDER BY r.resume_travel_timestamp
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
    resume_travel_timestamp,
    resume_location_id,
    rejoin_train_id,
    set_out_date_key,
    
    -- Calculate delay in days (6 hours to 3 days typical)
    CASE 
      WHEN resume_travel_timestamp IS NOT NULL THEN
        CAST((julianday(resume_travel_timestamp) - julianday(set_out_timestamp)) AS REAL)
      ELSE
        NULL  -- Still waiting
    END AS total_delay_days,
    
    -- Categorize delay
    CASE 
      WHEN resume_travel_timestamp IS NULL THEN 'pending'
      WHEN CAST((julianday(resume_travel_timestamp) - julianday(set_out_timestamp)) AS REAL) < 0.5 THEN 'short'     -- < 12 hours
      WHEN CAST((julianday(resume_travel_timestamp) - julianday(set_out_timestamp)) AS REAL) < 1.0 THEN 'medium'    -- 12-24 hours
      WHEN CAST((julianday(resume_travel_timestamp) - julianday(set_out_timestamp)) AS REAL) <= 3.0 THEN 'long'     -- 1-3 days
      ELSE 'extended'  -- > 3 days
    END AS delay_category
    
  FROM straggler_periods
),

-- Calculate impact on original train
straggler_impact AS (
  SELECT
    sd.*,
    -- Calculate time car was delayed relative to original train's journey
    -- This would measure if the car caught up or how far behind it ended up
    CASE 
      WHEN sd.resume_travel_timestamp IS NOT NULL THEN
        CAST((julianday(sd.resume_travel_timestamp) - julianday(sd.set_out_timestamp)) * 24 AS REAL)
      ELSE
        NULL
    END AS delay_hours
    
  FROM straggler_delays sd
)

-- Final select with all straggler metrics
SELECT
  straggler_id,
  car_id,
  original_train_id AS train_id,  -- Reference to original train
  set_out_location_id,
  set_out_timestamp,
  resume_travel_timestamp,
  resume_location_id,
  rejoin_train_id,
  set_out_date_key,
  total_delay_days,
  delay_hours,
  delay_category
FROM straggler_impact
ORDER BY set_out_timestamp
