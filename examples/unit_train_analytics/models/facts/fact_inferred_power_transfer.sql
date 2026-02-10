{{ config "materialized" "table" }}

-- Fact Inferred Power Transfer
-- Tracks inferred locomotive power changes based on turnaround time heuristic
-- Logic: <1 hour turnaround = same locomotives, >1 hour = different locomotives
-- Grain: one row per potential power transfer (train arrival at origin/destination)

WITH train_arrivals AS (
  -- Get all train arrival events at origins and destinations
  SELECT
    f.train_id,
    f.location_id,
    f.event_timestamp AS arrival_timestamp,
    f.event_type,
    l.location_type
  FROM {{ ref "fact_car_location_event" }} f
  INNER JOIN {{ ref "dim_location" }} l ON f.location_id = l.location_id
  WHERE f.train_id IS NOT NULL
    AND f.event_type IN ('arrived_destination', 'arrived_origin')
),

train_departures AS (
  -- Get all train departure events
  SELECT
    f.train_id,
    f.location_id,
    f.event_timestamp AS departure_timestamp,
    f.event_type
  FROM {{ ref "fact_car_location_event" }} f
  WHERE f.train_id IS NOT NULL
    AND f.event_type IN ('departed_origin', 'departed_destination')
),

-- Match arrivals with subsequent departures at same location
turnaround_pairs AS (
  SELECT
    a.train_id,
    a.location_id,
    a.arrival_timestamp,
    a.location_type,
    -- Find next departure from same location
    (
      SELECT MIN(d.departure_timestamp)
      FROM train_departures d
      WHERE d.train_id = a.train_id
        AND d.location_id = a.location_id
        AND d.departure_timestamp > a.arrival_timestamp
    ) AS departure_timestamp
  FROM train_arrivals a
),

-- Calculate gap and infer power transfer
power_inference AS (
  SELECT
    train_id,
    location_id,
    arrival_timestamp,
    departure_timestamp,
    location_type,
    
    -- Calculate gap in hours
    CAST((julianday(departure_timestamp) - julianday(arrival_timestamp)) * 24 AS REAL) AS gap_hours,
    
    -- Infer power transfer: 1 = same locomotives (fast turnaround), 0 = different locomotives (slow turnaround)
    CASE 
      WHEN CAST((julianday(departure_timestamp) - julianday(arrival_timestamp)) * 24 AS REAL) < 1.0 THEN 1
      ELSE 0
    END AS inferred_same_power
    
  FROM turnaround_pairs
  WHERE departure_timestamp IS NOT NULL  -- Only include complete turnarounds
),

-- Add date dimension and generate transfer ID
transfer_records AS (
  SELECT
    train_id || '_' || location_id || '_' || DATETIME(arrival_timestamp) AS transfer_id,
    train_id,
    location_id,
    arrival_timestamp AS transfer_timestamp,
    arrival_timestamp,
    departure_timestamp,
    gap_hours,
    inferred_same_power,
    d.date_key AS transfer_date_key
  FROM power_inference pi
  INNER JOIN {{ ref "dim_date" }} d
    ON DATE(pi.arrival_timestamp) = d.full_date
)

-- Final select with all power transfer inference metrics
SELECT
  transfer_id,
  train_id,
  location_id,
  transfer_timestamp,
  arrival_timestamp,
  departure_timestamp,
  gap_hours,
  inferred_same_power,
  transfer_date_key
FROM transfer_records
ORDER BY transfer_timestamp
