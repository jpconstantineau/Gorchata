{{ config "materialized" "table" }}

-- Fact Train Trip
-- Aggregated fact table for train trips
-- Grain: one row per train round trip (origin → destination → origin)
-- Captures complete trip metrics including transit times, queue waits, and straggler counts
-- Uses window functions (LAG, LEAD, ROW_NUMBER) for trip boundary identification

WITH train_events AS (
  -- Get all events for trains with timestamps
  SELECT
    f.train_id,
    f.event_timestamp,
    f.event_type,
    f.location_id,
    f.car_id,
    f.is_loaded,
    l.location_type
  FROM {{ ref "fact_car_location_event" }} f
  INNER JOIN {{ ref "dim_location" }} l ON f.location_id = l.location_id
  WHERE f.train_id IS NOT NULL
),

-- Identify trip boundaries using window functions (LAG, LEAD, ROW_NUMBER)
trip_boundaries AS (
  SELECT
    train_id,
    event_timestamp,
    event_type,
    location_id,
    car_id,
    is_loaded,
    location_type,
    -- Use LAG to look at previous event
    LAG(event_type) OVER (PARTITION BY train_id ORDER BY event_timestamp) AS prev_event_type,
    -- Use LEAD to look at next event
    LEAD(event_type) OVER (PARTITION BY train_id ORDER BY event_timestamp) AS next_event_type,
    -- Use ROW_NUMBER for event sequencing
    ROW_NUMBER() OVER (PARTITION BY train_id ORDER BY event_timestamp) AS event_seq,
    -- Identify origin departure as trip start
    CASE 
      WHEN event_type = 'departed_origin' THEN 1
      ELSE 0
    END AS is_trip_start,
    -- Identify destination arrival as leg boundary
    CASE 
      WHEN event_type = 'arrived_destination' THEN 1
      ELSE 0
    END AS is_destination_arrival,
    -- Identify origin return as trip end
    CASE 
      WHEN event_type = 'arrived_origin' THEN 1
      ELSE 0
    END AS is_trip_end
  FROM train_events
),

-- Assign trip IDs by counting trip starts
trip_assignments AS (
  SELECT
    *,
    SUM(is_trip_start) OVER (PARTITION BY train_id ORDER BY event_timestamp) AS trip_number
  FROM trip_boundaries
),

-- Calculate trip-level timestamps and locations
trip_timestamps AS (
  SELECT DISTINCT
    train_id,
    trip_number,
    -- Origin departure (trip start)
    MIN(CASE WHEN event_type = 'departed_origin' THEN event_timestamp END) 
      OVER (PARTITION BY train_id, trip_number) AS departure_timestamp,
    MIN(CASE WHEN event_type = 'departed_origin' THEN location_id END) 
      OVER (PARTITION BY train_id, trip_number) AS origin_location_id,
    -- Destination arrival (loaded leg end)
    MIN(CASE WHEN event_type = 'arrived_destination' THEN event_timestamp END) 
      OVER (PARTITION BY train_id, trip_number) AS destination_arrival_timestamp,
    MIN(CASE WHEN event_type = 'arrived_destination' THEN location_id END) 
      OVER (PARTITION BY train_id, trip_number) AS destination_location_id,
    -- Destination departure (empty leg start)
    MIN(CASE WHEN event_type = 'departed_destination' THEN event_timestamp END) 
      OVER (PARTITION BY train_id, trip_number) AS destination_departure_timestamp,
    -- Origin return (trip end)
    MAX(CASE WHEN event_type = 'arrived_origin' THEN event_timestamp END) 
      OVER (PARTITION BY train_id, trip_number) AS return_timestamp,
    -- Formation time (load_start captures origin queue start)
    MIN(CASE WHEN event_type = 'load_start' THEN event_timestamp END) 
      OVER (PARTITION BY train_id, trip_number) AS load_start_timestamp,
    -- Loading complete
    MIN(CASE WHEN event_type = 'load_complete' THEN event_timestamp END) 
      OVER (PARTITION BY train_id, trip_number) AS load_complete_timestamp,
    -- Unloading times
    MIN(CASE WHEN event_type = 'unload_start' THEN event_timestamp END) 
      OVER (PARTITION BY train_id, trip_number) AS unload_start_timestamp,
    MIN(CASE WHEN event_type = 'unload_complete' THEN event_timestamp END) 
      OVER (PARTITION BY train_id, trip_number) AS unload_complete_timestamp
  FROM trip_assignments
  WHERE trip_number > 0  -- Only include trips with valid start
),

-- Count cars at formation and destination
car_counts AS (
  SELECT
    train_id,
    trip_number,
    -- Cars at formation (train_formed events)
    COUNT(DISTINCT CASE WHEN event_type = 'train_formed' THEN car_id END) AS cars_at_formation,
    -- Cars at destination (arrived_destination events)
    COUNT(DISTINCT CASE WHEN event_type = 'arrived_destination' THEN car_id END) AS cars_at_destination,
    -- Stragglers (car_set_out events during this trip)
    COUNT(DISTINCT CASE WHEN event_type = 'car_set_out' THEN car_id END) AS num_stragglers
  FROM trip_assignments
  GROUP BY train_id, trip_number
),

-- Join corridor dimension
trip_corridors AS (
  SELECT
    tt.*,
    c.corridor_id
  FROM trip_timestamps tt
  INNER JOIN {{ ref "dim_corridor" }} c
    ON tt.origin_location_id = c.origin_location_id
    AND tt.destination_location_id = c.destination_location_id
),

-- Calculate all trip metrics
trip_metrics AS (
  SELECT
    tc.train_id || '_T' || tc.trip_number AS trip_id,
    tc.train_id,
    tc.corridor_id,
    -- Date key from departure date
    d.date_key AS departure_date_key,
    
    -- Queue wait times (hours)
    CAST((julianday(tc.load_complete_timestamp) - julianday(tc.load_start_timestamp)) * 24 AS REAL) AS origin_queue_hours,
    CAST((julianday(tc.unload_complete_timestamp) - julianday(tc.unload_start_timestamp)) * 24 AS REAL) AS destination_queue_hours,
    
    -- Transit time (hours) - origin to destination (loaded leg)
    CAST((julianday(tc.destination_arrival_timestamp) - julianday(tc.departure_timestamp)) * 24 AS REAL) AS transit_hours,
    
    -- Total trip time (hours) - includes queue waits and transit
    CAST((julianday(tc.return_timestamp) - julianday(tc.departure_timestamp)) * 24 AS REAL) AS total_trip_hours,
    
    -- Car counts
    cc.cars_at_formation,
    cc.cars_at_destination,
    cc.num_stragglers,
    
    -- Turnaround times (hours)
    CAST((julianday(tc.destination_departure_timestamp) - julianday(tc.destination_arrival_timestamp)) * 24 AS REAL) AS destination_turnaround_hours,
    
    -- Timestamps for reference
    tc.departure_timestamp,
    tc.destination_arrival_timestamp,
    tc.return_timestamp
    
  FROM trip_corridors tc
  LEFT JOIN car_counts cc
    ON tc.train_id = cc.train_id
    AND tc.trip_number = cc.trip_number
  INNER JOIN {{ ref "dim_date" }} d
    ON DATE(tc.departure_timestamp) = d.full_date
  WHERE tc.departure_timestamp IS NOT NULL
    AND tc.destination_arrival_timestamp IS NOT NULL
)

-- Final select with all trip metrics
SELECT
  trip_id,
  train_id,
  corridor_id,
  departure_date_key,
  origin_queue_hours,
  destination_queue_hours,
  transit_hours,
  total_trip_hours,
  cars_at_formation,
  cars_at_destination,
  num_stragglers,
  destination_turnaround_hours,
  departure_timestamp,
  destination_arrival_timestamp,
  return_timestamp
FROM trip_metrics
WHERE total_trip_hours > 0  -- Filter out incomplete trips
ORDER BY departure_timestamp
