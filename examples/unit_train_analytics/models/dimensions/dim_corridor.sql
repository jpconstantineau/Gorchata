{{ config "materialized" "table" }}

-- Corridor Dimension
-- Origin-destination pairs with transit characteristics
-- 6 corridors: 2 origins × 3 destinations
-- Type 1 SCD (current state only)

WITH origin_dest_pairs AS (
  -- Get origin for each train
  SELECT DISTINCT
    r1.train_id,
    r1.location_id AS origin_id,
    r2.location_id AS destination_id
  FROM {{ seed "raw_clm_events" }} r1
  INNER JOIN {{ seed "raw_clm_events" }} r2
    ON r1.train_id = r2.train_id
  WHERE r1.event_type = 'train_formed'
    AND r2.event_type = 'arrived_destination'
    AND r1.train_id IS NOT NULL 
    AND r1.train_id != ''
),

train_routes AS (
  -- Get transit times for each trip
  SELECT
    od.origin_id,
    od.destination_id,
    od.train_id,
    CAST((julianday(MAX(CASE WHEN r.event_type = 'arrived_destination' THEN r.event_timestamp END)) -
          julianday(MIN(CASE WHEN r.event_type = 'train_formed' THEN r.event_timestamp END))) * 24 AS REAL) AS transit_hours
  FROM origin_dest_pairs od
  INNER JOIN {{ seed "raw_clm_events" }} r
    ON od.train_id = r.train_id
  WHERE r.event_type IN ('train_formed', 'arrived_destination')
  GROUP BY od.origin_id, od.destination_id, od.train_id
),

corridor_stats AS (
  -- Calculate statistics per origin-destination pair
  SELECT
    origin_id,
    destination_id,
    AVG(transit_hours) AS avg_transit_hours,
    COUNT(*) AS trip_count
  FROM train_routes
  WHERE transit_hours IS NOT NULL AND transit_hours > 0
  GROUP BY origin_id, destination_id
),

station_visits AS (
  -- Get all station visits for each origin-destination pair
  SELECT DISTINCT
    od.origin_id,
    od.destination_id,
    r.location_id AS station_id,
    MIN(julianday(r.event_timestamp)) AS first_visit
  FROM origin_dest_pairs od
  INNER JOIN {{ seed "raw_clm_events" }} r
    ON od.train_id = r.train_id
  WHERE r.event_type IN ('ARRIVE_STATION', 'DEPART_STATION')
    AND r.location_id != od.origin_id
    AND r.location_id != od.destination_id
  GROUP BY od.origin_id, od.destination_id, r.location_id
),

station_counts AS (
  -- Count and aggregate stations per corridor
  SELECT
    origin_id,
    destination_id,
    COUNT(DISTINCT station_id) AS station_count,
    GROUP_CONCAT(station_id, ',') AS intermediate_stations
  FROM station_visits
  GROUP BY origin_id, destination_id
),

corridors AS (
  SELECT
    -- Create corridor ID from origin and destination
    cs.origin_id || '_TO_' || cs.destination_id AS corridor_id,
    cs.origin_id AS origin_location_id,
    cs.destination_id AS destination_location_id,
    -- Classify transit time (round to nearest day class)
    CASE
      WHEN cs.avg_transit_hours <= 60 THEN '2-day'
      WHEN cs.avg_transit_hours <= 84 THEN '3-day'
      ELSE '4-day'
    END AS transit_time_class,
    -- Use average as expected transit hours
    ROUND(cs.avg_transit_hours, 1) AS expected_transit_hours,
    -- Estimate distance (rough calculation: 40 mph average speed)
    CAST(ROUND(cs.avg_transit_hours * 40, 0) AS INTEGER) AS distance_miles,
    -- Station count from actual data
    COALESCE(sc.station_count, 0) AS station_count,
    -- Intermediate stations as comma-separated list
    sc.intermediate_stations
  FROM corridor_stats cs
  LEFT JOIN station_counts sc
    ON cs.origin_id = sc.origin_id
    AND cs.destination_id = sc.destination_id
)

SELECT
  corridor_id,
  origin_location_id,
  destination_location_id,
  transit_time_class,
  expected_transit_hours,
  distance_miles,
  station_count,
  intermediate_stations
FROM corridors
ORDER BY origin_location_id, destination_location_id
