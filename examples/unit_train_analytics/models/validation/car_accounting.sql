-- Car Accounting Validation
-- Verifies all 228 cars are accounted for at all times
-- Checks for duplicate car appearances and validates car state transitions
-- Expected outcome: 0 violations = clean data

-- =============================================================================
-- VALIDATION 1: Total Car Count Verification
-- =============================================================================
-- All 228 cars should be present in dim_car
WITH car_count_check AS (
  SELECT 
    COUNT(DISTINCT car_id) AS actual_car_count,
    228 AS expected_car_count,
    CASE 
      WHEN COUNT(DISTINCT car_id) != 228 THEN 'CRITICAL'
      ELSE 'PASS'
    END AS severity
  FROM dim_car
),

-- =============================================================================
-- VALIDATION 2: Car Uniqueness in dim_car
-- =============================================================================
-- Each car_id should appear exactly once in dim_car
duplicate_cars AS (
  SELECT 
    car_id,
    COUNT(*) AS occurrence_count
  FROM dim_car
  GROUP BY car_id
  HAVING COUNT(*) > 1
),

-- =============================================================================
-- VALIDATION 3: Car Appearance Accounting Over Time
-- =============================================================================
-- Verify that at any given timestamp, each car appears in at most one location
-- (This catches overlapping events where a car is in multiple places simultaneously)
car_locations_at_time AS (
  SELECT 
    event_timestamp,
    car_id,
    location_id,
    COUNT(*) OVER (PARTITION BY car_id, event_timestamp) AS simultaneous_locations
  FROM fact_car_location_event
),

simultaneous_location_violations AS (
  SELECT DISTINCT
    car_id,
    event_timestamp,
    simultaneous_locations,
    'WARNING' AS severity
  FROM car_locations_at_time
  WHERE simultaneous_locations > 1
),

-- =============================================================================
-- VALIDATION 4: Cars in fact_car_location_event but not in dim_car
-- =============================================================================
-- Every car in events must exist in dim_car (referential integrity)
orphan_cars AS (
  SELECT DISTINCT
    f.car_id,
    'CRITICAL' AS severity
  FROM fact_car_location_event f
  LEFT JOIN dim_car d ON f.car_id = d.car_id
  WHERE d.car_id IS NULL
),

-- =============================================================================
-- VALIDATION 5: Cars in dim_car but never used
-- =============================================================================
-- All 228 cars should have at least one event (utilization check)
unused_cars AS (
  SELECT 
    d.car_id,
    'WARNING' AS severity
  FROM dim_car d
  LEFT JOIN fact_car_location_event f ON d.car_id = f.car_id
  WHERE f.car_id IS NULL
),

-- =============================================================================
-- VALIDATION 6: Car State Transition Validation
-- =============================================================================
-- Verify reasonable event sequences (e.g., can't depart before arriving)
car_event_sequence AS (
  SELECT 
    car_id,
    event_type,
    event_timestamp,
    LAG(event_type) OVER (PARTITION BY car_id ORDER BY event_timestamp) AS prev_event_type,
    LAG(event_timestamp) OVER (PARTITION BY car_id ORDER BY event_timestamp) AS prev_timestamp
  FROM fact_car_location_event
),

invalid_transitions AS (
  SELECT DISTINCT
    car_id,
    prev_event_type,
    event_type,
    'WARNING' AS severity
  FROM car_event_sequence
  WHERE 
    -- Example: can't have train_completed before train_formed for same car
    (event_type = 'train_completed' AND prev_event_type IS NULL)
    OR (event_type = 'departed_origin' AND prev_event_type NOT IN ('load_complete', 'train_formed'))
    OR (event_timestamp = prev_timestamp) -- Same timestamp for different events
),

-- =============================================================================
-- VALIDATION 7: Car Recycling Overlap Detection 
-- =============================================================================
-- Known issue: Check if cars are recycled before previous trip completes
-- This is a known issue in the current implementation
car_active_ranges AS (
  SELECT 
    car_id,
    train_id,
    MIN(event_timestamp) AS trip_start,
    MAX(event_timestamp) AS trip_end
  FROM fact_car_location_event
  WHERE train_id IS NOT NULL
  GROUP BY car_id, train_id
),

overlapping_trips AS (
  SELECT 
    a.car_id,
    a.train_id AS train_1,
    b.train_id AS train_2,
    a.trip_start AS trip1_start,
    a.trip_end AS trip1_end,
    b.trip_start AS trip2_start,
    'INFO' AS severity
  FROM car_active_ranges a
  JOIN car_active_ranges b 
    ON a.car_id = b.car_id 
    AND a.train_id != b.train_id
  WHERE b.trip_start < a.trip_end  -- Trip 2 starts before Trip 1 ends
),

-- =============================================================================
-- CONSOLIDATED VIOLATION REPORT
-- =============================================================================
all_violations AS (
  -- Total car count
  SELECT 
    'CAR_COUNT_MISMATCH' AS violation_type,
    severity,
    'Expected 228 cars, found ' || CAST(actual_car_count AS TEXT) AS violation_details,
    NULL AS car_id,
    NULL AS event_timestamp
  FROM car_count_check
  WHERE severity != 'PASS'
  
  UNION ALL
  
  -- Duplicate cars in dim_car
  SELECT 
    'DUPLICATE_CAR_IN_DIM' AS violation_type,
    'CRITICAL' AS severity,
    'Car appears ' || CAST(occurrence_count AS TEXT) || ' times in dim_car' AS violation_details,
    car_id,
    NULL AS event_timestamp
  FROM duplicate_cars
  
  UNION ALL
  
  -- Simultaneous locations
  SELECT 
    'SIMULTANEOUS_LOCATIONS' AS violation_type,
    severity,
    'Car in ' || CAST(simultaneous_locations AS TEXT) || ' locations at same time' AS violation_details,
    car_id,
    event_timestamp
  FROM simultaneous_location_violations
  
  UNION ALL
  
  -- Orphan cars
  SELECT 
    'ORPHAN_CAR' AS violation_type,
    severity,
    'Car in events but not in dim_car' AS violation_details,
    car_id,
    NULL AS event_timestamp
  FROM orphan_cars
  
  UNION ALL
  
  -- Unused cars
  SELECT 
    'UNUSED_CAR' AS violation_type,
    severity,
    'Car in dim_car but has no events' AS violation_details,
    car_id,
    NULL AS event_timestamp
  FROM unused_cars
  
  UNION ALL
  
  -- Invalid transitions
  SELECT 
    'INVALID_TRANSITION' AS violation_type,
    severity,
    'Invalid state transition: ' || COALESCE(prev_event_type, 'NULL') || ' -> ' || event_type AS violation_details,
    car_id,
    NULL AS event_timestamp
  FROM invalid_transitions
  
  UNION ALL
  
  -- Overlapping trips (known issue)
  SELECT 
    'CAR_RECYCLING_OVERLAP' AS violation_type,
    severity,
    'Car on ' || train_2 || ' while still active on ' || train_1 AS violation_details,
    car_id,
    trip2_start AS event_timestamp
  FROM overlapping_trips
)

-- Return all violations ordered by severity
SELECT 
  violation_type,
  severity,
  violation_details,
  car_id,
  event_timestamp,
  COUNT(*) OVER (PARTITION BY violation_type) AS violation_count
FROM all_violations
ORDER BY 
  CASE severity
    WHEN 'CRITICAL' THEN 1
    WHEN 'WARNING' THEN 2
    WHEN 'INFO' THEN 3
  END,
  violation_type,
  car_id;
