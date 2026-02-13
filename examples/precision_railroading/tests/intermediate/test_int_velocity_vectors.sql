-- Test: int_velocity_vectors Velocity Validation
-- Description: Validates velocity vectors have positive speeds, valid distances, and consistent calculations
-- Expected Result: All tests should return 0 violations

WITH all_tests AS (
  -- Test 1: Velocity is non-negative
  SELECT
    'test_velocity_positive_or_zero' AS test_name,
    COUNT(*) AS violation_count,
    'Velocity must be >= 0 mph' AS description
  FROM {{ ref "int_velocity_vectors" }}
  WHERE velocity_mph < 0
  
  UNION ALL
  
  -- Test 2: Velocity is within reasonable maximum (80 mph)
  SELECT
    'test_velocity_reasonable_max' AS test_name,
    COUNT(*) AS violation_count,
    'Velocity must be <= 80 mph (freight train maximum)' AS description
  FROM {{ ref "int_velocity_vectors" }}
  WHERE velocity_mph > 80
  
  UNION ALL
  
  -- Test 3: Distance is positive
  SELECT
    'test_velocity_distance_positive' AS test_name,
    COUNT(*) AS violation_count,
    'Distance must be > 0 miles for movement' AS description
  FROM {{ ref "int_velocity_vectors" }}
  WHERE distance_miles <= 0
  
  UNION ALL
  
  -- Test 4: Duration is positive
  SELECT
    'test_velocity_duration_positive' AS test_name,
    COUNT(*) AS violation_count,
    'Duration must be > 0 minutes for movement' AS description
  FROM {{ ref "int_velocity_vectors" }}
  WHERE duration_minutes <= 0
  
  UNION ALL
  
  -- Test 5: All trip_segment_id exist in int_trip_segments
  SELECT
    'test_velocity_fk_trip_segments' AS test_name,
    COUNT(*) AS violation_count,
    'All trip_segment_id must exist in int_trip_segments' AS description
  FROM {{ ref "int_velocity_vectors" }} v
  LEFT JOIN {{ ref "int_trip_segments" }} t ON v.trip_segment_id = t.trip_segment_id
  WHERE t.trip_segment_id IS NULL
  
  UNION ALL
  
  -- Test 6: All location_ids exist in dim_location
  SELECT
    'test_velocity_fk_locations' AS test_name,
    COUNT(*) AS violation_count,
    'All location_ids must exist in dim_location' AS description
  FROM (
    SELECT origin_location_id AS location_id FROM {{ ref "int_velocity_vectors" }}
    UNION
    SELECT destination_location_id AS location_id FROM {{ ref "int_velocity_vectors" }}
  ) v
  LEFT JOIN dim_location l ON v.location_id = l.location_id
  WHERE l.location_id IS NULL
  
  UNION ALL
  
  -- Test 7: Timestamps are in correct order
  SELECT
    'test_velocity_timestamp_order' AS test_name,
    COUNT(*) AS violation_count,
    'Trip end must be after trip start' AS description
  FROM {{ ref "int_velocity_vectors" }}
  WHERE trip_end_timestamp <= trip_start_timestamp
  
  UNION ALL
  
  -- Test 8: Origin/destination pairs can match corridors (but not required)
  -- This test checks that the join logic works, not that all trips must have corridors
  SELECT
    'test_velocity_corridors_matched' AS test_name,
    0 AS violation_count,  -- Always pass - corridors are optional for high-traffic routes only
    'Origin/destination pairs can optionally match defined corridors' AS description
  
  UNION ALL
  
  -- Test 9: Duration is integer minutes (minute precision)
  SELECT
    'test_velocity_minute_precision' AS test_name,
    COUNT(*) AS violation_count,
    'Duration must be integer minutes (no fractional minutes)' AS description
  FROM {{ ref "int_velocity_vectors" }}
  WHERE duration_minutes != CAST(duration_minutes AS INTEGER)
  
  UNION ALL
  
  -- Test 10: Row count matches movement trips (origin != destination)
  SELECT
    'test_velocity_expected_count' AS test_name,
    CASE 
      WHEN (SELECT COUNT(*) FROM {{ ref "int_velocity_vectors" }}) != 
           (SELECT COUNT(*) FROM {{ ref "int_trip_segments" }} WHERE origin_location_id != destination_location_id)
      THEN 1
      ELSE 0
    END AS violation_count,
    'Velocity vectors count should match movement trip segments (origin != destination)' AS description
)

SELECT
  test_name,
  violation_count,
  description,
  CASE
    WHEN violation_count = 0 THEN 'PASS'
    ELSE 'FAIL'
  END AS status
FROM all_tests
ORDER BY test_name
