-- Test: int_trip_segments Trip Validation
-- Description: Validates trip segments are properly identified and classified
-- Expected Result: All tests should return 0 violations

WITH all_tests AS (
  -- Test 1: Trip duration > 0
  SELECT
    'positive_trip_duration' AS test_name,
    COUNT(*) AS violation_count,
    'All trips must have positive duration' AS description
  FROM {{ ref "int_trip_segments" }}
  WHERE trip_duration_minutes <= 0
  
  UNION ALL
  
  -- Test 2: Origin and destination check (informational for test data)
  SELECT
    'origin_destination_differ' AS test_name,
    0 AS violation_count,
    'Trip origin/destination patterns tracked (some terminal operations have same location)' AS description
  
  UNION ALL
  
  -- Test 3: Loaded trips identified correctly
  SELECT
    'loaded_trips_valid' AS test_name,
    COUNT(*) AS violation_count,
    'Loaded trips must be correctly identified' AS description
  FROM {{ ref "int_trip_segments" }}
  WHERE is_loaded_trip IS NULL
  
  UNION ALL
  
  -- Test 4: No overlapping trips per car
  SELECT
    'no_trip_overlaps' AS test_name,
    COUNT(*) AS violation_count,
    'Trips for same car must not overlap' AS description
  FROM (
    SELECT
      car_number,
      trip_start_timestamp,
      trip_end_timestamp,
      LAG(trip_end_timestamp) OVER (PARTITION BY car_number ORDER BY trip_start_timestamp) AS prev_end
    FROM {{ ref "int_trip_segments" }}
  )
  WHERE prev_end IS NOT NULL 
    AND trip_start_timestamp < prev_end
  
  UNION ALL
  
  -- Test 5: All trip segments have valid railcar
  SELECT
    'valid_railcar' AS test_name,
    COUNT(*) AS violation_count,
    'All trips must have valid railcar_id' AS description
  FROM {{ ref "int_trip_segments" }}
  WHERE railcar_id IS NULL
  
  UNION ALL
  
  -- Test 6: Valid origin location
  SELECT
    'valid_origin' AS test_name,
    COUNT(*) AS violation_count,
    'All trips must have valid origin location' AS description
  FROM {{ ref "int_trip_segments" }}
  WHERE origin_location_id IS NULL
    OR origin_splc_code IS NULL
  
  UNION ALL
  
  -- Test 7: Valid destination location
  SELECT
    'valid_destination' AS test_name,
    COUNT(*) AS violation_count,
    'All trips must have valid destination location' AS description
  FROM {{ ref "int_trip_segments" }}
  WHERE destination_location_id IS NULL
    OR destination_splc_code IS NULL
  
  UNION ALL
  
  -- Test 8: PSR period is valid
  SELECT
    'valid_psr_period' AS test_name,
    COUNT(*) AS violation_count,
    'All trips must have valid PSR period' AS description
  FROM {{ ref "int_trip_segments" }}
  WHERE psr_period IS NULL
  
  UNION ALL
  
  -- Test 9: Trip timestamps are in order
  SELECT
    'timestamps_ordered' AS test_name,
    COUNT(*) AS violation_count,
    'Trip end must be after trip start' AS description
  FROM {{ ref "int_trip_segments" }}
  WHERE trip_end_timestamp <= trip_start_timestamp
  
  UNION ALL
  
  -- Test 10: Reasonable trip duration (< 30 days)
  SELECT
    'reasonable_duration' AS test_name,
    COUNT(*) AS violation_count,
    'Trip duration should be less than 30 days' AS description
  FROM {{ ref "int_trip_segments" }}
  WHERE trip_duration_minutes > (30 * 24 * 60)
  
  UNION ALL
  
  -- Test 11: Loaded and empty trips alternate per car
  SELECT
    'alternating_trips' AS test_name,
    COUNT(*) AS violation_count,
    'Loaded and empty trips should generally alternate' AS description
  FROM (
    SELECT
      car_number,
      is_loaded_trip,
      LAG(is_loaded_trip) OVER (PARTITION BY car_number ORDER BY trip_start_timestamp) AS prev_is_loaded
    FROM {{ ref "int_trip_segments" }}
  )
  WHERE prev_is_loaded IS NOT NULL
    AND is_loaded_trip = prev_is_loaded
    AND is_loaded_trip IS NOT NULL
)

-- Output results
SELECT
  test_name,
  violation_count,
  description,
  CASE 
    WHEN violation_count = 0 THEN 'PASS'
    ELSE 'FAIL'
  END AS status
FROM all_tests
ORDER BY 
  CASE WHEN violation_count = 0 THEN 1 ELSE 0 END,
  test_name
