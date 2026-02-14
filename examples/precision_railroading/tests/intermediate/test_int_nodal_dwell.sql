-- Test: int_nodal_dwell Dwell Time Validation
-- Description: Validates dwell times are positive, meet minimum thresholds, and identify stops correctly
-- Expected Result: All tests should return 0 violations

WITH all_tests AS (
  -- Test 1: Dwell duration is positive
  SELECT
    'test_dwell_positive_duration' AS test_name,
    COUNT(*) AS violation_count,
    'Dwell duration must be > 0 minutes' AS description
  FROM int_nodal_dwell
  WHERE dwell_duration_minutes <= 0
  
  UNION ALL
  
  -- Test 2: Dwell meets minimum threshold (>= 5 minutes)
  SELECT
    'test_dwell_minimum_threshold' AS test_name,
    COUNT(*) AS violation_count,
    'Dwell duration must be >= 5 minutes (filters insignificant stops)' AS description
  FROM int_nodal_dwell
  WHERE dwell_duration_minutes < 5
  
  UNION ALL
  
  -- Test 3: Timestamps are in correct order
  SELECT
    'test_dwell_timestamp_order' AS test_name,
    COUNT(*) AS violation_count,
    'Dwell end must be after dwell start' AS description
  FROM int_nodal_dwell
  WHERE dwell_end_timestamp <= dwell_start_timestamp
  
  UNION ALL
  
  -- Test 4: All railcar_ids are valid
  SELECT
    'test_dwell_fk_railcars' AS test_name,
    COUNT(*) AS violation_count,
    'All railcar_id must exist in dim_railcar' AS description
  FROM int_nodal_dwell d
  LEFT JOIN dim_railcar r ON d.railcar_id = r.railcar_id
  WHERE r.railcar_id IS NULL
  
  UNION ALL
  
  -- Test 5: All location_ids are valid
  SELECT
    'test_dwell_fk_locations' AS test_name,
    COUNT(*) AS violation_count,
    'All location_id must exist in dim_location' AS description
  FROM int_nodal_dwell d
  LEFT JOIN dim_location l ON d.location_id = l.location_id
  WHERE l.location_id IS NULL
  
  UNION ALL
  
  -- Test 6: Derived from intervals where start_location = end_location
  SELECT
    'test_dwell_same_location' AS test_name,
    COUNT(*) AS violation_count,
    'Dwell events must be derived from intervals with same start/end location' AS description
  FROM int_nodal_dwell d
  LEFT JOIN int_state_intervals i 
    ON d.railcar_id = i.railcar_id 
    AND d.dwell_start_timestamp = i.start_timestamp
  WHERE i.start_location_id != i.end_location_id
  
  UNION ALL
  
  -- Test 7: Duration is integer minutes (minute precision)
  SELECT
    'test_dwell_minute_precision' AS test_name,
    COUNT(*) AS violation_count,
    'Duration must be integer minutes (no fractional minutes)' AS description
  FROM int_nodal_dwell
  WHERE dwell_duration_minutes != CAST(dwell_duration_minutes AS INTEGER)
  
  UNION ALL
  
  -- Test 8: No movement during dwell
  SELECT
    'test_dwell_no_movement' AS test_name,
    COUNT(*) AS violation_count,
    'Dwell events represent stops (no origin->destination movement)' AS description
  FROM int_nodal_dwell d
  JOIN int_state_intervals i 
    ON d.railcar_id = i.railcar_id 
    AND d.dwell_start_timestamp = i.start_timestamp
  WHERE i.start_location_id IS NULL 
    OR i.end_location_id IS NULL 
    OR i.start_location_id != i.end_location_id
  
  UNION ALL
  
  -- Test 9: is_loaded flag is valid (0 or 1)
  SELECT
    'test_dwell_loaded_flag_valid' AS test_name,
    COUNT(*) AS violation_count,
    'is_loaded must be 0 or 1' AS description
  FROM int_nodal_dwell
  WHERE is_loaded NOT IN (0, 1)
  
  UNION ALL
  
  -- Test 10: Expected pattern - reasonable dwell count
  SELECT
    'test_dwell_expected_pattern' AS test_name,
    CASE 
      WHEN (SELECT COUNT(*) FROM int_nodal_dwell) < 10
        OR (SELECT COUNT(*) FROM int_nodal_dwell) > 100
      THEN 1
      ELSE 0
    END AS violation_count,
    'Expected 10-100 dwell events from intervals (depends on data)' AS description
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
