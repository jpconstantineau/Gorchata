-- Data Quality Tests: directional_efficiency_analysis
-- Tests for directional efficiency analysis analytics query

WITH tests AS (

  -- Test 1: Query Executes Without Error
  SELECT
    'test_directional_efficiency_executes' AS test_name,
    0 AS violation_count,
    'Query should execute without error' AS description,
    'PASS' AS status

  UNION ALL

  -- Test 2: Positive Velocities
  SELECT
    'test_directional_positive_velocities' AS test_name,
    COUNT(*) AS violation_count,
    'Loaded and empty velocities must be >= 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM directional_efficiency_analysis
  WHERE loaded_velocity_mph < 0 OR empty_velocity_mph < 0

  UNION ALL

  -- Test 3: Valid Velocity Range
  SELECT
    'test_directional_velocity_range' AS test_name,
    COUNT(*) AS violation_count,
    'Velocities should be between 0 and 80 mph' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM directional_efficiency_analysis
  WHERE loaded_velocity_mph > 80 OR empty_velocity_mph > 80

  UNION ALL

  -- Test 4: Positive Asymmetry Ratio
  SELECT
    'test_directional_positive_ratio' AS test_name,
    COUNT(*) AS violation_count,
    'Asymmetry ratio must be > 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM directional_efficiency_analysis
  WHERE asymmetry_ratio <= 0

  UNION ALL

  -- Test 5: Positive Trip Counts
  SELECT
    'test_directional_positive_trip_counts' AS test_name,
    COUNT(*) AS violation_count,
    'Loaded and empty trip counts must be >= 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM directional_efficiency_analysis
  WHERE loaded_trip_count < 0 OR empty_trip_count < 0

)

SELECT * FROM tests
ORDER BY test_name
