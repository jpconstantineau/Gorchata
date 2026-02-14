-- Data Quality Tests: seasonal_performance_trends
-- Tests for seasonal performance trends analytics query

WITH tests AS (

  -- Test 1: Query Executes Without Error
  SELECT
    'test_seasonal_trends_executes' AS test_name,
    0 AS violation_count,
    'Query should execute without error' AS description,
    'PASS' AS status

  UNION ALL

  -- Test 2: Grouped by Time Period
  SELECT
    'test_seasonal_trends_time_grouping' AS test_name,
    CASE WHEN COUNT(DISTINCT time_period) > 0 THEN 0 ELSE 1 END AS violation_count,
    'Results should be grouped by distinct time periods' AS description,
    CASE WHEN COUNT(DISTINCT time_period) > 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM seasonal_performance_trends

  UNION ALL

  -- Test 3: Valid Quarter Values
  SELECT
    'test_seasonal_trends_valid_quarters' AS test_name,
    COUNT(*) AS violation_count,
    'Quarter should be between 1 and 4' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM seasonal_performance_trends
  WHERE quarter < 1 OR quarter > 4

  UNION ALL

  -- Test 4: Valid Velocity Range
  SELECT
    'test_seasonal_trends_velocity_range' AS test_name,
    COUNT(*) AS violation_count,
    'Average velocity should be between 0 and 80 mph' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM seasonal_performance_trends
  WHERE avg_velocity_mph < 0 OR avg_velocity_mph > 80

  UNION ALL

  -- Test 5: Positive Trip Counts
  SELECT
    'test_seasonal_trends_positive_trips' AS test_name,
    COUNT(*) AS violation_count,
    'Trip count must be > 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM seasonal_performance_trends
  WHERE trip_count <= 0

)

SELECT * FROM tests
ORDER BY test_name
