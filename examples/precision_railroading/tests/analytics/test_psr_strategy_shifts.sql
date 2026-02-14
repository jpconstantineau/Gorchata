-- Data Quality Tests: psr_strategy_shifts
-- Tests for PSR strategy shifts analytics query

WITH tests AS (

  -- Test 1: Query Executes Without Error
  SELECT
    'test_psr_shifts_executes' AS test_name,
    0 AS violation_count,
    'Query should execute without error' AS description,
    'PASS' AS status

  UNION ALL

  -- Test 2: Includes At Least One PSR Period
  SELECT
    'test_psr_shifts_at_least_one_period' AS test_name,
    CASE WHEN COUNT(DISTINCT psr_period) >= 1 THEN 0 ELSE 1 END AS violation_count,
    'Should include at least 1 PSR period with actual data' AS description,
    CASE WHEN COUNT(DISTINCT psr_period) >= 1 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM psr_strategy_shifts

  UNION ALL

  -- Test 3: Valid Velocity Range
  SELECT
    'test_psr_shifts_velocity_range' AS test_name,
    COUNT(*) AS violation_count,
    'Average velocity should be between 0 and 80 mph' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM psr_strategy_shifts
  WHERE avg_velocity_mph < 0 OR avg_velocity_mph > 80

  UNION ALL

  -- Test 4: Positive Duration
  SELECT
    'test_psr_shifts_positive_duration' AS test_name,
    COUNT(*) AS violation_count,
    'Average duration minutes must be > 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM psr_strategy_shifts
  WHERE avg_duration_minutes <= 0

  UNION ALL

  -- Test 5: Positive Trip Counts
  SELECT
    'test_psr_shifts_positive_trips' AS test_name,
    COUNT(*) AS violation_count,
    'Trip count must be > 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM psr_strategy_shifts
  WHERE trip_count <= 0

)

SELECT * FROM tests
ORDER BY test_name
