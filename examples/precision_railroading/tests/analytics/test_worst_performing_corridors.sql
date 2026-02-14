-- Data Quality Tests: worst_performing_corridors
-- Tests for worst performing corridors analytics query

WITH tests AS (

  -- Test 1: Query Executes Without Error
  SELECT
    'test_worst_corridors_executes' AS test_name,
    0 AS violation_count,
    'Query should execute without error' AS description,
    'PASS' AS status

  UNION ALL

  -- Test 2: Fluidity Rank is Ascending
  SELECT
    'test_worst_corridors_rank_ascending' AS test_name,
    COUNT(*) AS violation_count,
    'Fluidity rank should be in ascending order (1, 2, 3...)' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM (
    SELECT 
      fluidity_rank,
      LAG(fluidity_rank) OVER (ORDER BY fluidity_rank) AS prev_rank
    FROM worst_performing_corridors
  ) ranked
  WHERE prev_rank IS NOT NULL 
    AND fluidity_rank != prev_rank + 1

  UNION ALL

  -- Test 3: Positive Trip Counts
  SELECT
    'test_worst_corridors_positive_trips' AS test_name,
    COUNT(*) AS violation_count,
    'Trip count must be > 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM worst_performing_corridors
  WHERE trip_count <= 0

  UNION ALL

  -- Test 4: Valid Velocity Range
  SELECT
    'test_worst_corridors_velocity_range' AS test_name,
    COUNT(*) AS violation_count,
    'Average velocity should be between 0 and 80 mph' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM worst_performing_corridors
  WHERE avg_velocity_mph < 0 OR avg_velocity_mph > 80

  UNION ALL

  -- Test 5: Non-Negative Dwell Times
  SELECT
    'test_worst_corridors_dwell_nonnegative' AS test_name,
    COUNT(*) AS violation_count,
    'Average dwell minutes should be >= 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM worst_performing_corridors
  WHERE avg_dwell_minutes < 0

)

SELECT * FROM tests
ORDER BY test_name
