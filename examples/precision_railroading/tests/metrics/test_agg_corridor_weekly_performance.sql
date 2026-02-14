-- Data Quality Tests: agg_corridor_weekly_performance
-- Tests for corridor weekly performance time series

WITH tests AS (

  -- Test 1: Trip Count Positive
  SELECT
    'test_corridor_weekly_trip_count_positive' AS test_name,
    COUNT(*) AS violation_count,
    'Trip count must be > 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM agg_corridor_weekly_performance
  WHERE trip_count <= 0

  UNION ALL

  -- Test 2: Velocity Reasonable
  SELECT
    'test_corridor_weekly_velocity_reasonable' AS test_name,
    COUNT(*) AS violation_count,
    'Average velocity mph must be between 0 and 80' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM agg_corridor_weekly_performance
  WHERE avg_velocity_mph < 0 OR avg_velocity_mph > 80

  UNION ALL

  -- Test 3: FK Corridors
  SELECT
    'test_corridor_weekly_fk_corridors' AS test_name,
    COUNT(*) AS violation_count,
    'All corridor_id values must exist in dim_corridor or be NULL' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM agg_corridor_weekly_performance cwp
  LEFT JOIN dim_corridor dc ON cwp.corridor_id = dc.corridor_id
  WHERE cwp.corridor_id IS NOT NULL AND dc.corridor_id IS NULL

  UNION ALL

  -- Test 4: Time Period Format
  SELECT
    'test_corridor_weekly_time_period_format' AS test_name,
    COUNT(*) AS violation_count,
    'Week format should match YYYY-WNN pattern' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM agg_corridor_weekly_performance
  WHERE week_period NOT LIKE '____-W__'

)

SELECT * FROM tests
ORDER BY test_name
