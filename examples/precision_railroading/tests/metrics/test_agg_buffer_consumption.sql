-- Data Quality Tests: agg_buffer_consumption
-- Tests for schedule buffer consumption metrics

WITH tests AS (

  -- Test 1: Scheduled Positive
  SELECT
    'test_buffer_consumption_scheduled_positive' AS test_name,
    COUNT(*) AS violation_count,
    'Average scheduled duration must be > 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM agg_buffer_consumption
  WHERE avg_scheduled_duration_minutes <= 0

  UNION ALL

  -- Test 2: Actual Positive
  SELECT
    'test_buffer_consumption_actual_positive' AS test_name,
    COUNT(*) AS violation_count,
    'Average actual duration must be > 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM agg_buffer_consumption
  WHERE avg_actual_duration_minutes <= 0

  UNION ALL

  -- Test 3: Percentage Reasonable
  SELECT
    'test_buffer_consumption_percentage_reasonable' AS test_name,
    COUNT(*) AS violation_count,
    'Buffer consumption percentage should be within -50% to 200%' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM agg_buffer_consumption
  WHERE buffer_consumption_percentage < -50 OR buffer_consumption_percentage > 200

  UNION ALL

  -- Test 4: FK Corridors
  SELECT
    'test_buffer_consumption_fk_corridors' AS test_name,
    COUNT(*) AS violation_count,
    'All corridor_id values must exist in dim_corridor or be NULL' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM agg_buffer_consumption bc
  LEFT JOIN dim_corridor dc ON bc.corridor_id = dc.corridor_id
  WHERE bc.corridor_id IS NOT NULL AND dc.corridor_id IS NULL

)

SELECT * FROM tests
ORDER BY test_name
