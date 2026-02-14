-- Data Quality Tests: agg_slot_adherence
-- Tests for slot adherence metrics aggregation

WITH tests AS (

  -- Test 1: Adherence Score Range
  SELECT
    'test_slot_adherence_score_range' AS test_name,
    COUNT(*) AS violation_count,
    'Adherence score must be between 0 and 100' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM agg_slot_adherence
  WHERE adherence_score < 0 OR adherence_score > 100

  UNION ALL

  -- Test 2: Positive Count
  SELECT
    'test_slot_adherence_positive_count' AS test_name,
    COUNT(*) AS violation_count,
    'Arrival count must be > 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM agg_slot_adherence
  WHERE arrival_count <= 0

  UNION ALL

  -- Test 3: Stddev Non-Negative
  SELECT
    'test_slot_adherence_stddev_nonnegative' AS test_name,
    COUNT(*) AS violation_count,
    'Standard deviation hours must be >= 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM agg_slot_adherence
  WHERE stddev_hours < 0

  UNION ALL

  -- Test 4: FK Locations
  SELECT
    'test_slot_adherence_fk_locations' AS test_name,
    COUNT(*) AS violation_count,
    'All location_id values must exist in dim_location' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM agg_slot_adherence sa
  LEFT JOIN dim_location dl ON sa.location_id = dl.location_id
  WHERE dl.location_id IS NULL

)

SELECT * FROM tests
ORDER BY test_name
