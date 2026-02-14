-- Data Quality Tests: agg_shadow_yards
-- Tests for shadow yard detection aggregation

WITH tests AS (

  -- Test 1: Location Count
  SELECT
    'test_shadow_yards_location_count' AS test_name,
    CASE WHEN COUNT(*) >= 1 THEN 0 ELSE 1 END AS violation_count,
    'Should have at least 1 location analyzed' AS description,
    CASE WHEN COUNT(*) >= 1 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "agg_shadow_yards" }}

  UNION ALL

  -- Test 2: Percentage Range
  SELECT
    'test_shadow_yards_percentage_range' AS test_name,
    COUNT(*) AS violation_count,
    'Shadow yard percentage must be between 0 and 100' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "agg_shadow_yards" }}
  WHERE shadow_yard_percentage < 0 OR shadow_yard_percentage > 100

  UNION ALL

  -- Test 3: Positive Counts
  SELECT
    'test_shadow_yards_positive_counts' AS test_name,
    COUNT(*) AS violation_count,
    'Total dwell events must be > 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "agg_shadow_yards" }}
  WHERE total_dwell_events <= 0

  UNION ALL

  -- Test 4: Avg Duration Positive
  SELECT
    'test_shadow_yards_avg_duration_positive' AS test_name,
    COUNT(*) AS violation_count,
    'Average dwell duration minutes must be > 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "agg_shadow_yards" }}
  WHERE avg_dwell_duration_minutes <= 0

  UNION ALL

  -- Test 5: FK Locations
  SELECT
    'test_shadow_yards_fk_locations' AS test_name,
    COUNT(*) AS violation_count,
    'All location_id values must exist in dim_location' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "agg_shadow_yards" }} sy
  LEFT JOIN dim_location dl ON sy.location_id = dl.location_id
  WHERE dl.location_id IS NULL

  UNION ALL

  -- Test 6: Detection Threshold
  SELECT
    'test_shadow_yards_detection_threshold' AS test_name,
    CASE WHEN COUNT(*) >= 1 THEN 0 ELSE 1 END AS violation_count,
    'At least 1 location should have shadow_yard_percentage > 30%' AS description,
    CASE WHEN COUNT(*) >= 1 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "agg_shadow_yards" }}
  WHERE shadow_yard_percentage > 30

)

SELECT * FROM tests
ORDER BY test_name
