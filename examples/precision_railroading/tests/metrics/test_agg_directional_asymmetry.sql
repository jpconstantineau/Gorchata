-- Data Quality Tests: agg_directional_asymmetry
-- Tests for loaded vs empty trip asymmetry analysis

WITH tests AS (

  -- Test 1: Trip Counts Positive
  SELECT
    'test_directional_asymmetry_trip_counts_positive' AS test_name,
    COUNT(*) AS violation_count,
    'Loaded and empty trip counts must be > 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "agg_directional_asymmetry" }}
  WHERE loaded_trip_count <= 0 OR empty_trip_count <= 0

  UNION ALL

  -- Test 2: Velocity Positive
  SELECT
    'test_directional_asymmetry_velocity_positive' AS test_name,
    COUNT(*) AS violation_count,
    'Velocities must be >= 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "agg_directional_asymmetry" }}
  WHERE loaded_avg_velocity_mph < 0 OR empty_avg_velocity_mph < 0

  UNION ALL

  -- Test 3: Ratio Positive
  SELECT
    'test_directional_asymmetry_ratio_positive' AS test_name,
    COUNT(*) AS violation_count,
    'Asymmetry ratio must be > 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "agg_directional_asymmetry" }}
  WHERE asymmetry_ratio <= 0

  UNION ALL

  -- Test 4: FK Corridors
  SELECT
    'test_directional_asymmetry_fk_corridors' AS test_name,
    COUNT(*) AS violation_count,
    'All corridor_id values must exist in dim_corridor or be NULL' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "agg_directional_asymmetry" }} da
  LEFT JOIN dim_corridor dc ON da.corridor_id = dc.corridor_id
  WHERE da.corridor_id IS NOT NULL AND dc.corridor_id IS NULL

  UNION ALL

  -- Test 5: Priority Direction Valid
  SELECT
    'test_directional_asymmetry_priority_direction_valid' AS test_name,
    COUNT(*) AS violation_count,
    'Priority direction must be loaded, empty, or balanced' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "agg_directional_asymmetry" }}
  WHERE priority_direction NOT IN ('loaded', 'empty', 'balanced')

)

SELECT * FROM tests
ORDER BY test_name
