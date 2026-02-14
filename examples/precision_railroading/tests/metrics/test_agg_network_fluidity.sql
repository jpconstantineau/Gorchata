-- Data Quality Tests: agg_network_fluidity
-- Tests for network fluidity metrics aggregation

WITH tests AS (

  -- Test 1: Fluidity Index Range
  SELECT
    'test_network_fluidity_range' AS test_name,
    COUNT(*) AS violation_count,
    'Fluidity index (mph) should be between 0 and 80' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "agg_network_fluidity" }}
  WHERE fluidity_index_mph < 0 OR fluidity_index_mph > 80

  UNION ALL

  -- Test 2: Positive Distance
  SELECT
    'test_network_fluidity_positive_distance' AS test_name,
    COUNT(*) AS violation_count,
    'Total distance miles must be >= 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "agg_network_fluidity" }}
  WHERE total_distance_miles < 0

  UNION ALL

  -- Test 3: Positive Duration
  SELECT
    'test_network_fluidity_positive_duration' AS test_name,
    COUNT(*) AS violation_count,
    'Total duration minutes must be > 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "agg_network_fluidity" }}
  WHERE total_duration_minutes <= 0

  UNION ALL

  -- Test 4: Positive Counts
  SELECT
    'test_network_fluidity_positive_counts' AS test_name,
    COUNT(*) AS violation_count,
    'Car count and trip count must be > 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "agg_network_fluidity" }}
  WHERE car_count <= 0 OR trip_count <= 0

  UNION ALL

  -- Test 5: FK Corridors
  SELECT
    'test_network_fluidity_fk_corridors' AS test_name,
    COUNT(*) AS violation_count,
    'All corridor_id values must exist in dim_corridor or be NULL' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "agg_network_fluidity" }} nf
  LEFT JOIN dim_corridor dc ON nf.corridor_id = dc.corridor_id
  WHERE nf.corridor_id IS NOT NULL AND dc.corridor_id IS NULL

)

SELECT * FROM tests
ORDER BY test_name
