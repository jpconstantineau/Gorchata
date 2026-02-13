-- Data Quality Tests: fact_corridor_transit
-- Tests for the aggregated corridor transit fact table

WITH tests AS (

  -- Test 1: FK Integrity - Corridors (NULL allowed)
  SELECT
    'test_fact_corridor_transit_fk_corridors' AS test_name,
    COUNT(*) AS violation_count,
    'All non-NULL corridor_id values must exist in dim_corridor' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_corridor_transit" }} fct
  LEFT JOIN dim_corridor dc ON fct.corridor_id = dc.corridor_id
  WHERE fct.corridor_id IS NOT NULL AND dc.corridor_id IS NULL

  UNION ALL

  -- Test 2: Counts Positive
  SELECT
    'test_fact_corridor_transit_counts_positive' AS test_name,
    COUNT(*) AS violation_count,
    'Car count and trip count must be positive' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_corridor_transit" }}
  WHERE car_count <= 0 OR trip_count <= 0

  UNION ALL

  -- Test 3: Velocity Reasonable
  SELECT
    'test_fact_corridor_transit_velocity_reasonable' AS test_name,
    COUNT(*) AS violation_count,
    'Average velocity must be NULL or between 0 and 80 mph' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_corridor_transit" }}
  WHERE average_velocity_mph IS NOT NULL 
    AND (average_velocity_mph < 0 OR average_velocity_mph > 80)

  UNION ALL

  -- Test 4: Trip Type Sum
  SELECT
    'test_fact_corridor_transit_trip_type_sum' AS test_name,
    COUNT(*) AS violation_count,
    'Loaded + empty trip count must equal total trip count' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_corridor_transit" }}
  WHERE (loaded_trip_count + empty_trip_count) != trip_count

  UNION ALL

  -- Test 5: Aggregation Logic - Distance Sum
  SELECT
    'test_fact_corridor_transit_distance_sum' AS test_name,
    COUNT(*) AS violation_count,
    'Total distance must be non-negative' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_corridor_transit" }}
  WHERE total_distance_miles < 0

  UNION ALL

  -- Test 6: Duration Positive
  SELECT
    'test_fact_corridor_transit_duration_positive' AS test_name,
    COUNT(*) AS violation_count,
    'Total duration and average trip duration must be positive' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_corridor_transit" }}
  WHERE total_duration_minutes <= 0 OR average_trip_duration_minutes <= 0

  UNION ALL

  -- Test 7: Dwell Count Non-Negative
  SELECT
    'test_fact_corridor_transit_dwell_count_nonnegative' AS test_name,
    COUNT(*) AS violation_count,
    'Average dwell count must be non-negative' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_corridor_transit" }}
  WHERE average_dwell_count < 0

)

SELECT * FROM tests
ORDER BY test_name
