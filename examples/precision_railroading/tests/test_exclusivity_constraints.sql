-- Test: Exclusivity Constraints
-- Description: Validates mutually exclusive states and classifications
-- Expected Result: All tests should return 0 violations

WITH all_tests AS (
  -- Test 1: Trip type is either loaded XOR empty (not both, not neither)
  SELECT
    'test_trip_type_exclusivity' AS test_name,
    COUNT(*) AS violation_count,
    'Each trip must be exactly one of: loaded or empty' AS description
  FROM fact_trip
  WHERE trip_type NOT IN ('loaded', 'empty')
  
  UNION ALL
  
  -- Test 2: Dwell classification is mutually exclusive (one type per dwell)
  SELECT
    'test_dwell_classification_exclusivity' AS test_name,
    COUNT(*) AS violation_count,
    'Each dwell must have exactly one classification' AS description
  FROM fact_dwell
  WHERE dwell_classification NOT IN ('operational', 'shadow_yard', 'interchange', 'customer', 'other')
  
  UNION ALL
  
  -- Test 3: State intervals don't overlap for same railcar
  SELECT
    'test_state_interval_no_railcar_overlap' AS test_name,
    COUNT(*) AS violation_count,
    'A railcar cannot be in two states simultaneously' AS description
  FROM (
    SELECT
      a.railcar_id,
      a.start_timestamp AS a_start,
      a.end_timestamp AS a_end,
      b.start_timestamp AS b_start,
      b.end_timestamp AS b_end
    FROM int_state_intervals a
    INNER JOIN int_state_intervals b
      ON a.railcar_id = b.railcar_id
      AND a.interval_id != b.interval_id
    WHERE a_start < b_end AND b_start < a_end  -- Overlap condition
  )
  
  UNION ALL
  
  -- Test 4: Cycle classification is unique per cycle
  SELECT
    'test_cycle_classification_unique' AS test_name,
    COUNT(*) - COUNT(DISTINCT cycle_id) AS violation_count,
    'Each cycle_id must appear exactly once in cycle classification' AS description
  FROM int_cycle_classification
  
  UNION ALL
  
  -- Test 5: Shadow yard flag is boolean (0 or 1)
  SELECT
    'test_shadow_yard_flag_exclusive' AS test_name,
    COUNT(*) AS violation_count,
    'Shadow yard flag must be 0 or 1' AS description
  FROM fact_dwell
  WHERE shadow_yard_flag NOT IN (0, 1)
  
  UNION ALL
  
  -- Test 6: Trip segments don't overlap for same railcar
  SELECT
    'test_trip_segment_no_overlap' AS test_name,
    COUNT(*) AS violation_count,
    'Trip segments for same railcar should not overlap in time' AS description
  FROM (
    SELECT
      a.railcar_id,
      a.trip_start_timestamp AS a_start,
      a.trip_end_timestamp AS a_end,
      b.trip_start_timestamp AS b_start,
      b.trip_end_timestamp AS b_end
    FROM int_trip_segments a
    INNER JOIN int_trip_segments b
      ON a.railcar_id = b.railcar_id
      AND a.trip_segment_id != b.trip_segment_id
    WHERE a_start < b_end AND b_start < a_end  -- Overlap condition
  )
  
  UNION ALL
  
  -- Test 7: Location classification is exclusive (one facility type per location)
  SELECT
    'test_location_classification_exclusive' AS test_name,
    COUNT(*) - COUNT(DISTINCT location_id) AS violation_count,
    'Each location must have exactly one facility type' AS description
  FROM dim_location
  
  UNION ALL
  
  -- Test 8: PSR periods don't overlap in time
  SELECT
    'test_psr_period_no_overlap' AS test_name,
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM fact_trip a
        INNER JOIN fact_trip b
          ON a.trip_segment_id != b.trip_segment_id
          AND a.psr_period != b.psr_period
          AND a.trip_start_timestamp = b.trip_start_timestamp
      )
      THEN 1
      ELSE 0
    END AS violation_count,
    'Trips at same timestamp should have same PSR period' AS description
)

SELECT
  test_name,
  violation_count,
  description,
  CASE
    WHEN violation_count = 0 THEN 'PASS'
    ELSE 'FAIL'
  END AS status
FROM all_tests
ORDER BY test_name
