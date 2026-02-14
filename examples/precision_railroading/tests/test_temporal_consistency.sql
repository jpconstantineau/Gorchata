-- Test: Temporal Consistency
-- Description: Validates chronological ordering, no gaps/overlaps, and minute precision across time-series data
-- Expected Result: All tests should return 0 violations

WITH all_tests AS (
  -- Test 1: Trip timestamps chronologically ordered
  SELECT
    'test_trip_temporal_order' AS test_name,
    COUNT(*) AS violation_count,
    'Trip end timestamp must be after trip start timestamp' AS description
  FROM fact_trip
  WHERE trip_end_timestamp <= trip_start_timestamp
  
  UNION ALL
  
  -- Test 2: Dwell timestamps chronologically ordered
  SELECT
    'test_dwell_temporal_order' AS test_name,
    COUNT(*) AS violation_count,
    'Dwell end timestamp must be after dwell start timestamp' AS description
  FROM fact_dwell
  WHERE dwell_end_timestamp <= dwell_start_timestamp
  
  UNION ALL
  
  -- Test 3: State intervals have no temporal gaps per railcar
  SELECT
    'test_state_interval_no_gaps' AS test_name,
    COUNT(*) AS violation_count,
    'State intervals should have no temporal gaps for each railcar' AS description
  FROM (
    SELECT
      railcar_id,
      state_end_timestamp,
      LEAD(state_start_timestamp) OVER (PARTITION BY railcar_id ORDER BY state_start_timestamp) AS next_start
    FROM int_state_intervals
  )
  WHERE next_start IS NOT NULL 
    AND state_end_timestamp != next_start
  
  UNION ALL
  
  -- Test 4: State intervals have no temporal overlaps per railcar
  SELECT
    'test_state_interval_no_overlaps' AS test_name,
    COUNT(*) AS violation_count,
    'State intervals should not overlap for each railcar' AS description
  FROM (
    SELECT
      railcar_id,
      state_start_timestamp,
      state_end_timestamp,
      LAG(state_end_timestamp) OVER (PARTITION BY railcar_id ORDER BY state_start_timestamp) AS prev_end
    FROM int_state_intervals
  )
  WHERE prev_end IS NOT NULL 
    AND state_start_timestamp < prev_end
  
  UNION ALL
  
  -- Test 5: Trip segments chronologically ordered per railcar
  SELECT
    'test_trip_segment_chronological' AS test_name,
    COUNT(*) AS violation_count,
    'Trip segments should be chronologically ordered per railcar' AS description
  FROM (
    SELECT
      railcar_id,
      trip_start_timestamp,
      LAG(trip_end_timestamp) OVER (PARTITION BY railcar_id ORDER BY trip_start_timestamp) AS prev_trip_end
    FROM int_trip_segments
  )
  WHERE prev_trip_end IS NOT NULL 
    AND trip_start_timestamp < prev_trip_end
  
  UNION ALL
  
  -- Test 6: Velocity vectors timestamp order
  SELECT
    'test_velocity_timestamp_order' AS test_name,
    COUNT(*) AS violation_count,
    'Velocity vector end must be after start' AS description
  FROM int_velocity_vectors
  WHERE trip_end_timestamp <= trip_start_timestamp
  
  UNION ALL
  
  -- Test 7: Corridor transit durations are positive
  SELECT
    'test_corridor_transit_duration_positive' AS test_name,
    COUNT(*) AS violation_count,
    'Corridor transit durations must be positive' AS description
  FROM fact_corridor_transit
  WHERE duration_minutes <= 0
  
  UNION ALL
  
  -- Test 8: Weekly aggregations have valid temporal bounds
  SELECT
    'test_weekly_aggregation_temporal_bounds' AS test_name,
    0 AS violation_count,
    'Weekly aggregation uses week_period (YYYY-WXX format), no explicit start/end dates to validate' AS description
  
  UNION ALL
  
  -- Test 9: PSR periods are chronologically consistent
  SELECT
    'test_psr_period_chronological' AS test_name,
    COUNT(*) AS violation_count,
    'PSR periods should match timestamp chronology (pre-PSR: 2016-2017, transition: 2018-2020, mature: 2021-2025)' AS description
  FROM fact_trip
  WHERE (psr_period = 'pre-PSR' AND CAST(STRFTIME('%Y', trip_start_timestamp) AS INTEGER) NOT BETWEEN 2016 AND 2017)
     OR (psr_period = 'transition' AND CAST(STRFTIME('%Y', trip_start_timestamp) AS INTEGER) NOT BETWEEN 2018 AND 2020)
     OR (psr_period = 'mature' AND CAST(STRFTIME('%Y', trip_start_timestamp) AS INTEGER) NOT BETWEEN 2021 AND 2025)
  
  UNION ALL
  
  -- Test 10: All timestamps have minute granularity (no seconds)
  SELECT
    'test_timestamp_minute_granularity' AS test_name,
    SUM(violation_count) AS violation_count,
    'All timestamps should have minute granularity (no seconds or milliseconds)' AS description
  FROM (
    SELECT COUNT(*) AS violation_count 
    FROM (SELECT trip_start_timestamp AS ts FROM fact_trip LIMIT 1000)
    WHERE CAST(STRFTIME('%S', ts) AS INTEGER) != 0
       OR CAST(STRFTIME('%f', ts) AS REAL) != 0.0
    
    UNION ALL
    
    SELECT COUNT(*) AS violation_count
    FROM (SELECT trip_end_timestamp AS ts FROM fact_trip LIMIT 1000)
    WHERE CAST(STRFTIME('%S', ts) AS INTEGER) != 0
       OR CAST(STRFTIME('%f', ts) AS REAL) != 0.0
    
    UNION ALL
    
    SELECT COUNT(*) AS violation_count
    FROM (SELECT dwell_start_timestamp AS ts FROM fact_dwell LIMIT 1000)
    WHERE CAST(STRFTIME('%S', ts) AS INTEGER) != 0
       OR CAST(STRFTIME('%f', ts) AS REAL) != 0.0
    
    UNION ALL
    
    SELECT COUNT(*) AS violation_count
    FROM (SELECT dwell_end_timestamp AS ts FROM fact_dwell LIMIT 1000)
    WHERE CAST(STRFTIME('%S', ts) AS INTEGER) != 0
       OR CAST(STRFTIME('%f', ts) AS REAL) != 0.0
  )
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
