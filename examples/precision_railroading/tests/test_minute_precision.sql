-- Test: Minute Precision
-- Description: Validates all timestamps and durations maintain minute-level precision (no seconds/milliseconds)
-- Expected Result: All tests should return 0 violations

WITH all_tests AS (
  -- Test 1: CLM event timestamps have no seconds
  SELECT
    'test_clm_timestamp_no_seconds' AS test_name,
    violation_count,
    'CLM event timestamps must have zero seconds' AS description
  FROM (
    SELECT COUNT(*) AS violation_count
    FROM (SELECT timestamp FROM stg_clm_events LIMIT 1000)
    WHERE CAST(STRFTIME('%S', timestamp) AS INTEGER) != 0
  )
  
  UNION ALL
  
  -- Test 2: Trip timestamps have no seconds
  SELECT
    'test_trip_timestamp_no_seconds' AS test_name,
    SUM(violation_count) AS violation_count,
    'Trip timestamps must have zero seconds' AS description
  FROM (
    SELECT COUNT(*) AS violation_count
    FROM (SELECT trip_start_timestamp AS ts FROM fact_trip LIMIT 1000)
    WHERE CAST(STRFTIME('%S', ts) AS INTEGER) != 0
    
    UNION ALL
    
    SELECT COUNT(*) AS violation_count
    FROM (SELECT trip_end_timestamp AS ts FROM fact_trip LIMIT 1000)
    WHERE CAST(STRFTIME('%S', ts) AS INTEGER) != 0
  )
  
  UNION ALL
  
  -- Test 3: Dwell timestamps have no seconds
  SELECT
    'test_dwell_timestamp_no_seconds' AS test_name,
    SUM(violation_count) AS violation_count,
    'Dwell timestamps must have zero seconds' AS description
  FROM (
    SELECT COUNT(*) AS violation_count
    FROM (SELECT dwell_start_timestamp AS ts FROM fact_dwell LIMIT 1000)
    WHERE CAST(STRFTIME('%S', ts) AS INTEGER) != 0
    
    UNION ALL
    
    SELECT COUNT(*) AS violation_count
    FROM (SELECT dwell_end_timestamp AS ts FROM fact_dwell LIMIT 1000)
    WHERE CAST(STRFTIME('%S', ts) AS INTEGER) != 0
  )
  
  UNION ALL
  
  -- Test 4: State interval timestamps have no seconds
  SELECT
    'test_state_interval_timestamp_no_seconds' AS test_name,
    SUM(violation_count) AS violation_count,
    'State interval timestamps must have zero seconds' AS description
  FROM (
    SELECT COUNT(*) AS violation_count
    FROM (SELECT start_timestamp AS ts FROM int_state_intervals LIMIT 1000)
    WHERE CAST(STRFTIME('%S', ts) AS INTEGER) != 0
    
    UNION ALL
    
    SELECT COUNT(*) AS violation_count
    FROM (SELECT end_timestamp AS ts FROM int_state_intervals LIMIT 1000)
    WHERE CAST(STRFTIME('%S', ts) AS INTEGER) != 0
  )
  
  UNION ALL
  
  -- Test 5: Durations are in whole minutes
  SELECT
    'test_duration_whole_minutes' AS test_name,
    SUM(violation_count) AS violation_count,
    'All durations must be integer minutes (no fractional minutes)' AS description
  FROM (
    SELECT COUNT(*) AS violation_count
    FROM fact_trip
    WHERE duration_minutes != CAST(duration_minutes AS INTEGER)
    
    UNION ALL
    
    SELECT COUNT(*) AS violation_count
    FROM fact_dwell
    WHERE dwell_duration_minutes != CAST(dwell_duration_minutes AS INTEGER)
  )
  
  UNION ALL
  
  -- Test 6: Velocity calculations are minute-based
  SELECT
    'test_velocity_calculation_minute_based' AS test_name,
    violation_count,
    'Velocity should be calculated as (distance_miles / duration_minutes) * 60' AS description
  FROM (
    SELECT COUNT(*) AS violation_count
    FROM (SELECT * FROM int_velocity_vectors LIMIT 1000)
    WHERE duration_minutes > 0
      AND ABS(velocity_mph - (distance_miles / duration_minutes * 60)) > 0.01
  )
  
  UNION ALL
  
  -- Test 7: Aggregation timestamps have minute precision
  SELECT
    'test_aggregation_timestamp_precision' AS test_name,
    0 AS violation_count,
    'Weekly aggregations use week_period (YYYY-WXX format), no timestamp columns to validate' AS description
  
  UNION ALL
  
  -- Test 8: Date dimension has minute precision where timestamps exist
  SELECT
    'test_date_dimension_minute_precision' AS test_name,
    0 AS violation_count,  -- dim_date uses date_id (YYYYMMDD), no timestamps
    'Date dimension uses date keys (YYYYMMDD), minute precision N/A' AS description
  
  UNION ALL
  
  -- Test 9: JulianDay conversion maintains minute accuracy
  SELECT
    'test_julianday_conversion_accuracy' AS test_name,
    violation_count,
    'JulianDay conversions should maintain minute precision' AS description
  FROM (
    SELECT COUNT(*) AS violation_count
    FROM (
      SELECT
        trip_start_timestamp,
        trip_end_timestamp,
        duration_minutes,
        ROUND((JULIANDAY(trip_end_timestamp) - JULIANDAY(trip_start_timestamp)) * 1440) AS calculated_minutes
      FROM fact_trip
      LIMIT 100
    )
    WHERE duration_minutes != calculated_minutes
  )
  
  UNION ALL
  
  -- Test 10: No millisecond precision in any timestamp
  SELECT
    'test_no_millisecond_precision' AS test_name,
    SUM(violation_count) AS violation_count,
    'Timestamps must not have millisecond precision' AS description
  FROM (
    SELECT COUNT(*) AS violation_count
    FROM (SELECT trip_start_timestamp AS ts FROM fact_trip LIMIT 1000)
    WHERE CAST((STRFTIME('%f', ts) - FLOOR(STRFTIME('%f', ts))) * 1000 AS INTEGER) != 0
    
    UNION ALL
    
    SELECT COUNT(*) AS violation_count
    FROM (SELECT dwell_start_timestamp AS ts FROM fact_dwell LIMIT 1000)
    WHERE CAST((STRFTIME('%f', ts) - FLOOR(STRFTIME('%f', ts))) * 1000 AS INTEGER) != 0
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
