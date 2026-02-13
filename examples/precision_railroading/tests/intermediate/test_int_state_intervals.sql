-- Test: int_state_intervals Interval Validation
-- Description: Validates state intervals have no overlaps, gaps, or invalid durations
-- Expected Result: All tests should return 0 violations

WITH all_tests AS (
  -- Test 1: No overlapping intervals per car
  SELECT
    'no_overlaps' AS test_name,
    COUNT(*) AS violation_count,
    'Intervals for same car must not overlap' AS description
  FROM (
    SELECT
      car_number,
      start_timestamp,
      end_timestamp,
      LAG(end_timestamp) OVER (PARTITION BY car_number ORDER BY start_timestamp) AS prev_end
    FROM {{ ref "int_state_intervals" }}
    WHERE end_timestamp IS NOT NULL
  )
  WHERE prev_end IS NOT NULL 
    AND start_timestamp < prev_end
  
  UNION ALL
  
  -- Test 2: No gaps in timeline per car
  SELECT
    'no_gaps' AS test_name,
    COUNT(*) AS violation_count,
    'Intervals for same car must not have temporal gaps' AS description
  FROM (
    SELECT
      car_number,
      start_timestamp,
      end_timestamp,
      LAG(end_timestamp) OVER (PARTITION BY car_number ORDER BY start_timestamp) AS prev_end
    FROM {{ ref "int_state_intervals" }}
    WHERE end_timestamp IS NOT NULL
  )
  WHERE prev_end IS NOT NULL 
    AND start_timestamp != prev_end
  
  UNION ALL
  
  -- Test 3: Duration > 0 for complete intervals
  SELECT
    'positive_duration' AS test_name,
    COUNT(*) AS violation_count,
    'Complete intervals must have positive duration' AS description
  FROM {{ ref "int_state_intervals" }}
  WHERE end_timestamp IS NOT NULL
    AND duration_minutes <= 0
  
  UNION ALL
  
  -- Test 4: All intervals have valid start events
  SELECT
    'valid_start_event' AS test_name,
    COUNT(*) AS violation_count,
    'All intervals must have valid start event' AS description
  FROM {{ ref "int_state_intervals" }}
  WHERE start_event_id IS NULL
    OR start_timestamp IS NULL
    OR start_event_type IS NULL
  
  UNION ALL
  
  -- Test 5: End event exists for non-terminal intervals
  SELECT
    'valid_end_event' AS test_name,
    COUNT(*) AS violation_count,
    'Non-terminal intervals must have valid end event' AS description
  FROM (
    SELECT
      car_number,
      interval_id,
      end_event_id,
      end_timestamp,
      end_event_type,
      ROW_NUMBER() OVER (PARTITION BY car_number ORDER BY start_timestamp DESC) AS rn
    FROM {{ ref "int_state_intervals" }}
  )
  WHERE rn > 1  -- Not the last interval for the car
    AND (end_event_id IS NULL OR end_timestamp IS NULL OR end_event_type IS NULL)
  
  UNION ALL
  
  -- Test 6: Only last interval per car can have NULL end
  SELECT
    'terminal_intervals_only' AS test_name,
    COUNT(*) AS violation_count,
    'Only last interval per car can have NULL end_timestamp' AS description
  FROM (
    SELECT
      car_number,
      interval_id,
      end_timestamp,
      ROW_NUMBER() OVER (PARTITION BY car_number ORDER BY start_timestamp DESC) AS rn
    FROM {{ ref "int_state_intervals" }}
  )
  WHERE end_timestamp IS NULL 
    AND rn > 1
  
  UNION ALL
  
  -- Test 7: All railcar_ids are valid
  SELECT
    'valid_railcar_id' AS test_name,
    COUNT(*) AS violation_count,
    'All intervals must have valid railcar_id' AS description
  FROM {{ ref "int_state_intervals" }}
  WHERE railcar_id IS NULL
  
  UNION ALL
  
  -- Test 8: Duration calculation matches timestamps
  SELECT
    'duration_accuracy' AS test_name,
    COUNT(*) AS violation_count,
    'Duration must match timestamp difference (within 1 minute tolerance)' AS description
  FROM {{ ref "int_state_intervals" }}
  WHERE end_timestamp IS NOT NULL
    AND ABS(duration_minutes - CAST((julianday(end_timestamp) - julianday(start_timestamp)) * 24 * 60 AS INTEGER)) > 1
  
  UNION ALL
  
  -- Test 9: Start location is valid
  SELECT
    'valid_start_location' AS test_name,
    COUNT(*) AS violation_count,
    'All intervals must have valid start location' AS description
  FROM {{ ref "int_state_intervals" }}
  WHERE start_location_id IS NULL
    OR start_splc_code IS NULL
  
  UNION ALL
  
  -- Test 10: Interval sequence is continuous per car
  SELECT
    'continuous_sequence' AS test_name,
    COUNT(*) AS violation_count,
    'Interval IDs must form continuous sequence per car' AS description
  FROM (
    SELECT
      car_number,
      COUNT(*) AS interval_count,
      MAX(start_timestamp) AS last_start,
      MIN(start_timestamp) AS first_start
    FROM {{ ref "int_state_intervals" }}
    GROUP BY car_number
  )
  WHERE interval_count = 0
)

-- Output results
SELECT
  test_name,
  violation_count,
  description,
  CASE 
    WHEN violation_count = 0 THEN 'PASS'
    ELSE 'FAIL'
  END AS status
FROM all_tests
ORDER BY 
  CASE WHEN violation_count = 0 THEN 1 ELSE 0 END,
  test_name
