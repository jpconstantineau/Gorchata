-- Test: stg_clm_events Basic Staging Validation
-- Description: Validates basic staging layer data quality for CLM events
-- Expected Result: All tests should return 0 violations

WITH all_tests AS (
  -- Test 1: No duplicate event_ids
  SELECT
    'no_duplicate_event_ids' AS test_name,
    COUNT(*) AS violation_count,
    'Each event_id should appear exactly once' AS description
  FROM (
    SELECT event_id, COUNT(*) AS cnt
    FROM {{ ref "stg_clm_events" }}
    GROUP BY event_id
    HAVING COUNT(*) > 1
  )
  
  UNION ALL
  
  -- Test 2: Valid event types only
  SELECT
    'valid_event_types' AS test_name,
    COUNT(*) AS violation_count,
    'Event type must be DEPA, ARRI, PULL, or PLAC' AS description
  FROM {{ ref "stg_clm_events" }}
  WHERE event_type NOT IN ('DEPA', 'ARRI', 'PULL', 'PLAC')
  
  UNION ALL
  
  -- Test 3: All timestamps populated
  SELECT
    'timestamps_not_null' AS test_name,
    COUNT(*) AS violation_count,
    'All events must have timestamps' AS description
  FROM {{ ref "stg_clm_events" }}
  WHERE timestamp IS NULL
  
  UNION ALL
  
  -- Test 4: Timestamp minute precision (no seconds)
  SELECT
    'timestamp_minute_precision' AS test_name,
    COUNT(*) AS violation_count,
    'Timestamps should have minute precision only (no seconds or milliseconds)' AS description
  FROM {{ ref "stg_clm_events" }}
  WHERE CAST(strftime('%S', timestamp) AS INTEGER) != 0
  
  UNION ALL
  
  -- Test 5: All car numbers populated
  SELECT
    'car_numbers_not_null' AS test_name,
    COUNT(*) AS violation_count,
    'All events must have car numbers' AS description
  FROM {{ ref "stg_clm_events" }}
  WHERE car_number IS NULL OR TRIM(car_number) = ''
  
  UNION ALL
  
  -- Test 6: SPLC codes populated
  SELECT
    'splc_codes_not_null' AS test_name,
    COUNT(*) AS violation_count,
    'All events must have SPLC codes' AS description
  FROM {{ ref "stg_clm_events" }}
  WHERE splc_code IS NULL OR TRIM(splc_code) = ''
  
  UNION ALL
  
  -- Test 7: Event IDs are valid format
  SELECT
    'event_id_not_null' AS test_name,
    COUNT(*) AS violation_count,
    'All events must have event IDs' AS description
  FROM {{ ref "stg_clm_events" }}
  WHERE event_id IS NULL OR TRIM(event_id) = ''
  
  UNION ALL
  
  -- Test 8: Timestamps are within valid range (2016-2025)
  SELECT
    'timestamp_valid_range' AS test_name,
    COUNT(*) AS violation_count,
    'Timestamps must be within 2016-2025 date range' AS description
  FROM {{ ref "stg_clm_events" }}
  WHERE timestamp < '2016-01-01 00:00:00' 
     OR timestamp >= '2026-01-01 00:00:00'
  
  UNION ALL
  
  -- Test 9: Event key is unique (if generated)
  SELECT
    'unique_event_key' AS test_name,
    COUNT(*) AS violation_count,
    'Event key must be unique' AS description
  FROM (
    SELECT event_key, COUNT(*) AS cnt
    FROM {{ ref "stg_clm_events" }}
    GROUP BY event_key
    HAVING COUNT(*) > 1
  )
  
  UNION ALL
  
  -- Test 10: Load timestamp is populated
  SELECT
    'load_timestamp_not_null' AS test_name,
    COUNT(*) AS violation_count,
    'All records must have load timestamp' AS description
  FROM {{ ref "stg_clm_events" }}
  WHERE load_timestamp IS NULL
)

-- Return all violations
SELECT
  test_name,
  violation_count,
  description
FROM all_tests
WHERE violation_count > 0
ORDER BY test_name
