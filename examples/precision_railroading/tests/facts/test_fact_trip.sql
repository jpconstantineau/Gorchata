-- Data Quality Tests: fact_trip
-- Tests for the trip fact table following the test pattern from Phase 5

WITH tests AS (

  -- Test 1: FK Integrity - Railcars
  SELECT
    'test_fact_trip_fk_railcars' AS test_name,
    COUNT(*) AS violation_count,
    'All railcar_id values must exist in dim_railcar' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_trip" }} ft
  LEFT JOIN dim_railcar dr ON ft.railcar_id = dr.railcar_id
  WHERE dr.railcar_id IS NULL

  UNION ALL

  -- Test 2: FK Integrity - Trains
  SELECT
    'test_fact_trip_fk_trains' AS test_name,
    COUNT(*) AS violation_count,
    'All train_id values must exist in dim_train' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_trip" }} ft
  LEFT JOIN dim_train dt ON ft.train_id = dt.train_id
  WHERE dt.train_id IS NULL

  UNION ALL

  -- Test 3: FK Integrity - Origin Locations
  SELECT
    'test_fact_trip_fk_origin_locations' AS test_name,
    COUNT(*) AS violation_count,
    'All origin_location_id values must exist in dim_location' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_trip" }} ft
  LEFT JOIN dim_location dl ON ft.origin_location_id = dl.location_id
  WHERE dl.location_id IS NULL

  UNION ALL

  -- Test 4: FK Integrity - Destination Locations
  SELECT
    'test_fact_trip_fk_destination_locations' AS test_name,
    COUNT(*) AS violation_count,
    'All destination_location_id values must exist in dim_location' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_trip" }} ft
  LEFT JOIN dim_location dl ON ft.destination_location_id = dl.location_id
  WHERE dl.location_id IS NULL

  UNION ALL

  -- Test 5: FK Integrity - Dates
  SELECT
    'test_fact_trip_fk_dates' AS test_name,
    COUNT(*) AS violation_count,
    'All trip_start_date_id values must exist in dim_date' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_trip" }} ft
  LEFT JOIN dim_date dd ON ft.trip_start_date_id = dd.date_id
  WHERE dd.date_id IS NULL

  UNION ALL

  -- Test 6: Duration Positive
  SELECT
    'test_fact_trip_duration_positive' AS test_name,
    COUNT(*) AS violation_count,
    'Duration must be positive (> 0 minutes)' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_trip" }}
  WHERE duration_minutes <= 0

  UNION ALL

  -- Test 7: Timestamp Order
  SELECT
    'test_fact_trip_timestamp_order' AS test_name,
    COUNT(*) AS violation_count,
    'Trip end timestamp must be after trip start timestamp' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_trip" }}
  WHERE trip_end_timestamp <= trip_start_timestamp

  UNION ALL

  -- Test 8: Trip Type Valid
  SELECT
    'test_fact_trip_type_valid' AS test_name,
    COUNT(*) AS violation_count,
    'Trip type must be loaded or empty' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_trip" }}
  WHERE trip_type NOT IN ('loaded', 'empty')

  UNION ALL

  -- Test 9: Dwell Count Non-Negative
  SELECT
    'test_fact_trip_dwell_count_nonnegative' AS test_name,
    COUNT(*) AS violation_count,
    'Dwell count must be non-negative' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_trip" }}
  WHERE dwell_count < 0

  UNION ALL

  -- Test 10: PSR Period Valid
  SELECT
    'test_fact_trip_psr_period_valid' AS test_name,
    COUNT(*) AS violation_count,
    'PSR period must be pre-PSR, transition, or mature' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_trip" }}
  WHERE psr_period NOT IN ('pre-PSR', 'transition', 'mature')

  UNION ALL

  -- Test 11: Row Count
  SELECT
    'test_fact_trip_row_count' AS test_name,
    CASE WHEN COUNT(*) >= 25 AND COUNT(*) <= 35 THEN 0 ELSE ABS(COUNT(*) - 30) END AS violation_count,
    'Row count should be approximately 30 (matches int_trip_segments)' AS description,
    CASE WHEN COUNT(*) >= 25 AND COUNT(*) <= 35 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_trip" }}

  UNION ALL

  -- Test 12: Velocity Reasonable
  SELECT
    'test_fact_trip_velocity_reasonable' AS test_name,
    COUNT(*) AS violation_count,
    'Average velocity must be NULL or between 0 and 80 mph' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_trip" }}
  WHERE average_velocity_mph IS NOT NULL 
    AND (average_velocity_mph < 0 OR average_velocity_mph > 80)

)

SELECT * FROM tests
ORDER BY test_name
