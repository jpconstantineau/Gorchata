-- Data Quality Tests: fact_stop_classification
-- Tests for the aggregated stop classification fact table

WITH tests AS (

  -- Test 1: FK Integrity - Trips
  SELECT
    'test_fact_stop_classification_fk_trips' AS test_name,
    COUNT(*) AS violation_count,
    'All trip_segment_id values must exist in fact_trip' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_stop_classification" }} fsc
  LEFT JOIN {{ ref "fact_trip" }} ft ON fsc.trip_segment_id = ft.trip_segment_id
  WHERE ft.trip_segment_id IS NULL

  UNION ALL

  -- Test 2: FK Integrity - Railcars
  SELECT
    'test_fact_stop_classification_fk_railcars' AS test_name,
    COUNT(*) AS violation_count,
    'All railcar_id values must exist in dim_railcar' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_stop_classification" }} fsc
  LEFT JOIN dim_railcar dr ON fsc.railcar_id = dr.railcar_id
  WHERE dr.railcar_id IS NULL

  UNION ALL

  -- Test 3: Timestamp Order
  SELECT
    'test_fact_stop_classification_timestamp_order' AS test_name,
    COUNT(*) AS violation_count,
    'Trip end timestamp must be after trip start timestamp' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_stop_classification" }}
  WHERE trip_end_timestamp <= trip_start_timestamp

  UNION ALL

  -- Test 4: Stop Counts Non-Negative
  SELECT
    'test_fact_stop_classification_stop_counts_nonnegative' AS test_name,
    COUNT(*) AS violation_count,
    'All stop counts must be non-negative' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_stop_classification" }}
  WHERE total_stops < 0
     OR shadow_yard_stops < 0
     OR crew_change_stops < 0
     OR terminal_stops < 0
     OR mainline_stops < 0
     OR maintenance_stops < 0
     OR unclassified_stops < 0

  UNION ALL

  -- Test 5: Stop Sum Consistency
  SELECT
    'test_fact_stop_classification_stop_sum' AS test_name,
    COUNT(*) AS violation_count,
    'Total stops must equal sum of individual stop types' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_stop_classification" }}
  WHERE total_stops != (
    shadow_yard_stops + crew_change_stops + terminal_stops + 
    mainline_stops + maintenance_stops + unclassified_stops
  )

  UNION ALL

  -- Test 6: Dwell Minutes Non-Negative
  SELECT
    'test_fact_stop_classification_dwell_minutes_nonnegative' AS test_name,
    COUNT(*) AS violation_count,
    'Total dwell minutes must be non-negative' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_stop_classification" }}
  WHERE total_dwell_minutes < 0

  UNION ALL

  -- Test 7: PSR Period Valid
  SELECT
    'test_fact_stop_classification_psr_period_valid' AS test_name,
    COUNT(*) AS violation_count,
    'PSR period must be pre-PSR, transition, or mature' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_stop_classification" }}
  WHERE psr_period NOT IN ('pre-PSR', 'transition', 'mature')

  UNION ALL

  -- Test 8: Row Count
  SELECT
    'test_fact_stop_classification_row_count' AS test_name,
    CASE WHEN COUNT(*) >= 25 AND COUNT(*) <= 35 THEN 0 ELSE ABS(COUNT(*) - 30) END AS violation_count,
    'Row count should be approximately 30 (matches fact_trip)' AS description,
    CASE WHEN COUNT(*) >= 25 AND COUNT(*) <= 35 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "fact_stop_classification" }}

)

SELECT * FROM tests
ORDER BY test_name
