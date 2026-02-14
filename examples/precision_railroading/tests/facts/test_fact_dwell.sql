-- Data Quality Tests: fact_dwell
-- Tests for the dwell fact table with shadow yard classifications

WITH tests AS (

  -- Test 1: FK Integrity - Railcars
  SELECT
    'test_fact_dwell_fk_railcars' AS test_name,
    COUNT(*) AS violation_count,
    'All railcar_id values must exist in dim_railcar' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM fact_dwell fd
  LEFT JOIN dim_railcar dr ON fd.railcar_id = dr.railcar_id
  WHERE dr.railcar_id IS NULL

  UNION ALL

  -- Test 2: FK Integrity - Locations
  SELECT
    'test_fact_dwell_fk_locations' AS test_name,
    COUNT(*) AS violation_count,
    'All location_id values must exist in dim_location' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM fact_dwell fd
  LEFT JOIN dim_location dl ON fd.location_id = dl.location_id
  WHERE dl.location_id IS NULL

  UNION ALL

  -- Test 3: FK Integrity - Dates
  SELECT
    'test_fact_dwell_fk_dates' AS test_name,
    COUNT(*) AS violation_count,
    'All dwell_start_date_id values must exist in dim_date' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM fact_dwell fd
  LEFT JOIN dim_date dd ON fd.dwell_start_date_id = dd.date_id
  WHERE dd.date_id IS NULL

  UNION ALL

  -- Test 4: Duration Positive
  SELECT
    'test_fact_dwell_duration_positive' AS test_name,
    COUNT(*) AS violation_count,
    'Dwell duration must be positive (> 0 minutes)' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM fact_dwell
  WHERE dwell_duration_minutes <= 0

  UNION ALL

  -- Test 5: Timestamp Order
  SELECT
    'test_fact_dwell_timestamp_order' AS test_name,
    COUNT(*) AS violation_count,
    'Dwell end timestamp must be after dwell start timestamp' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM fact_dwell
  WHERE dwell_end_timestamp <= dwell_start_timestamp

  UNION ALL

  -- Test 6: Shadow Yard Flags Valid
  SELECT
    'test_fact_dwell_shadow_yard_flags' AS test_name,
    COUNT(*) AS violation_count,
    'Shadow yard flag must be 0 or 1' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM fact_dwell
  WHERE shadow_yard_flag NOT IN (0, 1)

  UNION ALL

  -- Test 7: Classification Valid
  SELECT
    'test_fact_dwell_classification_valid' AS test_name,
    COUNT(*) AS violation_count,
    'Dwell classification must be valid type' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM fact_dwell
  WHERE dwell_classification NOT IN (
    'shadow_yard_hold', 'crew_change', 'terminal', 
    'maintenance', 'mainline_hold', 'unclassified'
  )

  UNION ALL

  -- Test 8: Shadow Consistency
  SELECT
    'test_fact_dwell_shadow_consistency' AS test_name,
    COUNT(*) AS violation_count,
    'Shadow yard flag must be 1 if and only if classification is shadow_yard_hold' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM fact_dwell
  WHERE (shadow_yard_flag = 1 AND dwell_classification != 'shadow_yard_hold')
     OR (shadow_yard_flag = 0 AND dwell_classification = 'shadow_yard_hold')

  UNION ALL

  -- Test 9: Is Loaded Valid
  SELECT
    'test_fact_dwell_is_loaded_valid' AS test_name,
    COUNT(*) AS violation_count,
    'Is loaded flag must be 0 or 1' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM fact_dwell
  WHERE is_loaded NOT IN (0, 1)

  UNION ALL

  -- Test 10: PSR Period Valid
  SELECT
    'test_fact_dwell_psr_period_valid' AS test_name,
    COUNT(*) AS violation_count,
    'PSR period must be pre-PSR, transition, or mature' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM fact_dwell
  WHERE psr_period NOT IN ('pre-PSR', 'transition', 'mature')

  UNION ALL

  -- Test 11: Row Count
  SELECT
    'test_fact_dwell_row_count' AS test_name,
    CASE WHEN COUNT(*) >= 25 AND COUNT(*) <= 35 THEN 0 ELSE ABS(COUNT(*) - 30) END AS violation_count,
    'Row count should be approximately 30 (matches int_dwell_classification)' AS description,
    CASE WHEN COUNT(*) >= 25 AND COUNT(*) <= 35 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM fact_dwell

)

SELECT * FROM tests
ORDER BY test_name
