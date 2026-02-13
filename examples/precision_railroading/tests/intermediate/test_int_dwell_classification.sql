-- Test: int_dwell_classification Dwell Classification Validation
-- Description: Validates dwell classification logic, shadow yard detection, and business rules
-- Expected Result: All tests should return 0 violations

WITH all_tests AS (
  -- Test 1: Classification has only valid types
  SELECT
    'test_classification_valid_types' AS test_name,
    COUNT(*) AS violation_count,
    'Classification must be one of: terminal, crew_change, mainline_hold, maintenance, shadow_yard_hold, unclassified' AS description
  FROM {{ ref "int_dwell_classification" }}
  WHERE dwell_classification NOT IN ('terminal', 'crew_change', 'mainline_hold', 'maintenance', 'shadow_yard_hold', 'unclassified')
  
  UNION ALL
  
  -- Test 2: Terminal operations logic (480-2880 minutes at terminals)
  SELECT
    'test_classification_terminal_logic' AS test_name,
    COUNT(*) AS violation_count,
    'Terminal classification: 480-2880 min at terminal locations' AS description
  FROM {{ ref "int_dwell_classification" }} dc
  JOIN dim_location l ON dc.location_id = l.location_id
  WHERE l.location_type = 'terminal'
    AND dc.dwell_duration_minutes BETWEEN 480 AND 2880
    AND dc.dwell_classification != 'terminal'
    AND dc.dwell_classification != 'shadow_yard_hold'  -- Shadow yard takes precedence
  
  UNION ALL
  
  -- Test 3: Crew change logic (60-240 minutes at crew bases)
  SELECT
    'test_classification_crew_change_logic' AS test_name,
    COUNT(*) AS violation_count,
    'Crew change classification: 60-240 min at crew base locations' AS description
  FROM {{ ref "int_dwell_classification" }} dc
  JOIN dim_location l ON dc.location_id = l.location_id
  WHERE l.location_type = 'crew_base'
    AND dc.dwell_duration_minutes BETWEEN 60 AND 240
    AND dc.dwell_classification != 'crew_change'
    AND dc.dwell_classification != 'shadow_yard_hold'  -- Shadow yard takes precedence
  
  UNION ALL
  
  -- Test 4: Mainline hold logic (30-360 minutes at sidings)
  SELECT
    'test_classification_mainline_logic' AS test_name,
    COUNT(*) AS violation_count,
    'Mainline hold classification: 30-360 min at siding locations' AS description
  FROM {{ ref "int_dwell_classification" }} dc
  JOIN dim_location l ON dc.location_id = l.location_id
  WHERE l.location_type = 'siding'
    AND dc.dwell_duration_minutes BETWEEN 30 AND 360
    AND dc.dwell_classification != 'mainline_hold'
    AND dc.dwell_classification != 'shadow_yard_hold'  -- Shadow yard takes precedence
  
  UNION ALL
  
  -- Test 5: Maintenance logic (>360 minutes at repair facilities)
  SELECT
    'test_classification_maintenance_logic' AS test_name,
    COUNT(*) AS violation_count,
    'Maintenance classification: >360 min at repair facility locations' AS description
  FROM {{ ref "int_dwell_classification" }} dc
  JOIN dim_location l ON dc.location_id = l.location_id
  WHERE l.location_type = 'repair_facility'
    AND dc.dwell_duration_minutes > 360
    AND dc.dwell_classification != 'maintenance'
    AND dc.dwell_classification != 'shadow_yard_hold'  -- Shadow yard takes precedence
  
  UNION ALL
  
  -- Test 6: Shadow yard logic (risk_score > 0.5 AND 120-1440 minutes)
  SELECT
    'test_classification_shadow_yard_logic' AS test_name,
    COUNT(*) AS violation_count,
    'Shadow yard: risk_score > 0.5 AND 120-1440 min should be flagged' AS description
  FROM {{ ref "int_dwell_classification" }} dc
  JOIN dim_location l ON dc.location_id = l.location_id
  WHERE l.shadow_yard_risk_score > 50
    AND dc.dwell_duration_minutes BETWEEN 120 AND 1440
    AND dc.shadow_yard_flag != 1
  
  UNION ALL
  
  -- Test 7: Shadow yard count (should identify 5-7 locations)
  SELECT
    'test_classification_shadow_yard_count' AS test_name,
    CASE 
      WHEN (SELECT COUNT(DISTINCT location_id) FROM {{ ref "int_dwell_classification" }} WHERE shadow_yard_flag = 1) < 3
        OR (SELECT COUNT(DISTINCT location_id) FROM {{ ref "int_dwell_classification" }} WHERE shadow_yard_flag = 1) > 15
      THEN 1
      ELSE 0
    END AS violation_count,
    'Expected 3-15 shadow yard locations to be flagged' AS description
  
  UNION ALL
  
  -- Test 8: Shadow yard flag is boolean (0 or 1)
  SELECT
    'test_classification_shadow_yard_flag_boolean' AS test_name,
    COUNT(*) AS violation_count,
    'shadow_yard_flag must be 0 or 1' AS description
  FROM {{ ref "int_dwell_classification" }}
  WHERE shadow_yard_flag NOT IN (0, 1)
  
  UNION ALL
  
  -- Test 9: All location_ids are valid
  SELECT
    'test_classification_fk_locations' AS test_name,
    COUNT(*) AS violation_count,
    'All location_id must exist in dim_location' AS description
  FROM {{ ref "int_dwell_classification" }} dc
  LEFT JOIN dim_location l ON dc.location_id = l.location_id
  WHERE l.location_id IS NULL
  
  UNION ALL
  
  -- Test 10: No NULL classifications
  SELECT
    'test_classification_no_nulls' AS test_name,
    COUNT(*) AS violation_count,
    'dwell_classification must never be NULL' AS description
  FROM {{ ref "int_dwell_classification" }}
  WHERE dwell_classification IS NULL
  
  UNION ALL
  
  -- Test 11: Facility type matches dim_location
  SELECT
    'test_classification_facility_type_valid' AS test_name,
    COUNT(*) AS violation_count,
    'facility_type must match dim_location.location_type' AS description
  FROM {{ ref "int_dwell_classification" }} dc
  JOIN dim_location l ON dc.location_id = l.location_id
  WHERE dc.facility_type != l.location_type
  
  UNION ALL
  
  -- Test 12: Row count matches nodal dwell (no drops)
  SELECT
    'test_classification_row_count_matches' AS test_name,
    CASE 
      WHEN (SELECT COUNT(*) FROM {{ ref "int_dwell_classification" }}) != 
           (SELECT COUNT(*) FROM {{ ref "int_nodal_dwell" }})
      THEN 1
      ELSE 0
    END AS violation_count,
    'Row count must match int_nodal_dwell (no rows dropped)' AS description
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
