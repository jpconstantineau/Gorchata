-- Test: dim_location Integrity and Business Rules
-- Description: Validates location dimension structure, SPLC codes, shadow yard detection
-- Expected Result: violation_count = 0

WITH location_tests AS (
  -- Test 1: Check for duplicate SPLC codes
  SELECT
    'duplicate_splc_codes' AS test_name,
    COUNT(*) AS violation_count,
    'SPLC codes must be unique' AS description
  FROM (
    SELECT splc_code, COUNT(*) AS cnt
    FROM dim_location
    GROUP BY splc_code
    HAVING COUNT(*) > 1
  )
  
  UNION ALL
  
  -- Test 2: Validate location types
  SELECT
    'invalid_location_types' AS test_name,
    COUNT(*) AS violation_count,
    'Location types must be: terminal, interchange, yard, customer_site, siding' AS description
  FROM dim_location
  WHERE location_type NOT IN ('terminal', 'interchange', 'yard', 'customer_site', 'siding')
  
  UNION ALL
  
  -- Test 3: Validate latitude range
  SELECT
    'invalid_latitude' AS test_name,
    COUNT(*) AS violation_count,
    'Latitude must be between -90 and 90' AS description
  FROM dim_location
  WHERE latitude < -90 OR latitude > 90
  
  UNION ALL
  
  -- Test 4: Validate longitude range
  SELECT
    'invalid_longitude' AS test_name,
    COUNT(*) AS violation_count,
    'Longitude must be between -180 and 180' AS description
  FROM dim_location
  WHERE longitude < -180 OR longitude > 180
  
  UNION ALL
  
  -- Test 5: Validate shadow_yard_risk_score range
  SELECT
    'invalid_shadow_yard_score' AS test_name,
    COUNT(*) AS violation_count,
    'Shadow yard risk score must be between 0 and 100' AS description
  FROM dim_location
  WHERE shadow_yard_risk_score < 0 OR shadow_yard_risk_score > 100
  
  UNION ALL
  
  -- Test 6: Check total location count
  SELECT
    'incorrect_location_count' AS test_name,
    CASE WHEN COUNT(*) = 200 THEN 0 ELSE 1 END AS violation_count,
    'Must have exactly 200 locations' AS description
  FROM dim_location
  
  UNION ALL
  
  -- Test 7: Check shadow yard detection (should identify 5-7 locations)
  SELECT
    'shadow_yard_count_range' AS test_name,
    CASE 
      WHEN COUNT(*) >= 5 AND COUNT(*) <= 7 THEN 0
      ELSE 1
    END AS violation_count,
    'Should identify 5-7 shadow yards with risk score > 70' AS description
  FROM dim_location
  WHERE shadow_yard_risk_score > 70
  
  UNION ALL
  
  -- Test 8: Validate capacity classification
  SELECT
    'invalid_capacity_classification' AS test_name,
    COUNT(*) AS violation_count,
    'Capacity classification must be: high, medium, low' AS description
  FROM dim_location
  WHERE capacity_classification NOT IN ('high', 'medium', 'low')
  
  UNION ALL
  
  -- Test 9: Validate region
  SELECT
    'invalid_region' AS test_name,
    COUNT(*) AS violation_count,
    'Region must be: Northeast, Southeast, Midwest, Southwest, West' AS description
  FROM dim_location
  WHERE region NOT IN ('Northeast', 'Southeast', 'Midwest', 'Southwest', 'West')
  
  UNION ALL
  
  -- Test 10: Check for null SPLC codes
  SELECT
    'null_splc_codes' AS test_name,
    COUNT(*) AS violation_count,
    'SPLC codes cannot be null' AS description
  FROM dim_location
  WHERE splc_code IS NULL
)

SELECT
  test_name,
  violation_count,
  description,
  CASE WHEN violation_count = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM location_tests
WHERE violation_count > 0
