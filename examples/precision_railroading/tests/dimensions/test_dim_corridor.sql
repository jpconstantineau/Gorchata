-- Test: dim_corridor Integrity and Business Rules
-- Description: Validates corridor dimension structure, referential integrity, distances
-- Expected Result: violation_count = 0

WITH corridor_tests AS (
  -- Test 1: Check for duplicate corridor codes
  SELECT
    'duplicate_corridor_codes' AS test_name,
    COUNT(*) AS violation_count,
    'Corridor codes must be unique' AS description
  FROM (
    SELECT corridor_code, COUNT(*) AS cnt
    FROM dim_corridor
    GROUP BY corridor_code
    HAVING COUNT(*) > 1
  )
  
  UNION ALL
  
  -- Test 2: Validate lane types
  SELECT
    'invalid_lane_types' AS test_name,
    COUNT(*) AS violation_count,
    'Lane type must be: mainline, branch, shortline' AS description
  FROM dim_corridor
  WHERE lane_type NOT IN ('mainline', 'branch', 'shortline')
  
  UNION ALL
  
  -- Test 3: Validate distance is positive
  SELECT
    'invalid_distance' AS test_name,
    COUNT(*) AS violation_count,
    'Distance must be greater than 0' AS description
  FROM dim_corridor
  WHERE distance_miles <= 0
  
  UNION ALL
  
  -- Test 4: Validate congestion level range
  SELECT
    'invalid_congestion_level' AS test_name,
    COUNT(*) AS violation_count,
    'Congestion level must be between 0 and 100' AS description
  FROM dim_corridor
  WHERE congestion_level < 0 OR congestion_level > 100
  
  UNION ALL
  
  -- Test 5: Check origin SPLC exists in dim_location
  SELECT
    'orphaned_origin_splc' AS test_name,
    COUNT(*) AS violation_count,
    'All origin SPLC codes must exist in dim_location' AS description
  FROM dim_corridor c
  LEFT JOIN dim_location l ON c.origin_splc = l.splc_code
  WHERE l.splc_code IS NULL
  
  UNION ALL
  
  -- Test 6: Check destination SPLC exists in dim_location
  SELECT
    'orphaned_destination_splc' AS test_name,
    COUNT(*) AS violation_count,
    'All destination SPLC codes must exist in dim_location' AS description
  FROM dim_corridor c
  LEFT JOIN dim_location l ON c.destination_splc = l.splc_code
  WHERE l.splc_code IS NULL
  
  UNION ALL
  
  -- Test 7: Validate traffic volume class
  SELECT
    'invalid_traffic_volume' AS test_name,
    COUNT(*) AS violation_count,
    'Traffic volume class must be: high, medium, low' AS description
  FROM dim_corridor
  WHERE traffic_volume_class NOT IN ('high', 'medium', 'low')
  
  UNION ALL
  
  -- Test 8: Check for null corridor codes
  SELECT
    'null_corridor_codes' AS test_name,
    COUNT(*) AS violation_count,
    'Corridor codes cannot be null' AS description
  FROM dim_corridor
  WHERE corridor_code IS NULL
  
  UNION ALL
  
  -- Test 9: Check corridor count is reasonable (30-50)
  SELECT
    'corridor_count_out_of_range' AS test_name,
    CASE 
      WHEN COUNT(*) >= 30 AND COUNT(*) <= 50 THEN 0
      ELSE 1
    END AS violation_count,
    'Should have 30-50 major corridors' AS description
  FROM dim_corridor
)

SELECT
  test_name,
  violation_count,
  description,
  CASE WHEN violation_count = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM corridor_tests
WHERE violation_count > 0
