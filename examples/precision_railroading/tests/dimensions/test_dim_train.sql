-- Test: dim_train Integrity and Business Rules
-- Description: Validates train dimension structure, types, priority levels
-- Expected Result: violation_count = 0

WITH train_tests AS (
  -- Test 1: Check for duplicate train numbers
  SELECT
    'duplicate_train_numbers' AS test_name,
    COUNT(*) AS violation_count,
    'Train numbers must be unique' AS description
  FROM (
    SELECT train_number, COUNT(*) AS cnt
    FROM dim_train
    GROUP BY train_number
    HAVING COUNT(*) > 1
  )
  
  UNION ALL
  
  -- Test 2: Validate train types
  SELECT
    'invalid_train_types' AS test_name,
    COUNT(*) AS violation_count,
    'Train type must be: manifest, intermodal, unit, autorack' AS description
  FROM dim_train
  WHERE train_type NOT IN ('manifest', 'intermodal', 'unit', 'autorack')
  
  UNION ALL
  
  -- Test 3: Validate priority level range
  SELECT
    'invalid_priority_level' AS test_name,
    COUNT(*) AS violation_count,
    'Priority level must be between 1 and 5' AS description
  FROM dim_train
  WHERE priority_level < 1 OR priority_level > 5
  
  UNION ALL
  
  -- Test 4: Validate typical car count range
  SELECT
    'invalid_car_count' AS test_name,
    COUNT(*) AS violation_count,
    'Typical car count must be between 50 and 150' AS description
  FROM dim_train
  WHERE typical_car_count < 50 OR typical_car_count > 150
  
  UNION ALL
  
  -- Test 5: Check for null train numbers
  SELECT
    'null_train_numbers' AS test_name,
    COUNT(*) AS violation_count,
    'Train numbers cannot be null' AS description
  FROM dim_train
  WHERE train_number IS NULL
  
  UNION ALL
  
  -- Test 6: Validate psr_optimized is boolean (0 or 1)
  SELECT
    'invalid_psr_optimized_flag' AS test_name,
    COUNT(*) AS violation_count,
    'PSR optimized flag must be 0 or 1' AS description
  FROM dim_train
  WHERE psr_optimized NOT IN (0, 1)
  
  UNION ALL
  
  -- Test 7: Check that we have train records
  SELECT
    'no_trains' AS test_name,
    CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END AS violation_count,
    'Must have at least one train' AS description
  FROM dim_train
)

SELECT
  test_name,
  violation_count,
  description,
  CASE WHEN violation_count = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM train_tests
WHERE violation_count > 0
