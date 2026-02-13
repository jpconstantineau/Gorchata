-- Test: dim_railcar Integrity and Business Rules
-- Description: Validates railcar dimension structure, fleet composition, attributes
-- Expected Result: violation_count = 0

WITH railcar_tests AS (
  -- Test 1: Check total railcar count
  SELECT
    'incorrect_railcar_count' AS test_name,
    CASE WHEN COUNT(*) = 12000 THEN 0 ELSE 1 END AS violation_count,
    'Must have exactly 12,000 railcars' AS description
  FROM dim_railcar
  
  UNION ALL
  
  -- Test 2: Check for duplicate car numbers
  SELECT
    'duplicate_car_numbers' AS test_name,
    COUNT(*) AS violation_count,
    'Car numbers must be unique' AS description
  FROM (
    SELECT car_number, COUNT(*) AS cnt
    FROM dim_railcar
    GROUP BY car_number
    HAVING COUNT(*) > 1
  )
  
  UNION ALL
  
  -- Test 3: Validate railroad owners
  SELECT
    'invalid_railroad_owners' AS test_name,
    COUNT(*) AS violation_count,
    'Railroad owner must be: BNSF, UP, CSX, NS, CN, CP, KCS' AS description
  FROM dim_railcar
  WHERE railroad_owner NOT IN ('BNSF', 'UP', 'CSX', 'NS', 'CN', 'CP', 'KCS')
  
  UNION ALL
  
  -- Test 4: Validate car types
  SELECT
    'invalid_car_types' AS test_name,
    COUNT(*) AS violation_count,
    'Car type must be: hopper, tank, box, gondola, intermodal' AS description
  FROM dim_railcar
  WHERE car_type NOT IN ('hopper', 'tank', 'box', 'gondola', 'intermodal')
  
  UNION ALL
  
  -- Test 5: Validate capacity ranges by car type
  SELECT
    'invalid_hopper_capacity' AS test_name,
    COUNT(*) AS violation_count,
    'Hopper capacity must be between 100 and 120 tons' AS description
  FROM dim_railcar
  WHERE car_type = 'hopper' AND (capacity_tons < 100 OR capacity_tons > 120)
  
  UNION ALL
  
  SELECT
    'invalid_tank_capacity' AS test_name,
    COUNT(*) AS violation_count,
    'Tank capacity must be between 80 and 100 tons' AS description
  FROM dim_railcar
  WHERE car_type = 'tank' AND (capacity_tons < 80 OR capacity_tons > 100)
  
  UNION ALL
  
  SELECT
    'invalid_box_capacity' AS test_name,
    COUNT(*) AS violation_count,
    'Box capacity must be between 70 and 90 tons' AS description
  FROM dim_railcar
  WHERE car_type = 'box' AND (capacity_tons < 70 OR capacity_tons > 90)
  
  UNION ALL
  
  SELECT
    'invalid_gondola_capacity' AS test_name,
    COUNT(*) AS violation_count,
    'Gondola capacity must be between 90 and 110 tons' AS description
  FROM dim_railcar
  WHERE car_type = 'gondola' AND (capacity_tons < 90 OR capacity_tons > 110)
  
  UNION ALL
  
  SELECT
    'invalid_intermodal_capacity' AS test_name,
    COUNT(*) AS violation_count,
    'Intermodal capacity must be between 60 and 80 tons' AS description
  FROM dim_railcar
  WHERE car_type = 'intermodal' AND (capacity_tons < 60 OR capacity_tons > 80)
  
  UNION ALL
  
  -- Test 6: Validate manufacture year
  SELECT
    'invalid_manufacture_year' AS test_name,
    COUNT(*) AS violation_count,
    'Manufacture year must be between 2000 and 2020' AS description
  FROM dim_railcar
  WHERE manufacture_year < 2000 OR manufacture_year > 2020
  
  UNION ALL
  
  -- Test 7: Validate in_service flag
  SELECT
    'invalid_in_service_flag' AS test_name,
    COUNT(*) AS violation_count,
    'All cars must be in service (in_service = TRUE)' AS description
  FROM dim_railcar
  WHERE in_service != 1
  
  UNION ALL
  
  -- Test 8: Check for null car numbers
  SELECT
    'null_car_numbers' AS test_name,
    COUNT(*) AS violation_count,
    'Car numbers cannot be null' AS description
  FROM dim_railcar
  WHERE car_number IS NULL
  
  UNION ALL
  
  -- Test 9: Validate acquisition dates are before 2016
  SELECT
    'invalid_acquisition_date' AS test_name,
    COUNT(*) AS violation_count,
    'Acquisition dates must be before 2016-01-01' AS description
  FROM dim_railcar
  WHERE acquisition_date >= '2016-01-01'
)

SELECT
  test_name,
  violation_count,
  description,
  CASE WHEN violation_count = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM railcar_tests
WHERE violation_count > 0
