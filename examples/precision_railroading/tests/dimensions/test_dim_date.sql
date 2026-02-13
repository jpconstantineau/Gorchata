-- Test: dim_date Integrity and Business Rules
-- Description: Validates date dimension completeness, PSR periods, date attributes
-- Expected Result: violation_count = 0

WITH date_tests AS (
  -- Test 1: Check for gaps in date sequence (2016-01-01 to 2025-12-31)
  SELECT
    'date_gaps' AS test_name,
    CASE 
      WHEN COUNT(*) = 3653 THEN 0  -- 10 years including leap years
      ELSE 1
    END AS violation_count,
    'Must have continuous dates from 2016-01-01 to 2025-12-31 (3653 days)' AS description
  FROM dim_date
  WHERE date BETWEEN '2016-01-01' AND '2025-12-31'
  
  UNION ALL
  
  -- Test 2: Check for duplicate date_id
  SELECT
    'duplicate_date_ids' AS test_name,
    COUNT(*) AS violation_count,
    'Date IDs must be unique' AS description
  FROM (
    SELECT date_id, COUNT(*) AS cnt
    FROM dim_date
    GROUP BY date_id
    HAVING COUNT(*) > 1
  )
  
  UNION ALL
  
  -- Test 3: Validate quarter range
  SELECT
    'invalid_quarter' AS test_name,
    COUNT(*) AS violation_count,
    'Quarter must be between 1 and 4' AS description
  FROM dim_date
  WHERE quarter < 1 OR quarter > 4
  
  UNION ALL
  
  -- Test 4: Validate month range
  SELECT
    'invalid_month' AS test_name,
    COUNT(*) AS violation_count,
    'Month must be between 1 and 12' AS description
  FROM dim_date
  WHERE month < 1 OR month > 12
  
  UNION ALL
  
  -- Test 5: Validate day_of_week range
  SELECT
    'invalid_day_of_week' AS test_name,
    COUNT(*) AS violation_count,
    'Day of week must be between 1 and 7' AS description
  FROM dim_date
  WHERE day_of_week < 1 OR day_of_week > 7
  
  UNION ALL
  
  -- Test 6: Validate PSR periods
  SELECT
    'invalid_psr_periods' AS test_name,
    COUNT(*) AS violation_count,
    'PSR period must be: pre_psr, transition, mature_psr' AS description
  FROM dim_date
  WHERE psr_period NOT IN ('pre_psr', 'transition', 'mature_psr')
  
  UNION ALL
  
  -- Test 7: Validate PSR period mapping for pre-PSR
  SELECT
    'incorrect_pre_psr_period' AS test_name,
    COUNT(*) AS violation_count,
    'Dates 2016-01-01 to 2017-12-31 must be pre_psr' AS description
  FROM dim_date
  WHERE date BETWEEN '2016-01-01' AND '2017-12-31'
    AND psr_period != 'pre_psr'
  
  UNION ALL
  
  -- Test 8: Validate PSR period mapping for transition
  SELECT
    'incorrect_transition_period' AS test_name,
    COUNT(*) AS violation_count,
    'Dates 2018-01-01 to 2020-12-31 must be transition' AS description
  FROM dim_date
  WHERE date BETWEEN '2018-01-01' AND '2020-12-31'
    AND psr_period != 'transition'
  
  UNION ALL
  
  -- Test 9: Validate PSR period mapping for mature PSR
  SELECT
    'incorrect_mature_psr_period' AS test_name,
    COUNT(*) AS violation_count,
    'Dates 2021-01-01 to 2025-12-31 must be mature_psr' AS description
  FROM dim_date
  WHERE date BETWEEN '2021-01-01' AND '2025-12-31'
    AND psr_period != 'mature_psr'
  
  UNION ALL
  
  -- Test 10: Validate seasons
  SELECT
    'invalid_seasons' AS test_name,
    COUNT(*) AS violation_count,
    'Season must be: Winter, Spring, Summer, Fall' AS description
  FROM dim_date
  WHERE season NOT IN ('Winter', 'Spring', 'Summer', 'Fall')
  
  UNION ALL
  
  -- Test 11: Weekend flag consistency
  SELECT
    'invalid_weekend_flag' AS test_name,
    COUNT(*) AS violation_count,
    'Weekend flag must match day_of_week (6=Saturday, 7=Sunday)' AS description
  FROM dim_date
  WHERE (day_of_week IN (6, 7) AND is_weekend != 1)
     OR (day_of_week NOT IN (6, 7) AND is_weekend != 0)
)

SELECT
  test_name,
  violation_count,
  description,
  CASE WHEN violation_count = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM date_tests
WHERE violation_count > 0
