-- Data Quality Tests: agg_psr_evolution
-- Tests for PSR evolution metrics across periods

WITH tests AS (

  -- Test 1: Periods Complete
  SELECT
    'test_psr_evolution_periods_complete' AS test_name,
    CASE 
      WHEN SUM(CASE WHEN psr_period = 'pre-PSR' THEN 1 ELSE 0 END) = 1
       AND SUM(CASE WHEN psr_period = 'transition' THEN 1 ELSE 0 END) = 1
       AND SUM(CASE WHEN psr_period = 'mature' THEN 1 ELSE 0 END) = 1
      THEN 0 ELSE 1 
    END AS violation_count,
    'Must have all 3 PSR periods (pre-PSR, transition, mature)' AS description,
    CASE 
      WHEN SUM(CASE WHEN psr_period = 'pre-PSR' THEN 1 ELSE 0 END) = 1
       AND SUM(CASE WHEN psr_period = 'transition' THEN 1 ELSE 0 END) = 1
       AND SUM(CASE WHEN psr_period = 'mature' THEN 1 ELSE 0 END) = 1
      THEN 'PASS' ELSE 'FAIL' 
    END AS status
  FROM {{ ref "agg_psr_evolution" }}

  UNION ALL

  -- Test 2: Trip Counts Non-Negative
  SELECT
    'test_psr_evolution_trip_counts_positive' AS test_name,
    COUNT(*) AS violation_count,
    'Total trips must be >= 0 for each period (may be 0 for periods without data)' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "agg_psr_evolution" }}
  WHERE total_trips < 0

  UNION ALL

  -- Test 3: Velocity Positive
  SELECT
    'test_psr_evolution_velocity_positive' AS test_name,
    COUNT(*) AS violation_count,
    'Average velocity mph must be >= 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "agg_psr_evolution" }}
  WHERE avg_velocity_mph < 0

  UNION ALL

  -- Test 4: Duration Positive (when trips exist)
  SELECT
    'test_psr_evolution_duration_positive' AS test_name,
    COUNT(*) AS violation_count,
    'Average trip duration minutes must be > 0 when trips exist, or 0 when no trips' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "agg_psr_evolution" }}
  WHERE (total_trips > 0 AND avg_trip_duration_minutes <= 0)  -- Violation only if trips exist but duration is 0
     OR (total_trips = 0 AND avg_trip_duration_minutes != 0)  -- Or if no trips but duration is non-zero

  UNION ALL

  -- Test 5: Stddev Non-Negative
  SELECT
    'test_psr_evolution_stddev_nonnegative' AS test_name,
    COUNT(*) AS violation_count,
    'All standard deviation measures must be >= 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "agg_psr_evolution" }}
  WHERE stddev_velocity < 0 OR stddev_trip_duration < 0

  UNION ALL

  -- Test 6: Row Count
  SELECT
    'test_psr_evolution_row_count' AS test_name,
    CASE WHEN COUNT(*) = 3 THEN 0 ELSE ABS(COUNT(*) - 3) END AS violation_count,
    'Must have exactly 3 rows (one per PSR period)' AS description,
    CASE WHEN COUNT(*) = 3 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "agg_psr_evolution" }}

)

SELECT * FROM tests
ORDER BY test_name
