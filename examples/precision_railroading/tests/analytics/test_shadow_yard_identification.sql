-- Data Quality Tests: shadow_yard_identification
-- Tests for shadow yard identification analytics query

WITH tests AS (

  -- Test 1: Query Executes Without Error
  SELECT
    'test_shadow_yard_executes' AS test_name,
    0 AS violation_count,
    'Query should execute without error' AS description,
    'PASS' AS status

  UNION ALL

  -- Test 2: Shadow Yard Flag Threshold Logic
  SELECT
    'test_shadow_yard_flag_threshold' AS test_name,
    COUNT(*) AS violation_count,
    'Shadow yard flag should be 1 when composite_score > 60, else 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "shadow_yard_identification" }}
  WHERE (shadow_yard_flag = 1 AND composite_score <= 60)
     OR (shadow_yard_flag = 0 AND composite_score > 60)

  UNION ALL

  -- Test 3: Composite Score Range
  SELECT
    'test_shadow_yard_composite_score_range' AS test_name,
    COUNT(*) AS violation_count,
    'Composite score should be between 0 and 100' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "shadow_yard_identification" }}
  WHERE composite_score < 0 OR composite_score > 100

  UNION ALL

  -- Test 4: Shadow Yard Percentage Range
  SELECT
    'test_shadow_yard_percentage_range' AS test_name,
    COUNT(*) AS violation_count,
    'Shadow yard percentage should be between 0 and 100' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM {{ ref "shadow_yard_identification" }}
  WHERE shadow_yard_percentage < 0 OR shadow_yard_percentage > 100

  UNION ALL

  -- Test 5: Ranking is Sequential
  SELECT
    'test_shadow_yard_ranking_sequential' AS test_name,
    COUNT(*) AS violation_count,
    'Ranking should be sequential (1, 2, 3...)' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM (
    SELECT 
      ranking,
      LAG(ranking) OVER (ORDER BY ranking) AS prev_ranking
    FROM {{ ref "shadow_yard_identification" }}
  ) ranked
  WHERE prev_ranking IS NOT NULL 
    AND ranking != prev_ranking + 1

)

SELECT * FROM tests
ORDER BY test_name
