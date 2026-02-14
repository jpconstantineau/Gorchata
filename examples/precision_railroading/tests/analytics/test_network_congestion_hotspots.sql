-- Data Quality Tests: network_congestion_hotspots
-- Tests for network congestion hotspots analytics query

WITH tests AS (

  -- Test 1: Query Executes Without Error
  SELECT
    'test_congestion_hotspots_executes' AS test_name,
    0 AS violation_count,
    'Query should execute without error' AS description,
    'PASS' AS status

  UNION ALL

  -- Test 2: Congestion Score Non-Negative
  SELECT
    'test_congestion_score_positive' AS test_name,
    COUNT(*) AS violation_count,
    'Congestion score should be >= 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM network_congestion_hotspots
  WHERE congestion_score < 0

  UNION ALL

  -- Test 3: Positive Dwell Event Counts
  SELECT
    'test_congestion_positive_events' AS test_name,
    COUNT(*) AS violation_count,
    'Dwell event count must be > 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM network_congestion_hotspots
  WHERE dwell_event_count <= 0

  UNION ALL

  -- Test 4: Positive Average Dwell
  SELECT
    'test_congestion_positive_dwell' AS test_name,
    COUNT(*) AS violation_count,
    'Average dwell minutes must be > 0' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM network_congestion_hotspots
  WHERE avg_dwell_minutes <= 0

  UNION ALL

  -- Test 5: Congestion Rank Sequential
  SELECT
    'test_congestion_rank_sequential' AS test_name,
    COUNT(*) AS violation_count,
    'Congestion rank should be sequential (1, 2, 3...)' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM (
    SELECT 
      congestion_rank,
      LAG(congestion_rank) OVER (ORDER BY congestion_rank) AS prev_rank
    FROM network_congestion_hotspots
  ) ranked
  WHERE prev_rank IS NOT NULL 
    AND congestion_rank != prev_rank + 1

)

SELECT * FROM tests
ORDER BY test_name
