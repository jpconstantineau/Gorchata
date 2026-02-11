-- Test: Temporal Consistency Validation
-- Description: Validates time-based rules - cycle_end > cycle_start, no overlaps
-- Expected Result: violation_count = 0 (all temporal rules pass)

WITH violations AS (
  -- Check cycle_end > cycle_start
  SELECT 
    cycle_id,
    cycle_start,
    cycle_end,
    'cycle_end <= cycle_start' as violation_type
  FROM fact_haul_cycle
  WHERE cycle_end <= cycle_start
  
  UNION ALL
  
  -- Check for overlapping cycles (same truck)
  SELECT 
    fc1.cycle_id,
    fc1.cycle_start,
    fc1.cycle_end,
    'overlapping cycles' as violation_type
  FROM fact_haul_cycle fc1
  JOIN fact_haul_cycle fc2 ON fc1.truck_id = fc2.truck_id 
    AND fc1.cycle_id != fc2.cycle_id
    AND fc1.cycle_start < fc2.cycle_end 
    AND fc1.cycle_end > fc2.cycle_start
  
  UNION ALL
  
  -- Check state_end > state_start in staging
  SELECT 
    truck_id || '_' || state_start as cycle_id,
    state_start as cycle_start,
    state_end as cycle_end,
    'state_end <= state_start' as violation_type
  FROM stg_truck_states
  WHERE state_end <= state_start
  
  UNION ALL
  
  -- Check for excessive gaps between states (>2 hours indicates missing data)
  SELECT 
    s1.truck_id || '_gap' as cycle_id,
    s1.state_end as cycle_start,
    s2.state_start as cycle_end,
    'excessive gap between states (>2hr)' as violation_type
  FROM stg_truck_states s1
  JOIN stg_truck_states s2 ON s1.truck_id = s2.truck_id
  WHERE s2.state_start > s1.state_end
    AND (julianday(s2.state_start) - julianday(s1.state_end)) * 24 > 2
    -- Ensure s2 is the next state for s1
    AND NOT EXISTS (
      SELECT 1 FROM stg_truck_states s3
      WHERE s3.truck_id = s1.truck_id
        AND s3.state_start > s1.state_end
        AND s3.state_start < s2.state_start
    )
)

SELECT 
  COUNT(*) as violation_count,
  'Temporal Consistency' as test_name,
  'All timestamps must be properly ordered with no overlaps or excessive gaps' as test_description,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as test_result
FROM violations;
