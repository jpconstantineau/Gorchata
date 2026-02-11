-- Test: State Transition Validity
-- Description: Validates operational state sequences follow valid haul cycle logic
-- Expected Result: Multiple rows showing transition types and counts

WITH 

-- Get all state transitions (current state -> next state)
state_transitions AS (
  SELECT 
    truck_id,
    operational_state as current_state,
    state_start,
    state_end,
    LEAD(operational_state) OVER (PARTITION BY truck_id ORDER BY state_start) as next_state,
    LEAD(state_start) OVER (PARTITION BY truck_id ORDER BY state_start) as next_state_start
  FROM stg_truck_states
),

-- Define valid transitions
-- Valid haul cycle: queued_at_shovel -> loading -> hauling_loaded -> queued_at_crusher -> dumping -> returning_empty (-> queued_at_shovel)
-- spot_delay and idle can occur at various points
valid_transition_check AS (
  SELECT 
    truck_id,
    current_state,
    next_state,
    state_start,
    CASE 
      -- Loading can come from queued_at_shovel or returning_empty or spot_delay
      WHEN current_state = 'queued_at_shovel' AND next_state IN ('loading', 'spot_delay', 'idle') THEN 1
      WHEN current_state = 'loading' AND next_state IN ('hauling_loaded', 'spot_delay') THEN 1
      
      -- Hauling loaded must go to crusher area
      WHEN current_state = 'hauling_loaded' AND next_state IN ('queued_at_crusher', 'dumping', 'spot_delay') THEN 1
      
      -- Crusher queue leads to dumping
      WHEN current_state = 'queued_at_crusher' AND next_state IN ('dumping', 'spot_delay') THEN 1
      
      -- Dumping must lead to returning empty
      WHEN current_state = 'dumping' AND next_state IN ('returning_empty', 'spot_delay') THEN 1
      
      -- Returning empty leads back to shovel area
      WHEN current_state = 'returning_empty' AND next_state IN ('queued_at_shovel', 'loading', 'spot_delay', 'idle') THEN 1
      
      -- Spot delays can transition to/from any operational state
      WHEN current_state = 'spot_delay' THEN 1
      WHEN next_state = 'spot_delay' THEN 1
      
      -- Idle can transition to operational states
      WHEN current_state = 'idle' AND next_state IN ('queued_at_shovel', 'loading', 'spot_delay') THEN 1
      
      -- End of sequence (no next state)
      WHEN next_state IS NULL THEN 1
      
      ELSE 0
    END as is_valid
  FROM state_transitions
),

-- Identify invalid transitions
invalid_transitions AS (
  SELECT 
    current_state || ' -> ' || next_state as transition_type,
    COUNT(*) as occurrence_count
  FROM valid_transition_check
  WHERE is_valid = 0
  GROUP BY current_state, next_state
),

-- Check for missing required states in cycles
-- Each complete cycle should have: loading, hauling_loaded, dumping, returning_empty
cycle_states AS (
  SELECT 
    truck_id,
    DATE(state_start) as cycle_date,
    GROUP_CONCAT(DISTINCT operational_state) as states_in_cycle,
    CASE 
      WHEN GROUP_CONCAT(DISTINCT operational_state) LIKE '%loading%'
       AND GROUP_CONCAT(DISTINCT operational_state) LIKE '%hauling_loaded%'
       AND GROUP_CONCAT(DISTINCT operational_state) LIKE '%dumping%'
       AND GROUP_CONCAT(DISTINCT operational_state) LIKE '%returning_empty%'
      THEN 1
      ELSE 0
    END as has_all_required_states
  FROM stg_truck_states
  GROUP BY truck_id, DATE(state_start)
),

incomplete_cycles AS (
  SELECT 'Incomplete Cycle (missing states)' as transition_type,
         COUNT(*) as occurrence_count
  FROM cycle_states
  WHERE has_all_required_states = 0
)

-- Return all transition validation results
SELECT transition_type,
       occurrence_count,
       CASE WHEN occurrence_count = 0 THEN 'PASS' ELSE 'WARNING' END as test_result
FROM invalid_transitions

UNION ALL

SELECT transition_type,
       occurrence_count,
       CASE WHEN occurrence_count = 0 THEN 'PASS' ELSE 'WARNING' END as test_result
FROM incomplete_cycles

ORDER BY occurrence_count DESC;
