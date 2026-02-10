-- Operational Constraints Validation
-- Verifies business rule compliance including queue constraints, straggler logic,
-- power transfer timing, and seasonal effects
-- Expected outcome: Document expected vs actual compliance

-- =============================================================================
-- VALIDATION 1: Queue Constraints (Max 1 train at origins/destinations)
-- =============================================================================
WITH train_at_location AS (
  SELECT 
    location_id,
    train_id,
    event_timestamp,
    event_type,
    CASE 
      WHEN event_type IN ('load_start', 'train_formed') THEN 'ENTER_ORIGIN'
      WHEN event_type = 'departed_origin' THEN 'EXIT_ORIGIN'
      WHEN event_type = 'arrived_destination' THEN 'ENTER_DESTINATION'
      WHEN event_type = 'departed_destination' THEN 'EXIT_DESTINATION'
      ELSE NULL
    END AS queue_event
  FROM fact_car_location_event
  WHERE event_type IN ('load_start', 'train_formed', 'departed_origin', 
                       'arrived_destination', 'departed_destination')
    AND train_id IS NOT NULL
),

-- Build queue occupancy timeline
queue_occupancy AS (
  SELECT 
    location_id,
    train_id,
    event_timestamp,
    queue_event,
    SUM(CASE 
      WHEN queue_event IN ('ENTER_ORIGIN', 'ENTER_DESTINATION') THEN 1
      WHEN queue_event IN ('EXIT_ORIGIN', 'EXIT_DESTINATION') THEN -1
      ELSE 0
    END) OVER (PARTITION BY location_id ORDER BY event_timestamp) AS trains_at_location
  FROM train_at_location
  WHERE queue_event IS NOT NULL
),

queue_violations AS (
  SELECT DISTINCT
    location_id,
    event_timestamp,
    trains_at_location,
    'WARNING' AS severity
  FROM queue_occupancy
  WHERE trains_at_location > 1
),

-- =============================================================================
-- VALIDATION 2: SET_OUT/PICK_UP Event Pairing
-- =============================================================================
-- Every car_set_out should have a corresponding car_picked_up
set_out_events AS (
  SELECT 
    car_id,
    event_id,
    event_timestamp AS set_out_time,
    location_id
  FROM fact_car_location_event
  WHERE event_type = 'car_set_out'
),

pick_up_events AS (
  SELECT 
    car_id,
    event_id,
    event_timestamp AS pick_up_time,
    location_id
  FROM fact_car_location_event
  WHERE event_type = 'car_picked_up'
),

unmatched_set_outs AS (
  SELECT 
    s.car_id,
    s.event_id AS set_out_event_id,
    s.set_out_time,
    s.location_id,
    'WARNING' AS severity
  FROM set_out_events s
  LEFT JOIN pick_up_events p 
    ON s.car_id = p.car_id 
    AND p.pick_up_time > s.set_out_time
  WHERE p.car_id IS NULL
),

unmatched_pick_ups AS (
  SELECT 
    p.car_id,
    p.event_id AS pick_up_event_id,
    p.pick_up_time,
    p.location_id,
    'WARNING' AS severity
  FROM pick_up_events p
  LEFT JOIN set_out_events s 
    ON p.car_id = s.car_id 
    AND s.set_out_time < p.pick_up_time
  WHERE s.car_id IS NULL
),

-- =============================================================================
-- VALIDATION 3: Straggler Cars Eventually Rejoin
-- =============================================================================
-- Stragglers should have a fact_straggler record and eventually rejoin trains
stragglers_in_fact AS (
  SELECT DISTINCT car_id, set_out_timestamp
  FROM fact_straggler
),

stragglers_from_events AS (
  SELECT DISTINCT car_id, event_timestamp AS set_out_timestamp
  FROM fact_car_location_event
  WHERE event_type = 'car_set_out'
),

straggler_discrepancies AS (
  SELECT 
    COALESCE(f.car_id, e.car_id) AS car_id,
    f.set_out_timestamp AS fact_timestamp,
    e.set_out_timestamp AS event_timestamp,
    CASE 
      WHEN f.car_id IS NULL THEN 'Straggler in events but not in fact_straggler'
      WHEN e.car_id IS NULL THEN 'Straggler in fact_straggler but not in events'
      ELSE NULL
    END AS discrepancy,
    'WARNING' AS severity
  FROM stragglers_in_fact f
  FULL OUTER JOIN stragglers_from_events e 
    ON f.car_id = e.car_id 
    AND ABS(JULIANDAY(f.set_out_timestamp) - JULIANDAY(e.set_out_timestamp)) < 0.001  -- Within ~1 minute
  WHERE discrepancy IS NOT NULL
),

-- =============================================================================
-- VALIDATION 4: Power Transfer Timing Validation
-- =============================================================================
-- Verify power inference logic: <1 hour = same locomotives, >1 hour = different
power_transfer_validation AS (
  SELECT 
    transfer_id,
    train_id,
    location_id,
    gap_hours,
    inferred_same_power,
    CASE
      WHEN gap_hours < 1.0 AND inferred_same_power != 1 THEN 'Gap < 1h should infer same power'
      WHEN gap_hours >= 1.0 AND inferred_same_power != 0 THEN 'Gap >= 1h should infer different power'
      ELSE NULL
    END AS inference_error,
    CASE
      WHEN gap_hours < 0 THEN 'CRITICAL'
      ELSE 'WARNING'
    END AS severity
  FROM fact_inferred_power_transfer
  WHERE 
    (gap_hours < 1.0 AND inferred_same_power != 1)
    OR (gap_hours >= 1.0 AND inferred_same_power != 0)
    OR gap_hours < 0
),

-- =============================================================================
-- VALIDATION 5: Seasonal Effects Present (Week 5 Slowdown)
-- =============================================================================
-- Check that Week 5 shows ~20% slowdown in transit times
baseline_transit AS (
  SELECT 
    AVG(t.transit_hours) AS avg_baseline_transit
  FROM fact_train_trip t
  JOIN dim_date d ON t.departure_date_key = d.date_key
  WHERE d.week NOT IN (5, 8)  -- Exclude seasonal weeks
),

week5_transit AS (
  SELECT 
    AVG(t.transit_hours) AS avg_week5_transit,
    COUNT(*) AS week5_trip_count
  FROM fact_train_trip t
  JOIN dim_date d ON t.departure_date_key = d.date_key
  WHERE d.week = 5
),

week5_slowdown_check AS (
  SELECT 
    w5.avg_week5_transit,
    b.avg_baseline_transit,
    ((w5.avg_week5_transit - b.avg_baseline_transit) / b.avg_baseline_transit) * 100 AS slowdown_pct,
    w5.week5_trip_count,
    CASE 
      WHEN w5.week5_trip_count = 0 THEN 'No Week 5 trips found'
      WHEN ABS(((w5.avg_week5_transit - b.avg_baseline_transit) / b.avg_baseline_transit) * 100 - 20) > 10 
        THEN 'Week 5 slowdown not ~20% (actual: ' || CAST(ROUND(((w5.avg_week5_transit - b.avg_baseline_transit) / b.avg_baseline_transit) * 100, 1) AS TEXT) || '%)'
      ELSE NULL
    END AS seasonal_issue,
    CASE
      WHEN w5.week5_trip_count = 0 THEN 'CRITICAL'
      ELSE 'INFO'
    END AS severity
  FROM week5_transit w5, baseline_transit b
),

valid_week5_issues AS (
  SELECT * FROM week5_slowdown_check WHERE seasonal_issue IS NOT NULL
),

-- =============================================================================
-- VALIDATION 6: Seasonal Effects Present (Week 8 Straggler Spike)
-- =============================================================================
-- Check that Week 8 shows ~2x straggler rate
baseline_stragglers AS (
  SELECT 
    COUNT(*) * 1.0 / COUNT(DISTINCT d.week) AS avg_weekly_stragglers
  FROM fact_straggler s
  JOIN dim_date d ON s.set_out_date_key = d.date_key
  WHERE d.week NOT IN (5, 8)
),

week8_stragglers AS (
  SELECT 
    COUNT(*) AS week8_straggler_count
  FROM fact_straggler s
  JOIN dim_date d ON s.set_out_date_key = d.date_key
  WHERE d.week = 8
),

week8_spike_check AS (
  SELECT 
    w8.week8_straggler_count,
    b.avg_weekly_stragglers,
    (w8.week8_straggler_count / b.avg_weekly_stragglers) AS spike_ratio,
    CASE 
      WHEN w8.week8_straggler_count = 0 THEN 'No Week 8 stragglers found'
      WHEN ABS((w8.week8_straggler_count / b.avg_weekly_stragglers) - 2.0) > 0.5 
        THEN 'Week 8 straggler spike not ~2x (actual: ' || CAST(ROUND(w8.week8_straggler_count / b.avg_weekly_stragglers, 2) AS TEXT) || 'x)'
      ELSE NULL
    END AS seasonal_issue,
    CASE
      WHEN w8.week8_straggler_count = 0 THEN 'CRITICAL'
      ELSE 'INFO'
    END AS severity
  FROM week8_stragglers w8, baseline_stragglers b
),

valid_week8_issues AS (
  SELECT * FROM week8_spike_check WHERE seasonal_issue IS NOT NULL
),

-- =============================================================================
-- VALIDATION 7: Known Issue - Car Recycling Overlap
-- =============================================================================
-- Document known issue: TestCarExclusivity fails due to car recycling overlap
-- Cars are sometimes assigned to new trains before previous trip completes
car_exclusivity_note AS (
  SELECT 
    COUNT(*) AS overlap_count
  FROM (
    SELECT car_id, train_id, MIN(event_timestamp) AS start_time, MAX(event_timestamp) AS end_time
    FROM fact_car_location_event
    WHERE train_id IS NOT NULL
    GROUP BY car_id, train_id
  ) t1
  JOIN (
    SELECT car_id, train_id, MIN(event_timestamp) AS start_time, MAX(event_timestamp) AS end_time
    FROM fact_car_location_event
    WHERE train_id IS NOT NULL
    GROUP BY car_id, train_id
  ) t2 ON t1.car_id = t2.car_id AND t1.train_id != t2.train_id
  WHERE t2.start_time < t1.end_time
),

-- =============================================================================
-- CONSOLIDATED VIOLATION REPORT
-- =============================================================================
all_violations AS (
  -- Queue violations
  SELECT 
    'QUEUE_CONSTRAINT' AS violation_type,
    severity,
    'Location ' || location_id || ' has ' || CAST(trains_at_location AS TEXT) || ' trains (max 1 allowed)' AS violation_details,
    location_id,
    event_timestamp
  FROM queue_violations
  
  UNION ALL
  
  -- Unmatched SET_OUT events
  SELECT 
    'UNMATCHED_SET_OUT' AS violation_type,
    severity,
    'Car set out but never picked up' AS violation_details,
    location_id,
    set_out_time AS event_timestamp
  FROM unmatched_set_outs
  
  UNION ALL
  
  -- Unmatched PICK_UP events
  SELECT 
    'UNMATCHED_PICK_UP' AS violation_type,
    severity,
    'Car picked up but no prior set out' AS violation_details,
    location_id,
    pick_up_time AS event_timestamp
  FROM unmatched_pick_ups
  
  UNION ALL
  
  -- Straggler discrepancies
  SELECT 
    'STRAGGLER_DISCREPANCY' AS violation_type,
    severity,
    discrepancy AS violation_details,
    NULL AS location_id,
    fact_timestamp AS event_timestamp
  FROM straggler_discrepancies
  
  UNION ALL
  
  -- Power transfer inference errors
  SELECT 
    'POWER_INFERENCE_ERROR' AS violation_type,
    severity,
    inference_error || ' (gap: ' || CAST(ROUND(gap_hours, 2) AS TEXT) || 'h)' AS violation_details,
    location_id,
    NULL AS event_timestamp
  FROM power_transfer_validation
  
  UNION ALL
  
  -- Week 5 slowdown issues
  SELECT 
    'SEASONAL_WEEK5' AS violation_type,
    severity,
    seasonal_issue AS violation_details,
    NULL AS location_id,
    NULL AS event_timestamp
  FROM valid_week5_issues
  
  UNION ALL
  
  -- Week 8 straggler spike issues
  SELECT 
    'SEASONAL_WEEK8' AS violation_type,
    severity,
    seasonal_issue AS violation_details,
    NULL AS location_id,
    NULL AS event_timestamp
  FROM valid_week8_issues
  
  UNION ALL
  
  -- Known issue note
  SELECT 
    'KNOWN_ISSUE_CAR_RECYCLING' AS violation_type,
    'INFO' AS severity,
    'Car recycling overlap detected: ' || CAST(overlap_count AS TEXT) || ' instances (TestCarExclusivity fails)' AS violation_details,
    NULL AS location_id,
    NULL AS event_timestamp
  FROM car_exclusivity_note
  WHERE overlap_count > 0
)

-- Return all violations ordered by severity
SELECT 
  violation_type,
  severity,
  violation_details,
  location_id,
  event_timestamp,
  COUNT(*) OVER (PARTITION BY violation_type) AS violation_count
FROM all_violations
ORDER BY 
  CASE severity
    WHEN 'CRITICAL' THEN 1
    WHEN 'WARNING' THEN 2
    WHEN 'INFO' THEN 3
  END,
  violation_type;
