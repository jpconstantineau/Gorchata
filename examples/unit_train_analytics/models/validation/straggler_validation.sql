-- Straggler Validation
-- Verifies straggler-specific logic including delay ranges (6-72 hours),
-- independent travel after SET_OUT, and Week 8 spike (2x rate)
-- Expected outcome: Straggler logic working as designed

-- =============================================================================
-- VALIDATION 1: Straggler Delay Range Validation
-- =============================================================================
-- Verify straggler delays are within expected range (6-72 hours as designed)
WITH straggler_delay_check AS (
  SELECT 
    straggler_id,
    car_id,
    set_out_timestamp,
    picked_up_timestamp,
    delay_hours,
    delay_category,
    CASE
      WHEN delay_hours < 0 THEN 'Negative delay'
      WHEN delay_hours < 6 THEN 'Delay too short (< 6 hours)'
      WHEN delay_hours > 72 THEN 'Delay too long (> 72 hours)'
      WHEN delay_hours IS NULL THEN 'Missing delay calculation'
      ELSE NULL
    END AS delay_issue,
    CASE
      WHEN delay_hours < 0 THEN 'CRITICAL'
      WHEN delay_hours IS NULL THEN 'CRITICAL'
      WHEN delay_hours < 6 OR delay_hours > 72 THEN 'WARNING'
      ELSE 'PASS'
    END AS severity
  FROM fact_straggler
),

invalid_delays AS (
  SELECT * FROM straggler_delay_check WHERE delay_issue IS NOT NULL
),

-- =============================================================================
-- VALIDATION 2: Straggler Delay Category Validation
-- =============================================================================
-- Verify delay_category matches actual delay_hours
delay_category_validation AS (
  SELECT 
    straggler_id,
    car_id,
    delay_hours,
    delay_category,
    CASE
      WHEN delay_hours < 12 AND delay_category != 'short' THEN 'Should be short (<12h)'
      WHEN delay_hours >= 12 AND delay_hours < 24 AND delay_category != 'medium' THEN 'Should be medium (12-24h)'
      WHEN delay_hours >= 24 AND delay_hours <= 72 AND delay_category != 'long' THEN 'Should be long (24-72h)'
      WHEN delay_hours > 72 AND delay_category != 'extended' THEN 'Should be extended (>72h)'
      ELSE NULL
    END AS category_error,
    'WARNING' AS severity
  FROM fact_straggler
),

invalid_categories AS (
  SELECT * FROM delay_category_validation WHERE category_error IS NOT NULL
),

-- =============================================================================
-- VALIDATION 3: Stragglers Travel Independently After SET_OUT
-- =============================================================================
-- After car_set_out, the car should not appear in train events until car_picked_up
straggler_independence_check AS (
  SELECT 
    s.straggler_id,
    s.car_id,
    s.set_out_timestamp,
    s.picked_up_timestamp,
    e.event_timestamp,
    e.event_type,
    e.train_id
  FROM fact_straggler s
  JOIN fact_car_location_event e 
    ON s.car_id = e.car_id
  WHERE e.event_timestamp > s.set_out_timestamp
    AND (s.picked_up_timestamp IS NULL OR e.event_timestamp < s.picked_up_timestamp)
    AND e.train_id IS NOT NULL  -- Car should not be associated with train during straggler period
    AND e.event_type NOT IN ('car_set_out', 'car_picked_up')  -- Exclude the boundary events
),

independent_travel_violations AS (
  SELECT 
    straggler_id,
    car_id,
    set_out_timestamp,
    picked_up_timestamp,
    event_timestamp,
    event_type,
    train_id,
    'WARNING' AS severity
  FROM straggler_independence_check
),

-- =============================================================================
-- VALIDATION 4: Stragglers Don't Appear in Train Events After SET_OUT
-- =============================================================================
-- Verify cars in straggler period don't participate in train-level events
train_event_participation AS (
  SELECT 
    s.straggler_id,
    s.car_id,
    s.set_out_timestamp,
    s.picked_up_timestamp,
    e.event_timestamp,
    e.event_type
  FROM fact_straggler s
  JOIN fact_car_location_event e 
    ON s.car_id = e.car_id
  WHERE e.event_timestamp > s.set_out_timestamp
    AND (s.picked_up_timestamp IS NULL OR e.event_timestamp < s.picked_up_timestamp)
    AND e.event_type IN ('train_formed', 'departed_origin', 'arrived_destination', 'train_completed')
),

invalid_train_participation AS (
  SELECT 
    straggler_id,
    car_id,
    set_out_timestamp,
    event_type,
    event_timestamp,
    'WARNING' AS severity
  FROM train_event_participation
),

-- =============================================================================
-- VALIDATION 5: Straggler Rates Align with Design (Higher in Week 8)
-- =============================================================================
-- Calculate weekly straggler rates and verify Week 8 has ~2x baseline rate
weekly_straggler_rates AS (
  SELECT 
    d.week,
    COUNT(*) AS straggler_count,
    COUNT(*) * 1.0 / (SELECT COUNT(DISTINCT trip_id) FROM fact_train_trip) AS straggler_rate
  FROM fact_straggler s
  JOIN dim_date d ON s.set_out_date_key = d.date_key
  GROUP BY d.week
),

baseline_rate AS (
  SELECT 
    AVG(straggler_rate) AS avg_baseline_rate
  FROM weekly_straggler_rates
  WHERE week NOT IN (5, 8)
),

week8_rate_check AS (
  SELECT 
    w.week,
    w.straggler_count,
    w.straggler_rate,
    b.avg_baseline_rate,
    (w.straggler_rate / b.avg_baseline_rate) AS rate_ratio
  FROM weekly_straggler_rates w, baseline_rate b
  WHERE w.week = 8
),

week8_rate_validation AS (
  SELECT 
    week,
    straggler_count,
    straggler_rate,
    avg_baseline_rate,
    rate_ratio,
    CASE
      WHEN straggler_count = 0 THEN 'No stragglers in Week 8'
      WHEN ABS(rate_ratio - 2.0) > 0.5 THEN 'Week 8 rate not ~2x baseline (actual: ' || CAST(ROUND(rate_ratio, 2) AS TEXT) || 'x)'
      ELSE NULL
    END AS rate_issue,
    CASE
      WHEN straggler_count = 0 THEN 'CRITICAL'
      ELSE 'INFO'
    END AS severity
  FROM week8_rate_check
),

valid_rate_issues AS (
  SELECT * FROM week8_rate_validation WHERE rate_issue IS NOT NULL
),

-- =============================================================================
-- VALIDATION 6: Cross-Validate fact_straggler Against fact_car_location_event
-- =============================================================================
-- Every straggler in fact_straggler should have corresponding SET_OUT event
straggler_event_cross_check AS (
  SELECT 
    s.straggler_id,
    s.car_id,
    s.set_out_timestamp,
    e.event_id,
    e.event_timestamp,
    e.event_type
  FROM fact_straggler s
  LEFT JOIN fact_car_location_event e 
    ON s.car_id = e.car_id 
    AND e.event_type = 'car_set_out'
    AND ABS(JULIANDAY(s.set_out_timestamp) - JULIANDAY(e.event_timestamp)) < 0.001  -- Within ~1 minute
  WHERE e.event_id IS NULL
),

missing_set_out_events AS (
  SELECT 
    straggler_id,
    car_id,
    set_out_timestamp,
    'CRITICAL' AS severity
  FROM straggler_event_cross_check
),

-- =============================================================================
-- VALIDATION 7: Straggler Completeness Check
-- =============================================================================
-- All stragglers should eventually be picked up (or be recent)
incomplete_stragglers AS (
  SELECT 
    straggler_id,
    car_id,
    set_out_timestamp,
    picked_up_timestamp,
    delay_hours,
    JULIANDAY('now') - JULIANDAY(set_out_timestamp) AS days_since_set_out
  FROM fact_straggler
  WHERE picked_up_timestamp IS NULL
    AND JULIANDAY('now') - JULIANDAY(set_out_timestamp) > 7  -- More than 7 days without pickup
),

stalled_stragglers AS (
  SELECT 
    straggler_id,
    car_id,
    set_out_timestamp,
    CAST(ROUND(days_since_set_out, 1) AS TEXT) AS days_waiting,
    'WARNING' AS severity
  FROM incomplete_stragglers
),

-- =============================================================================
-- VALIDATION 8: Straggler Statistical Summary
-- =============================================================================
-- Provide summary stats to verify overall straggler behavior
straggler_stats AS (
  SELECT 
    COUNT(*) AS total_stragglers,
    AVG(delay_hours) AS avg_delay_hours,
    MIN(delay_hours) AS min_delay_hours,
    MAX(delay_hours) AS max_delay_hours,
    SUM(CASE WHEN delay_category = 'short' THEN 1 ELSE 0 END) AS short_count,
    SUM(CASE WHEN delay_category = 'medium' THEN 1 ELSE 0 END) AS medium_count,
    SUM(CASE WHEN delay_category = 'long' THEN 1 ELSE 0 END) AS long_count,
    SUM(CASE WHEN delay_category = 'extended' THEN 1 ELSE 0 END) AS extended_count
  FROM fact_straggler
),

stats_validation AS (
  SELECT 
    'STRAGGLER_STATS' AS check_type,
    CASE
      WHEN total_stragglers = 0 THEN 'No stragglers found'
      WHEN avg_delay_hours < 6 OR avg_delay_hours > 72 THEN 'Average delay outside expected range'
      WHEN min_delay_hours < 6 THEN 'Minimum delay below 6 hours'
      WHEN max_delay_hours > 72 THEN 'Maximum delay exceeds 72 hours'
      ELSE NULL
    END AS stats_issue,
    CASE
      WHEN total_stragglers = 0 THEN 'CRITICAL'
      ELSE 'INFO'
    END AS severity,
    'Total: ' || CAST(total_stragglers AS TEXT) || 
    ', Avg: ' || CAST(ROUND(avg_delay_hours, 1) AS TEXT) || 'h' ||
    ', Min: ' || CAST(ROUND(min_delay_hours, 1) AS TEXT) || 'h' ||
    ', Max: ' || CAST(ROUND(max_delay_hours, 1) AS TEXT) || 'h' ||
    ', Short: ' || CAST(short_count AS TEXT) ||
    ', Medium: ' || CAST(medium_count AS TEXT) ||
    ', Long: ' || CAST(long_count AS TEXT) ||
    ', Extended: ' || CAST(extended_count AS TEXT) AS stats_details
  FROM straggler_stats
),

valid_stats_issues AS (
  SELECT * FROM stats_validation WHERE stats_issue IS NOT NULL
),

-- =============================================================================
-- CONSOLIDATED VIOLATION REPORT
-- =============================================================================
all_violations AS (
  -- Invalid delays
  SELECT 
    'INVALID_DELAY' AS violation_type,
    severity,
    delay_issue || ' (delay: ' || COALESCE(CAST(ROUND(delay_hours, 1) AS TEXT), 'NULL') || 'h)' AS violation_details,
    straggler_id,
    car_id,
    set_out_timestamp
  FROM invalid_delays
  
  UNION ALL
  
  -- Invalid delay categories
  SELECT 
    'INVALID_DELAY_CATEGORY' AS violation_type,
    severity,
    category_error || ' (delay: ' || CAST(ROUND(delay_hours, 1) AS TEXT) || 'h, category: ' || delay_category || ')' AS violation_details,
    straggler_id,
    car_id,
    NULL AS set_out_timestamp
  FROM invalid_categories
  
  UNION ALL
  
  -- Independent travel violations
  SELECT 
    'NOT_INDEPENDENT_TRAVEL' AS violation_type,
    severity,
    'Car associated with train ' || train_id || ' during straggler period (event: ' || event_type || ')' AS violation_details,
    straggler_id,
    car_id,
    event_timestamp AS set_out_timestamp
  FROM independent_travel_violations
  
  UNION ALL
  
  -- Invalid train participation
  SELECT 
    'INVALID_TRAIN_PARTICIPATION' AS violation_type,
    severity,
    'Straggler car in train event: ' || event_type AS violation_details,
    straggler_id,
    car_id,
    event_timestamp AS set_out_timestamp
  FROM invalid_train_participation
  
  UNION ALL
  
  -- Week 8 rate issues
  SELECT 
    'WEEK8_RATE_ISSUE' AS violation_type,
    severity,
    rate_issue AS violation_details,
    NULL AS straggler_id,
    NULL AS car_id,
    NULL AS set_out_timestamp
  FROM valid_rate_issues
  
  UNION ALL
  
  -- Missing SET_OUT events
  SELECT 
    'MISSING_SET_OUT_EVENT' AS violation_type,
    severity,
    'Straggler in fact_straggler but no corresponding car_set_out event' AS violation_details,
    straggler_id,
    car_id,
    set_out_timestamp
  FROM missing_set_out_events
  
  UNION ALL
  
  -- Stalled stragglers
  SELECT 
    'STALLED_STRAGGLER' AS violation_type,
    severity,
    'Car waiting ' || days_waiting || ' days without pickup' AS violation_details,
    straggler_id,
    car_id,
    set_out_timestamp
  FROM stalled_stragglers
  
  UNION ALL
  
  -- Stats issues
  SELECT 
    'STRAGGLER_STATS_ISSUE' AS violation_type,
    severity,
    stats_issue || ' - ' || stats_details AS violation_details,
    NULL AS straggler_id,
    NULL AS car_id,
    NULL AS set_out_timestamp
  FROM valid_stats_issues
)

-- Return all violations ordered by severity
SELECT 
  violation_type,
  severity,
  violation_details,
  straggler_id,
  car_id,
  set_out_timestamp,
  COUNT(*) OVER (PARTITION BY violation_type) AS violation_count
FROM all_violations
ORDER BY 
  CASE severity
    WHEN 'CRITICAL' THEN 1
    WHEN 'WARNING' THEN 2
    WHEN 'INFO' THEN 3
  END,
  violation_type,
  straggler_id;
