-- Train Integrity Validation
-- Verifies train operations have valid start/end locations, consistent corridors,
-- reasonable durations, and proper capacity constraints
-- Expected outcome: 0 violations = valid operations

-- =============================================================================
-- VALIDATION 1: Train Trip Location Validation
-- =============================================================================
-- Every trip should have valid origin and destination from dim_location
WITH invalid_location_trips AS (
  SELECT 
    t.trip_id,
    t.train_id,
    t.corridor_id,
    c.origin_location_id,
    c.destination_location_id,
    'CRITICAL' AS severity
  FROM fact_train_trip t
  JOIN dim_corridor c ON t.corridor_id = c.corridor_id
  LEFT JOIN dim_location lo ON c.origin_location_id = lo.location_id
  LEFT JOIN dim_location ld ON c.destination_location_id = ld.location_id
  WHERE lo.location_id IS NULL OR ld.location_id IS NULL
),

-- =============================================================================
-- VALIDATION 2: Corridor Consistency
-- =============================================================================
-- Each train's corridor should match its origin/destination in events
trip_corridor_consistency AS (
  SELECT 
    t.trip_id,
    t.train_id,
    t.corridor_id,
    c.origin_location_id AS expected_origin,
    c.destination_location_id AS expected_destination
  FROM fact_train_trip t
  JOIN dim_corridor c ON t.corridor_id = c.corridor_id
),

-- Get actual origin/destination from car location events
actual_trip_endpoints AS (
  SELECT 
    train_id,
    MIN(CASE WHEN event_type = 'departed_origin' THEN location_id END) AS actual_origin,
    MAX(CASE WHEN event_type = 'arrived_destination' THEN location_id END) AS actual_destination
  FROM fact_car_location_event
  WHERE train_id IS NOT NULL
  GROUP BY train_id
),

corridor_mismatch AS (
  SELECT 
    tc.trip_id,
    tc.train_id,
    tc.corridor_id,
    tc.expected_origin,
    tc.expected_destination,
    ae.actual_origin,
    ae.actual_destination,
    'CRITICAL' AS severity
  FROM trip_corridor_consistency tc
  LEFT JOIN actual_trip_endpoints ae ON tc.train_id = ae.train_id
  WHERE 
    (ae.actual_origin IS NOT NULL AND ae.actual_origin != tc.expected_origin)
    OR (ae.actual_destination IS NOT NULL AND ae.actual_destination != tc.expected_destination)
),

-- =============================================================================
-- VALIDATION 3: Trip Duration Validation
-- =============================================================================
-- Verify trip durations are reasonable (no negatives, no extreme outliers)
duration_checks AS (
  SELECT 
    trip_id,
    train_id,
    corridor_id,
    origin_queue_hours,
    destination_queue_hours,
    transit_hours,
    total_trip_hours,
    CASE
      WHEN origin_queue_hours < 0 THEN 'Negative origin queue time'
      WHEN destination_queue_hours < 0 THEN 'Negative destination queue time'
      WHEN transit_hours < 0 THEN 'Negative transit time'
      WHEN total_trip_hours < 0 THEN 'Negative total trip time'
      WHEN transit_hours > 240 THEN 'Transit time exceeds 10 days (240h)'
      WHEN origin_queue_hours > 72 THEN 'Origin queue exceeds 72 hours'
      WHEN destination_queue_hours > 72 THEN 'Destination queue exceeds 72 hours'
      WHEN ABS(total_trip_hours - (origin_queue_hours + transit_hours + destination_queue_hours)) > 1 
        THEN 'Total trip time calculation mismatch'
      ELSE NULL
    END AS duration_issue
  FROM fact_train_trip
),

invalid_durations AS (
  SELECT 
    trip_id,
    train_id,
    corridor_id,
    duration_issue,
    CASE 
      WHEN duration_issue LIKE 'Negative%' THEN 'CRITICAL'
      WHEN duration_issue LIKE '%exceeds%' THEN 'WARNING'
      ELSE 'WARNING'
    END AS severity
  FROM duration_checks
  WHERE duration_issue IS NOT NULL
),

-- =============================================================================
-- VALIDATION 4: Train Capacity Validation
-- =============================================================================
-- Verify trains don't exceed capacity (typically 75 cars per train)
train_capacity AS (
  SELECT 
    train_id,
    num_cars AS declared_capacity
  FROM dim_train
),

actual_car_counts AS (
  SELECT 
    train_id,
    COUNT(DISTINCT car_id) AS actual_car_count
  FROM fact_car_location_event
  WHERE train_id IS NOT NULL
    AND event_type IN ('train_formed', 'departed_origin', 'arrived_destination')
  GROUP BY train_id
),

capacity_violations AS (
  SELECT 
    tc.train_id,
    tc.declared_capacity,
    acc.actual_car_count,
    'WARNING' AS severity
  FROM train_capacity tc
  JOIN actual_car_counts acc ON tc.train_id = acc.train_id
  WHERE acc.actual_car_count > tc.declared_capacity
    OR acc.actual_car_count < (tc.declared_capacity - 5)  -- Allow small variance for stragglers
),

-- =============================================================================
-- VALIDATION 5: Expected vs Actual Transit Time
-- =============================================================================
-- Compare actual transit times to corridor expected transit times
transit_time_comparison AS (
  SELECT 
    t.trip_id,
    t.train_id,
    t.corridor_id,
    t.transit_hours AS actual_transit,
    c.expected_transit_hours,
    ABS(t.transit_hours - c.expected_transit_hours) AS variance_hours,
    (ABS(t.transit_hours - c.expected_transit_hours) / c.expected_transit_hours) * 100 AS variance_pct
  FROM fact_train_trip t
  JOIN dim_corridor c ON t.corridor_id = c.corridor_id
),

excessive_transit_variance AS (
  SELECT 
    trip_id,
    train_id,
    corridor_id,
    actual_transit,
    expected_transit_hours,
    variance_hours,
    variance_pct,
    CASE
      WHEN variance_pct > 50 THEN 'CRITICAL'
      WHEN variance_pct > 30 THEN 'WARNING'
      ELSE 'INFO'
    END AS severity
  FROM transit_time_comparison
  WHERE variance_pct > 30  -- Flag if more than 30% variance
),

-- =============================================================================
-- VALIDATION 6: Orphan Trips
-- =============================================================================
-- Trips in fact_train_trip should have corresponding train in dim_train
orphan_trips AS (
  SELECT 
    t.trip_id,
    t.train_id,
    'CRITICAL' AS severity
  FROM fact_train_trip t
  LEFT JOIN dim_train d ON t.train_id = d.train_id
  WHERE d.train_id IS NULL
),

-- =============================================================================
-- VALIDATION 7: Missing Trip Metrics
-- =============================================================================
-- Trips should have all required metrics populated
incomplete_trips AS (
  SELECT 
    trip_id,
    train_id,
    corridor_id,
    CASE
      WHEN origin_queue_hours IS NULL THEN 'Missing origin queue time'
      WHEN destination_queue_hours IS NULL THEN 'Missing destination queue time'
      WHEN transit_hours IS NULL THEN 'Missing transit time'
      WHEN total_trip_hours IS NULL THEN 'Missing total trip time'
      WHEN num_stragglers IS NULL THEN 'Missing straggler count'
      ELSE NULL
    END AS missing_metric,
    'WARNING' AS severity
  FROM fact_train_trip
),

valid_incomplete_trips AS (
  SELECT * FROM incomplete_trips WHERE missing_metric IS NOT NULL
),

-- =============================================================================
-- CONSOLIDATED VIOLATION REPORT
-- =============================================================================
all_violations AS (
  -- Invalid location trips
  SELECT 
    'INVALID_LOCATION' AS violation_type,
    severity,
    'Trip references invalid location' AS violation_details,
    trip_id,
    train_id,
    corridor_id
  FROM invalid_location_trips
  
  UNION ALL
  
  -- Corridor mismatches
  SELECT 
    'CORRIDOR_MISMATCH' AS violation_type,
    severity,
    'Expected origin: ' || expected_origin || ', actual: ' || COALESCE(actual_origin, 'NULL') || 
    '; Expected dest: ' || expected_destination || ', actual: ' || COALESCE(actual_destination, 'NULL') AS violation_details,
    trip_id,
    train_id,
    corridor_id
  FROM corridor_mismatch
  
  UNION ALL
  
  -- Invalid durations
  SELECT 
    'INVALID_DURATION' AS violation_type,
    severity,
    duration_issue AS violation_details,
    trip_id,
    train_id,
    corridor_id
  FROM invalid_durations
  
  UNION ALL
  
  -- Capacity violations
  SELECT 
    'CAPACITY_VIOLATION' AS violation_type,
    severity,
    'Declared: ' || CAST(declared_capacity AS TEXT) || ' cars, actual: ' || CAST(actual_car_count AS TEXT) AS violation_details,
    NULL AS trip_id,
    train_id,
    NULL AS corridor_id
  FROM capacity_violations
  
  UNION ALL
  
  -- Excessive transit variance
  SELECT 
    'EXCESSIVE_TRANSIT_VARIANCE' AS violation_type,
    severity,
    'Actual: ' || CAST(ROUND(actual_transit, 1) AS TEXT) || 'h, expected: ' || 
    CAST(ROUND(expected_transit_hours, 1) AS TEXT) || 'h (variance: ' || 
    CAST(ROUND(variance_pct, 1) AS TEXT) || '%)' AS violation_details,
    trip_id,
    train_id,
    corridor_id
  FROM excessive_transit_variance
  
  UNION ALL
  
  -- Orphan trips
  SELECT 
    'ORPHAN_TRIP' AS violation_type,
    severity,
    'Trip references non-existent train' AS violation_details,
    trip_id,
    train_id,
    NULL AS corridor_id
  FROM orphan_trips
  
  UNION ALL
  
  -- Incomplete trips
  SELECT 
    'INCOMPLETE_TRIP' AS violation_type,
    severity,
    missing_metric AS violation_details,
    trip_id,
    train_id,
    corridor_id
  FROM valid_incomplete_trips
)

-- Return all violations ordered by severity
SELECT 
  violation_type,
  severity,
  violation_details,
  trip_id,
  train_id,
  corridor_id,
  COUNT(*) OVER (PARTITION BY violation_type) AS violation_count
FROM all_violations
ORDER BY 
  CASE severity
    WHEN 'CRITICAL' THEN 1
    WHEN 'WARNING' THEN 2
    WHEN 'INFO' THEN 3
  END,
  violation_type,
  trip_id;
