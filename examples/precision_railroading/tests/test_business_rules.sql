-- Test: Business Rules
-- Description: Validates domain-specific business rules (velocities, distances, PSR periods, risk scores, etc.)
-- Expected Result: All tests should return 0 violations

WITH all_tests AS (
  -- Test 1: Velocity within physical limits (0-80 mph for freight trains)
  SELECT
    'test_velocity_physical_limits' AS test_name,
    COUNT(*) AS violation_count,
    'Average velocity must be between 0 and 80 mph (freight train limits)' AS description
  FROM fact_trip
  WHERE average_velocity_mph < 0 OR average_velocity_mph > 80
  
  UNION ALL
  
  -- Test 2: Dwell durations reasonable (1 minute to 7 days)
  SELECT
    'test_dwell_duration_reasonable' AS test_name,
    COUNT(*) AS violation_count,
    'Dwell durations must be between 1 minute and 7 days (10,080 minutes)' AS description
  FROM fact_dwell
  WHERE dwell_duration_minutes < 1 OR dwell_duration_minutes > 10080
  
  UNION ALL
  
  -- Test 3: Trip distance matches corridor distance (±5% tolerance)
  SELECT
    'test_trip_distance_corridor_match' AS test_name,
    COUNT(*) AS violation_count,
    'Trip distances should match corridor distances within ±5% for corridor trips' AS description
  FROM fact_trip f
  INNER JOIN dim_corridor c ON f.corridor_id = c.corridor_id
  WHERE ABS(f.distance_miles - c.distance_miles) > (c.distance_miles * 0.05)
  
  UNION ALL
  
  -- Test 4: Load status consistency throughout trip cycle
  SELECT
    'test_load_status_consistency' AS test_name,
    COUNT(*) AS violation_count,
    'Trip type must be either loaded or empty (not NULL)' AS description
  FROM fact_trip
  WHERE trip_type IS NULL OR trip_type NOT IN ('loaded', 'empty')
  
  UNION ALL
  
  -- Test 5: PSR period assignments correct
  SELECT
    'test_psr_period_assignments' AS test_name,
    COUNT(*) AS violation_count,
    'PSR period must be one of: pre-PSR, transition, mature' AS description
  FROM fact_trip
  WHERE psr_period NOT IN ('pre-PSR', 'transition', 'mature')
  
  UNION ALL
  
  -- Test 6: Shadow yard risk scores in valid range [0, 100]
  SELECT
    'test_shadow_yard_risk_score_range' AS test_name,
    COUNT(*) AS violation_count,
    'Shadow yard risk scores must be between 0 and 100' AS description
  FROM dim_location
  WHERE shadow_yard_risk_score < 0 OR shadow_yard_risk_score > 100
  
  UNION ALL
  
  -- Test 7: Fluidity scores in valid range [0, 100]
  SELECT
    'test_fluidity_score_range' AS test_name,
    COUNT(*) AS violation_count,
    'Fluidity scores must be between 0 and 100' AS description
  FROM agg_network_fluidity
  WHERE fluidity_score < 0 OR fluidity_score > 100
  
  UNION ALL
  
  -- Note: Test 8 (seasonal_performance_trends) commented out - analytics table may not exist yet
  -- 'test_seasonal_variance_within_threshold'
  
  -- Test 9: Train ID format valid (should match pattern if not NULL)
  SELECT
    'test_train_id_format' AS test_name,
    COUNT(*) AS violation_count,
    'Train IDs should be positive integers' AS description
  FROM fact_trip
  WHERE train_id IS NOT NULL AND train_id <= 0
  
  UNION ALL
  
  -- Test 10: Railcar types are valid
  SELECT
    'test_railcar_type_valid' AS test_name,
    COUNT(*) AS violation_count,
    'Railcar types must be one of: hopper, tank, box, gondola, intermodal' AS description
  FROM dim_railcar
  WHERE car_type NOT IN ('hopper', 'tank', 'box', 'gondola', 'intermodal')
  
  UNION ALL
  
  -- Test 11: Location types are valid
  SELECT
    'test_location_type_valid' AS test_name,
    COUNT(*) AS violation_count,
    'Location types must be: terminal, interchange, yard, customer_site, or siding' AS description
  FROM dim_location
  WHERE facility_type NOT IN ('terminal', 'interchange', 'yard', 'customer_site', 'siding')
  
  UNION ALL
  
  -- Test 12: Corridor directionality exists (origin != destination)
  SELECT
    'test_corridor_directionality' AS test_name,
    COUNT(*) AS violation_count,
    'Corridors must have different origin and destination' AS description
  FROM dim_corridor
  WHERE origin_splc = destination_splc
  
  UNION ALL
  
  -- Test 13: Slot adherence is a valid percentage [0, 100]
  SELECT
    'test_slot_adherence_percentage' AS test_name,
    COUNT(*) AS violation_count,
    'Slot adherence must be between 0 and 100' AS description
  FROM agg_slot_adherence
  WHERE adherence_score < 0 OR adherence_score > 100
  
  UNION ALL
  
  -- Test 14: Asymmetry ratios are positive
  SELECT
    'test_asymmetry_ratio_positive' AS test_name,
    COUNT(*) AS violation_count,
    'Directional asymmetry ratios must be positive' AS description
  FROM agg_directional_asymmetry
  WHERE asymmetry_ratio < 0
  
  UNION ALL
  
  -- Test 15: Buffer consumption is a valid percentage [0, 200] (can exceed 100% if overutilized)
  SELECT
    'test_buffer_consumption_percentage' AS test_name,
    COUNT(*) AS violation_count,
    'Buffer consumption must be between 0 and 200% (allowing overutilization)' AS description
  FROM agg_buffer_consumption
  WHERE buffer_consumption_percentage < 0 OR buffer_consumption_percentage > 200
)

SELECT
  test_name,
  violation_count,
  description,
  CASE
    WHEN violation_count = 0 THEN 'PASS'
    ELSE 'FAIL'
  END AS status
FROM all_tests
ORDER BY test_name
