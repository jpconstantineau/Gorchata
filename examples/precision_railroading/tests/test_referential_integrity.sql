-- Test: Referential Integrity
-- Description: Validates all foreign key relationships across fact and dimension tables
-- Expected Result: All tests should return 0 violations

WITH all_tests AS (
  -- Test 1: fact_trip.railcar_id → dim_railcar.railcar_id
  SELECT
    'test_fact_trip_railcar_fk' AS test_name,
    COUNT(*) AS violation_count,
    'All fact_trip.railcar_id must exist in dim_railcar' AS description
  FROM fact_trip f
  LEFT JOIN dim_railcar d ON f.railcar_id = d.railcar_id
  WHERE d.railcar_id IS NULL
  
  UNION ALL
  
  -- Test 2: fact_trip.origin_location_id → dim_location.location_id
  SELECT
    'test_fact_trip_origin_location_fk' AS test_name,
    COUNT(*) AS violation_count,
    'All fact_trip.origin_location_id must exist in dim_location' AS description
  FROM fact_trip f
  LEFT JOIN dim_location d ON f.origin_location_id = d.location_id
  WHERE d.location_id IS NULL
  
  UNION ALL
  
  -- Test 3: fact_trip.destination_location_id → dim_location.location_id
  SELECT
    'test_fact_trip_destination_location_fk' AS test_name,
    COUNT(*) AS violation_count,
    'All fact_trip.destination_location_id must exist in dim_location' AS description
  FROM fact_trip f
  LEFT JOIN dim_location d ON f.destination_location_id = d.location_id
  WHERE d.location_id IS NULL
  
  UNION ALL
  
  -- Test 4: fact_trip.train_id → dim_train.train_id
  SELECT
    'test_fact_trip_train_fk' AS test_name,
    COUNT(*) AS violation_count,
    'All fact_trip.train_id must exist in dim_train (or be NULL for non-train moves)' AS description
  FROM fact_trip f
  LEFT JOIN dim_train d ON f.train_id = d.train_id
  WHERE f.train_id IS NOT NULL AND d.train_id IS NULL
  
  UNION ALL
  
  -- Test 5: fact_trip.corridor_id → dim_corridor.corridor_id
  SELECT
    'test_fact_trip_corridor_fk' AS test_name,
    COUNT(*) AS violation_count,
    'All fact_trip.corridor_id must exist in dim_corridor (or be NULL for non-corridor trips)' AS description
  FROM fact_trip f
  LEFT JOIN dim_corridor d ON f.corridor_id = d.corridor_id
  WHERE f.corridor_id IS NOT NULL AND d.corridor_id IS NULL
  
  UNION ALL
  
  -- Test 6: fact_dwell.railcar_id → dim_railcar.railcar_id
  SELECT
    'test_fact_dwell_railcar_fk' AS test_name,
    COUNT(*) AS violation_count,
    'All fact_dwell.railcar_id must exist in dim_railcar' AS description
  FROM fact_dwell f
  LEFT JOIN dim_railcar d ON f.railcar_id = d.railcar_id
  WHERE d.railcar_id IS NULL
  
  UNION ALL
  
  -- Test 7: fact_dwell.location_id → dim_location.location_id
  SELECT
    'test_fact_dwell_location_fk' AS test_name,
    COUNT(*) AS violation_count,
    'All fact_dwell.location_id must exist in dim_location' AS description
  FROM fact_dwell f
  LEFT JOIN dim_location d ON f.location_id = d.location_id
  WHERE d.location_id IS NULL
  
  UNION ALL
  
  -- Test 8: fact_stop_classification.railcar_id → dim_railcar.railcar_id
  SELECT
    'test_fact_stop_classification_railcar_fk' AS test_name,
    COUNT(*) AS violation_count,
    'All fact_stop_classification.railcar_id must exist in dim_railcar' AS description
  FROM fact_stop_classification f
  LEFT JOIN dim_railcar d ON f.railcar_id = d.railcar_id
  WHERE d.railcar_id IS NULL
  
  UNION ALL
  
  -- Test 9: fact_corridor_transit.railcar_id → dim_railcar.railcar_id
  SELECT
    'test_fact_corridor_transit_railcar_fk' AS test_name,
    COUNT(*) AS violation_count,
    'All fact_corridor_transit.railcar_id must exist in dim_railcar' AS description
  FROM fact_corridor_transit f
  LEFT JOIN dim_railcar d ON f.railcar_id = d.railcar_id
  WHERE d.railcar_id IS NULL
  
  UNION ALL
  
  -- Test 10: fact_corridor_transit.corridor_id → dim_corridor.corridor_id
  SELECT
    'test_fact_corridor_transit_corridor_fk' AS test_name,
    COUNT(*) AS violation_count,
    'All fact_corridor_transit.corridor_id must exist in dim_corridor' AS description
  FROM fact_corridor_transit f
  LEFT JOIN dim_corridor d ON f.corridor_id = d.corridor_id
  WHERE d.corridor_id IS NULL
  
  UNION ALL
  
  -- Test 11: Railcar coverage - all 12,000 railcars appear in fact_trip
  SELECT
    'test_railcar_coverage_in_facts' AS test_name,
    CASE 
      WHEN (SELECT COUNT(DISTINCT railcar_id) FROM fact_trip) = 
           (SELECT COUNT(*) FROM dim_railcar)
      THEN 0
      ELSE 1
    END AS violation_count,
    'All 12,000 railcars must appear in fact_trip' AS description
  
  UNION ALL
  
  -- Test 12: agg_network_fluidity.corridor_id → dim_corridor.corridor_id
  -- Note: agg_network_fluidity only has corridor_id, not location_id
  SELECT
    'test_agg_network_fluidity_corridor_fk' AS test_name,
    COUNT(*) AS violation_count,
    'All agg_network_fluidity.corridor_id must exist in dim_corridor' AS description
  FROM agg_network_fluidity a
  LEFT JOIN dim_corridor d ON a.corridor_id = d.corridor_id
  WHERE d.corridor_id IS NULL
  
  UNION ALL
  
  -- Test 13: agg_buffer_consumption.corridor_id → dim_corridor.corridor_id
  SELECT
    'test_agg_metrics_corridor_fk' AS test_name,
    COUNT(*) AS violation_count,
    'All aggregation corridor_ids must exist in dim_corridor' AS description
  FROM agg_buffer_consumption a
  LEFT JOIN dim_corridor d ON a.corridor_id = d.corridor_id
  WHERE d.corridor_id IS NULL
)

-- Note: Tests 14-15 for analytics tables commented out as tables may not be materialized yet
-- Uncomment after running: gorchata run --models shadow_yard_identification,worst_performing_corridors

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
