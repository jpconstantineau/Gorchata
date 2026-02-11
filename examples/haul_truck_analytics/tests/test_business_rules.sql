-- Test: Business Rules Validation
-- Description: Validates business logic rules (payload, cycle time, speeds, durations)
-- Expected Result: Multiple rows, one per rule, with violation counts

WITH 

-- Rule 1: Payload within 0-115% of truck capacity
payload_violations AS (
  SELECT fc.cycle_id
  FROM fact_haul_cycle fc
  JOIN dim_truck dt ON fc.truck_id = dt.truck_id
  WHERE fc.payload_tons < 0
     OR fc.payload_tons > (dt.payload_capacity_tons * 1.15)
),

-- Rule 2: Cycle time within 10-180 minutes
cycle_time_violations AS (
  SELECT cycle_id
  FROM fact_haul_cycle
  WHERE total_cycle_time_min < 10
     OR total_cycle_time_min > 180
),

-- Rule 3: Loading duration within 2-15 minutes
loading_duration_violations AS (
  SELECT cycle_id
  FROM fact_haul_cycle
  WHERE duration_loading_min < 2
     OR duration_loading_min > 15
),

-- Rule 4: Dumping duration within 0.5-5 minutes
dumping_duration_violations AS (
  SELECT cycle_id
  FROM fact_haul_cycle
  WHERE duration_dumping_min < 0.5
     OR duration_dumping_min > 5
),

-- Rule 5: Distance within 0-50 km (reasonable haul distance)
distance_violations AS (
  SELECT cycle_id
  FROM fact_haul_cycle
  WHERE distance_loaded_km < 0
     OR distance_loaded_km > 50
     OR distance_empty_km < 0
     OR distance_empty_km > 50
),

-- Rule 6: Speed within 0-80 km/h
speed_violations AS (
  SELECT cycle_id
  FROM fact_haul_cycle
  WHERE speed_avg_loaded_kmh < 0
     OR speed_avg_loaded_kmh >= 80
     OR speed_avg_empty_kmh < 0
     OR speed_avg_empty_kmh >= 80
),

-- Rule 7: Loaded speed < Empty speed (physics constraint)
speed_logic_violations AS (
  SELECT cycle_id
  FROM fact_haul_cycle
  WHERE speed_avg_loaded_kmh >= speed_avg_empty_kmh
),

-- Rule 8: Fuel consumed within 0-1000 liters per cycle
fuel_violations AS (
  SELECT cycle_id
  FROM fact_haul_cycle
  WHERE fuel_consumed_liters < 0
     OR fuel_consumed_liters > 1000
)

-- Return results for allrules
SELECT 'Payload Range (0-115% capacity)' as rule_name,
       (SELECT COUNT(*) FROM payload_violations) as violation_count,
       CASE WHEN (SELECT COUNT(*) FROM payload_violations) = 0 THEN 'PASS' ELSE 'FAIL' END as test_result

UNION ALL
SELECT 'Cycle Time Range (10-180 min)' as rule_name,
       (SELECT COUNT(*) FROM cycle_time_violations) as violation_count,
       CASE WHEN (SELECT COUNT(*) FROM cycle_time_violations) = 0 THEN 'PASS' ELSE 'FAIL' END as test_result

UNION ALL
SELECT 'Loading Duration (2-15 min)' as rule_name,
       (SELECT COUNT(*) FROM loading_duration_violations) as violation_count,
       CASE WHEN (SELECT COUNT(*) FROM loading_duration_violations) = 0 THEN 'PASS' ELSE 'FAIL' END as test_result

UNION ALL
SELECT 'Dumping Duration (0.5-5 min)' as rule_name,
       (SELECT COUNT(*) FROM dumping_duration_violations) as violation_count,
       CASE WHEN (SELECT COUNT(*) FROM dumping_duration_violations) = 0 THEN 'PASS' ELSE 'FAIL' END as test_result

UNION ALL
SELECT 'Distance Range (0-50 km)' as rule_name,
       (SELECT COUNT(*) FROM distance_violations) as violation_count,
       CASE WHEN (SELECT COUNT(*) FROM distance_violations) = 0 THEN 'PASS' ELSE 'FAIL' END as test_result

UNION ALL
SELECT 'Speed Range (0-80 km/h)' as rule_name,
       (SELECT COUNT(*) FROM speed_violations) as violation_count,
       CASE WHEN (SELECT COUNT(*) FROM speed_violations) = 0 THEN 'PASS' ELSE 'FAIL' END as test_result

UNION ALL
SELECT 'Speed Logic (loaded < empty)' as rule_name,
       (SELECT COUNT(*) FROM speed_logic_violations) as violation_count,
       CASE WHEN (SELECT COUNT(*) FROM speed_logic_violations) = 0 THEN 'PASS' ELSE 'FAIL' END as test_result

UNION ALL
SELECT 'Fuel Consumption (0-1000 L)' as rule_name,
       (SELECT COUNT(*) FROM fuel_violations) as violation_count,
       CASE WHEN (SELECT COUNT(*) FROM fuel_violations) = 0 THEN 'PASS' ELSE 'FAIL' END as test_result;
