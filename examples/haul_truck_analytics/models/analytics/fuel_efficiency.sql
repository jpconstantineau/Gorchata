{{ config "materialized" "view" }}

-- Fuel Efficiency Analysis
-- Business Question: Which trucks and operators are most/least fuel efficient?
-- 
-- This query analyzes fuel consumption patterns by truck and calculates
-- efficiency metrics including liters per ton and distance-adjusted fuel economy.
-- Use this to:
-- - Identify maintenance needs (poor fuel economy)
-- - Optimize truck utilization
-- - Benchmark operator performance
-- - Budget fuel costs accurately

WITH truck_fuel_aggregation AS (
  -- Aggregate fuel consumption and production by truck
  SELECT
    f.truck_id,
    t.model,
    t.fleet_class,
    t.payload_capacity_tons,
    
    -- Fuel consumption
    SUM(f.fuel_consumed_liters) AS total_fuel_consumed_liters,
    COUNT(f.cycle_id) AS total_cycles,
    
    -- Production
    SUM(f.payload_tons) AS total_tons_moved,
    
    -- Distance
    SUM(f.distance_loaded_km + f.distance_empty_km) AS total_distance_km,
    AVG(f.distance_loaded_km + f.distance_empty_km) AS avg_distance_per_cycle_km,
    
    -- Operating time
    SUM((julianday(f.cycle_end) - julianday(f.cycle_start)) * 24) AS total_operating_hours
    
  FROM {{ ref "fact_haul_cycle" }} f
  INNER JOIN {{ ref "dim_truck" }} t ON f.truck_id = t.truck_id
  GROUP BY f.truck_id, t.model, t.fleet_class, t.payload_capacity_tons
),

efficiency_calculation AS (
  -- Calculate fuel efficiency metrics
  SELECT
    truck_id,
    model,
    fleet_class,
    payload_capacity_tons,
    total_cycles,
    ROUND(total_fuel_consumed_liters, 2) AS total_fuel_consumed_liters,
    ROUND(total_tons_moved, 2) AS total_tons_moved,
    
    -- Liters per ton (primary efficiency metric)
    ROUND(
      total_fuel_consumed_liters / NULLIF(total_tons_moved, 0),
      3
    ) AS liters_per_ton,
    
    -- Distance metrics
    ROUND(avg_distance_per_cycle_km, 2) AS avg_distance_per_cycle_km,
    ROUND(total_distance_km, 2) AS total_distance_km,
    
    -- Ton-miles (ton-kilometers)
    ROUND(total_tons_moved * total_distance_km, 2) AS ton_miles,
    
    -- Liters per ton-mile (distance-adjusted efficiency)
    ROUND(
      total_fuel_consumed_liters / NULLIF(total_tons_moved * total_distance_km, 0),
      5
    ) AS liters_per_ton_mile,
    
    -- Fuel per operating hour
    ROUND(
      total_fuel_consumed_liters / NULLIF(total_operating_hours, 0),
      2
    ) AS liters_per_hour,
    
    total_operating_hours
    
  FROM truck_fuel_aggregation
),

fleet_benchmarks AS (
  -- Calculate fleet-wide benchmarks
  SELECT
    fleet_class,
    AVG(liters_per_ton) AS fleet_avg_liters_per_ton,
    AVG(liters_per_ton_mile) AS fleet_avg_liters_per_ton_mile,
    AVG(liters_per_hour) AS fleet_avg_liters_per_hour
  FROM efficiency_calculation
  GROUP BY fleet_class
),

efficiency_ranking AS (
  -- Rank trucks by efficiency
  SELECT
    ec.*,
    
    -- Efficiency rank within fleet class (1 = most efficient)
    ROW_NUMBER() OVER (
      PARTITION BY ec.fleet_class
      ORDER BY ec.liters_per_ton ASC
    ) AS efficiency_rank,
    
    -- Compare to fleet average
    ROUND(
      (ec.liters_per_ton / NULLIF(fb.fleet_avg_liters_per_ton, 0) - 1) * 100,
      2
    ) AS efficiency_vs_fleet_avg_pct,
    
    -- Distance-adjusted comparison
    ROUND(
      (ec.liters_per_ton_mile / NULLIF(fb.fleet_avg_liters_per_ton_mile, 0) - 1) * 100,
      2
    ) AS fuel_per_ton_mile_vs_avg_pct
    
  FROM efficiency_calculation ec
  INNER JOIN fleet_benchmarks fb ON ec.fleet_class = fb.fleet_class
)

-- Final output with efficiency assessment
SELECT
  truck_id,
  model,
  fleet_class,
  total_fuel_consumed_liters,
  total_tons_moved,
  liters_per_ton,
  avg_distance_per_cycle_km,
  ton_miles,
  liters_per_ton_mile,
  efficiency_rank,
  efficiency_vs_fleet_avg_pct,
  
  -- Efficiency assessment
  CASE
    WHEN efficiency_vs_fleet_avg_pct > 15 THEN
      'POOR EFFICIENCY - ' || 
      CAST(ROUND(efficiency_vs_fleet_avg_pct, 1) AS TEXT) || '% above fleet average. ' ||
      'Priority maintenance check: engine tune-up, tire pressure, drive train wear'
    WHEN efficiency_vs_fleet_avg_pct > 8 THEN
      'BELOW AVERAGE - ' || 
      CAST(ROUND(efficiency_vs_fleet_avg_pct, 1) AS TEXT) || '% above fleet average. ' ||
      'Schedule preventive maintenance and operator review'
    WHEN efficiency_vs_fleet_avg_pct < -8 THEN
      'EXCELLENT EFFICIENCY - ' || 
      CAST(ROUND(ABS(efficiency_vs_fleet_avg_pct), 1) AS TEXT) || '% below fleet average. ' ||
      'Benchmark equipment - maintain current practices'
    WHEN efficiency_vs_fleet_avg_pct < -3 THEN
      'GOOD EFFICIENCY - Better than fleet average. Continue monitoring'
    ELSE
      'AVERAGE EFFICIENCY - Within normal range. Monitor for changes'
  END AS efficiency_assessment,
  
  -- Cost implications (assuming $1.50/liter diesel)
  ROUND(total_fuel_consumed_liters * 1.50, 2) AS estimated_fuel_cost_usd,
  
  -- Savings potential vs fleet average
  ROUND(
    (liters_per_ton - (SELECT AVG(liters_per_ton) FROM efficiency_calculation WHERE fleet_class = efficiency_ranking.fleet_class)) *
    total_tons_moved * 1.50,
    2
  ) AS fuel_cost_vs_fleet_avg_usd,
  
  -- Operational metrics
  total_cycles,
  ROUND(total_operating_hours, 2) AS total_operating_hours,
  ROUND(liters_per_hour, 2) AS liters_per_hour,
  fuel_per_ton_mile_vs_avg_pct

FROM efficiency_ranking
ORDER BY fleet_class, efficiency_rank ASC
