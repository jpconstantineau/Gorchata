-- Test: Referential Integrity Validation
-- Description: Validates all foreign key relationships exist in dimension tables
-- Expected Result: violation_count = 0 (all foreign keys resolve)

WITH violations AS (
  -- Check truck_id foreign key
  SELECT 
    'fact_haul_cycle.truck_id' as fk_column,
    fc.truck_id as fk_value,
    'dim_truck' as ref_table
  FROM fact_haul_cycle fc
  LEFT JOIN dim_truck dt ON fc.truck_id = dt.truck_id
  WHERE dt.truck_id IS NULL
  
  UNION ALL
  
  -- Check shovel_id foreign key
  SELECT 
    'fact_haul_cycle.shovel_id' as fk_column,
    fc.shovel_id as fk_value,
    'dim_shovel' as ref_table
  FROM fact_haul_cycle fc
  LEFT JOIN dim_shovel ds ON fc.shovel_id = ds.shovel_id
  WHERE ds.shovel_id IS NULL
  
  UNION ALL
  
  -- Check crusher_id foreign key
  SELECT 
    'fact_haul_cycle.crusher_id' as fk_column,
    fc.crusher_id as fk_value,
    'dim_crusher' as ref_table
  FROM fact_haul_cycle fc
  LEFT JOIN dim_crusher dc ON fc.crusher_id = dc.crusher_id
  WHERE dc.crusher_id IS NULL
  
  UNION ALL
  
  -- Check operator_id foreign key
  SELECT 
    'fact_haul_cycle.operator_id' as fk_column,
    fc.operator_id as fk_value,
    'dim_operator' as ref_table
  FROM fact_haul_cycle fc
  LEFT JOIN dim_operator do ON fc.operator_id = do.operator_id
  WHERE do.operator_id IS NULL
  
  UNION ALL
  
  -- Check shift_id foreign key
  SELECT 
    'fact_haul_cycle.shift_id' as fk_column,
    fc.shift_id as fk_value,
    'dim_shift' as ref_table
  FROM fact_haul_cycle fc
  LEFT JOIN dim_shift dsh ON fc.shift_id = dsh.shift_id
  WHERE dsh.shift_id IS NULL
  
  UNION ALL
  
  -- Check date_id foreign key
  SELECT 
    'fact_haul_cycle.date_id' as fk_column,
    CAST(fc.date_id AS TEXT) as fk_value,
    'dim_date' as ref_table
  FROM fact_haul_cycle fc
  LEFT JOIN dim_date dd ON fc.date_id = dd.date_key
  WHERE dd.date_key IS NULL
)

SELECT 
  COUNT(*) as violation_count,
  'Referential Integrity' as test_name,
  'All foreign keys in fact_haul_cycle must reference valid dimension records' as test_description,
  CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as test_result
FROM violations;
