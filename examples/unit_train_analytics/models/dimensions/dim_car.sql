{{ config "materialized" "table" }}

-- Car Dimension
-- All unique rail cars in the fleet with metadata
-- Type 1 SCD (current state only)

WITH distinct_cars AS (
  -- Get all unique cars from CLM events
  SELECT DISTINCT
    car_id
  FROM {{ seed "raw_clm_events" }}
),

car_attributes AS (
  -- Add car attributes (all cars are coal hoppers in this example)
  SELECT
    car_id,
    'COAL_HOPPER' AS car_type,
    100.0 AS capacity_tons  -- Standard 100-ton coal hopper capacity
  FROM distinct_cars
)

SELECT
  car_id,
  car_type,
  capacity_tons
FROM car_attributes
ORDER BY car_id
