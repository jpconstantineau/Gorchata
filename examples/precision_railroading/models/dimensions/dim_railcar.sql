{{ config "materialized" "table" }}

-- Railcar Dimension
-- 12,000 unique railcars extracted from CLM events
-- Distributed across 7 Class I railroads with appropriate car types and capacities
-- Fleet composition: hopper 35%, tank 25%, box 20%, gondola 15%, intermodal 5%

WITH distinct_cars AS (
  -- Get all unique railcars from raw CLM events
  SELECT DISTINCT
    car_number
  FROM {{ seed "raw_clm_events" }}
),

car_enrichment AS (
  -- Add car attributes based on car number patterns and hash-based distribution
  SELECT
    car_number,
    
    -- Railroad owner (extract from prefix)
    CASE
      WHEN car_number LIKE 'BNSF%' THEN 'BNSF'
      WHEN car_number LIKE 'UP%' THEN 'UP'
      WHEN car_number LIKE 'CSX%' THEN 'CSX'
      WHEN car_number LIKE 'NS%' THEN 'NS'
      WHEN car_number LIKE 'CN%' THEN 'CN'
      WHEN car_number LIKE 'CP%' THEN 'CP'
      WHEN car_number LIKE 'KCS%' THEN 'KCS'
      ELSE 'BNSF'  -- Default
    END AS railroad_owner,
    
    -- Car type (distribute based on hash: 35% hopper, 25% tank, 20% box, 15% gondola, 5% intermodal)
    CASE
      WHEN (ABS(CAST(SUBSTR(car_number, -6) AS INTEGER) * 982451653) % 100) < 35 THEN 'hopper'
      WHEN (ABS(CAST(SUBSTR(car_number, -6) AS INTEGER) * 982451653) % 100) < 60 THEN 'tank'
      WHEN (ABS(CAST(SUBSTR(car_number, -6) AS INTEGER) * 982451653) % 100) < 80 THEN 'box'
      WHEN (ABS(CAST(SUBSTR(car_number, -6) AS INTEGER) * 982451653) % 100) < 95 THEN 'gondola'
      ELSE 'intermodal'
    END AS car_type,
    
    -- Capacity based on car type (varies by type)
    CASE
      -- Hopper: 100-120 tons
      WHEN (ABS(CAST(SUBSTR(car_number, -6) AS INTEGER) * 982451653) % 100) < 35 THEN
        100 + (ABS(CAST(SUBSTR(car_number, -6) AS INTEGER) * 765432191) % 21)
      -- Tank: 80-100 tons
      WHEN (ABS(CAST(SUBSTR(car_number, -6) AS INTEGER) * 982451653) % 100) < 60 THEN
        80 + (ABS(CAST(SUBSTR(car_number, -6) AS INTEGER) * 765432191) % 21)
      -- Box: 70-90 tons
      WHEN (ABS(CAST(SUBSTR(car_number, -6) AS INTEGER) * 982451653) % 100) < 80 THEN
        70 + (ABS(CAST(SUBSTR(car_number, -6) AS INTEGER) * 765432191) % 21)
      -- Gondola: 90-110 tons
      WHEN (ABS(CAST(SUBSTR(car_number, -6) AS INTEGER) * 982451653) % 100) < 95 THEN
        90 + (ABS(CAST(SUBSTR(car_number, -6) AS INTEGER) * 765432191) % 21)
      -- Intermodal: 60-80 tons
      ELSE
        60 + (ABS(CAST(SUBSTR(car_number, -6) AS INTEGER) * 765432191) % 21)
    END AS capacity_tons,
    
    -- Manufacture year (2000-2020)
    2000 + (ABS(CAST(SUBSTR(car_number, -6) AS INTEGER) * 123456789) % 21) AS manufacture_year,
    
    -- Acquisition date (random date between 2000-01-01 and 2015-12-31)
    -- All cars acquired before the analysis period starts
    DATE(
      '2000-01-01',
      '+' || CAST(ABS(CAST(SUBSTR(car_number, -6) AS INTEGER) * 987654321) % 5845 AS TEXT) || ' days'
    ) AS acquisition_date,
    
    -- In service (all TRUE for active fleet)
    1 AS in_service
    
  FROM distinct_cars
)

SELECT
  ROW_NUMBER() OVER (ORDER BY car_number) AS railcar_id,
  car_number,
  railroad_owner,
  car_type,
  capacity_tons,
  manufacture_year,
  acquisition_date,
  in_service
FROM car_enrichment
ORDER BY car_number
