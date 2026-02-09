{{ config "materialized" "table" }}

-- Process Area Dimension
-- Two refinery process units for alarm analytics

WITH source_data AS (
  SELECT
    CAST(column1 AS INTEGER) AS area_key,
    CAST(column2 AS TEXT) AS area_code,
    CAST(column3 AS TEXT) AS area_name,
    CAST(column4 AS TEXT) AS plant_id
  FROM (
    VALUES
      (1, 'C-100', 'Crude Distillation Unit', 'REFINERY-1'),
      (2, 'D-200', 'Vacuum Distillation Unit', 'REFINERY-1')
  )
)
SELECT
  area_key,
  area_code,
  area_name,
  plant_id
FROM source_data
ORDER BY area_key
