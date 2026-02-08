{{ config "materialized" "table" }}

-- Equipment Dimension
-- Process equipment hierarchy for alarm root cause analysis

WITH source_data AS (
  SELECT
    CAST(column1 AS INTEGER) AS equipment_key,
    CAST(column2 AS TEXT) AS equipment_id,
    CAST(column3 AS TEXT) AS equipment_name,
    CAST(column4 AS TEXT) AS equipment_type,
    CAST(column5 AS TEXT) AS parent_equipment_id,
    CAST(column6 AS TEXT) AS area_code
  FROM (
    VALUES
      -- C-100 (Crude Distillation) equipment
      (1, 'P-101', 'Feed Pump P-101', 'PUMP', NULL, 'C-100'),
      (2, 'T-105', 'Crude Tower T-105', 'COLUMN', NULL, 'C-100'),
      (3, 'H-120', 'Crude Furnace H-120', 'HEAT_EXCHANGER', NULL, 'C-100'),
      (4, 'V-130', 'Overhead Accumulator V-130', 'VESSEL', NULL, 'C-100'),
      
      -- D-200 (Vacuum Distillation) equipment
      (5, 'P-220', 'Vacuum Feed Pump P-220', 'PUMP', NULL, 'D-200'),
      (6, 'T-220', 'Vacuum Tower T-220', 'COLUMN', NULL, 'D-200'),
      (7, 'E-230', 'Vacuum Ejector E-230', 'EJECTOR', NULL, 'D-200'),
      (8, 'V-240', 'Vacuum Overhead Accumulator V-240', 'VESSEL', NULL, 'D-200')
  )
)
SELECT
  equipment_key,
  equipment_id,
  equipment_name,
  equipment_type,
  parent_equipment_id,
  area_code
FROM source_data
ORDER BY equipment_key
