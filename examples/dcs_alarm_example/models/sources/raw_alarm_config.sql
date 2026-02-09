{{ config "materialized" "table" }}

WITH source_data AS (
  SELECT
    CAST(column1 AS TEXT) AS tag_id,
    CAST(column2 AS TEXT) AS tag_name,
    CAST(column3 AS TEXT) AS tag_description,
    CAST(column4 AS TEXT) AS alarm_type,
    CAST(column5 AS TEXT) AS priority_code,
    CAST(column6 AS TEXT) AS equipment_id,
    CAST(column7 AS TEXT) AS area_code,
    CAST(column8 AS INTEGER) AS is_safety_critical,
    CAST(column9 AS INTEGER) AS is_active
  FROM (
    VALUES
      -- C-100 (Crude Distillation) alarm configurations
      ('FIC-101', 'FIC-101', 'Crude Feed Flow Control - High Alarm', 'HIGH', 'HIGH', 'P-101', 'C-100', 0, 1),
      ('TIC-105', 'TIC-105', 'Crude Tower Temperature Control - High Alarm', 'HIGH', 'HIGH', 'T-105', 'C-100', 0, 1),
      ('PSH-110', 'PSH-110', 'Crude Tower Pressure - High-High Alarm', 'HIGH-HIGH', 'CRITICAL', 'T-105', 'C-100', 1, 1),
      ('PSL-111', 'PSL-111', 'Crude Feed Pressure - Low Alarm', 'LOW', 'MEDIUM', 'P-101', 'C-100', 0, 1),
      ('FIC-112', 'FIC-112', 'Reflux Flow Control - High Alarm', 'HIGH', 'HIGH', 'T-105', 'C-100', 0, 1),
      ('LIC-115', 'LIC-115', 'Crude Tower Base Level Control - High Alarm', 'HIGH', 'HIGH', 'T-105', 'C-100', 0, 1),
      ('TAH-120', 'TAH-120', 'Furnace Outlet Temperature - High Alarm', 'HIGH', 'HIGH', 'H-120', 'C-100', 1, 1),
      ('LAH-130', 'LAH-130', 'Overhead Accumulator Level - High Alarm', 'HIGH', 'MEDIUM', 'V-130', 'C-100', 0, 1),
      ('TIC-135', 'TIC-135', 'Sidecut Temperature Control - High Alarm', 'HIGH', 'CRITICAL', 'T-105', 'C-100', 0, 1),
      
      -- D-200 (Vacuum Distillation) alarm configurations
      ('FIC-220', 'FIC-220', 'Vacuum Feed Flow Control - Low Alarm', 'LOW', 'LOW', 'P-220', 'D-200', 0, 1),
      ('FIC-221', 'FIC-221', 'Vacuum Tower Feed - High Alarm', 'HIGH', 'HIGH', 'T-220', 'D-200', 0, 1),
      ('FIC-222', 'FIC-222', 'Wash Oil Flow - High Alarm', 'HIGH', 'HIGH', 'T-220', 'D-200', 0, 1),
      ('TIC-225', 'TIC-225', 'Vacuum Tower Temperature - High-High Alarm', 'HIGH-HIGH', 'CRITICAL', 'T-220', 'D-200', 1, 1),
      ('PSL-230', 'PSL-230', 'Vacuum Tower Pressure - Low-Low Alarm', 'LOW-LOW', 'HIGH', 'T-220', 'D-200', 1, 1),
      ('PSH-231', 'PSH-231', 'Vacuum Tower Pressure - High Alarm', 'HIGH', 'MEDIUM', 'T-220', 'D-200', 0, 1),
      ('PSL-232', 'PSL-232', 'Ejector Suction Pressure - Low Alarm', 'LOW', 'HIGH', 'E-230', 'D-200', 1, 1),
      ('TAH-235', 'TAH-235', 'Flash Zone Temperature - High-High Alarm', 'HIGH-HIGH', 'CRITICAL', 'T-220', 'D-200', 1, 1),
      ('TIC-236', 'TIC-236', 'Sidecut Temperature - High Alarm', 'HIGH', 'HIGH', 'T-220', 'D-200', 0, 1),
      ('TIC-237', 'TIC-237', 'Bottom Temperature - High-High Alarm', 'HIGH-HIGH', 'CRITICAL', 'T-220', 'D-200', 1, 1),
      ('LIC-240', 'LIC-240', 'Vacuum Tower Base Level - High Alarm', 'HIGH', 'HIGH', 'T-220', 'D-200', 0, 1),
      ('LAL-241', 'LAL-241', 'Overhead Accumulator Level - Low Alarm', 'LOW', 'MEDIUM', 'V-240', 'D-200', 0, 1)
  )
)
SELECT * FROM source_data
