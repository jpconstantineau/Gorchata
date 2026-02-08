{{ config "materialized" "table" }}

-- Priority Dimension
-- Alarm priority levels with response targets

WITH source_data AS (
  SELECT
    CAST(column1 AS INTEGER) AS priority_key,
    CAST(column2 AS TEXT) AS priority_code,
    CAST(column3 AS INTEGER) AS priority_order,
    CAST(column4 AS INTEGER) AS response_time_target,
    CAST(column5 AS TEXT) AS color_code
  FROM (
    VALUES
      (1, 'CRITICAL', 1, 60, '#FF0000'),
      (2, 'HIGH', 2, 180, '#FF8800'),
      (3, 'MEDIUM', 3, 600, '#FFFF00'),
      (4, 'LOW', 4, 1800, '#00FF00')
  )
)
SELECT
  priority_key,
  priority_code,
  priority_order,
  response_time_target,
  color_code
FROM source_data
ORDER BY priority_key
