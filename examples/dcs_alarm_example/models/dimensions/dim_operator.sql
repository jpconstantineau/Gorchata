{{ config "materialized" "table" }}

-- Operator Dimension
-- Console operators for alarm response analysis

WITH source_data AS (
  SELECT
    CAST(column1 AS INTEGER) AS operator_key,
    CAST(column2 AS TEXT) AS operator_id,
    CAST(column3 AS TEXT) AS operator_name,
    CAST(column4 AS TEXT) AS shift_id,
    CAST(column5 AS TEXT) AS console_id,
    CAST(column6 AS TEXT) AS experience_level
  FROM (
    VALUES
      (1, 'OP001', 'Smith, John', 'A', 'OPS-1', 'EXPERT'),
      (2, 'OP002', 'Johnson, Mary', 'B', 'OPS-1', 'INTERMEDIATE'),
      (3, 'OP003', 'Williams, Robert', 'C', 'OPS-2', 'EXPERT'),
      (4, 'OP004', 'Brown, Sarah', 'D', 'OPS-2', 'NOVICE')
  )
)
SELECT
  operator_key,
  operator_id,
  operator_name,
  shift_id,
  console_id,
  experience_level
FROM source_data
ORDER BY operator_key
