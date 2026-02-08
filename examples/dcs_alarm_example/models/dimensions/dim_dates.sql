{{ config(materialized='table') }}

-- Date Dimension
-- Standard date dimension for temporal analysis

WITH source_data AS (
  SELECT
    CAST(column1 AS INTEGER) AS date_key,
    CAST(column2 AS TEXT) AS full_date,
    CAST(column3 AS INTEGER) AS year,
    CAST(column4 AS INTEGER) AS quarter,
    CAST(column5 AS INTEGER) AS month,
    CAST(column6 AS INTEGER) AS day,
    CAST(column7 AS INTEGER) AS day_of_week,
    CAST(column8 AS INTEGER) AS is_weekend,
    CAST(column9 AS TEXT) AS shift_date
  FROM (
    VALUES
      (20260206, '2026-02-06', 2026, 1, 2, 6, 5, 0, '2026-02-06'),
      (20260207, '2026-02-07', 2026, 1, 2, 7, 6, 1, '2026-02-07')
  )
)
SELECT
  date_key,
  full_date,
  year,
  quarter,
  month,
  day,
  day_of_week,
  is_weekend,
  shift_date
FROM source_data
ORDER BY date_key
