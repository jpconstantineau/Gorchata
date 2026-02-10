{{ config "materialized" "table" }}

-- Date Dimension
-- Standard date dimension covering the analysis period (2024-01-01 to 2024-03-31)
-- 91 days total including leap year February

WITH RECURSIVE date_range AS (
  -- Generate all dates in the analysis period using recursive CTE
  SELECT DATE('2024-01-01') AS full_date
  UNION ALL
  SELECT DATE(full_date, '+1 day')
  FROM date_range
  WHERE full_date < DATE('2024-03-31')
),

date_attributes AS (
  SELECT
    full_date,
    -- Date key in YYYYMMDD format
    CAST(REPLACE(full_date, '-', '') AS INTEGER) AS date_key,
    -- Year attributes
    CAST(strftime('%Y', full_date) AS INTEGER) AS year,
    CAST((CAST(strftime('%m', full_date) AS INTEGER) + 2) / 3 AS INTEGER) AS quarter,
    CAST(strftime('%m', full_date) AS INTEGER) AS month,
    -- Week number (important for seasonal analysis)
    CAST(strftime('%W', full_date) AS INTEGER) + 1 AS week,
    -- Day attributes (1=Monday in SQLite with %w+1 adjustment)
    CAST(CASE CAST(strftime('%w', full_date) AS INTEGER)
      WHEN 0 THEN 7  -- Sunday -> 7
      ELSE CAST(strftime('%w', full_date) AS INTEGER)
    END AS INTEGER) AS day_of_week,
    CASE 
      WHEN CAST(strftime('%w', full_date) AS INTEGER) IN (0, 6) THEN 1
      ELSE 0
    END AS is_weekend
  FROM date_range
)

SELECT
  date_key,
  full_date,
  year,
  quarter,
  month,
  week,
  day_of_week,
  is_weekend
FROM date_attributes
ORDER BY date_key
