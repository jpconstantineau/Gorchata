{{ config "materialized" "table" }}

-- Date Dimension
-- Full calendar for 10 years covering PSR transformation period (2016-01-01 to 2025-12-31)
-- 3653 days total including leap years (2016, 2020, 2024)
-- Maps PSR adoption periods: Pre-PSR, Transition, Mature PSR

WITH RECURSIVE date_range AS (
  -- Generate all dates in the 10-year analysis period using recursive CTE
  SELECT DATE('2016-01-01') AS full_date
  UNION ALL
  SELECT DATE(full_date, '+1 day')
  FROM date_range
  WHERE full_date < DATE('2025-12-31')
),

date_attributes AS (
  SELECT
    full_date,
    -- Date key in YYYYMMDD format as integer
    CAST(REPLACE(full_date, '-', '') AS INTEGER) AS date_key,
    
    -- Year attributes
    CAST(strftime('%Y', full_date) AS INTEGER) AS year,
    CAST((CAST(strftime('%m', full_date) AS INTEGER) + 2) / 3 AS INTEGER) AS quarter,
    CAST(strftime('%m', full_date) AS INTEGER) AS month,
    
    -- Month name
    CASE CAST(strftime('%m', full_date) AS INTEGER)
      WHEN 1 THEN 'January'
      WHEN 2 THEN 'February'
      WHEN 3 THEN 'March'
      WHEN 4 THEN 'April'
      WHEN 5 THEN 'May'
      WHEN 6 THEN 'June'
      WHEN 7 THEN 'July'
      WHEN 8 THEN 'August'
      WHEN 9 THEN 'September'
      WHEN 10 THEN 'October'
      WHEN 11 THEN 'November'
      WHEN 12 THEN 'December'
    END AS month_name,
    
    -- Week number (ISO week)
    CAST(strftime('%W', full_date) AS INTEGER) + 1 AS week_of_year,
    
    -- Day attributes
    CAST(strftime('%d', full_date) AS INTEGER) AS day_of_month,
    CASE CAST(strftime('%w', full_date) AS INTEGER)
      WHEN 0 THEN 7  -- Sunday -> 7 (Monday = 1)
      ELSE CAST(strftime('%w', full_date) AS INTEGER)
    END AS day_of_week,
    
    -- Day name
    CASE CAST(strftime('%w', full_date) AS INTEGER)
      WHEN 0 THEN 'Sunday'
      WHEN 1 THEN 'Monday'
      WHEN 2 THEN 'Tuesday'
      WHEN 3 THEN 'Wednesday'
      WHEN 4 THEN 'Thursday'
      WHEN 5 THEN 'Friday'
      WHEN 6 THEN 'Saturday'
    END AS day_name,
    
    -- Weekend flag
    CASE 
      WHEN CAST(strftime('%w', full_date) AS INTEGER) IN (0, 6) THEN 1
      ELSE 0
    END AS is_weekend,
    
    -- Season (meteorological definition)
    CASE 
      WHEN CAST(strftime('%m', full_date) AS INTEGER) IN (12, 1, 2) THEN 'Winter'
      WHEN CAST(strftime('%m', full_date) AS INTEGER) IN (3, 4, 5) THEN 'Spring'
      WHEN CAST(strftime('%m', full_date) AS INTEGER) IN (6, 7, 8) THEN 'Summer'
      ELSE 'Fall'
    END AS season,
    
    -- PSR Period mapping
    -- Pre-PSR: 2016-01-01 to 2017-12-31 (baseline period)
    -- Transition: 2018-01-01 to 2020-12-31 (PSR implementation)
    -- Mature PSR: 2021-01-01 to 2025-12-31 (mature PSR operations)
    CASE
      WHEN full_date <= '2017-12-31' THEN 'pre_psr'
      WHEN full_date <= '2020-12-31' THEN 'transition'
      ELSE 'mature_psr'
    END AS psr_period
    
  FROM date_range
)

SELECT
  date_key AS date_id,
  full_date AS date,
  year,
  quarter,
  month,
  month_name,
  week_of_year,
  day_of_month,
  day_of_week,
  day_name,
  is_weekend,
  season,
  psr_period
FROM date_attributes
ORDER BY date_key
