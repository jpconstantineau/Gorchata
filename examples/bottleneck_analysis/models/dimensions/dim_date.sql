{{ config "materialized" "table" }}

-- Date Dimension
-- Standard date dimension for temporal analysis covering analysis period

WITH RECURSIVE date_series AS (
    -- Start date: 2024-01-01
    SELECT DATE('2024-01-01') AS full_date
    
    UNION ALL
    
    -- Generate dates up to 2024-01-31 (covering 2-week analysis period + buffer)
    SELECT DATE(full_date, '+1 day')
    FROM date_series
    WHERE full_date < DATE('2024-01-31')
),

transformed AS (
    SELECT
        -- Surrogate key in YYYYMMDD format
        CAST(STRFTIME('%Y%m%d', full_date) AS INTEGER) AS date_key,
        full_date,
        CAST(STRFTIME('%Y', full_date) AS INTEGER) AS year,
        CAST(STRFTIME('%m', full_date) AS INTEGER) AS month,
        CASE CAST(STRFTIME('%m', full_date) AS INTEGER)
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
        CAST((STRFTIME('%m', full_date) + 2) / 3 AS INTEGER) AS quarter,
        CAST(STRFTIME('%d', full_date) AS INTEGER) AS day_of_month,
        CAST(STRFTIME('%w', full_date) AS INTEGER) AS day_of_week,
        CASE CAST(STRFTIME('%w', full_date) AS INTEGER)
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS day_name,
        CAST(STRFTIME('%W', full_date) AS INTEGER) AS week_of_year,
        CASE 
            WHEN CAST(STRFTIME('%w', full_date) AS INTEGER) IN (0, 6) THEN 1
            ELSE 0
        END AS is_weekend,
        -- Simple holiday marker (could be enhanced with real holiday calendar)
        0 AS is_holiday
    FROM date_series
)

SELECT * FROM transformed
ORDER BY date_key
