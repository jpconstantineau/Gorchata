{{ config "materialized" "table" }}

SELECT DISTINCT
    sale_date,
    CAST(strftime('%Y', sale_date) AS INTEGER) AS year,
    (CAST(strftime('%m', sale_date) AS INTEGER) + 2) / 3 AS quarter,
    CAST(strftime('%m', sale_date) AS INTEGER) AS month,
    CAST(strftime('%d', sale_date) AS INTEGER) AS day,
    strftime('%w', sale_date) AS day_of_week,
    CASE WHEN strftime('%w', sale_date) IN ('0', '6') THEN 1 ELSE 0 END AS is_weekend
FROM {{ seed "raw_sales" }}
ORDER BY sale_date
