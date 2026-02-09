{{ config "materialized" "table" }}

WITH operation_summary AS (
    SELECT
        f.resource_key,
        f.start_date_key,
        SUM(f.cycle_time_minutes) AS total_processing_minutes,
        COUNT(*) AS operation_count
    FROM {{ ref "fct_operation" }} f
    WHERE f.operation_type IN ('PROCESSING', 'SETUP')
    GROUP BY f.resource_key, f.start_date_key
),

downtime_daily AS (
    SELECT
        resource_key,
        date_key,
        SUM(downtime_minutes) AS total_downtime_minutes
    FROM {{ ref "int_downtime_summary" }}
    GROUP BY resource_key, date_key
),

resource_capacity AS (
    SELECT
        resource_key,
        resource_id,
        resource_name,
        (available_hours_per_shift * shifts_per_day * 60) AS available_minutes_per_day
    FROM {{ ref "dim_resource" }}
)

SELECT
    r.resource_key,
    r.resource_id,
    r.resource_name,
    d.date_key,
    COALESCE(o.total_processing_minutes, 0) AS total_processing_minutes,
    r.available_minutes_per_day,
    COALESCE(dt.total_downtime_minutes, 0) AS total_downtime_minutes,
    (r.available_minutes_per_day - COALESCE(dt.total_downtime_minutes, 0)) AS effective_available_minutes,
    ROUND((COALESCE(o.total_processing_minutes, 0) * 100.0) / 
          NULLIF((r.available_minutes_per_day - COALESCE(dt.total_downtime_minutes, 0)), 0), 2) AS utilization_pct,
    ROUND((COALESCE(o.total_processing_minutes, 0) * 100.0) / 
          r.available_minutes_per_day, 2) AS adjusted_utilization_pct,
    COALESCE(o.operation_count, 0) AS operation_count
FROM resource_capacity r
CROSS JOIN {{ ref "dim_date" }} d
LEFT JOIN operation_summary o ON r.resource_key = o.resource_key AND d.date_key = o.start_date_key
LEFT JOIN downtime_daily dt ON r.resource_key = dt.resource_key AND d.date_key = dt.date_key
WHERE d.full_date BETWEEN '{{ var "analysis_start_date" }}' AND '{{ var "analysis_end_date" }}'
ORDER BY r.resource_id, d.date_key
