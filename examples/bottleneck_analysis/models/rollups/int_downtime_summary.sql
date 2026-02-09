{{ config "materialized" "table" }}

WITH downtime_source AS (
    SELECT 
        downtime_id,
        resource_id,
        downtime_start,
        downtime_end,
        downtime_type,
        reason_code
    FROM {{ seed "raw_downtime" }}
),

downtime_with_duration AS (
    SELECT
        downtime_id,
        resource_id,
        downtime_start,
        downtime_end,
        downtime_type,
        reason_code,
        CAST((JULIANDAY(downtime_end) - JULIANDAY(downtime_start)) * 24 * 60 AS INTEGER) AS downtime_minutes,
        DATE(downtime_start) AS downtime_date
    FROM downtime_source
)

SELECT
    d.downtime_id,
    r.resource_key,
    r.resource_id,
    r.resource_name,
    dt.date_key,
    d.downtime_start,
    d.downtime_end,
    d.downtime_type,
    d.reason_code,
    d.downtime_minutes,
    CASE 
        WHEN d.downtime_type = 'SCHEDULED' THEN 1 
        ELSE 0 
    END AS is_scheduled,
    CASE 
        WHEN d.downtime_type = 'UNSCHEDULED' THEN 1 
        ELSE 0 
    END AS is_unscheduled
FROM downtime_with_duration d
LEFT JOIN {{ ref "dim_resource" }} r ON d.resource_id = r.resource_id
LEFT JOIN {{ ref "dim_date" }} dt ON d.downtime_date = dt.full_date
ORDER BY d.downtime_start
