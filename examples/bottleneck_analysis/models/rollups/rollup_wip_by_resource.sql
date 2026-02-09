{{ config "materialized" "table" }}

-- WIP tracking by resource per day
-- Counts distinct work orders processed at each resource, averaged by day as a WIP proxy
WITH operations_by_day AS (
    SELECT
        f.resource_key,
        r.resource_id,
        r.resource_name,
        DATE(f.start_timestamp) AS operation_date,
        COUNT(DISTINCT f.work_order_id) AS work_orders_processed,
        SUM(f.queue_time_minutes) AS total_queue_minutes,
        AVG(f.queue_time_minutes) AS avg_queue_minutes,
        COUNT(*) AS operation_count
    FROM {{ ref "fct_operation" }} f
    JOIN {{ ref "dim_resource" }} r ON f.resource_key = r.resource_key
    GROUP BY f.resource_key, r.resource_id, r.resource_name, operation_date
)

SELECT
    resource_key,
    resource_id,
    resource_name,
    operation_date,
    work_orders_processed AS wip_count,
    operation_count,
    total_queue_minutes,
    ROUND(avg_queue_minutes, 2) AS avg_queue_minutes,
    -- Create WIP score (higher = more accumulation)
    -- Based on work orders processed per day
    CASE 
        WHEN work_orders_processed >= 10 THEN 'High'
        WHEN work_orders_processed >= 5 THEN 'Medium'
        ELSE 'Low'
    END AS wip_level
FROM operations_by_day
ORDER BY operation_date, resource_id
