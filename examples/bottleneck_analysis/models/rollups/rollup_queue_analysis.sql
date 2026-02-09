{{ config "materialized" "table" }}

-- Queue analysis by resource
-- Purpose: Identify where work waits longest (bottleneck indicator)
-- Grain: One row per resource (summary level)

WITH queue_stats AS (
    SELECT
        f.resource_key,
        r.resource_id,
        r.resource_name,
        COUNT(*) AS total_operations,
        AVG(f.queue_time_minutes) AS avg_queue_time_minutes,
        MAX(f.queue_time_minutes) AS max_queue_time_minutes,
        MIN(f.queue_time_minutes) AS min_queue_time_minutes,
        SUM(f.queue_time_minutes) AS total_queue_minutes
    FROM {{ ref "fct_operation" }} f
    JOIN {{ ref "dim_resource" }} r ON f.resource_key = r.resource_key
    WHERE f.queue_time_minutes IS NOT NULL
    GROUP BY f.resource_key, r.resource_id, r.resource_name
)

SELECT
    resource_key,
    resource_id,
    resource_name,
    total_operations,
    ROUND(avg_queue_time_minutes, 2) AS avg_queue_time_minutes,
    max_queue_time_minutes,
    min_queue_time_minutes,
    total_queue_minutes,
    ROUND(avg_queue_time_minutes / 60.0, 2) AS avg_queue_time_hours,
    -- Rank by average queue time (1 = longest wait)
    RANK() OVER (ORDER BY avg_queue_time_minutes DESC) AS queue_rank
FROM queue_stats
ORDER BY avg_queue_time_minutes DESC
