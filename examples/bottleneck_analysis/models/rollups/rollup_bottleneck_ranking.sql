{{ config "materialized" "table" }}

-- Bottleneck identification combining multiple indicators
-- Purpose: Combine utilization, queue time, WIP, and downtime into composite score and ranking
-- Grain: One row per resource (summary level)
-- Key Requirement: NCX-10 (R001) and Heat Treat (R002) MUST rank as top 2 bottlenecks

WITH utilization_avg AS (
    SELECT
        resource_key,
        resource_id,
        resource_name,
        AVG(utilization_pct) AS avg_utilization_pct
    FROM {{ ref "int_resource_daily_utilization" }}
    WHERE utilization_pct IS NOT NULL
    GROUP BY resource_key, resource_id, resource_name
),

queue_metrics AS (
    SELECT
        resource_key,
        resource_id,
        avg_queue_time_minutes
    FROM {{ ref "rollup_queue_analysis" }}
),

wip_metrics AS (
    SELECT
        resource_key,
        resource_id,
        AVG(wip_count) AS avg_wip_count,
        MAX(wip_count) AS max_wip_count
    FROM {{ ref "rollup_wip_by_resource" }}
    GROUP BY resource_key, resource_id
),

downtime_metrics AS (
    SELECT
        resource_key,
        COUNT(*) AS downtime_event_count,
        SUM(downtime_minutes) AS total_downtime_minutes,
        AVG(downtime_minutes) AS avg_downtime_minutes
    FROM {{ ref "int_downtime_summary" }}
    GROUP BY resource_key
),

combined_metrics AS (
    SELECT
        u.resource_key,
        u.resource_id,
        u.resource_name,
        COALESCE(u.avg_utilization_pct, 0) AS avg_utilization_pct,
        COALESCE(q.avg_queue_time_minutes, 0) AS avg_queue_time_minutes,
        COALESCE(w.avg_wip_count, 0) AS avg_wip_count,
        COALESCE(w.max_wip_count, 0) AS max_wip_count,
        COALESCE(d.downtime_event_count, 0) AS downtime_event_count,
        COALESCE(d.total_downtime_minutes, 0) AS total_downtime_minutes
    FROM utilization_avg u
    LEFT JOIN queue_metrics q ON u.resource_key = q.resource_key
    LEFT JOIN wip_metrics w ON u.resource_key = w.resource_key
    LEFT JOIN downtime_metrics d ON u.resource_key = d.resource_key
),

metric_ranges AS (
    SELECT
        MAX(avg_utilization_pct) AS max_util,
        MIN(avg_utilization_pct) AS min_util,
        MAX(avg_queue_time_minutes) AS max_queue,
        MIN(avg_queue_time_minutes) AS min_queue,
        MAX(avg_wip_count) AS max_wip,
        MIN(avg_wip_count) AS min_wip,
        MAX(downtime_event_count) AS max_downtime,
        MIN(downtime_event_count) AS min_downtime
    FROM combined_metrics
),

normalized_metrics AS (
    SELECT
        c.*,
        -- Normalize each metric to 0-100 scale
        CASE 
            WHEN (r.max_util - r.min_util) > 0 
            THEN ((c.avg_utilization_pct - r.min_util) / (r.max_util - r.min_util)) * 100 
            ELSE 0 
        END AS norm_utilization,
        CASE 
            WHEN (r.max_queue - r.min_queue) > 0 
            THEN ((c.avg_queue_time_minutes - r.min_queue) / (r.max_queue - r.min_queue)) * 100 
            ELSE 0 
        END AS norm_queue,
        CASE 
            WHEN (r.max_wip - r.min_wip) > 0 
            THEN ((c.avg_wip_count - r.min_wip) / (r.max_wip - r.min_wip)) * 100 
            ELSE 0 
        END AS norm_wip,
        CASE 
            WHEN (r.max_downtime - r.min_downtime) > 0 
            THEN ((c.downtime_event_count - r.min_downtime) / (r.max_downtime - r.min_downtime)) * 100 
            ELSE 0 
        END AS norm_downtime
    FROM combined_metrics c
    CROSS JOIN metric_ranges r
)

SELECT
    resource_key,
    resource_id,
    resource_name,
    avg_utilization_pct,
    avg_queue_time_minutes,
    avg_wip_count,
    max_wip_count,
    downtime_event_count,
    total_downtime_minutes,
    -- Composite bottleneck score using normalized metrics (0-100 scale each)
    ROUND(
        (norm_utilization * 0.4) +      -- 40% weight on utilization
        (norm_queue * 0.3) +            -- 30% weight on queue time
        (norm_wip * 0.2) +              -- 20% weight on WIP
        (norm_downtime * 0.1),          -- 10% weight on downtime frequency
    2) AS bottleneck_score,
    -- Flag potential bottlenecks
    CASE 
        WHEN avg_utilization_pct > 85 OR avg_queue_time_minutes > 100 THEN 1 
        ELSE 0 
    END AS is_potential_bottleneck,
    -- Rank resources (1 = primary bottleneck)
    RANK() OVER (ORDER BY 
        (norm_utilization * 0.4) + 
        (norm_queue * 0.3) + 
        (norm_wip * 0.2) + 
        (norm_downtime * 0.1)
    DESC) AS bottleneck_rank
FROM normalized_metrics
ORDER BY bottleneck_score DESC
