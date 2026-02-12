{{ config "materialized" "table" }}

-- Failure Mode Pareto Analysis
-- Purpose: Rank failures by cumulative impact (frequency × duration) for 80/20 analysis
-- Dependencies: stg_equipment_state_history, dim_reason_code
-- Output: Pareto-ranked failure modes with cumulative percentages

WITH downtime_events AS (
    SELECT 
        s.equipment_id,
        s.reason_code_id,
        s.state_duration_min
    FROM stg_equipment_state_history s
    WHERE s.machine_state = 'Unplanned Downtime'
        AND s.reason_code_id IS NOT NULL
),

failure_impact AS (
    SELECT 
        d.equipment_id,
        d.reason_code_id,
        COUNT(*) AS failure_count,
        SUM(d.state_duration_min) AS total_downtime_min,
        -- Impact = total downtime (simple metric for Pareto)
        SUM(d.state_duration_min) AS downtime_impact
    FROM downtime_events d
    GROUP BY 
        d.equipment_id,
        d.reason_code_id
),

equipment_totals AS (
    SELECT 
        equipment_id,
        SUM(total_downtime_min) AS equipment_total_downtime
    FROM failure_impact
    GROUP BY equipment_id
),

ranked_failures AS (
    SELECT 
        fi.equipment_id,
        fi.reason_code_id,
        rc.reason_code_name,
        rc.reason_category,
        rc.six_big_losses_category,
        fi.failure_count,
        fi.total_downtime_min,
        fi.downtime_impact,
        et.equipment_total_downtime,
        ROW_NUMBER() OVER (
            PARTITION BY fi.equipment_id 
            ORDER BY fi.downtime_impact DESC
        ) AS pareto_rank,
        CAST(fi.downtime_impact AS REAL) / CAST(et.equipment_total_downtime AS REAL) * 100.0 AS impact_pct
    FROM failure_impact fi
    INNER JOIN equipment_totals et 
        ON fi.equipment_id = et.equipment_id
    INNER JOIN dim_reason_code rc 
        ON fi.reason_code_id = rc.reason_code_id
),

cumulative_impact AS (
    SELECT 
        rf.*,
        SUM(rf.impact_pct) OVER (
            PARTITION BY rf.equipment_id 
            ORDER BY rf.pareto_rank 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_pct
    FROM ranked_failures rf
)

SELECT 
    ci.equipment_id,
    ci.reason_code_id,
    ci.reason_code_name,
    ci.reason_category,
    ci.six_big_losses_category,
    ci.failure_count,
    ci.total_downtime_min,
    ci.downtime_impact,
    ci.impact_pct,
    ci.cumulative_pct,
    ci.pareto_rank,
    CASE 
        WHEN ci.cumulative_pct <= 80.0 THEN 1
        ELSE 0
    END AS is_pareto_vital_few
FROM cumulative_impact ci
ORDER BY 
    ci.equipment_id,
    ci.pareto_rank
