{{ config "materialized" "table" }}

-- Equipment Downtime Analysis by Reason Code
-- Purpose: Aggregate downtime events by equipment and failure mode
-- Dependencies: stg_equipment_state_history, dim_reason_code
-- Output: Equipment-level downtime metrics with chronic failure identification

WITH downtime_events AS (
    SELECT 
        s.equipment_id,
        s.reason_code_id,
        s.state_duration_min,
        s.date_id
    FROM stg_equipment_state_history s
    WHERE s.machine_state = 'Unplanned Downtime'
        AND s.reason_code_id IS NOT NULL
),

date_range AS (
    SELECT 
        MIN(date_id) AS first_date,
        MAX(date_id) AS last_date,
        -- Convert date_id (YYYYMMDD) to YYYY-MM-DD format for julianday()
        CAST(julianday(
            substr(MAX(date_id), 1, 4) || '-' || 
            substr(MAX(date_id), 5, 2) || '-' || 
            substr(MAX(date_id), 7, 2)
        ) - julianday(
            substr(MIN(date_id), 1, 4) || '-' || 
            substr(MIN(date_id), 5, 2) || '-' || 
            substr(MIN(date_id), 7, 2)
        ) + 1 AS REAL) AS analysis_period_days
    FROM downtime_events
),

downtime_by_reason AS (
    SELECT 
        d.equipment_id,
        d.reason_code_id,
        COUNT(*) AS failure_count,
        SUM(d.state_duration_min) AS total_downtime_min,
        AVG(d.state_duration_min) AS avg_downtime_min,
        MIN(d.state_duration_min) AS min_downtime_min,
        MAX(d.state_duration_min) AS max_downtime_min
    FROM downtime_events d
    GROUP BY 
        d.equipment_id,
        d.reason_code_id
)

SELECT 
    dr.equipment_id,
    dr.reason_code_id,
    rc.reason_code_name,
    rc.reason_category,
    rc.oee_classification,
    rc.six_big_losses_category,
    dr.failure_count,
    dr.total_downtime_min,
    dr.avg_downtime_min,
    dr.min_downtime_min,
    dr.max_downtime_min,
    CAST(dr.failure_count AS REAL) / (SELECT analysis_period_days FROM date_range) AS failures_per_day,
    CAST(dr.failure_count AS REAL) / (SELECT analysis_period_days FROM date_range) * 7.0 AS failures_per_week,
    CASE 
        WHEN CAST(dr.failure_count AS REAL) / (SELECT analysis_period_days FROM date_range) * 7.0 > 3.0 THEN 1
        ELSE 0
    END AS is_chronic_failure
FROM downtime_by_reason dr
INNER JOIN dim_reason_code rc 
    ON dr.reason_code_id = rc.reason_code_id
ORDER BY 
    dr.equipment_id,
    dr.total_downtime_min DESC
