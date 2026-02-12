{{ config "materialized" "table" }}

-- Equipment Reliability Metrics (MTBF, MTTR)
-- Purpose: Calculate Mean Time Between Failures and Mean Time To Repair
-- Dependencies: stg_equipment_state_history
-- Output: Equipment-level reliability metrics

WITH date_range AS (
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
    FROM stg_equipment_state_history
),

operating_time AS (
    SELECT 
        equipment_id,
        SUM(state_duration_min) AS total_operating_time_min
    FROM stg_equipment_state_history
    WHERE machine_state = 'Running'
    GROUP BY equipment_id
),

downtime_events AS (
    SELECT 
        equipment_id,
        state_duration_min
    FROM stg_equipment_state_history
    WHERE machine_state = 'Unplanned Downtime'
        AND reason_code_id IS NOT NULL
),

failure_metrics AS (
    SELECT 
        equipment_id,
        COUNT(*) AS failure_count,
        SUM(state_duration_min) AS total_downtime_min,
        AVG(state_duration_min) AS avg_downtime_min
    FROM downtime_events
    GROUP BY equipment_id
)

SELECT 
    ot.equipment_id,
    ot.total_operating_time_min,
    COALESCE(fm.failure_count, 0) AS failure_count,
    COALESCE(fm.total_downtime_min, 0.0) AS total_downtime_min,
    COALESCE(fm.avg_downtime_min, 0.0) AS avg_downtime_min,
    (SELECT analysis_period_days FROM date_range) AS analysis_period_days,
    COALESCE(CAST(fm.failure_count AS REAL) / (SELECT analysis_period_days FROM date_range), 0.0) AS failures_per_day,
    COALESCE(CAST(fm.failure_count AS REAL) / (SELECT analysis_period_days FROM date_range) * 7.0, 0.0) AS failures_per_week,
    -- MTBF = Total Operating Time / Number of Failures (in hours)
    CASE 
        WHEN fm.failure_count > 0 THEN 
            CAST(ot.total_operating_time_min AS REAL) / CAST(fm.failure_count AS REAL) / 60.0
        ELSE NULL
    END AS mtbf_hours,
    -- MTTR = Total Downtime / Number of Failures (in hours)
    CASE 
        WHEN fm.failure_count > 0 THEN 
            CAST(fm.total_downtime_min AS REAL) / CAST(fm.failure_count AS REAL) / 60.0
        ELSE NULL
    END AS mttr_hours
FROM operating_time ot
LEFT JOIN failure_metrics fm 
    ON ot.equipment_id = fm.equipment_id
ORDER BY 
    ot.equipment_id
