{{ config "materialized" "table" }}

-- Standing Alarm Duration Rollup
-- Calculates duration metrics for standing alarms (>10 min to acknowledge) by tag
-- Identifies worst offenders for targeted improvement

SELECT
    f.tag_key,
    f.area_key,

    -- Standing alarm counts
    COUNT(*) AS standing_alarm_count,

    -- Duration metrics (in seconds)
    SUM(f.duration_to_ack_sec) AS total_standing_duration_sec,
    AVG(f.duration_to_ack_sec) AS avg_standing_duration_sec,
    MAX(f.duration_to_ack_sec) AS max_standing_duration_sec,

    -- Derived display columns (converted to minutes/hours)
    AVG(f.duration_to_ack_sec) / 60.0 AS avg_standing_duration_min,
    MAX(f.duration_to_ack_sec) / 3600.0 AS max_standing_duration_hrs,
    SUM(f.duration_to_ack_sec) / 3600.0 AS total_standing_duration_hrs

FROM {{ ref "fct_alarm_occurrence" }} f

-- Only include standing alarms (>10 minutes to acknowledge)
WHERE f.is_standing_10min = 1

GROUP BY
    f.tag_key,
    f.area_key

-- Order by total duration to highlight worst offenders
ORDER BY total_standing_duration_sec DESC
