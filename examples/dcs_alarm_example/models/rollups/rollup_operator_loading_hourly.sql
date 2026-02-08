{{ config(materialized='table') }}

-- Operator Loading Rollup (10-minute buckets per ISA 18.2 standards)
-- Calculates alarm rate and categorizes loading as ACCEPTABLE, MANAGEABLE, or UNACCEPTABLE
-- Flags alarm floods (>10 alarms in 10 minutes)

SELECT
    -- Time dimensions
    dd.date_key,
    -- Calculate 10-minute time bucket (0-143): (hour * 6) + (minute / 10)
    (CAST(strftime('%H', f.activation_timestamp) AS INTEGER) * 6 + 
     CAST(strftime('%M', f.activation_timestamp) AS INTEGER) / 10) AS time_bucket_key,
    f.area_key,

    -- Alarm counts
    COUNT(*) AS alarm_count,
    SUM(CASE WHEN dp.priority_code = 'CRITICAL' THEN 1 ELSE 0 END) AS alarm_count_critical,
    SUM(CASE WHEN dp.priority_code = 'HIGH' THEN 1 ELSE 0 END) AS alarm_count_high,
    SUM(CASE WHEN dp.priority_code = 'MEDIUM' THEN 1 ELSE 0 END) AS alarm_count_medium,
    SUM(CASE WHEN dp.priority_code = 'LOW' THEN 1 ELSE 0 END) AS alarm_count_low,

    -- Response metrics
    AVG(CASE WHEN f.is_acknowledged = 1 THEN f.duration_to_ack_sec END) AS avg_time_to_ack_sec,
    MAX(CASE WHEN f.is_acknowledged = 1 THEN f.duration_to_ack_sec END) AS max_time_to_ack_sec,

    -- Standing alarms active in this bucket
    SUM(CASE WHEN f.is_standing_10min = 1 THEN 1 ELSE 0 END) AS standing_alarm_count,

    -- ISA 18.2 categorization
    CASE
        WHEN COUNT(*) BETWEEN 1 AND 2 THEN 'ACCEPTABLE'
        WHEN COUNT(*) BETWEEN 3 AND 10 THEN 'MANAGEABLE'
        WHEN COUNT(*) > 10 THEN 'UNACCEPTABLE'
        ELSE 'ACCEPTABLE'
    END AS loading_category,

    -- Alarm flood detection (>10 alarms in 10 minutes)
    CASE WHEN COUNT(*) > 10 THEN 1 ELSE 0 END AS is_alarm_flood

FROM {{ ref "fct_alarm_occurrence" }} f
INNER JOIN {{ ref "dim_dates" }} dd 
    ON f.activation_date_key = dd.date_key
INNER JOIN {{ ref "dim_priority" }} dp 
    ON f.priority_key = dp.priority_key

GROUP BY
    dd.date_key,
    time_bucket_key,
    f.area_key

ORDER BY
    dd.date_key,
    time_bucket_key
