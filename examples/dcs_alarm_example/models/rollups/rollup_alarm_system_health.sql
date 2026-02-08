{{ config "materialized" "table" }}

-- Alarm System Health Summary
-- Generates overall alarm system health metrics and ISA compliance score
-- Grain: One row for entire analysis period (overall summary)

WITH overall_alarm_metrics AS (
  -- Calculate basic alarm counts
  SELECT
    COUNT(*) AS total_alarm_count,
    COUNT(DISTINCT tag_key) AS unique_tag_count
  FROM {{ ref "fct_alarm_occurrence" }}
),

operator_loading_metrics AS (
  -- Aggregate operator loading statistics
  SELECT
    AVG(alarm_count) * 6.0 AS avg_alarms_per_hour,  -- 10-min buckets * 6 = hourly
    MAX(alarm_count) AS peak_alarms_per_10min,
    -- Calculate time distribution percentages
    COUNT(*) AS total_buckets,
    SUM(CASE WHEN loading_category = 'ACCEPTABLE' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct_time_acceptable,
    SUM(CASE WHEN loading_category = 'MANAGEABLE' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct_time_manageable,
    SUM(CASE WHEN loading_category = 'UNACCEPTABLE' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct_time_unacceptable,
    SUM(CASE WHEN loading_category = 'UNACCEPTABLE' THEN 1 ELSE 0 END) AS alarm_flood_count
  FROM {{ ref "rollup_operator_loading_hourly" }}
),

standing_alarm_metrics AS (
  -- Aggregate standing alarm statistics
  SELECT
    COUNT(*) AS total_standing_alarms,
    AVG(avg_standing_duration_sec) / 60.0 AS avg_standing_duration_min
  FROM {{ ref "rollup_standing_alarms" }}
),

chattering_metrics AS (
  -- Count chattering tags
  SELECT
    COUNT(*) AS chattering_tag_count
  FROM {{ ref "rollup_chattering_alarms" }}
),

bad_actor_metrics AS (
  -- Aggregate bad actor statistics
  SELECT
    SUM(contribution_pct) AS top_10_contribution_pct,
    SUM(CASE WHEN bad_actor_score >= 50 THEN 1 ELSE 0 END) AS bad_actor_count
  FROM {{ ref "rollup_bad_actor_tags" }}
  WHERE is_top_10_pct = 1
)

SELECT
  1 AS health_key,
  NULL AS analysis_date,
  NULL AS date_key,
  
  -- Overall alarm metrics
  CAST(oam.total_alarm_count AS INTEGER) AS total_alarm_count,
  CAST(oam.unique_tag_count AS INTEGER) AS unique_tag_count,
  
  -- Operator loading metrics
  CAST(olm.avg_alarms_per_hour AS REAL) AS avg_alarms_per_hour,
  CAST(olm.peak_alarms_per_10min AS INTEGER) AS peak_alarms_per_10min,
  CAST(olm.pct_time_acceptable AS REAL) AS pct_time_acceptable,
  CAST(olm.pct_time_manageable AS REAL) AS pct_time_manageable,
  CAST(olm.pct_time_unacceptable AS REAL) AS pct_time_unacceptable,
  CAST(olm.alarm_flood_count AS INTEGER) AS alarm_flood_count,
  
  -- Standing alarm metrics
  CAST(sam.total_standing_alarms AS INTEGER) AS total_standing_alarms,
  CAST(sam.avg_standing_duration_min AS REAL) AS avg_standing_duration_min,
  
  -- Chattering metrics
  CAST(cm.chattering_tag_count AS INTEGER) AS chattering_tag_count,
  
  -- Bad actor metrics
  CAST(bam.top_10_contribution_pct AS REAL) AS top_10_contribution_pct,
  CAST(bam.bad_actor_count AS INTEGER) AS bad_actor_count,
  
  -- ISA compliance score (0-100)
  -- Components: loading (40%), standing (30%), chattering (30%)
  CAST(
    ROUND(
      (olm.pct_time_acceptable * 0.4) +                           -- Loading score
      (MAX(0, 100 - sam.total_standing_alarms * 10) * 0.3) +    -- Standing penalty
      (MAX(0, 100 - cm.chattering_tag_count * 20) * 0.3)        -- Chattering penalty
    , 1)
  AS REAL) AS isa_compliance_score

FROM overall_alarm_metrics oam
CROSS JOIN operator_loading_metrics olm
CROSS JOIN standing_alarm_metrics sam
CROSS JOIN chattering_metrics cm
CROSS JOIN bad_actor_metrics bam
