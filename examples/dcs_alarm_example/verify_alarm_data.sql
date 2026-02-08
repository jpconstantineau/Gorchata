-- DCS Alarm Analytics - Sample Verification Queries
-- These queries demonstrate ISA 18.2 analytics capabilities and verify data quality

-- ============================================================================
-- TOP 10 BAD ACTOR TAGS BY ALARM FREQUENCY
-- ============================================================================
-- Identifies the worst offender tags contributing most to alarm load
-- Uses Pareto analysis to rank tags by alarm count and calculate contribution %

SELECT 
    d.tag_id,
    d.tag_description,
    b.total_activations AS alarm_count,
    ROUND(b.avg_activations_per_day, 1) AS avg_per_day,
    ROUND(b.contribution_pct, 1) AS contribution_pct,
    ROUND(b.cumulative_pct, 1) AS cumulative_pct,
    b.bad_actor_category,
    b.alarm_rank
FROM rollup_bad_actor_tags b
INNER JOIN dim_alarm_tag d ON b.tag_key = d.tag_key AND d.is_current = 1
ORDER BY b.alarm_rank
LIMIT 10;

-- Expected: TIC-105 should rank #1 as the worst offender


-- ============================================================================
-- ALARM STORM EVENT ANALYSIS (Feb 7, 08:00-08:08)
-- ============================================================================
-- Analyzes the D-200 area alarm storm with 11 alarms in 10 minutes
-- Demonstrates ISA 18.2 operator loading breach (>10 = UNACCEPTABLE)

SELECT 
    l.date_key,
    l.time_bucket_key,
    l.time_bucket_start,
    l.alarm_count,
    l.alarm_count_critical,
    l.loading_category,
    l.is_alarm_flood
FROM rollup_operator_loading_hourly l
WHERE l.date_key = 20260207 
  AND l.time_bucket_key BETWEEN 48 AND 49  -- 08:00-08:10, 08:10-08:20
ORDER BY l.time_bucket_key;

-- Expected: Bucket 48 (08:00-08:10) should show 11 alarms with UNACCEPTABLE loading


-- ============================================================================
-- CHATTERING ALARM EPISODES
-- ============================================================================
-- Identifies tags exhibiting rapid state cycling (chattering behavior)
-- ISA 18.2 defines chattering as ≥5 state transitions within 10 minutes

SELECT 
    d.tag_id,
    d.tag_description,
    c.chattering_episode_count AS episodes,
    c.total_state_changes AS total_transitions,
    ROUND(c.max_activations_per_hour, 1) AS peak_per_hour,
    c.min_cycle_time_sec,
    c.avg_cycle_time_sec
FROM rollup_chattering_alarms c
INNER JOIN dim_alarm_tag d ON c.tag_key = d.tag_key AND d.is_current = 1
WHERE c.chattering_episode_count > 0
ORDER BY c.chattering_episode_count DESC, c.total_state_changes DESC
LIMIT 10;

-- Expected: TIC-105 appears with 1 chattering episode and 6 state changes


-- ============================================================================
-- STANDING ALARM DURATION BY TAG
-- ============================================================================
-- Identifies tags with longest unacknowledged durations (>10 minutes)
-- Standing alarms indicate potential nuisance alarms or inadequate priority

SELECT 
    d.tag_id,
    d.tag_description,
    d.priority_code,
    s.standing_alarm_count AS count_10min,
    ROUND(s.avg_standing_duration_min, 1) AS avg_duration_min,
    ROUND(s.max_standing_duration_hrs, 2) AS max_duration_hrs,
    ROUND(s.total_standing_duration_hrs, 2) AS total_standing_hrs
FROM rollup_standing_alarms s
INNER JOIN dim_alarm_tag d ON s.tag_key = d.tag_key AND d.is_current = 1
WHERE s.standing_alarm_count > 0
ORDER BY s.total_standing_duration_hrs DESC
LIMIT 15;

-- Expected: Multiple tags with >10 minute acknowledgment times


-- ============================================================================
-- HOURLY ALARM LOADING TREND (TIME SERIES)
-- ============================================================================
-- Shows alarm rate over time to identify peak periods and patterns
-- Useful for identifying shift-based patterns or equipment failure modes

SELECT 
    dd.full_date,
    l.time_bucket_start,
    l.alarm_count,
    l.loading_category,
    l.avg_time_to_ack_sec,
    l.standing_alarm_count
FROM rollup_operator_loading_hourly l
INNER JOIN dim_dates dd ON l.date_key = dd.date_key
ORDER BY dd.full_date, l.time_bucket_key
LIMIT 50;

-- Expected: Visualization-ready data for hourly alarm rate charting


-- ============================================================================
-- OPERATOR PERFORMANCE COMPARISON
-- ============================================================================
-- Compares acknowledgment times across operators
-- Helps identify training needs or workload distribution issues

SELECT 
    o.operator_id,
    o.operator_name,
    COUNT(*) AS alarms_handled,
    ROUND(AVG(f.duration_to_ack_sec), 1) AS avg_ack_time_sec,
    ROUND(MAX(f.duration_to_ack_sec), 1) AS max_ack_time_sec,
    SUM(CASE WHEN f.is_standing_10min = 1 THEN 1 ELSE 0 END) AS standing_count,
    ROUND(SUM(CASE WHEN f.is_standing_10min = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS standing_pct
FROM fct_alarm_occurrence f
INNER JOIN dim_operator o ON f.operator_key_ack = o.operator_key
WHERE f.is_acknowledged = 1
GROUP BY o.operator_id, o.operator_name
ORDER BY alarms_handled DESC;

-- Expected: Operator response time metrics for performance review


-- ============================================================================
-- SYSTEM HEALTH SUMMARY
-- ============================================================================
-- Overall DCS alarm system health scorecard
-- Provides executive summary with key ISA 18.2 compliance metrics

SELECT 
    h.health_key,
    h.analysis_date,
    h.total_alarm_count,
    h.unique_tag_count,
    ROUND(h.avg_alarms_per_hour, 1) AS avg_per_hour,
    h.peak_alarms_per_10min,
    ROUND(h.pct_time_acceptable, 1) AS pct_acceptable,
    ROUND(h.pct_time_manageable, 1) AS pct_manageable,
    ROUND(h.pct_time_unacceptable, 1) AS pct_unacceptable,
    h.alarm_flood_count,
    h.total_standing_alarms,
    ROUND(h.avg_standing_duration_min, 1) AS avg_standing_min,
    h.chattering_tag_count,
    ROUND(h.top10_contribution_pct, 1) AS top10_pct,
    h.bad_actor_count,
    ROUND(h.isa_compliance_score, 1) AS compliance_score
FROM rollup_alarm_system_health h
WHERE h.analysis_date IS NULL  -- Overall summary
ORDER BY h.health_key;

-- Expected: Single row with overall system health metrics


-- ============================================================================
-- ALARM PRIORITY DISTRIBUTION
-- ============================================================================
-- Analyzes alarm distribution across priority levels
-- ISA 18.2 recommends: <5% CRITICAL, ~15% HIGH, ~80% MEDIUM/LOW

SELECT 
    p.priority_code,
    p.priority_description,
    COUNT(*) AS alarm_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS distribution_pct,
    ROUND(AVG(f.duration_to_ack_sec) / 60.0, 1) AS avg_ack_time_min
FROM fct_alarm_occurrence f
INNER JOIN dim_priority p ON f.priority_key = p.priority_key
WHERE f.is_acknowledged = 1
GROUP BY p.priority_code, p.priority_description, p.priority_rank
ORDER BY p.priority_rank;

-- Expected: Priority distribution aligned with ISA 18.2 guidelines


-- ============================================================================
-- EQUIPMENT-LEVEL ALARM SUMMARY
-- ============================================================================
-- Rolls up alarm counts by equipment to identify problematic assets
-- Useful for maintenance planning and equipment reliability analysis

SELECT 
    e.equipment_id,
    e.equipment_type,
    e.equipment_description,
    COUNT(DISTINCT dt.tag_id) AS unique_tags,
    COUNT(*) AS total_alarms,
    SUM(CASE WHEN f.is_standing_10min = 1 THEN 1 ELSE 0 END) AS standing_alarms,
    ROUND(AVG(f.duration_to_ack_sec) / 60.0, 1) AS avg_ack_min
FROM fct_alarm_occurrence f
INNER JOIN dim_alarm_tag dt ON f.tag_key = dt.tag_key AND dt.is_current = 1
INNER JOIN dim_equipment e ON dt.equipment_id = e.equipment_id
GROUP BY e.equipment_id, e.equipment_type, e.equipment_description
ORDER BY total_alarms DESC
LIMIT 15;

-- Expected: Equipment ranked by alarm frequency for targeted maintenance


-- ============================================================================
-- DATA INTEGRITY VERIFICATION
-- ============================================================================
-- Validates referential integrity and data quality rules

-- Check for orphan foreign keys (should return 0)
SELECT 'Orphan Tag Keys' AS check_name, COUNT(*) AS violation_count
FROM fct_alarm_occurrence f
WHERE NOT EXISTS (SELECT 1 FROM dim_alarm_tag d WHERE d.tag_key = f.tag_key)

UNION ALL

SELECT 'Orphan Area Keys', COUNT(*)
FROM fct_alarm_occurrence f
WHERE NOT EXISTS (SELECT 1 FROM dim_process_area d WHERE d.area_key = f.area_key)

UNION ALL

SELECT 'Orphan Priority Keys', COUNT(*)
FROM fct_alarm_occurrence f
WHERE NOT EXISTS (SELECT 1 FROM dim_priority d WHERE d.priority_key = f.priority_key)

UNION ALL

-- Check timestamp ordering (should return 0)
SELECT 'Timestamp Violations', COUNT(*)
FROM fct_alarm_occurrence
WHERE (acknowledged_timestamp IS NOT NULL AND acknowledged_timestamp < activation_timestamp)
   OR (inactive_timestamp IS NOT NULL AND acknowledged_timestamp IS NOT NULL 
       AND inactive_timestamp < acknowledged_timestamp);

-- Expected: All counts should be 0 (no violations)
