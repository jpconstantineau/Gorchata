-- ============================================================================
-- Manufacturing Bottleneck Analysis - Verification Queries
-- ============================================================================
-- 
-- This script contains verification queries to manually inspect and validate
-- the bottleneck analysis results. Run these queries after executing 
-- 'gorchata run' to build all models.
--
-- Usage:
--   sqlite3 bottleneck_analysis.db < verify_bottleneck_analysis.sql
--
-- Or interactively:
--   sqlite3 bottleneck_analysis.db
--   .mode column
--   .headers on
--   .read verify_bottleneck_analysis.sql
-- ============================================================================

.mode column
.headers on
.width 15 20 15 15 15

-- ============================================================================
-- Query 1: Top Resources by Average Utilization
-- ============================================================================
-- Purpose: Identify which resources are running at highest capacity
-- Expected: NCX-10 should show >85% utilization, indicating bottleneck
-- ============================================================================

.print ""
.print "========================================================================"
.print "Query 1: Top Resources by Average Utilization"
.print "========================================================================"
.print ""

SELECT 
    r.resource_name,
    r.resource_type,
    ROUND(AVG(u.utilization_pct), 2) AS avg_utilization_pct,
    ROUND(AVG(u.adjusted_utilization_pct), 2) AS avg_adjusted_util_pct,
    COUNT(DISTINCT u.operation_date) AS days_analyzed,
    ROUND(SUM(u.parts_produced), 0) AS total_parts_produced
FROM {{ ref('int_resource_daily_utilization') }} u
JOIN {{ ref('dim_resource') }} r 
    ON u.resource_key = r.resource_key
GROUP BY 
    r.resource_name,
    r.resource_type
ORDER BY 
    avg_utilization_pct DESC;

-- ============================================================================
-- Query 2: Resources Ranked by Average Queue Time
-- ============================================================================
-- Purpose: Identify resources where work queues up (indicating bottleneck)
-- Expected: NCX-10 should show 2-4 hours average queue time
-- ============================================================================

.print ""
.print "========================================================================"
.print "Query 2: Resources Ranked by Average Queue Time"
.print "========================================================================"
.print ""

SELECT 
    r.resource_name,
    ROUND(AVG(u.avg_queue_time_hrs), 2) AS avg_queue_time_hours,
    ROUND(AVG(u.utilization_pct), 2) AS avg_utilization_pct,
    COUNT(DISTINCT u.operation_date) AS days_with_data
FROM {{ ref('int_resource_daily_utilization') }} u
JOIN {{ ref('dim_resource') }} r 
    ON u.resource_key = r.resource_key
WHERE 
    u.avg_queue_time_hrs IS NOT NULL
GROUP BY 
    r.resource_name
ORDER BY 
    avg_queue_time_hours DESC;

-- ============================================================================
-- Query 3: WIP Accumulation by Resource Over Time
-- ============================================================================
-- Purpose: Show work-in-process buildup indicating bottleneck
-- Expected: WIP should accumulate before NCX-10 and potentially Heat Treat
-- ============================================================================

.print ""
.print "========================================================================"
.print "Query 3: WIP Accumulation by Resource (Last 7 Days)"
.print "========================================================================"
.print ""

SELECT 
    r.resource_name,
    u.operation_date,
    ROUND(u.total_operation_hours, 2) AS operation_hours,
    ROUND(u.available_hours, 2) AS available_hours,
    ROUND(u.utilization_pct, 2) AS utilization_pct,
    u.parts_produced AS parts_completed,
    ROUND(u.avg_queue_time_hrs, 2) AS avg_queue_hrs
FROM {{ ref('int_resource_daily_utilization') }} u
JOIN {{ ref('dim_resource') }} r 
    ON u.resource_key = r.resource_key
WHERE 
    r.resource_name IN ('NCX-10', 'Heat Treat', 'Milling')
    AND u.operation_date >= DATE('now', '-7 days')
ORDER BY 
    u.operation_date DESC,
    r.resource_name;

-- ============================================================================
-- Query 4: Bottleneck Ranking - The Main Analysis
-- ============================================================================
-- Purpose: Show composite bottleneck score ranking ALL resources
-- Expected: NCX-10 ranked #1, Heat Treat ranked #2
-- ============================================================================

.print ""
.print "========================================================================"
.print "Query 4: Bottleneck Ranking (Composite Score)"
.print "========================================================================"
.print ""

SELECT 
    bottleneck_rank,
    resource_name,
    ROUND(avg_utilization_pct, 2) AS avg_util_pct,
    ROUND(avg_queue_time_hrs, 2) AS avg_queue_hrs,
    ROUND(avg_wip_accumulation, 2) AS avg_wip,
    downtime_frequency AS downtime_events,
    ROUND(bottleneck_score, 2) AS composite_score,
    CASE 
        WHEN bottleneck_rank <= 2 THEN '*** BOTTLENECK ***'
        WHEN bottleneck_rank <= 3 THEN 'Watch closely'
        ELSE 'Non-constraint'
    END AS classification
FROM {{ ref('anl_bottleneck_ranking') }}
ORDER BY 
    bottleneck_rank;

-- ============================================================================
-- Query 5: Downtime Frequency by Resource
-- ============================================================================
-- Purpose: Analyze unplanned downtime events impacting throughput
-- Expected: NCX-10 downtime should have cascading impact
-- ============================================================================

.print ""
.print "========================================================================"
.print "Query 5: Downtime Frequency and Duration by Resource"
.print "========================================================================"
.print ""

SELECT 
    r.resource_name,
    COUNT(*) AS downtime_event_count,
    ROUND(SUM(d.duration_hours), 2) AS total_downtime_hours,
    ROUND(AVG(d.duration_hours), 2) AS avg_event_duration,
    SUM(CASE WHEN d.downtime_type = 'unplanned' THEN 1 ELSE 0 END) AS unplanned_count,
    SUM(CASE WHEN d.downtime_type = 'planned' THEN 1 ELSE 0 END) AS planned_count
FROM {{ ref('fact_downtime') }} d
JOIN {{ ref('dim_resource') }} r 
    ON d.resource_key = r.resource_key
GROUP BY 
    r.resource_name
ORDER BY 
    total_downtime_hours DESC;

-- ============================================================================
-- Query 6: Daily Utilization Trend for NCX-10 (The Primary Bottleneck)
-- ============================================================================
-- Purpose: Deep-dive into NCX-10 performance over time
-- Expected: Consistently high utilization (85-95%), minimal idle time
-- ============================================================================

.print ""
.print "========================================================================"
.print "Query 6: NCX-10 Daily Performance Trend"
.print "========================================================================"
.print ""

SELECT 
    u.operation_date,
    ROUND(u.total_operation_hours, 2) AS operation_hrs,
    ROUND(u.downtime_hours, 2) AS downtime_hrs,
    ROUND(u.available_hours, 2) AS available_hrs,
    ROUND(u.utilization_pct, 2) AS utilization_pct,
    ROUND(u.adjusted_utilization_pct, 2) AS adj_util_pct,
    u.parts_produced,
    ROUND(u.avg_queue_time_hrs, 2) AS avg_queue_hrs,
    CASE 
        WHEN u.utilization_pct >= 90 THEN 'SEVERE CONSTRAINT'
        WHEN u.utilization_pct >= 80 THEN 'BOTTLENECK'
        WHEN u.utilization_pct >= 70 THEN 'HIGH UTIL'
        ELSE 'NORMAL'
    END AS status
FROM {{ ref('int_resource_daily_utilization') }} u
JOIN {{ ref('dim_resource') }} r 
    ON u.resource_key = r.resource_key
WHERE 
    r.resource_name = 'NCX-10'
ORDER BY 
    u.operation_date DESC
LIMIT 14;

-- ============================================================================
-- Query 7: Composite Bottleneck Score Breakdown
-- ============================================================================
-- Purpose: Show how each factor contributes to the bottleneck score
-- Expected: NCX-10 scores high on utilization and queue time components
-- ============================================================================

.print ""
.print "========================================================================"
.print "Query 7: Bottleneck Score Component Breakdown"
.print "========================================================================"
.print ""

SELECT 
    resource_name,
    ROUND(avg_utilization_pct, 2) AS util_pct,
    ROUND(avg_utilization_pct * 0.40, 2) AS util_component_40pct,
    ROUND(avg_queue_time_hrs, 2) AS queue_hrs,
    ROUND(avg_queue_time_hrs * 100 * 0.30, 2) AS queue_component_30pct,
    ROUND(avg_wip_accumulation, 2) AS wip,
    ROUND(avg_wip_accumulation * 0.20, 2) AS wip_component_20pct,
    downtime_frequency AS downtime_ct,
    ROUND(downtime_frequency * 0.10, 2) AS downtime_component_10pct,
    ROUND(bottleneck_score, 2) AS total_score,
    bottleneck_rank AS rank
FROM {{ ref('anl_bottleneck_ranking') }}
ORDER BY 
    bottleneck_rank;

-- ============================================================================
-- Summary Statistics
-- ============================================================================

.print ""
.print "========================================================================"
.print "Summary: Data Volume and Coverage"
.print "========================================================================"
.print ""

SELECT 
    'Resources' AS metric,
    COUNT(*) AS count
FROM {{ ref('dim_resource') }}
UNION ALL
SELECT 
    'Work Orders' AS metric,
    COUNT(*) AS count
FROM {{ ref('dim_work_order') }}
UNION ALL
SELECT 
    'Operations' AS metric,
    COUNT(*) AS count
FROM {{ ref('fact_operation') }}
UNION ALL
SELECT 
    'Downtime Events' AS metric,
    COUNT(*) AS count
FROM {{ ref('fact_downtime') }}
UNION ALL
SELECT 
    'Daily Utilization Records' AS metric,
    COUNT(*) AS count
FROM {{ ref('int_resource_daily_utilization') }};

.print ""
.print "========================================================================"
.print "Theory of Constraints (TOC) Interpretation"
.print "========================================================================"
.print ""
.print "Based on the bottleneck ranking:"
.print ""
.print "1. IDENTIFY: The constraint is the resource with rank #1 (typically NCX-10)"
.print "2. EXPLOIT: Maximize uptime and efficiency of the bottleneck resource"
.print "3. SUBORDINATE: Align all other resources to feed the bottleneck"
.print "4. ELEVATE: If exploitation is maxed, add capacity to the bottleneck"
.print "5. REPEAT: Monitor for constraint shifting (e.g., to Heat Treat)"
.print ""
.print "Focus improvement efforts on the top 2 ranked resources to maximize"
.print "system throughput. Non-constraint resources should buffer the bottleneck."
.print ""
.print "========================================================================"

