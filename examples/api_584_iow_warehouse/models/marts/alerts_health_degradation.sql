-- Alert Model: Health Degradation
-- Purpose: Alert notifications for rapid health index deterioration (>20 point drop in 30 days)
-- Grain: One row per asset experiencing significant health degradation
-- Alert Logic: Compare current health_index vs 30 days ago, trigger if change < -20

WITH current_health AS (
    SELECT 
        asset_key,
        tag_id,
        equipment_name AS asset_name,
        unit_name,
        health_index AS health_index_current,
        health_status,
        trend_direction,
        days_since_last_excursion
    FROM {{ ref "metrics_asset_integrity_index" }}
),

-- Get damage events from last 30 days to determine primary reason for degradation
recent_excursions AS (
    SELECT 
        fee.asset_key,
        dpt.parameter_type,
        COUNT(*) AS excursion_count,
        ROW_NUMBER() OVER (PARTITION BY fee.asset_key ORDER BY COUNT(*) DESC) AS rank_by_count
    FROM {{ ref "fact_excursion_events" }} AS fee
    INNER JOIN dim_date AS dd
        ON fee.date_key = dd.date_key
    INNER JOIN dim_parameter_type AS dpt
        ON fee.parameter_type_key = dpt.parameter_type_key
    CROSS JOIN (SELECT MAX(full_date) AS as_of_date FROM dim_date) AS current_date
    WHERE julianday(current_date.as_of_date) - julianday(dd.full_date) <= 30
    GROUP BY fee.asset_key, dpt.parameter_type
),

-- Get most frequent excursion type in last 30 days as primary reason
primary_degradation_reason AS (
    SELECT 
        asset_key,
        parameter_type AS primary_reason
    FROM recent_excursions
    WHERE rank_by_count = 1
),

-- Simulate 30-day historical health by calculating what health would have been
-- This is a simplified approach: subtract damage from last 30 days
-- In production, this would use historical snapshots of metrics_asset_integrity_index
historical_health_estimate AS (
    SELECT 
        ch.asset_key,
        ch.health_index_current,
        -- Estimate prior health by adding back recent damage impact
        -- Simplified: each 100 damage units = ~5 health points
        CASE 
            WHEN fada.damage_last_30_days > 0 THEN
                LEAST(100.0, ch.health_index_current + (fada.damage_last_30_days / 100.0 * 5.0))
            ELSE ch.health_index_current
        END AS health_index_30d_ago
    FROM current_health AS ch
    LEFT JOIN {{ ref "fact_asset_damage_accumulation" }} AS fada
        ON ch.asset_key = fada.asset_key
),

-- Calculate health change and identify degrading assets
health_changes AS (
    SELECT 
        ch.asset_key,
        ch.tag_id,
        ch.asset_name,
        ch.unit_name,
        ch.health_index_current,
        hhe.health_index_30d_ago,
        ch.health_index_current - hhe.health_index_30d_ago AS health_change,
        ch.health_status,
        ch.trend_direction,
        ch.days_since_last_excursion,
        COALESCE(pdr.primary_reason, 'Multiple_Factors') AS primary_reason
    FROM current_health AS ch
    INNER JOIN historical_health_estimate AS hhe
        ON ch.asset_key = hhe.asset_key
    LEFT JOIN primary_degradation_reason AS pdr
        ON ch.asset_key = pdr.asset_key
    WHERE ch.health_index_current - hhe.health_index_30d_ago < -20
),

-- Categorize degradation severity and assign actions
degradation_alerts AS (
    SELECT 
        asset_key,
        tag_id,
        asset_name,
        unit_name,
        health_index_current,
        health_index_30d_ago,
        health_change,
        health_status,
        trend_direction,
        primary_reason,
        CASE 
            WHEN health_change < -30 THEN 'Severe'
            WHEN health_change < -25 THEN 'Significant'
            ELSE 'Moderate'
        END AS degradation_severity,
        CASE 
            WHEN health_change < -30 THEN 'Critical'
            WHEN health_change < -25 THEN 'High'
            ELSE 'Medium'
        END AS priority,
        CASE 
            WHEN health_change < -30 THEN 'Investigate_Root_Cause'
            WHEN health_change < -25 THEN 'Increase_Monitoring'
            ELSE 'Expedite_Inspection'
        END AS recommended_action
    FROM health_changes
),

-- Generate alert records with formatted messages
alert_records AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY 
            CASE priority 
                WHEN 'Critical' THEN 1 
                WHEN 'High' THEN 2 
                WHEN 'Medium' THEN 3 
                ELSE 4 
            END, 
            health_change ASC, 
            asset_key
        ) AS alert_id,
        DATETIME('now') AS alert_timestamp,
        asset_key,
        tag_id,
        asset_name,
        unit_name,
        'Health_Degradation' AS alert_type,
        priority,
        health_index_current,
        health_index_30d_ago,
        health_change,
        degradation_severity,
        health_status,
        trend_direction,
        primary_reason,
        recommended_action,
        'HEALTH DEGRADATION: ' || 
            asset_name || ' (' || tag_id || ') health dropped ' || 
            CAST(ROUND(ABS(health_change), 1) AS TEXT) || ' points in 30 days (from ' ||
            CAST(ROUND(health_index_30d_ago, 0) AS TEXT) || ' to ' || 
            CAST(ROUND(health_index_current, 0) AS TEXT) || '). ' ||
            'Severity: ' || degradation_severity || '. ' ||
            'Primary cause: ' || primary_reason || '. ' ||
            'Action: ' || recommended_action AS message,
        0 AS acknowledged_flag
    FROM degradation_alerts
)

SELECT 
    alert_id,
    alert_timestamp,
    asset_key,
    tag_id,
    asset_name,
    unit_name,
    alert_type,
    priority,
    health_index_current,
    health_index_30d_ago,
    health_change,
    degradation_severity,
    health_status,
    trend_direction,
    primary_reason,
    recommended_action,
    message,
    acknowledged_flag
FROM alert_records
ORDER BY 
    CASE priority 
        WHEN 'Critical' THEN 1 
        WHEN 'High' THEN 2 
        WHEN 'Medium' THEN 3 
        ELSE 4 
    END, 
    health_change ASC, 
    alert_id
