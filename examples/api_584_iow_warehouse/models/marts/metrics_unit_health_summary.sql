-- Metrics: Unit Health Summary
-- Purpose: Aggregate asset health metrics by refinery unit (CDU, VDU, FCC, HCU)
-- Grain: One row per unit with aggregated health statistics
-- Usage: Unit-level operational KPI dashboard for management

WITH asset_health AS (
    SELECT 
        asset_key,
        tag_id,
        equipment_name,
        unit_name,
        integrity_health_index,
        health_status,
        total_excursion_count,
        critical_excursion_count,
        cumulative_damage_total,
        damage_last_90_days
    FROM {{ ref "metrics_asset_integrity_index" }}
),

-- Find worst asset per unit using window function
worst_asset_per_unit AS (
    SELECT 
        unit_name,
        tag_id AS worst_asset_tag_id,
        integrity_health_index AS worst_asset_health_index,
        ROW_NUMBER() OVER (
            PARTITION BY unit_name 
            ORDER BY integrity_health_index ASC, critical_excursion_count DESC
        ) AS rn
    FROM asset_health
),

-- Aggregate metrics by unit
unit_aggregations AS (
    SELECT 
        unit_name,
        COUNT(*) AS asset_count,
        ROUND(AVG(integrity_health_index), 2) AS unit_avg_health_index,
        MIN(integrity_health_index) AS unit_min_health_index,
        MAX(integrity_health_index) AS unit_max_health_index,
        SUM(CASE WHEN health_status = 'Critical' THEN 1 ELSE 0 END) AS assets_in_critical_status,
        SUM(CASE WHEN health_status = 'Poor' THEN 1 ELSE 0 END) AS assets_in_poor_status,
        SUM(critical_excursion_count) AS unit_critical_excursion_count,
        SUM(total_excursion_count) AS unit_total_excursion_count,
        ROUND(SUM(cumulative_damage_total), 2) AS unit_damage_total,
        ROUND(SUM(damage_last_90_days), 2) AS unit_damage_last_90_days
    FROM asset_health
    GROUP BY unit_name
)

-- Final selection with unit-level health status
SELECT 
    ua.unit_name,
    ua.asset_count,
    ua.unit_avg_health_index,
    ua.unit_min_health_index,
    ua.unit_max_health_index,
    ua.assets_in_critical_status,
    ua.assets_in_poor_status,
    ua.unit_critical_excursion_count,
    ua.unit_total_excursion_count,
    ua.unit_damage_total,
    ua.unit_damage_last_90_days,
    w.worst_asset_tag_id,
    w.worst_asset_health_index,
    -- Unit-level health status based on average health index
    CASE 
        WHEN ua.unit_avg_health_index >= 90 THEN 'Excellent'
        WHEN ua.unit_avg_health_index >= 70 THEN 'Good'
        WHEN ua.unit_avg_health_index >= 50 THEN 'Fair'
        WHEN ua.unit_avg_health_index >= 30 THEN 'Poor'
        ELSE 'Critical'
    END AS unit_health_status
FROM unit_aggregations AS ua
LEFT JOIN worst_asset_per_unit AS w
    ON ua.unit_name = w.unit_name
    AND w.rn = 1
ORDER BY ua.unit_avg_health_index ASC, ua.unit_critical_excursion_count DESC
