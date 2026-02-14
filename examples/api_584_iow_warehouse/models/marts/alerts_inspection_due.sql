-- Alert Model: Inspection Due
-- Purpose: Alert notifications for assets requiring inspection based on damage accumulation or health degradation
-- Grain: One row per asset that meets inspection threshold criteria
-- Alert Logic: Trigger when cumulative_damage_365d > 80% of design margin, health_index < 50, or 90+ days since last critical

WITH damage_data AS (
    SELECT 
        asset_key,
        cumulative_damage_to_date,
        cumulative_damage_365d,
        damage_last_90_days,
        critical_excursion_count,
        days_since_last_critical_excursion,
        asset_install_date,
        design_life_years
    FROM {{ ref "fact_asset_damage_accumulation" }}
),

-- Get health index for each asset
health_data AS (
    SELECT 
        asset_key,
        health_index,
        health_status,
        years_in_service
    FROM {{ ref "metrics_asset_integrity_index" }}
),

-- Get asset attributes
asset_info AS (
    SELECT 
        asset_key,
        tag_id,
        equipment_name AS asset_name,
        unit_name,
        damage_mechanism_primary
    FROM dim_asset
),

-- Calculate design margin (simplified: using design_life_years * 1000 as baseline)
-- Trigger conditions: damage exceeds 80% of threshold, health < 50, or 90+ days since critical
assets_needing_inspection AS (
    SELECT 
        dd.asset_key,
        ai.tag_id,
        ai.asset_name,
        ai.unit_name,
        ai.damage_mechanism_primary,
        dd.cumulative_damage_to_date,
        dd.cumulative_damage_365d,
        dd.damage_last_90_days,
        dd.design_life_years,
        (dd.design_life_years * 1000.0) AS design_margin,
        dd.cumulative_damage_365d / (dd.design_life_years * 1000.0) AS damage_pct_of_limit,
        hd.health_index,
        hd.health_status,
        hd.years_in_service,
        dd.critical_excursion_count,
        dd.days_since_last_critical_excursion,
        CASE 
            WHEN hd.health_index < 50 THEN 1
            WHEN dd.cumulative_damage_365d > (dd.design_life_years * 1000.0 * 0.80) THEN 1
            WHEN dd.days_since_last_critical_excursion > 90 AND dd.critical_excursion_count > 0 THEN 1
            ELSE 0
        END AS inspection_due_flag
    FROM damage_data AS dd
    INNER JOIN health_data AS hd
        ON dd.asset_key = hd.asset_key
    INNER JOIN asset_info AS ai
        ON dd.asset_key = ai.asset_key
    WHERE 
        hd.health_index < 50
        OR dd.cumulative_damage_365d > (dd.design_life_years * 1000.0 * 0.80)
        OR (dd.days_since_last_critical_excursion > 90 AND dd.critical_excursion_count > 0)
),

-- Calculate days until inspection needed (based on damage rate)
inspection_timing AS (
    SELECT 
        asset_key,
        tag_id,
        asset_name,
        unit_name,
        damage_mechanism_primary,
        cumulative_damage_to_date,
        cumulative_damage_365d,
        damage_last_90_days,
        design_margin,
        damage_pct_of_limit,
        health_index,
        health_status,
        years_in_service,
        critical_excursion_count,
        days_since_last_critical_excursion,
        CASE 
            WHEN damage_last_90_days > 0 THEN
                CAST((design_margin - cumulative_damage_365d) / (damage_last_90_days / 90.0) AS INTEGER)
            ELSE 365
        END AS days_until_inspection,
        CASE 
            WHEN health_index < 30 THEN 'Critical'
            WHEN health_index < 50 THEN 'High'
            WHEN damage_pct_of_limit > 0.90 THEN 'High'
            ELSE 'Medium'
        END AS priority,
        CASE 
            WHEN health_index < 30 THEN 'Immediate_Inspection'
            WHEN health_index < 50 AND damage_pct_of_limit > 0.80 THEN 'Schedule_Within_30_Days'
            WHEN days_since_last_critical_excursion > 120 THEN 'Schedule_Within_30_Days'
            ELSE 'Schedule_Next_Turnaround'
        END AS recommended_action
    FROM assets_needing_inspection
),

-- Generate alert records with messages
alert_records AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY 
            CASE priority 
                WHEN 'Critical' THEN 1 
                WHEN 'High' THEN 2 
                WHEN 'Medium' THEN 3 
                ELSE 4 
            END, 
            health_index ASC, 
            asset_key
        ) AS alert_id,
        DATETIME('now') AS alert_timestamp,
        asset_key,
        tag_id,
        asset_name,
        unit_name,
        'Inspection_Due' AS alert_type,
        priority,
        cumulative_damage_to_date,
        cumulative_damage_365d,
        ROUND(damage_pct_of_limit * 100.0, 1) AS damage_pct_of_limit,
        health_index,
        health_status,
        days_since_last_critical_excursion,
        days_until_inspection,
        recommended_action,
        'INSPECTION DUE: ' || 
            asset_name || ' (' || tag_id || ') has ' || 
            CAST(ROUND(damage_pct_of_limit * 100.0, 1) AS TEXT) || '% of damage limit (' ||
            CAST(ROUND(cumulative_damage_365d, 0) AS TEXT) || ' of ' || CAST(ROUND(design_margin, 0) AS TEXT) || 
            '), health index: ' || CAST(ROUND(health_index, 0) AS TEXT) || 
            '. Action: ' || recommended_action AS message,
        0 AS acknowledged_flag
    FROM inspection_timing
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
    cumulative_damage_to_date,
    cumulative_damage_365d,
    damage_pct_of_limit,
    health_index,
    health_status,
    days_since_last_critical_excursion,
    days_until_inspection,
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
    health_index ASC, 
    alert_id
