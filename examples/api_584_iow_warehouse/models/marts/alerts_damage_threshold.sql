-- Alert Model: Damage Threshold Exceeded
-- Purpose: Alert notifications for assets exceeding damage mechanism-specific thresholds
-- Grain: One row per asset that has exceeded its damage mechanism threshold
-- Alert Logic: Asset-specific thresholds based on damage_mechanism_primary

WITH damage_data AS (
    SELECT 
        asset_key,
        cumulative_damage_to_date,
        damage_last_30_days,
        damage_last_90_days,
        excursion_count_total
    FROM {{ ref "fact_asset_damage_accumulation" }}
),

-- Get asset attributes including damage mechanism
asset_info AS (
    SELECT 
        asset_key,
        tag_id,
        equipment_name AS asset_name,
        unit_name,
        damage_mechanism_primary,
        material_grade
    FROM dim_asset
),

-- Define mechanism-specific damage thresholds
-- Sulfidation: 1000, HTHA: 800, Creep: 600, CUI: 1200, Naphthenic Acid: 900, Default: 1000
damage_with_thresholds AS (
    SELECT 
        dd.asset_key,
        ai.tag_id,
        ai.asset_name,
        ai.unit_name,
        ai.damage_mechanism_primary,
        ai.material_grade,
        dd.cumulative_damage_to_date,
        dd.damage_last_30_days,
        dd.damage_last_90_days,
        dd.excursion_count_total,
        CASE 
            WHEN LOWER(ai.damage_mechanism_primary) LIKE '%sulfidation%' THEN 1000.0
            WHEN LOWER(ai.damage_mechanism_primary) LIKE '%htha%' THEN 800.0
            WHEN LOWER(ai.damage_mechanism_primary) LIKE '%hydrogen%attack%' THEN 800.0
            WHEN LOWER(ai.damage_mechanism_primary) LIKE '%creep%' THEN 600.0
            WHEN LOWER(ai.damage_mechanism_primary) LIKE '%cui%' THEN 1200.0
            WHEN LOWER(ai.damage_mechanism_primary) LIKE '%corrosion%insulation%' THEN 1200.0
            WHEN LOWER(ai.damage_mechanism_primary) LIKE '%naphthenic%' THEN 900.0
            WHEN LOWER(ai.damage_mechanism_primary) LIKE '%acid%' THEN 900.0
            ELSE 1000.0
        END AS damage_threshold
    FROM damage_data AS dd
    INNER JOIN asset_info AS ai
        ON dd.asset_key = ai.asset_key
),

-- Filter to only assets that have exceeded their threshold
exceeded_thresholds AS (
    SELECT 
        asset_key,
        tag_id,
        asset_name,
        unit_name,
        damage_mechanism_primary,
        material_grade,
        cumulative_damage_to_date,
        damage_threshold,
        damage_last_30_days,
        damage_last_90_days,
        excursion_count_total,
        (cumulative_damage_to_date - damage_threshold) / damage_threshold AS damage_pct_over_threshold,
        CASE 
            WHEN cumulative_damage_to_date > damage_threshold * 1.5 THEN 'Critical'
            WHEN cumulative_damage_to_date > damage_threshold * 1.2 THEN 'High'
            ELSE 'Medium'
        END AS priority
    FROM damage_with_thresholds
    WHERE cumulative_damage_to_date > damage_threshold
),

-- Calculate estimated days to failure based on recent damage rate
failure_estimation AS (
    SELECT 
        asset_key,
        tag_id,
        asset_name,
        unit_name,
        damage_mechanism_primary,
        material_grade,
        cumulative_damage_to_date,
        damage_threshold,
        damage_last_30_days,
        damage_last_90_days,
        damage_pct_over_threshold,
        priority,
        -- Estimate days to critical failure (assume failure at 2x threshold)
        CASE 
            WHEN damage_last_30_days > 0 THEN
                CAST(((damage_threshold * 2.0) - cumulative_damage_to_date) / (damage_last_30_days / 30.0) AS INTEGER)
            ELSE 999
        END AS estimated_days_to_failure,
        CASE 
            WHEN cumulative_damage_to_date > damage_threshold * 1.5 THEN 'Immediate_Shutdown_Inspection'
            WHEN cumulative_damage_to_date > damage_threshold * 1.3 THEN 'Plan_Replacement'
            WHEN cumulative_damage_to_date > damage_threshold * 1.1 THEN 'Schedule_Repair'
            ELSE 'Monitor_Closely'
        END AS recommended_action
    FROM exceeded_thresholds
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
            damage_pct_over_threshold DESC, 
            asset_key
        ) AS alert_id,
        DATETIME('now') AS alert_timestamp,
        asset_key,
        tag_id,
        asset_name,
        unit_name,
        damage_mechanism_primary,
        material_grade,
        'Damage_Threshold_Exceeded' AS alert_type,
        priority,
        cumulative_damage_to_date,
        damage_threshold,
        ROUND(damage_pct_over_threshold * 100.0, 1) AS damage_pct_over_threshold,
        damage_last_30_days,
        damage_last_90_days,
        estimated_days_to_failure,
        recommended_action,
        'DAMAGE THRESHOLD EXCEEDED: ' || 
            asset_name || ' (' || tag_id || ') ' ||
            damage_mechanism_primary || ' damage at ' || 
            CAST(ROUND((1.0 + damage_pct_over_threshold) * 100.0, 1) AS TEXT) || '% of limit (' ||
            CAST(ROUND(cumulative_damage_to_date, 0) AS TEXT) || ' vs threshold ' || 
            CAST(ROUND(damage_threshold, 0) AS TEXT) || '). ' ||
            'Estimated days to failure: ' || CAST(estimated_days_to_failure AS TEXT) || '. ' ||
            'Action: ' || recommended_action AS message,
        0 AS acknowledged_flag
    FROM failure_estimation
)

SELECT 
    alert_id,
    alert_timestamp,
    asset_key,
    tag_id,
    asset_name,
    unit_name,
    damage_mechanism_primary,
    material_grade,
    alert_type,
    priority,
    cumulative_damage_to_date,
    damage_threshold,
    damage_pct_over_threshold,
    damage_last_30_days,
    damage_last_90_days,
    estimated_days_to_failure,
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
    damage_pct_over_threshold DESC, 
    alert_id
