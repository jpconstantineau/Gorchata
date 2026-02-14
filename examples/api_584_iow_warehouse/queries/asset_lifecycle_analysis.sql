-- ===================================================================
-- Asset Lifecycle Analysis
-- Purpose: Compare actual damage accumulation vs design life consumption
-- Business Value: Identify assets consuming design life faster than expected
-- ===================================================================

WITH asset_damage_data AS (
    SELECT
        a.asset_key,
        a.tag_id,
        a.equipment_name,
        a.unit_name,
        a.damage_mechanism_primary,
        a.install_date,
        a.design_life_years,
        d.cumulative_damage_to_date,
        d.cumulative_damage_365d,
        d.avg_daily_damage_30d,
        m.health_index
    FROM {{ ref "dim_asset" }} a
    INNER JOIN {{ ref "fact_asset_damage_accumulation" }} d
        ON a.asset_key = d.asset_key
    INNER JOIN {{ ref "metrics_asset_integrity_index" }} m
        ON a.asset_key = m.asset_key
),

lifecycle_calculations AS (
    SELECT
        asset_key,
        tag_id,
        equipment_name,
        unit_name,
        damage_mechanism_primary,
        install_date,
        design_life_years,
        cumulative_damage_to_date,
        health_index,
        avg_daily_damage_30d,
        
        -- Calculate chronological age
        CAST((julianday('now') - julianday(install_date)) / 365.25 AS REAL) AS chronological_age_years,
        
        -- Calculate percentage of design life elapsed (time-based)
        (CAST((julianday('now') - julianday(install_date)) / 365.25 AS REAL) / design_life_years) * 100.0 AS pct_design_life_elapsed,
        
        -- Calculate percentage of design life consumed by damage
        -- Assuming theoretical design margin of 10000 damage units over design life
        (cumulative_damage_to_date / (design_life_years * 365.25 * 10.0)) * 100.0 AS pct_design_life_consumed_by_damage
        
    FROM asset_damage_data
),

aging_analysis AS (
    SELECT
        asset_key,
        tag_id,
        equipment_name,
        unit_name,
        damage_mechanism_primary,
        install_date,
        design_life_years,
        chronological_age_years,
        pct_design_life_elapsed,
        pct_design_life_consumed_by_damage,
        health_index,
        avg_daily_damage_30d,
        
        -- Aging acceleration factor: >1.0 = aging faster than design
        CASE
            WHEN pct_design_life_elapsed > 0
            THEN pct_design_life_consumed_by_damage / NULLIF(pct_design_life_elapsed, 0)
            ELSE 1.0
        END AS aging_acceleration_factor,
        
        -- Remaining design life (time-based)
        design_life_years - chronological_age_years AS remaining_life_years_design
        
    FROM lifecycle_calculations
)

SELECT
    asset_key,
    tag_id,
    equipment_name,
    unit_name,
    install_date,
    design_life_years,
    chronological_age_years,
    pct_design_life_elapsed,
    pct_design_life_consumed_by_damage,
    aging_acceleration_factor,
    
    -- Lifecycle status classification
    CASE
        WHEN aging_acceleration_factor > 1.2 THEN 'Accelerated_Aging'
        WHEN aging_acceleration_factor >= 0.8 AND aging_acceleration_factor <= 1.2 THEN 'Normal_Aging'
        WHEN aging_acceleration_factor < 0.8 THEN 'Better_Than_Expected'
        ELSE 'Unknown'
    END AS lifecycle_status,
    
    remaining_life_years_design,
    
    -- Remaining life adjusted for actual damage rate
    CASE
        WHEN aging_acceleration_factor > 0
        THEN remaining_life_years_design / NULLIF(aging_acceleration_factor, 0)
        ELSE remaining_life_years_design
    END AS remaining_life_years_actual,
    
    -- Years lost to excursions (difference between design and actual)
    remaining_life_years_design - 
    (CASE
        WHEN aging_acceleration_factor > 0
        THEN remaining_life_years_design / NULLIF(aging_acceleration_factor, 0)
        ELSE remaining_life_years_design
    END) AS years_lost_to_excursions,
    
    health_index,
    damage_mechanism_primary,
    
    -- Recommended action based on lifecycle analysis
    CASE
        WHEN aging_acceleration_factor > 2.0 AND remaining_life_years_design < 2 
            THEN 'Replace_Soon'
        WHEN aging_acceleration_factor > 1.5 AND remaining_life_years_design < 5 
            THEN 'Monitor_Closely'
        WHEN aging_acceleration_factor > 1.2 
            THEN 'Increase_Inspection_Frequency'
        ELSE 'Continue_Normal_Operations'
    END AS recommended_action,
    
    -- Flag assets with concerning lifecycle status
    CASE
        WHEN aging_acceleration_factor > 1.5 AND health_index < 60 THEN 1
        ELSE 0
    END AS high_priority_flag
    
FROM aging_analysis
WHERE chronological_age_years > 0  -- Only assets with valid install dates
ORDER BY 
    aging_acceleration_factor DESC,  -- Fastest aging first
    remaining_life_years_actual ASC   -- Shortest remaining life second
LIMIT 100;  -- Top 100 assets of concern
