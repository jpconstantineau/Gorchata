-- ===================================================================
-- Inspection Priority Queue
-- Purpose: Ranked list of assets requiring inspection prioritized by risk
-- Business Value: Provides maintenance planners with actionable inspection schedule
-- ===================================================================

WITH asset_metrics AS (
    SELECT
        a.asset_key,
        a.tag_id,
        a.equipment_name,
        a.unit_name,
        a.criticality_level,
        a.damage_mechanism_primary,
        m.health_index,
        m.critical_excursion_count,
        m.days_since_last_critical,
        d.cumulative_damage_365d,
        d.avg_daily_damage_30d
    FROM {{ ref "dim_asset" }} a
    INNER JOIN {{ ref "metrics_asset_integrity_index" }} m
        ON a.asset_key = m.asset_key
    INNER JOIN {{ ref "fact_asset_damage_accumulation" }} d
        ON a.asset_key = d.asset_key
),

risk_scoring AS (
    SELECT
        asset_key,
        tag_id,
        equipment_name,
        unit_name,
        criticality_level,
        damage_mechanism_primary,
        health_index,
        cumulative_damage_365d,
        critical_excursion_count,
        days_since_last_critical,
        avg_daily_damage_30d,
        
        -- Calculate priority score combining multiple risk factors
        -- Formula: (100 - health_index) * 2 + (cumulative_damage_365d / 100) * 3 + critical_excursion_count * 5
        -- Higher score = more urgent inspection needed
        ((100 - health_index) * 2.0) +
        ((cumulative_damage_365d / 100.0) * 3.0) +
        (critical_excursion_count * 5.0) AS priority_score,
        
        -- Consequence category based on unit importance and criticality
        CASE
            WHEN unit_name = 'CDU' AND criticality_level = 'Critical' THEN 'High'
            WHEN unit_name = 'FCC' AND criticality_level = 'Critical' THEN 'High'
            WHEN criticality_level = 'Critical' THEN 'High'
            WHEN criticality_level = 'Standard' THEN 'Medium'
            ELSE 'Low'
        END AS consequence_category,
        
        -- Estimate inspection cost (simplified model based on equipment type and unit)
        CASE
            WHEN equipment_name LIKE '%Reactor%' THEN 75000
            WHEN equipment_name LIKE '%Tower%' THEN 50000
            WHEN equipment_name LIKE '%Column%' THEN 45000
            WHEN equipment_name LIKE '%Drum%' THEN 30000
            WHEN equipment_name LIKE '%Exchanger%' THEN 25000
            WHEN equipment_name LIKE '%Vessel%' THEN 35000
            WHEN equipment_name LIKE '%Heater%' THEN 40000
            ELSE 20000
        END * 
        CASE
            WHEN unit_name = 'CDU' THEN 1.2  -- CDU is more complex
            WHEN unit_name = 'FCC' THEN 1.3  -- FCC is highest complexity
            ELSE 1.0
        END AS estimated_inspection_cost,
        
        -- Calculate days until recommended inspection based on damage rate
        -- If damage rate is high (>10/day), inspect within 30 days
        -- If moderate (5-10/day), inspect within 90 days
        -- If low (<5/day), inspect within 180 days
        CASE
            WHEN avg_daily_damage_30d >= 10 THEN 30
            WHEN avg_daily_damage_30d >= 5 THEN 90
            WHEN avg_daily_damage_30d >= 2 THEN 180
            ELSE 365
        END AS days_until_recommended_inspection
        
    FROM asset_metrics
)

SELECT
    asset_key,
    tag_id,
    equipment_name,
    unit_name,
    priority_score,
    health_index,
    cumulative_damage_365d,
    critical_excursion_count,
    days_since_last_critical,
    damage_mechanism_primary,
    days_until_recommended_inspection,
    consequence_category,
    estimated_inspection_cost,
    ROW_NUMBER() OVER (ORDER BY priority_score DESC) AS inspection_priority_rank
FROM risk_scoring
WHERE health_index < 90  -- Focus on assets that need attention
ORDER BY priority_score DESC, health_index ASC
LIMIT 50;  -- Top 50 highest priority assets
