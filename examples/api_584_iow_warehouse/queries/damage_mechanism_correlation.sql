-- ===================================================================
-- Damage Mechanism Correlation Analysis
-- Purpose: Correlate IOW excursion types with specific damage mechanisms
-- Business Value: Help engineers understand which parameters drive damage for each mechanism
-- ===================================================================

WITH excursion_mechanism_pairs AS (
    SELECT
        a.damage_mechanism_primary,
        e.parameter_type,
        e.excursion_id,
        e.cumulative_damage_from_excursion,
        e.excursion_magnitude,
        e.duration_minutes
    FROM {{ ref "fact_excursion_events" }} e
    INNER JOIN {{ ref "dim_asset" }} a
        ON e.asset_key = a.asset_key
    WHERE e.cumulative_damage_from_excursion > 0  -- Only excursions that caused damage
),

mechanism_parameter_aggregates AS (
    SELECT
        damage_mechanism_primary,
        parameter_type,
        COUNT(*) AS excursion_count,
        SUM(cumulative_damage_from_excursion) AS total_damage_from_excursions,
        AVG(excursion_magnitude) AS avg_excursion_magnitude,
        AVG(duration_minutes) AS avg_duration_minutes,
        COUNT(DISTINCT excursion_id) AS affected_asset_count
    FROM excursion_mechanism_pairs
    GROUP BY damage_mechanism_primary, parameter_type
),

mechanism_totals AS (
    -- Calculate total damage per mechanism across all parameter types
    SELECT
        damage_mechanism_primary,
        SUM(total_damage_from_excursions) AS mechanism_total_damage
    FROM mechanism_parameter_aggregates
    GROUP BY damage_mechanism_primary
)

SELECT
    m.damage_mechanism_primary,
    m.parameter_type,
    m.excursion_count,
    m.total_damage_from_excursions,
    m.avg_excursion_magnitude,
    m.avg_duration_minutes,
    m.affected_asset_count,
    
    -- Calculate what percentage of this mechanism's damage comes from this parameter type
    (m.total_damage_from_excursions / NULLIF(t.mechanism_total_damage, 0)) * 100.0 AS pct_of_mechanism_damage,
    
    -- Flag dominant parameter types (>40% of mechanism's damage)
    CASE
        WHEN (m.total_damage_from_excursions / NULLIF(t.mechanism_total_damage, 0)) > 0.40 THEN 1
        ELSE 0
    END AS dominant_parameter_flag,
    
    -- Explain the correlation (business insight)
    CASE
        -- High-temperature mechanisms
        WHEN m.damage_mechanism_primary IN ('Creep', 'Thermal_Fatigue', 'HTHA') AND m.parameter_type = 'Temperature' 
            THEN 'Expected: Temperature excursions drive high-temp damage mechanisms'
        WHEN m.damage_mechanism_primary IN ('Creep', 'Thermal_Fatigue', 'HTHA') AND m.parameter_type != 'Temperature' 
            THEN 'Investigate: Non-temperature parameter affecting temp-sensitive mechanism'
        
        -- Corrosion mechanisms
        WHEN m.damage_mechanism_primary IN ('Naphthenic_Acid_Corrosion', 'SCC') AND m.parameter_type = 'pH' 
            THEN 'Expected: pH excursions drive corrosion mechanisms'
        WHEN m.damage_mechanism_primary = 'Sulfidation' AND m.parameter_type IN ('Temperature', 'pH') 
            THEN 'Expected: Temp/pH excursions accelerate sulfidation'
        
        -- Mechanical damage
        WHEN m.damage_mechanism_primary IN ('Fatigue', 'Erosion_Corrosion') AND m.parameter_type = 'Pressure' 
            THEN 'Expected: Pressure cycling drives mechanical damage'
        WHEN m.damage_mechanism_primary IN ('Fatigue', 'Erosion_Corrosion') AND m.parameter_type = 'Flow' 
            THEN 'Expected: Flow excursions cause erosion/fatigue'
        
        -- Environmental corrosion
        WHEN m.damage_mechanism_primary = 'CUI' AND m.parameter_type = 'Temperature' 
            THEN 'Expected: Temp cycling in insulation causes CUI'
        
        ELSE 'Review: Unexpected parameter-mechanism correlation'
    END AS correlation_insight
    
FROM mechanism_parameter_aggregates m
INNER JOIN mechanism_totals t
    ON m.damage_mechanism_primary = t.damage_mechanism_primary
ORDER BY 
    m.damage_mechanism_primary,
    pct_of_mechanism_damage DESC;
