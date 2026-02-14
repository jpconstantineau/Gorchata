-- ===================================================================
-- Unit Performance Comparison
-- Purpose: Normalized metrics across units for fair comparison
-- Business Value: Enable management to compare unit integrity performance on apples-to-apples basis
-- ===================================================================

WITH unit_asset_counts AS (
    -- Count assets per unit for normalization
    SELECT
        unit_name,
        COUNT(*) AS asset_count
    FROM {{ ref "dim_asset" }}
    GROUP BY unit_name
),

unit_health_metrics AS (
    -- Aggregate health metrics by unit
    SELECT
        a.unit_name,
        AVG(m.health_index) AS unit_avg_health_index,
        MIN(m.health_index) AS unit_min_health_index,
        MAX(m.health_index) AS unit_max_health_index,
        COUNT(CASE WHEN m.health_index < 50 THEN 1 END) AS critical_asset_count,
        COUNT(CASE WHEN m.health_index >= 50 AND m.health_index < 70 THEN 1 END) AS poor_asset_count
    FROM {{ ref "dim_asset" }} a
    INNER JOIN {{ ref "metrics_asset_integrity_index" }} m
        ON a.asset_key = m.asset_key
    GROUP BY a.unit_name
),

unit_excursion_metrics AS (
    -- Aggregate excursion metrics by unit
    SELECT
        a.unit_name,
        COUNT(*) AS total_excursion_count,
        SUM(CASE WHEN e.severity_level = 'Critical' THEN 1 ELSE 0 END) AS total_critical_excursion_count
    FROM {{ ref "fact_excursion_events" }} e
    INNER JOIN {{ ref "dim_asset" }} a
        ON e.asset_key = a.asset_key
    GROUP BY a.unit_name
),

unit_damage_metrics AS (
    -- Aggregate damage accumulation by unit
    SELECT
        a.unit_name,
        SUM(d.cumulative_damage_365d) AS total_cumulative_damage
    FROM {{ ref "fact_asset_damage_accumulation" }} d
    INNER JOIN {{ ref "dim_asset" }} a
        ON d.asset_key = a.asset_key
    GROUP BY a.unit_name
),

unit_damage_mechanisms AS (
    -- Identify primary damage mechanism per unit (most common)
    SELECT
        unit_name,
        damage_mechanism_primary,
        COUNT(*) AS mechanism_count
    FROM {{ ref "dim_asset" }}
    GROUP BY unit_name, damage_mechanism_primary
    HAVING COUNT(*) = (
        SELECT MAX(cnt) FROM (
            SELECT unit_name AS u, COUNT(*) AS cnt
            FROM {{ ref "dim_asset" }}
            WHERE unit_name = {{ ref "dim_asset" }}.unit_name
            GROUP BY unit_name, damage_mechanism_primary
        ) sub WHERE u = unit_name
    )
),

unit_summary AS (
    SELECT
        c.unit_name,
        c.asset_count,
        h.unit_avg_health_index,
        h.unit_min_health_index,
        h.unit_max_health_index,
        COALESCE(e.total_excursion_count, 0) AS total_excursion_count,
        COALESCE(e.total_critical_excursion_count, 0) AS total_critical_excursion_count,
        COALESCE(d.total_cumulative_damage, 0) AS total_cumulative_damage,
        h.critical_asset_count,
        h.poor_asset_count,
        m.damage_mechanism_primary AS primary_damage_mechanism,
        
        -- Normalized metrics (per-asset averages for fair comparison)
        CAST(COALESCE(e.total_excursion_count, 0) AS REAL) / c.asset_count AS excursions_per_asset,
        CAST(COALESCE(e.total_critical_excursion_count, 0) AS REAL) / c.asset_count AS critical_excursions_per_asset,
        COALESCE(d.total_cumulative_damage, 0) / c.asset_count AS damage_per_asset,
        CAST(h.critical_asset_count AS REAL) / c.asset_count * 100.0 AS pct_critical_assets,
        CAST(h.poor_asset_count AS REAL) / c.asset_count * 100.0 AS pct_poor_assets
        
    FROM unit_asset_counts c
    INNER JOIN unit_health_metrics h
        ON c.unit_name = h.unit_name
    LEFT JOIN unit_excursion_metrics e
        ON c.unit_name = e.unit_name
    LEFT JOIN unit_damage_metrics d
        ON c.unit_name = d.unit_name
    LEFT JOIN unit_damage_mechanisms m
        ON c.unit_name = m.unit_name
)

SELECT
    unit_name,
    asset_count,
    unit_avg_health_index,
    unit_min_health_index,
    unit_max_health_index,
    total_excursion_count,
    excursions_per_asset,
    total_critical_excursion_count,
    critical_excursions_per_asset,
    total_cumulative_damage,
    damage_per_asset,
    critical_asset_count,
    pct_critical_assets,
    poor_asset_count,
    pct_poor_assets,
    
    -- Unit performance rank (lower is better)
    RANK() OVER (
        ORDER BY 
            unit_avg_health_index DESC,  -- Higher health is better
            excursions_per_asset ASC     -- Fewer excursions is better
    ) AS unit_performance_rank,
    
    -- Unit status classification
    CASE
        WHEN unit_avg_health_index > 80 THEN 'Excellent'
        WHEN unit_avg_health_index >= 70 THEN 'Good'
        WHEN unit_avg_health_index >= 60 THEN 'Fair'
        WHEN unit_avg_health_index >= 50 THEN 'Concerning'
        ELSE 'Critical'
    END AS unit_status,
    
    primary_damage_mechanism,
    
    -- Recommended action based on unit status
    CASE
        WHEN unit_avg_health_index < 50 OR pct_critical_assets > 20 
            THEN 'Plan_Turnaround'
        WHEN unit_avg_health_index < 60 OR pct_critical_assets > 10 
            THEN 'Schedule_Unit_Inspection'
        WHEN unit_avg_health_index < 70 OR critical_excursions_per_asset > 5 
            THEN 'Increase_Monitoring'
        ELSE 'Continue_Operations'
    END AS recommended_action
    
FROM unit_summary
ORDER BY unit_performance_rank ASC;  -- Best performing units first
