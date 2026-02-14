-- Fact Table: Asset Damage Accumulation
-- Purpose: Aggregate damage by asset over time for lifecycle tracking
-- Grain: One row per asset per date (or one per asset with current totals)
-- Damage Calculation: Cumulative damage using rolling windows (30, 90, 365 days)

WITH excursion_events AS (
    SELECT 
        asset_key,
        date_key,
        excursion_start_timestamp,
        cumulative_damage_index,
        severity_category,
        criticality_key
    FROM {{ ref "fact_excursion_events" }}
),

-- Get asset attributes
asset_attributes AS (
    SELECT 
        asset_key,
        tag_id,
        install_date,
        design_life_years
    FROM dim_asset
),

-- Get criticality level for filtering
criticality_lookup AS (
    SELECT 
        criticality_key,
        criticality_level
    FROM dim_criticality_level
),

-- Join date dimension to convert date_key to actual dates
excursions_with_dates AS (
    SELECT 
        e.asset_key,
        d.full_date AS excursion_date,
        e.excursion_start_timestamp,
        e.cumulative_damage_index,
        e.severity_category,
        cl.criticality_level
    FROM excursion_events AS e
    INNER JOIN dim_date AS d
        ON e.date_key = d.date_key
    INNER JOIN criticality_lookup AS cl
        ON e.criticality_key = cl.criticality_key
),

-- Create one record per asset with current totals
asset_totals AS (
    SELECT 
        asset_key,
        MAX(excursion_date) AS last_excursion_date,
        MAX(excursion_start_timestamp) AS last_excursion_timestamp,
        COUNT(*) AS excursion_count_total,
        SUM(CASE WHEN criticality_level = 'Critical' THEN 1 ELSE 0 END) AS critical_excursion_count,
        SUM(CASE WHEN criticality_level = 'Standard' THEN 1 ELSE 0 END) AS standard_excursion_count,
        SUM(CASE WHEN criticality_level = 'Informational' THEN 1 ELSE 0 END) AS informational_excursion_count,
        SUM(cumulative_damage_index) AS cumulative_damage_to_date
    FROM excursions_with_dates
    GROUP BY asset_key
),

-- Calculate time-based damage windows using self-join approach
-- Get current date as max date in calendar
current_date_calc AS (
    SELECT MAX(full_date) AS as_of_date
    FROM dim_date
),

-- Calculate damage for last 30 days
damage_30_days AS (
    SELECT 
        e.asset_key,
        SUM(e.cumulative_damage_index) AS damage_last_30_days
    FROM excursions_with_dates AS e
    CROSS JOIN current_date_calc AS c
    WHERE julianday(c.as_of_date) - julianday(e.excursion_date) <= 30
    GROUP BY e.asset_key
),

-- Calculate damage for last 90 days
damage_90_days AS (
    SELECT 
        e.asset_key,
        SUM(e.cumulative_damage_index) AS damage_last_90_days
    FROM excursions_with_dates AS e
    CROSS JOIN current_date_calc AS c
    WHERE julianday(c.as_of_date) - julianday(e.excursion_date) <= 90
    GROUP BY e.asset_key
),

-- Calculate damage for last 365 days
damage_365_days AS (
    SELECT 
        e.asset_key,
        SUM(e.cumulative_damage_index) AS damage_last_365_days
    FROM excursions_with_dates AS e
    CROSS JOIN current_date_calc AS c
    WHERE julianday(c.as_of_date) - julianday(e.excursion_date) <= 365
    GROUP BY e.asset_key
),

-- Calculate days since last critical excursion
last_critical_excursion AS (
    SELECT 
        e.asset_key,
        MAX(e.excursion_date) AS last_critical_date
    FROM excursions_with_dates AS e
    WHERE e.criticality_level = 'Critical'
    GROUP BY e.asset_key
),

-- Combine all metrics
combined_metrics AS (
    SELECT 
        at.asset_key,
        CAST(strftime('%Y%m%d', c.as_of_date) AS INTEGER) AS as_of_date_key,
        COALESCE(at.cumulative_damage_to_date, 0) AS cumulative_damage_to_date,
        COALESCE(d30.damage_last_30_days, 0) AS damage_last_30_days,
        COALESCE(d90.damage_last_90_days, 0) AS damage_last_90_days,
        COALESCE(d365.damage_last_365_days, 0) AS damage_last_365_days,
        COALESCE(at.excursion_count_total, 0) AS excursion_count_total,
        COALESCE(at.critical_excursion_count, 0) AS critical_excursion_count,
        COALESCE(at.standard_excursion_count, 0) AS standard_excursion_count,
        COALESCE(at.informational_excursion_count, 0) AS informational_excursion_count,
        CASE 
            WHEN lce.last_critical_date IS NOT NULL 
            THEN CAST(julianday(c.as_of_date) - julianday(lce.last_critical_date) AS INTEGER)
            ELSE NULL
        END AS days_since_last_critical_excursion,
        at.last_excursion_date,
        aa.install_date AS asset_install_date,
        aa.design_life_years
    FROM asset_totals AS at
    CROSS JOIN current_date_calc AS c
    LEFT JOIN damage_30_days AS d30
        ON at.asset_key = d30.asset_key
    LEFT JOIN damage_90_days AS d90
        ON at.asset_key = d90.asset_key
    LEFT JOIN damage_365_days AS d365
        ON at.asset_key = d365.asset_key
    LEFT JOIN last_critical_excursion AS lce
        ON at.asset_key = lce.asset_key
    INNER JOIN asset_attributes AS aa
        ON at.asset_key = aa.asset_key
),

-- Add surrogate key
final_output AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY asset_key) AS damage_record_key,
        asset_key,
        as_of_date_key,
        cumulative_damage_to_date,
        damage_last_30_days,
        damage_last_90_days,
        damage_last_365_days,
        excursion_count_total,
        critical_excursion_count,
        standard_excursion_count,
        informational_excursion_count,
        days_since_last_critical_excursion,
        last_excursion_date,
        asset_install_date,
        design_life_years
    FROM combined_metrics
)

SELECT * FROM final_output
ORDER BY damage_record_key
