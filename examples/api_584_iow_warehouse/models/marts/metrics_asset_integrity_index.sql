-- Metrics: Asset Integrity Health Index
-- Purpose: Calculate asset-level integrity health scores on 0-100 scale for operational KPIs
-- Grain: One row per asset with current health metrics
-- Health Scoring: 100 = perfect health (no damage), 0 = critically unhealthy

WITH damage_data AS (
    SELECT 
        asset_key,
        cumulative_damage_to_date,
        damage_last_30_days,
        damage_last_90_days,
        excursion_count_total,
        critical_excursion_count,
        standard_excursion_count,
        informational_excursion_count,
        days_since_last_critical_excursion,
        last_excursion_date,
        asset_install_date,
        design_life_years
    FROM {{ ref "fact_asset_damage_accumulation" }}
),

-- Get asset attributes for denormalization
asset_info AS (
    SELECT 
        asset_key,
        tag_id,
        equipment_name,
        unit_name
    FROM dim_asset
),

-- Calculate weighted excursion score for health index
-- Formula: (critical × 3) + (standard × 2) + (informational × 1)
weighted_scores AS (
    SELECT 
        asset_key,
        cumulative_damage_to_date,
        damage_last_30_days,
        damage_last_90_days,
        excursion_count_total,
        critical_excursion_count,
        standard_excursion_count,
        informational_excursion_count,
        days_since_last_critical_excursion,
        last_excursion_date,
        asset_install_date,
        design_life_years,
        (critical_excursion_count * 3.0 + 
         standard_excursion_count * 2.0 + 
         informational_excursion_count * 1.0) AS weighted_excursion_score
    FROM damage_data
),

-- Calculate health trend comparing last 30 days vs previous 30 days
-- Get damage for period 30-60 days ago using current date
current_date_calc AS (
    SELECT MAX(full_date) AS as_of_date
    FROM dim_date
),

excursions_with_dates AS (
    SELECT 
        e.asset_key,
        d.full_date AS excursion_date,
        e.cumulative_damage_index
    FROM {{ ref "fact_excursion_events" }} AS e
    INNER JOIN dim_date AS d
        ON e.date_key = d.date_key
),

-- Damage 30-60 days ago
damage_prev_30_days AS (
    SELECT 
        e.asset_key,
        SUM(e.cumulative_damage_index) AS damage_prev_30_days
    FROM excursions_with_dates AS e
    CROSS JOIN current_date_calc AS c
    WHERE julianday(c.as_of_date) - julianday(e.excursion_date) > 30
      AND julianday(c.as_of_date) - julianday(e.excursion_date) <= 60
    GROUP BY e.asset_key
),

-- Calculate years in service
years_in_service_calc AS (
    SELECT 
        ws.asset_key,
        ws.cumulative_damage_to_date,
        ws.damage_last_30_days,
        ws.damage_last_90_days,
        ws.excursion_count_total,
        ws.critical_excursion_count,
        ws.standard_excursion_count,
        ws.informational_excursion_count,
        ws.days_since_last_critical_excursion,
        ws.last_excursion_date,
        ws.asset_install_date,
        ws.design_life_years,
        ws.weighted_excursion_score,
        COALESCE(dp.damage_prev_30_days, 0.0) AS damage_prev_30_days,
        ROUND(
            (julianday((SELECT as_of_date FROM current_date_calc)) - 
             julianday(ws.asset_install_date)) / 365.25, 
            1
        ) AS years_in_service
    FROM weighted_scores AS ws
    LEFT JOIN damage_prev_30_days AS dp
        ON ws.asset_key = dp.asset_key
),

-- Calculate health index and trend
health_metrics AS (
    SELECT 
        asset_key,
        cumulative_damage_to_date,
        damage_last_30_days,
        damage_last_90_days,
        excursion_count_total,
        critical_excursion_count,
        standard_excursion_count,
        informational_excursion_count,
        days_since_last_critical_excursion,
        last_excursion_date,
        asset_install_date,
        design_life_years,
        years_in_service,
        weighted_excursion_score,
        damage_prev_30_days,
        -- Health Index: 100 - (weighted_score / theoretical_max * 100)
        -- Theoretical max = 30 (represents very unhealthy asset: 10 critical events)
        ROUND(
            100.0 - LEAST((weighted_excursion_score / 30.0) * 100.0, 100.0),
            2
        ) AS integrity_health_index,
        -- Health Trend: percent change in damage (last 30 days vs previous 30 days)
        CASE 
            WHEN damage_prev_30_days = 0 AND damage_last_30_days = 0 THEN 0.0
            WHEN damage_prev_30_days = 0 AND damage_last_30_days > 0 THEN 100.0
            ELSE ROUND(
                ((damage_last_30_days - damage_prev_30_days) / damage_prev_30_days) * 100.0,
                2
            )
        END AS health_trend_30d
    FROM years_in_service_calc
)

-- Final selection with health status classification
SELECT 
    hm.asset_key,
    ai.tag_id,
    ai.equipment_name,
    ai.unit_name,
    hm.integrity_health_index,
    CASE 
        WHEN hm.integrity_health_index >= 90 THEN 'Excellent'
        WHEN hm.integrity_health_index >= 70 THEN 'Good'
        WHEN hm.integrity_health_index >= 50 THEN 'Fair'
        WHEN hm.integrity_health_index >= 30 THEN 'Poor'
        ELSE 'Critical'
    END AS health_status,
    hm.health_trend_30d,
    CASE 
        WHEN hm.health_trend_30d < -10 THEN 'Improving'
        WHEN hm.health_trend_30d > 10 THEN 'Degrading'
        ELSE 'Stable'
    END AS trend_direction,
    hm.excursion_count_total AS total_excursion_count,
    hm.critical_excursion_count,
    hm.cumulative_damage_to_date AS cumulative_damage_total,
    hm.damage_last_90_days,
    hm.days_since_last_critical_excursion AS days_since_last_critical,
    hm.last_excursion_date,
    hm.asset_install_date,
    hm.design_life_years,
    hm.years_in_service
FROM health_metrics AS hm
INNER JOIN asset_info AS ai
    ON hm.asset_key = ai.asset_key
ORDER BY hm.integrity_health_index ASC, hm.critical_excursion_count DESC
