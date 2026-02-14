-- Metrics: Bad Actors (Worst Performing Assets)
-- Purpose: Identify bottom 10% of assets by composite score for prioritized attention
-- Grain: One row per bad actor asset (approx 10 assets out of 100)
-- Scoring: Higher bad_actor_score = worse performance
  
WITH health_data AS (
    SELECT 
        asset_key,
        tag_id,
        equipment_name,
        unit_name,
        integrity_health_index,
        health_trend_30d,
        total_excursion_count,
        critical_excursion_count,
        cumulative_damage_total,
        damage_last_90_days,
        days_since_last_critical
    FROM {{ ref "metrics_asset_integrity_index" }}
),

-- Calculate normalized metrics for bad actor scoring
-- Normalization: scale each metric to 0-100 range
normalized_metrics AS (
    SELECT 
        asset_key,
        tag_id,
        equipment_name,
        unit_name,
        integrity_health_index,
        health_trend_30d,
        total_excursion_count,
        critical_excursion_count,
        cumulative_damage_total,
        damage_last_90_days,
        days_since_last_critical,
        -- Normalize excursion frequency (scale by max)
        CASE 
            WHEN MAX(total_excursion_count) OVER () = 0 THEN 0.0
            ELSE (total_excursion_count * 100.0) / MAX(total_excursion_count) OVER ()
        END AS norm_excursion_freq,
        -- Normalize damage accumulation (scale by max)
        CASE 
            WHEN MAX(cumulative_damage_total) OVER () = 0 THEN 0.0
            ELSE (cumulative_damage_total * 100.0) / MAX(cumulative_damage_total) OVER ()
        END AS norm_damage,
        -- Normalize critical event count (scale by max)
        CASE 
            WHEN MAX(critical_excursion_count) OVER () = 0 THEN 0.0
            ELSE (critical_excursion_count * 100.0) / MAX(critical_excursion_count) OVER ()
        END AS norm_critical_count,
        -- Invert health index (lower health = higher bad actor component)
        100.0 - integrity_health_index AS inv_health_index
    FROM health_data
),

-- Calculate composite bad actor score
-- Formula: weighted average of normalized metrics
bad_actor_scores AS (
    SELECT 
        asset_key,
        tag_id,
        equipment_name,
        unit_name,
        integrity_health_index,
        health_trend_30d,
        total_excursion_count,
        critical_excursion_count,
        cumulative_damage_total,
        damage_last_90_days,
        days_since_last_critical,
        norm_excursion_freq,
        norm_damage,
        norm_critical_count,
        inv_health_index,
        -- Composite score: weighted average
        -- Weights: critical_events (30%), damage (25%), excursion_freq (20%), health (25%)
        ROUND(
            (norm_critical_count * 0.30 + 
             norm_damage * 0.25 + 
             norm_excursion_freq * 0.20 + 
             inv_health_index * 0.25),
            2
        ) AS bad_actor_score
    FROM normalized_metrics
),

-- Rank assets and calculate percentile
ranked_assets AS (
    SELECT 
        asset_key,
        tag_id,
        equipment_name,
        unit_name,
        integrity_health_index,
        health_trend_30d,
        total_excursion_count,
        critical_excursion_count,
        cumulative_damage_total,
        damage_last_90_days,
        days_since_last_critical,
        bad_actor_score,
        norm_excursion_freq,
        norm_damage,
        norm_critical_count,
        inv_health_index,
        ROW_NUMBER() OVER (ORDER BY bad_actor_score DESC) AS bad_actor_rank,
        ROUND(
            PERCENT_RANK() OVER (ORDER BY bad_actor_score DESC) * 100.0,
            2
        ) AS percentile_rank
    FROM bad_actor_scores
),

-- Filter to bottom 10% (worst performers)
bad_actors_filtered AS (
    SELECT 
        bad_actor_rank,
        asset_key,
        tag_id,
        equipment_name,
        unit_name,
        bad_actor_score,
        integrity_health_index,
        health_trend_30d,
        total_excursion_count,
        critical_excursion_count,
        cumulative_damage_total,
        damage_last_90_days,
        days_since_last_critical,
        norm_excursion_freq,
        norm_damage,
        norm_critical_count,
        inv_health_index,
        percentile_rank
    FROM ranked_assets
    WHERE percentile_rank <= 10.0
)

-- Assign reason codes and recommended actions
SELECT 
    bad_actor_rank,
    asset_key,
    tag_id,
    equipment_name,
    unit_name,
    bad_actor_score,
    integrity_health_index,
    -- Primary reason code: highest contributing factor
    CASE 
        WHEN norm_critical_count >= norm_damage 
         AND norm_critical_count >= norm_excursion_freq 
         AND norm_critical_count >= inv_health_index THEN 'Repeated_Critical_Events'
        WHEN norm_damage >= norm_excursion_freq 
         AND norm_damage >= inv_health_index THEN 'Severe_Damage_Accumulation'
        WHEN norm_excursion_freq >= inv_health_index THEN 'High_Excursion_Frequency'
        ELSE 'Degrading_Health_Trend'
    END AS primary_reason_code,
    -- Secondary reason code: second highest contributing factor
    CASE 
        WHEN norm_critical_count < norm_damage 
         AND norm_damage >= norm_excursion_freq 
         AND norm_damage >= inv_health_index THEN 'Severe_Damage_Accumulation'
        WHEN norm_critical_count < norm_excursion_freq 
         AND norm_excursion_freq >= norm_damage 
         AND norm_excursion_freq >= inv_health_index THEN 'High_Excursion_Frequency'
        WHEN norm_damage < norm_critical_count 
         AND norm_critical_count >= norm_excursion_freq THEN 'Repeated_Critical_Events'
        WHEN inv_health_index > 50 THEN 'Degrading_Health_Trend'
        ELSE NULL
    END AS secondary_reason_code,
    total_excursion_count,
    critical_excursion_count,
    cumulative_damage_total,
    damage_last_90_days,
    days_since_last_critical,
    health_trend_30d,
    -- Recommended action based on severity and recency
    CASE 
        WHEN critical_excursion_count > 5 
         AND (days_since_last_critical IS NULL OR days_since_last_critical < 7) THEN 'Immediate_Inspection'
        WHEN integrity_health_index < 30 
         OR (critical_excursion_count > 3 AND days_since_last_critical < 30) THEN 'Schedule_Inspection'
        WHEN health_trend_30d > 20 THEN 'Increase_Monitoring_Frequency'
        ELSE 'Monitor_Closely'
    END AS recommended_action
FROM bad_actors_filtered
ORDER BY bad_actor_rank ASC
