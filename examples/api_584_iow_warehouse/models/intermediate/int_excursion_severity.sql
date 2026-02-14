-- Intermediate model: Excursion Severity Scoring
-- Purpose: Calculate severity scores for prioritization and categorization
-- Logic: Score = (peak_magnitude × 0.4) + (duration × 0.3) + (criticality_weight × 0.3)
-- Output: All event columns plus severity_score and severity_category

WITH excursion_events AS (
    SELECT 
        excursion_event_id,
        tag_id,
        asset_key,
        parameter_type,
        criticality_level,
        excursion_start_time,
        excursion_end_time,
        duration_minutes,
        reading_count,
        peak_magnitude,
        average_magnitude,
        breach_type,
        limit_key
    FROM {{ ref "int_excursion_windows" }}
),

-- Calculate severity components
severity_calc AS (
    SELECT 
        excursion_event_id,
        tag_id,
        asset_key,
        parameter_type,
        criticality_level,
        excursion_start_time,
        excursion_end_time,
        duration_minutes,
        reading_count,
        peak_magnitude,
        average_magnitude,
        breach_type,
        limit_key,
        
        CASE criticality_level
            WHEN 'Critical' THEN 3.0
            WHEN 'Standard' THEN 2.0
            WHEN 'Informational' THEN 1.0
            ELSE 0.0
        END AS criticality_weight,
        
        CASE 
            WHEN duration_minutes > 480 THEN 10.0
            ELSE duration_minutes * 1.0 / 48.0
        END AS duration_score,
        
        CASE 
            WHEN peak_magnitude > 100 THEN 10.0
            ELSE peak_magnitude * 1.0 / 10.0
        END AS magnitude_score
        
    FROM excursion_events
),

severity_scored AS (
    SELECT 
        excursion_event_id,
        tag_id,
        asset_key,
        parameter_type,
        criticality_level,
        excursion_start_time,
        excursion_end_time,
        duration_minutes,
        reading_count,
        peak_magnitude,
        average_magnitude,
        breach_type,
        limit_key,
        ROUND(magnitude_score * 0.4 + duration_score * 0.3 + criticality_weight * 0.3, 2) AS severity_score
    FROM severity_calc
)

-- Final output with severity category
SELECT 
    excursion_event_id,
    tag_id,
    asset_key,
    parameter_type,
    criticality_level,
    excursion_start_time,
    excursion_end_time,
    duration_minutes,
    reading_count,
    peak_magnitude,
    average_magnitude,
    breach_type,
    limit_key,
    severity_score,
    CASE
        WHEN severity_score > 8.0 THEN 'Extreme'
        WHEN severity_score > 5.0 THEN 'High'
        WHEN severity_score > 2.0 THEN 'Moderate'
        ELSE 'Low'
    END AS severity_category
FROM severity_scored
ORDER BY severity_score DESC, excursion_start_time
