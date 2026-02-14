-- Fact Table: Excursion Events with Damage Metrics
-- Purpose: Core fact table with one row per IOW excursion event
-- Grain: One row per excursion_event_id
-- Damage Calculation: Area Under Curve (AUC) = average_magnitude × duration_minutes

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
        limit_key,
        severity_score,
        severity_category
    FROM {{ ref "int_excursion_severity" }}
),

-- Join to dimensions to get foreign keys
events_with_dimensions AS (
    SELECT 
        e.excursion_event_id,
        e.tag_id,
        e.asset_key,
        CAST(strftime('%Y%m%d', e.excursion_start_time) AS INTEGER) AS date_key,
        pt.parameter_type_key,
        il.limit_key,
        cl.criticality_key,
        e.excursion_start_time,
        e.excursion_end_time,
        e.duration_minutes,
        e.reading_count,
        e.peak_magnitude,
        e.average_magnitude,
        e.breach_type,
        e.parameter_type,
        e.severity_score,
        e.severity_category
    FROM excursion_events AS e
    INNER JOIN dim_parameter_type AS pt
        ON e.parameter_type = pt.parameter_type
    INNER JOIN dim_iow_limit AS il
        ON e.limit_key = il.limit_key
    INNER JOIN dim_criticality_level AS cl
        ON e.criticality_level = cl.criticality_level
),

-- Calculate cumulative damage index using Area Under Curve methodology
damage_calculation AS (
    SELECT 
        excursion_event_id,
        tag_id,
        asset_key,
        date_key,
        parameter_type_key,
        limit_key,
        criticality_key,
        excursion_start_time,
        excursion_end_time,
        duration_minutes,
        reading_count,
        peak_magnitude,
        average_magnitude,
        breach_type,
        parameter_type,
        severity_score,
        severity_category,
        -- Area Under Curve: average_magnitude × duration_minutes
        ROUND(average_magnitude * duration_minutes, 2) AS cumulative_damage_index
    FROM events_with_dimensions
),

-- Add excursion type classification
final_output AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY excursion_event_id) AS excursion_event_key,
        excursion_event_id,
        asset_key,
        date_key,
        parameter_type_key,
        limit_key,
        criticality_key,
        excursion_start_time AS excursion_start_timestamp,
        excursion_end_time AS excursion_end_timestamp,
        duration_minutes,
        reading_count,
        peak_magnitude,
        average_magnitude,
        cumulative_damage_index,
        severity_score,
        severity_category,
        breach_type,
        -- Event type classification
        CASE 
            WHEN parameter_type = 'Pressure' AND breach_type = 'High' THEN 'Overpressure'
            WHEN parameter_type = 'Pressure' AND breach_type = 'Low' THEN 'Underpressure'
            WHEN parameter_type = 'Temperature' AND breach_type = 'High' THEN 'Overtemp'
            WHEN parameter_type = 'Temperature' AND breach_type = 'Low' THEN 'Undertemp'
            WHEN parameter_type = 'pH' AND breach_type = 'High' THEN 'pH_High'
            WHEN parameter_type = 'pH' AND breach_type = 'Low' THEN 'pH_Low'
            WHEN parameter_type = 'Flow' AND breach_type = 'High' THEN 'Flow_High'
            WHEN parameter_type = 'Flow' AND breach_type = 'Low' THEN 'Flow_Low'
            ELSE 'Unknown'
        END AS excursion_type
    FROM damage_calculation
)

SELECT * FROM final_output
ORDER BY excursion_event_key
