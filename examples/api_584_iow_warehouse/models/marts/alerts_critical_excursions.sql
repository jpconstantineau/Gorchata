-- Alert Model: Critical IOW Excursions
-- Purpose: Generate alerts for all critical-level IOW limit breaches requiring immediate operator response
-- Grain: One row per critical excursion event that needs operator acknowledgment
-- Alert Logic: All excursions at Critical criticality level

WITH critical_excursions AS (
    SELECT 
        fee.excursion_event_id,
        fee.asset_key,
        fee.excursion_start_timestamp,
        fee.cumulative_damage_index,
        fee.peak_magnitude,
        fee.average_magnitude,
        fee.duration_minutes,
        fee.parameter_type_key,
        fee.limit_key,
        fee.criticality_key
    FROM {{ ref "fact_excursion_events" }} AS fee
    WHERE fee.criticality_key IN (
        SELECT criticality_key 
        FROM dim_criticality_level 
        WHERE criticality_level = 'Critical'
    )
),

-- Join to dimension tables for denormalized alert fields
excursions_with_dimensions AS (
    SELECT 
        ce.excursion_event_id,
        ce.asset_key,
        da.tag_id,
        da.equipment_name AS asset_name,
        da.unit_name,
        ce.excursion_start_timestamp,
        dpt.parameter_type,
        dil.upper_limit AS limit_value,
        ce.peak_magnitude AS measured_peak_value,
        ce.average_magnitude AS measured_avg_value,
        ce.peak_magnitude - COALESCE(dil.upper_limit, 0) AS excursion_magnitude,
        ce.duration_minutes,
        ce.cumulative_damage_index,
        dcl.criticality_level
    FROM critical_excursions AS ce
    INNER JOIN dim_asset AS da
        ON ce.asset_key = da.asset_key
    INNER JOIN dim_parameter_type AS dpt
        ON ce.parameter_type_key = dpt.parameter_type_key
    INNER JOIN dim_iow_limit AS dil
        ON ce.limit_key = dil.limit_key
    INNER JOIN dim_criticality_level AS dcl
        ON ce.criticality_key = dcl.criticality_key
),

-- Generate alert records with formatted messages
alert_records AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY excursion_start_timestamp DESC, asset_key) AS alert_id,
        excursion_start_timestamp AS alert_timestamp,
        asset_key,
        tag_id,
        asset_name,
        unit_name,
        'Critical_IOW_Excursion' AS alert_type,
        'Critical' AS priority,
        parameter_type,
        limit_value,
        measured_peak_value,
        measured_avg_value,
        excursion_magnitude,
        duration_minutes,
        cumulative_damage_index,
        'CRITICAL IOW EXCURSION: ' || 
            asset_name || ' (' || tag_id || ') ' ||
            parameter_type || ' exceeded limit of ' || 
            CAST(ROUND(limit_value, 2) AS TEXT) || ' by ' || 
            CAST(ROUND(excursion_magnitude, 2) AS TEXT) || 
            ' for ' || CAST(ROUND(duration_minutes, 1) AS TEXT) || ' minutes' AS message,
        0 AS acknowledged_flag,
        excursion_event_id
    FROM excursions_with_dimensions
)

SELECT 
    alert_id,
    alert_timestamp,
    asset_key,
    tag_id,
    asset_name,
    unit_name,
    alert_type,
    priority,
    parameter_type,
    limit_value,
    measured_peak_value,
    measured_avg_value,
    excursion_magnitude,
    duration_minutes,
    cumulative_damage_index,
    message,
    acknowledged_flag,
    excursion_event_id
FROM alert_records
ORDER BY alert_timestamp DESC, alert_id
