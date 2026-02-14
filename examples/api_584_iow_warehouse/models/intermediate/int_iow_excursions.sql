-- Intermediate model: IOW Excursion Detection
-- Purpose: Identify individual sensor readings that breach IOW limits at any criticality level
-- Logic: Cross-join sensor readings with IOW limits, detect breaches, calculate magnitude
-- Output: One record per reading that exceeds ANY limit (most restrictive criticality assigned)

WITH sensor_readings AS (
    SELECT 
        reading_id,
        timestamp,
        tag_id,
        parameter_type,
        measured_value
    FROM {{ ref "stg_sensor_readings" }}
),

asset_info AS (
    SELECT 
        asset_key,
        tag_id
    FROM dim_asset
),

iow_limits AS (
    SELECT 
        limit_key,
        parameter_type,
        criticality_level,
        lower_limit,
        upper_limit
    FROM dim_iow_limit
),

-- Detect all limit breaches
breaches AS (
    SELECT 
        sr.reading_id,
        sr.timestamp,
        sr.tag_id,
        ai.asset_key,
        sr.parameter_type,
        sr.measured_value,
        lim.limit_key,
        lim.criticality_level,
        lim.lower_limit,
        lim.upper_limit,
        
        -- Determine breach type and breached limit
        CASE
            WHEN lim.lower_limit IS NOT NULL AND sr.measured_value < lim.lower_limit THEN 'Low'
            WHEN lim.upper_limit IS NOT NULL AND sr.measured_value > lim.upper_limit THEN 'High'
            ELSE NULL
        END AS breach_type,
        
        CASE
            WHEN lim.lower_limit IS NOT NULL AND sr.measured_value < lim.lower_limit THEN lim.lower_limit
            WHEN lim.upper_limit IS NOT NULL AND sr.measured_value > lim.upper_limit THEN lim.upper_limit
            ELSE NULL
        END AS breached_limit_value,
        
        -- Calculate excursion magnitude
        CASE
            WHEN lim.lower_limit IS NOT NULL AND sr.measured_value < lim.lower_limit 
                THEN ABS(sr.measured_value - lim.lower_limit)
            WHEN lim.upper_limit IS NOT NULL AND sr.measured_value > lim.upper_limit 
                THEN ABS(sr.measured_value - lim.upper_limit)
            ELSE NULL
        END AS excursion_magnitude,
        
        -- Criticality ranking for selecting most restrictive
        CASE lim.criticality_level
            WHEN 'Critical' THEN 1
            WHEN 'Standard' THEN 2
            WHEN 'Informational' THEN 3
            ELSE 4
        END AS criticality_rank
        
    FROM sensor_readings sr
    INNER JOIN asset_info ai ON sr.tag_id = ai.tag_id
    CROSS JOIN iow_limits lim
    WHERE 
        sr.parameter_type = lim.parameter_type
        AND (
            (lim.lower_limit IS NOT NULL AND sr.measured_value < lim.lower_limit)
            OR (lim.upper_limit IS NOT NULL AND sr.measured_value > lim.upper_limit)
        )
),

-- Select most restrictive criticality level per reading
most_restrictive AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY reading_id 
            ORDER BY criticality_rank ASC, excursion_magnitude DESC
        ) AS rn
    FROM breaches
)

-- Final output: one record per excursion
SELECT 
    ROW_NUMBER() OVER (ORDER BY timestamp, reading_id) AS excursion_id,
    reading_id,
    timestamp,
    tag_id,
    asset_key,
    parameter_type,
    measured_value,
    breached_limit_value,
    breach_type,
    excursion_magnitude,
    criticality_level,
    limit_key
FROM most_restrictive
WHERE rn = 1
ORDER BY timestamp, tag_id
