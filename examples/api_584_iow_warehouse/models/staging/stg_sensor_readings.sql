-- Staging model for raw sensor telemetry data
-- Reads from raw_sensor_readings seed and enriches with calculated fields
-- Filters out 'Bad' quality readings for downstream analysis

SELECT
    reading_id,
    timestamp,
    tag_id,
    parameter_type,
    measured_value,
    data_quality_flag,
    
    -- Calculated fields for analysis
    CAST(strftime('%Y%m%d', timestamp) AS INTEGER) AS reading_date_key,
    CAST(strftime('%H', timestamp) AS INTEGER) AS hour_of_day,
    
    -- Flag for potential IOW excursions (simplified check - will be refined in Phase 3)
    CASE 
        WHEN parameter_type = 'Pressure' AND (measured_value < 50 OR measured_value > 750) THEN 1
        WHEN parameter_type = 'Temperature' AND (measured_value < 300 OR measured_value > 950) THEN 1
        WHEN parameter_type = 'pH' AND (measured_value < 5.0 OR measured_value > 9.0) THEN 1
        WHEN parameter_type = 'Flow' AND (measured_value < 5000 OR measured_value > 85000) THEN 1
        ELSE 0
    END AS is_excursion_candidate

FROM {{ ref('raw_sensor_readings') }}

-- Join to validate tag_id exists in dim_asset
INNER JOIN {{ ref('dim_asset') }} AS asset
    ON raw_sensor_readings.tag_id = asset.tag_id

-- Join to validate parameter_type exists in dim_parameter_type
INNER JOIN {{ ref('dim_parameter_type') }} AS param
    ON raw_sensor_readings.parameter_type = param.parameter_type

WHERE 
    -- Filter out bad quality readings
    data_quality_flag != 'Bad'
    
    -- Data validation: ensure values are within physical limits
    AND measured_value IS NOT NULL
    AND timestamp IS NOT NULL
    AND tag_id IS NOT NULL
    AND parameter_type IS NOT NULL
    
    -- Specific range validations per parameter type
    AND (
        (parameter_type = 'Pressure' AND measured_value >= 0 AND measured_value <= 3000)
        OR (parameter_type = 'Temperature' AND measured_value >= 32 AND measured_value <= 1400)
        OR (parameter_type = 'pH' AND measured_value >= 0 AND measured_value <= 14)
        OR (parameter_type = 'Flow' AND measured_value >= 0 AND measured_value <= 50000)
    )
