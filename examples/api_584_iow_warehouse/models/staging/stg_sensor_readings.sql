-- Staging model for raw sensor telemetry data
-- Reads from raw_sensor_readings seed and enriches with calculated fields
-- Filters out 'Bad' quality readings for downstream analysis

SELECT
    raw.reading_id,
    raw.timestamp,
    raw.tag_id,
    raw.parameter_type,
    raw.measured_value,
    raw.data_quality_flag,
    
    -- Calculated fields for analysis
    CAST(strftime('%Y%m%d', raw.timestamp) AS INTEGER) AS reading_date_key,
    CAST(strftime('%H', raw.timestamp) AS INTEGER) AS hour_of_day,
    
    -- Flag for potential IOW excursions (simplified check - will be refined in Phase 3)
    CASE 
        WHEN raw.parameter_type = 'Pressure' AND (raw.measured_value < 50 OR raw.measured_value > 750) THEN 1
        WHEN raw.parameter_type = 'Temperature' AND (raw.measured_value < 300 OR raw.measured_value > 950) THEN 1
        WHEN raw.parameter_type = 'pH' AND (raw.measured_value < 5.0 OR raw.measured_value > 9.0) THEN 1
        WHEN raw.parameter_type = 'Flow' AND (raw.measured_value < 5000 OR raw.measured_value > 85000) THEN 1
        ELSE 0
    END AS is_excursion_candidate

FROM raw_sensor_readings AS raw

-- Join to validate tag_id exists in dim_asset
INNER JOIN dim_asset AS asset
    ON raw.tag_id = asset.tag_id

-- Join to validate parameter_type exists in dim_parameter_type
INNER JOIN dim_parameter_type AS param
    ON raw.parameter_type = param.parameter_type

WHERE 
    -- Filter out bad quality readings
    raw.data_quality_flag != 'Bad'
    
    -- Data validation: ensure values are within physical limits
    AND raw.measured_value IS NOT NULL
    AND raw.timestamp IS NOT NULL
    AND raw.tag_id IS NOT NULL
    AND raw.parameter_type IS NOT NULL
    
    -- Specific range validations per parameter type
    AND (
        (raw.parameter_type = 'Pressure' AND raw.measured_value >= 0 AND raw.measured_value <= 3000)
        OR (raw.parameter_type = 'Temperature' AND raw.measured_value >= 32 AND raw.measured_value <= 1400)
        OR (raw.parameter_type = 'pH' AND raw.measured_value >= 0 AND raw.measured_value <= 14)
        OR (raw.parameter_type = 'Flow' AND raw.measured_value >= 0 AND raw.measured_value <= 50000)
    )
