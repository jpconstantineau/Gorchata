-- ===================================================================
-- Parameter Trending Analysis
-- Purpose: Detect parameter drift and predict approaching IOW limits
-- Business Value: Early warning for parameters approaching IOW limits
-- ===================================================================

WITH sensor_data AS (
    SELECT
        s.tag_id,
        s.parameter_type,
        s.reading_timestamp,
        s.measured_value,
        a.equipment_name,
        a.unit_name,
        l.critical_upper AS iow_critical_upper,
        l.critical_lower AS iow_critical_lower
    FROM {{ ref "stg_sensor_readings" }} s
    INNER JOIN {{ ref "dim_asset" }} a
        ON s.asset_key = a.asset_key
    INNER JOIN {{ ref "dim_iow_limit" }} l
        ON s.asset_key = l.asset_key
        AND s.parameter_type = l.parameter_type
    WHERE s.reading_timestamp >= date('now', '-30 days')  -- Last 30 days only
),

baseline_values AS (
    -- Calculate baseline from first 7 days for drift detection
    SELECT
        tag_id,
        parameter_type,
        AVG(measured_value) AS baseline_avg
    FROM sensor_data
    WHERE reading_timestamp <= date('now', '-23 days')
    GROUP BY tag_id, parameter_type
),

trending_calculations AS (
    SELECT
        s.tag_id,
        s.equipment_name,
        s.unit_name,
        s.parameter_type,
        s.reading_timestamp,
        s.measured_value,
        s.iow_critical_upper,
        s.iow_critical_lower,
        
        -- 30-day moving average
        AVG(s.measured_value) OVER (
            PARTITION BY s.tag_id, s.parameter_type
            ORDER BY s.reading_timestamp
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS moving_avg_30d,
        
        -- Standard deviation for control limits (3-sigma)
        COALESCE(
            STDDEV(s.measured_value) OVER (
                PARTITION BY s.tag_id, s.parameter_type
                ORDER BY s.reading_timestamp
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ), 
            0
        ) AS stddev_30d,
        
        b.baseline_avg
        
    FROM sensor_data s
    LEFT JOIN baseline_values b
        ON s.tag_id = b.tag_id
        AND s.parameter_type = b.parameter_type
),

control_limits AS (
    SELECT
        tag_id,
        equipment_name,
        unit_name,
        parameter_type,
        reading_timestamp,
        measured_value,
        moving_avg_30d,
        stddev_30d,
        baseline_avg,
        iow_critical_upper,
        iow_critical_lower,
        
        -- 3-sigma control limits (statistical process control)
        moving_avg_30d + (3.0 * stddev_30d) AS upper_control_limit,
        moving_avg_30d - (3.0 * stddev_30d) AS lower_control_limit,
        
        -- Calculate drift from baseline (positive = increasing, negative = decreasing)
        CASE
            WHEN baseline_avg IS NOT NULL AND baseline_avg != 0
            THEN ((moving_avg_30d - baseline_avg) / ABS(baseline_avg)) * 100.0
            ELSE 0.0
        END AS drift_from_baseline,
        
        -- Calculate how close to IOW critical limits (percentage)
        CASE
            WHEN iow_critical_upper IS NOT NULL AND measured_value >= moving_avg_30d
            THEN ((iow_critical_upper - measured_value) / (iow_critical_upper - moving_avg_30d + 0.01)) * 100.0
            WHEN iow_critical_lower IS NOT NULL AND measured_value < moving_avg_30d
            THEN ((measured_value - iow_critical_lower) / (moving_avg_30d - iow_critical_lower + 0.01)) * 100.0
            ELSE 100.0
        END AS pct_to_critical_limit
        
    FROM trending_calculations
)

SELECT
    tag_id,
    equipment_name,
    unit_name,
    parameter_type,
    reading_timestamp,
    measured_value,
    moving_avg_30d,
    stddev_30d,
    upper_control_limit,
    lower_control_limit,
    iow_critical_upper,
    iow_critical_lower,
    pct_to_critical_limit,
    drift_from_baseline,
    
    -- Flag trending status
    CASE
        WHEN pct_to_critical_limit < 10 THEN 'Approaching_Limit'
        WHEN measured_value > upper_control_limit OR measured_value < lower_control_limit THEN 'Out_of_Control'
        ELSE 'Normal'
    END AS trend_flag
    
FROM control_limits
WHERE 
    -- Focus on readings from last 7 days for actionable insights
    reading_timestamp >= date('now', '-7 days')
    -- Filter to concerning trends only
    AND (
        pct_to_critical_limit < 20  -- Within 20% of IOW limit
        OR measured_value > upper_control_limit 
        OR measured_value < lower_control_limit
        OR ABS(drift_from_baseline) > 10  -- Drifted >10% from baseline
    )
ORDER BY 
    pct_to_critical_limit ASC,  -- Closest to limits first
    ABS(drift_from_baseline) DESC
LIMIT 100;  -- Top 100 concerning trends
