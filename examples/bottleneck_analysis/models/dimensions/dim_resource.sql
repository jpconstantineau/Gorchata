{{ config "materialized" "table" }}

-- Resource Dimension
-- Tracks manufacturing resources (machines, workcenters) with capacity attributes

WITH source_data AS (
    SELECT 
        resource_id,
        resource_name,
        resource_type,
        available_hours_per_shift,
        shifts_per_day,
        theoretical_capacity_per_hour
    FROM {{ seed "raw_resources" }}
),

transformed AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY resource_id) AS resource_key,
        resource_id,
        resource_name,
        resource_type,
        available_hours_per_shift,
        shifts_per_day,
        theoretical_capacity_per_hour,
        -- Calculated fields
        (theoretical_capacity_per_hour * available_hours_per_shift * shifts_per_day) AS daily_capacity,
        CASE 
            WHEN resource_id IN ('R001', 'R002') THEN 1 
            ELSE 0 
        END AS is_bottleneck_candidate,
        -- SCD Type 2 metadata (static for this example)
        1 AS is_current,
        CURRENT_TIMESTAMP AS valid_from,
        '9999-12-31' AS valid_to
    FROM source_data
)

SELECT * FROM transformed
ORDER BY resource_key
