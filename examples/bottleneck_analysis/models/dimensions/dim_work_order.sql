{{ config "materialized" "table" }}

-- Work Order Dimension
-- Tracks work orders with release/due dates and priority attributes

WITH source_data AS (
    SELECT
        work_order_id,
        part_number,
        quantity,
        release_timestamp,
        due_timestamp,
        priority,
        status
    FROM {{ seed "raw_work_orders" }}
),

transformed AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY work_order_id) AS work_order_key,
        work_order_id,
        part_number,
        quantity,
        release_timestamp,
        due_timestamp,
        priority,
        status,
        -- Calculated fields
        DATE(release_timestamp) AS release_date,
        DATE(due_timestamp) AS due_date,
        CAST(JULIANDAY(due_timestamp) - JULIANDAY(release_timestamp) AS INTEGER) AS lead_time_days,
        -- Classification
        CASE 
            WHEN priority = 'HIGH' THEN 1
            WHEN priority = 'MEDIUM' THEN 2
            ELSE 3
        END AS priority_rank
    FROM source_data
)

SELECT * FROM transformed
ORDER BY work_order_key
