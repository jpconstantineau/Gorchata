{{ config "materialized" "table" }}

-- =============================================================================
-- Fact Table: fct_operation
-- =============================================================================
-- Purpose: Captures granular operation events with calculated metrics for
--          bottleneck analysis including queue time and cycle time.
--
-- Grain: One row per operation per work order
--
-- Foreign Keys:
--   - resource_key → dim_resource
--   - work_order_key → dim_work_order
--   - part_key → dim_part
--   - start_date_key → dim_date (operation start)
--   - end_date_key → dim_date (operation end)
--
-- Measures:
--   - quantity_completed: Units completed in this operation
--   - quantity_scrapped: Units scrapped in this operation
--   - cycle_time_minutes: Time from operation start to end (processing time)
--   - queue_time_minutes: Time waiting before operation start (arrival to start)
--
-- Queue Time Calculation:
--   - For first operation: start_timestamp - work_order.release_timestamp
--   - For subsequent operations: start_timestamp - previous_operation.end_timestamp
--   - Uses LAG() window function partitioned by work_order_id
-- =============================================================================

WITH operations_source AS (
    SELECT * FROM {{ seed "raw_operations" }}
),

work_orders_source AS (
    SELECT * FROM {{ seed "raw_work_orders" }}
),

-- Calculate arrival time (end of previous operation or work order release)
operations_with_arrival AS (
    SELECT
        o.*,
        LAG(o.end_timestamp) OVER (
            PARTITION BY o.work_order_id 
            ORDER BY o.operation_seq
        ) AS previous_operation_end,
        wo.release_timestamp AS work_order_release
    FROM operations_source o
    JOIN work_orders_source wo ON o.work_order_id = wo.work_order_id
),

-- Calculate queue time: time between arrival at resource and start of processing
operations_with_queue AS (
    SELECT
        *,
        COALESCE(previous_operation_end, work_order_release) AS arrival_timestamp,
        CAST(
            (JULIANDAY(start_timestamp) - JULIANDAY(COALESCE(previous_operation_end, work_order_release))) * 24 * 60
        AS INTEGER) AS queue_time_minutes
    FROM operations_with_arrival
),

-- Join to dimensions and calculate remaining metrics
fact_with_keys AS (
    SELECT
        -- Natural keys (grain)
        o.operation_id,
        o.work_order_id,
        o.resource_id,
        o.operation_seq,
        
        -- Attributes
        o.operation_type,
        o.start_timestamp,
        o.end_timestamp,
        
        -- Base measures from source
        o.quantity_completed,
        o.quantity_scrapped,
        
        -- Calculated measures
        CAST(
            (JULIANDAY(o.end_timestamp) - JULIANDAY(o.start_timestamp)) * 24 * 60
        AS INTEGER) AS cycle_time_minutes,
        o.queue_time_minutes,
        
        -- Foreign keys to dimensions
        r.resource_key,
        wo.work_order_key,
        p.part_key,
        d_start.date_key AS start_date_key,
        d_end.date_key AS end_date_key
        
    FROM operations_with_queue o
    LEFT JOIN {{ ref "dim_resource" }} r ON o.resource_id = r.resource_id
    LEFT JOIN {{ ref "dim_work_order" }} wo ON o.work_order_id = wo.work_order_id
    -- Join to part via work order's part_number
    LEFT JOIN work_orders_source wo_for_part ON o.work_order_id = wo_for_part.work_order_id
    LEFT JOIN {{ ref "dim_part" }} p ON wo_for_part.part_number = p.part_number
    LEFT JOIN {{ ref "dim_date" }} d_start ON DATE(o.start_timestamp) = d_start.full_date
    LEFT JOIN {{ ref "dim_date" }} d_end ON DATE(o.end_timestamp) = d_end.full_date
)

SELECT 
    operation_id,
    work_order_id,
    resource_id,
    operation_seq,
    operation_type,
    start_timestamp,
    end_timestamp,
    quantity_completed,
    quantity_scrapped,
    cycle_time_minutes,
    queue_time_minutes,
    resource_key,
    work_order_key,
    part_key,
    start_date_key,
    end_date_key
FROM fact_with_keys
ORDER BY work_order_id, operation_seq
