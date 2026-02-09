-- Test: Operation Lifecycle Validation
-- Purpose: Validates operation sequences, temporal logic, and quantity constraints
--
-- This test ensures:
-- 1. Operations follow sequential numbering per work order
-- 2. End timestamps are after start timestamps
-- 3. Quantity completed does not exceed work order quantity
-- 4. Cycle times are positive values

-- Test 1: Operation sequences should be sequential per work order
WITH sequence_gaps AS (
    SELECT
        work_order_id,
        operation_seq,
        LAG(operation_seq) OVER (PARTITION BY work_order_id ORDER BY operation_seq) AS prev_seq
    FROM {{ ref('fct_operation') }}
)
SELECT 
    work_order_id,
    operation_seq,
    prev_seq,
    'Non-sequential operation sequence' AS issue
FROM sequence_gaps
WHERE prev_seq IS NOT NULL 
  AND operation_seq <> prev_seq + 10  -- Sequences should increment by 10

UNION ALL

-- Test 2: End timestamp must be after start timestamp
SELECT
    work_order_id,
    operation_seq,
    NULL AS prev_seq,
    'End timestamp before start timestamp' AS issue
FROM {{ ref('fct_operation') }}
WHERE end_timestamp <= start_timestamp

UNION ALL

-- Test 3: Quantity completed should not exceed work order quantity
SELECT
    f.work_order_id,
    f.operation_seq,
    NULL AS prev_seq,
    'Quantity completed exceeds order quantity' AS issue
FROM {{ ref('fct_operation') }} f
JOIN {{ ref('dim_work_order') }} w ON f.work_order_key = w.work_order_key
WHERE f.quantity_completed > w.quantity

UNION ALL

-- Test 4: Cycle time should be positive
SELECT
    work_order_id,
    operation_seq,
    NULL AS prev_seq,
    'Cycle time is not positive' AS issue
FROM {{ ref('fct_operation') }}
WHERE cycle_time_minutes <= 0

-- This test should return 0 rows if all operations follow valid lifecycle rules
