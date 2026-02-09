-- Test: Valid Timestamp Validation
-- Purpose: Ensures all timestamps are within reasonable bounds
--
-- This test verifies:
-- 1. No null timestamps in operations
-- 2. No timestamps beyond the analysis period
-- 3. No timestamps before the analysis start date
--
-- Uses project variables from gorchata_project.yml:
-- - analysis_start_date: Lower bound for valid timestamps
-- - analysis_end_date: Upper bound for valid timestamps

-- Test 1: Check for null timestamps in operations
SELECT 
    'fct_operation' AS table_name,
    operation_id AS record_id,
    'Null start_timestamp' AS issue
FROM {{ ref('fct_operation') }}
WHERE start_timestamp IS NULL

UNION ALL

SELECT 
    'fct_operation' AS table_name,
    operation_id AS record_id,
    'Null end_timestamp' AS issue
FROM {{ ref('fct_operation') }}
WHERE end_timestamp IS NULL

UNION ALL

-- Test 2: Check for timestamps in the future (beyond analysis period)
SELECT
    'fct_operation' AS table_name,
    operation_id AS record_id,
    'Timestamp beyond analysis period' AS issue
FROM {{ ref('fct_operation') }}
WHERE start_timestamp > '{{ var("analysis_end_date") }}'

UNION ALL

-- Test 3: Check for timestamps before project start
SELECT
    'fct_operation' AS table_name,
    operation_id AS record_id,
    'Timestamp before analysis period' AS issue
FROM {{ ref('fct_operation') }}
WHERE start_timestamp < '{{ var("analysis_start_date") }}'

-- This test should return 0 rows if all timestamps are valid
