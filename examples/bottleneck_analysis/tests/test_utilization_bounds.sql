-- Test: Utilization Percentage Bounds
-- Purpose: Ensures utilization metrics are between 0 and 100 percent
--
-- This generic test validates that utilization percentages fall within
-- the valid range of 0% to 100%. Utilization above 100% indicates data
-- quality issues (e.g., overlapping operations, incorrect capacity calculations).
-- Negative utilization indicates calculation errors.
--
-- Checks both:
-- - utilization_pct (raw utilization)
-- - adjusted_utilization_pct (utilization adjusted for downtime)

SELECT
    resource_id,
    resource_name,
    date_key,
    utilization_pct,
    'Utilization exceeds 100%' AS issue
FROM {{ ref('int_resource_daily_utilization') }}
WHERE utilization_pct > 100

UNION ALL

SELECT
    resource_id,
    resource_name,
    date_key,
    utilization_pct,
    'Utilization is negative' AS issue
FROM {{ ref('int_resource_daily_utilization') }}
WHERE utilization_pct < 0

UNION ALL

SELECT
    resource_id,
    resource_name,
    date_key,
    adjusted_utilization_pct,
    'Adjusted utilization exceeds 100%' AS issue
FROM {{ ref('int_resource_daily_utilization') }}
WHERE adjusted_utilization_pct > 100

UNION ALL

SELECT
    resource_id,
    resource_name,
    date_key,
    adjusted_utilization_pct,
    'Adjusted utilization is negative' AS issue
FROM {{ ref('int_resource_daily_utilization') }}
WHERE adjusted_utilization_pct < 0

-- This test should return 0 rows if all utilization values are within bounds
