{{ config(materialized='incremental', unique_key=['customer_sk']) }}

-- SCD Type 2 Customer Dimension
-- Tracks historical changes to customer attributes (city, state, email)

WITH customer_changes AS (
  -- Extract all unique combinations of customer attributes with their first appearance date
  SELECT DISTINCT
    customer_id,
    customer_name,
    customer_email,
    customer_city,
    customer_state,
    sale_date
  FROM {{ ref "raw_sales" }}
),

customer_versions AS (
  -- Group by all customer attributes to identify unique versions
  -- Each unique combination of attributes represents a version
  SELECT
    customer_id,
    customer_name,
    customer_email,
    customer_city,
    customer_state,
    MIN(sale_date) AS valid_from,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id 
      ORDER BY MIN(sale_date)
    ) AS version_num
  FROM customer_changes
  GROUP BY 
    customer_id,
    customer_name,
    customer_email,
    customer_city,
    customer_state
),

customer_with_end_dates AS (
  -- Calculate the end date for each version using LEAD window function
  -- The end date is the start date of the next version
  SELECT
    customer_id,
    customer_name,
    customer_email,
    customer_city,
    customer_state,
    valid_from,
    LEAD(valid_from) OVER (
      PARTITION BY customer_id 
      ORDER BY valid_from
    ) AS next_valid_from,
    version_num
  FROM customer_versions
)

-- Final SELECT with SCD Type 2 columns
SELECT
  -- Surrogate key: customer_id * 1000 + version_num ensures uniqueness across all versions
  customer_id * 1000 + version_num AS customer_sk,
  customer_id,
  customer_name,
  customer_email,
  customer_city,
  customer_state,
  valid_from,
  -- valid_to is the next version's valid_from, or '9999-12-31' for current version
  COALESCE(next_valid_from, '9999-12-31') AS valid_to,
  -- is_current flag: 1 for current version, 0 for historical versions
  CASE WHEN next_valid_from IS NULL THEN 1 ELSE 0 END AS is_current
FROM customer_with_end_dates
ORDER BY customer_id, valid_from
