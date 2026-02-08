-- Test: Fact Table Integrity
-- Purpose: Verify fact table grain, referential integrity, and business rules
--          for the star schema sales data warehouse

-- 1. Check for orphaned sales (should be caught by relationships test, but double-checking)
WITH orphaned_sales AS (
  SELECT 
    f.sale_id,
    f.customer_sk,
    f.product_id, 
    f.sale_date,
    'Missing customer' AS issue_type
  FROM fct_sales f
  LEFT JOIN dim_customers c ON f.customer_sk = c.customer_sk
  WHERE c.customer_sk IS NULL
  
  UNION ALL
  
  SELECT 
    f.sale_id,
    f.customer_sk,
    f.product_id,
    f.sale_date,
    'Missing product' AS issue_type
  FROM fct_sales f
  LEFT JOIN dim_products p ON f.product_id = p.product_id
  WHERE p.product_id IS NULL
  
  UNION ALL
  
  SELECT 
    f.sale_id,
    f.customer_sk,
    f.product_id,
    f.sale_date,
    'Missing date' AS issue_type
  FROM fct_sales f
  LEFT JOIN dim_dates d ON f.sale_date = d.sale_date
  WHERE d.sale_date IS NULL
),

-- 2. Check for invalid sales amounts (negative, zero, or unreasonably high)
invalid_amounts AS (
  SELECT
    sale_id,
    customer_sk,
    product_id,
    sale_date,
    sale_amount,
    quantity,
    'Invalid sale amount: ' || CAST(sale_amount AS TEXT) AS issue_type
  FROM fct_sales
  WHERE 
    sale_amount <= 0
    OR sale_amount > 100000  -- Unreasonably high single transaction
    OR sale_amount != ROUND(sale_amount, 2)  -- More than 2 decimal places
),

-- 3. Check for invalid quantities
invalid_quantities AS (
  SELECT
    sale_id,
    customer_sk,
    product_id,
    sale_date,
    quantity,
    'Invalid quantity: ' || CAST(quantity AS TEXT) AS issue_type
  FROM fct_sales
  WHERE 
    quantity <= 0
    OR quantity != CAST(quantity AS INTEGER)  -- Non-integer quantity
),

-- 4. Verify point-in-time join correctness for SCD Type 2
-- Sales should join to customer version active at time of sale
scd_integrity AS (
  SELECT
    f.sale_id,
    f.customer_sk,
    f.sale_date,
    c.valid_from,
    c.valid_to,
    'SCD Type 2 violation: sale date outside customer version validity' AS issue_type
  FROM fct_sales f
  INNER JOIN dim_customers c ON f.customer_sk = c.customer_sk
  WHERE 
    -- Verify point-in-time logic
    NOT (f.sale_date >= c.valid_from AND f.sale_date < c.valid_to)
),

-- 5. Check for duplicate sales (grain violation)
duplicate_sales AS (
  SELECT
    sale_id,
    COUNT(*) as duplicate_count,
    'Duplicate sale_id (grain violation)' AS issue_type
  FROM fct_sales
  GROUP BY sale_id
  HAVING COUNT(*) > 1
),

-- 6. Verify sale amount vs product price consistency
-- Sale amount should be close to product_price * quantity (allowing for discounts)
price_mismatch AS (
  SELECT
    f.sale_id,
    f.customer_sk,
    f.product_id,
    f.sale_date,
    f.sale_amount,
    f.quantity,
    p.product_price,
    (p.product_price * f.quantity) AS expected_amount,
    'Price mismatch: ' || 
      'expected ~' || CAST(ROUND(p.product_price * f.quantity, 2) AS TEXT) ||
      ', got ' || CAST(f.sale_amount AS TEXT) AS issue_type
  FROM fct_sales f
  INNER JOIN dim_products p ON f.product_id = p.product_id
  WHERE 
    -- Allow for 50% discount maximum
    f.sale_amount < (p.product_price * f.quantity * 0.5)
    -- Or overcharge by more than 10%
    OR f.sale_amount > (p.product_price * f.quantity * 1.1)
)

-- Combine all integrity checks
SELECT sale_id, customer_sk, product_id, sale_date, issue_type
FROM orphaned_sales

UNION ALL

SELECT sale_id, customer_sk, product_id, sale_date, issue_type
FROM invalid_amounts

UNION ALL

SELECT sale_id, customer_sk, product_id, sale_date, issue_type
FROM invalid_quantities

UNION ALL

SELECT sale_id, customer_sk, NULL as product_id, sale_date, issue_type
FROM scd_integrity

UNION ALL

SELECT sale_id, NULL as customer_sk, NULL as product_id, NULL as sale_date, issue_type
FROM duplicate_sales

UNION ALL

SELECT sale_id, customer_sk, product_id, sale_date, issue_type
FROM price_mismatch

ORDER BY sale_id, issue_type
