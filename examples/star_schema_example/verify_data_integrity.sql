-- Verification query to check data integrity
-- This query checks that sale_amount = product_price * quantity for all records

WITH integrity_check AS (
  SELECT 
    sale_id,
    sale_amount,
    product_price,
    quantity,
    (product_price * quantity) AS calculated_amount,
    ROUND(sale_amount - (product_price * quantity), 2) AS difference
  FROM raw_sales
)
SELECT 
  COUNT(*) as total_records,
  SUM(CASE WHEN ABS(difference) > 0.01 THEN 1 ELSE 0 END) as violations
FROM integrity_check;

-- Expected result: total_records=30, violations=0
