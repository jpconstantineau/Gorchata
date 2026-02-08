-- Singular test: validates that completed orders have positive totals
-- Note: Using direct table name (raw_orders) to avoid model dependency
SELECT
    id,
    total_amount,
    status
FROM raw_orders
WHERE status = 'completed'
  AND total_amount <= 0
