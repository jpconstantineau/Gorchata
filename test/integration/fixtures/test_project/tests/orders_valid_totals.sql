-- Singular test: validates that completed orders have positive totals
SELECT
    id,
    total_amount,
    status
FROM {{ ref "orders" }}
WHERE status = 'completed'
  AND total_amount <= 0
