-- Stage clean orders data
-- {{ config(materialized='view') }}

SELECT 
    id,
    user_id,
    amount,
    order_date
FROM raw_orders
WHERE status = 'completed'
