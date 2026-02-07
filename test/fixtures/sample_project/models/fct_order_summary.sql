-- Create order summary fact table
-- {{ config(materialized='table') }}

SELECT 
    u.id as user_id,
    u.name as user_name,
    u.email,
    COUNT(o.id) as order_count,
    SUM(o.amount) as total_amount,
    MAX(o.order_date) as last_order_date
FROM {{ ref "stg_users" }} u
LEFT JOIN {{ ref "stg_orders" }} o ON u.id = o.user_id
GROUP BY u.id, u.name, u.email
