{{ config "materialized" "table" }}

SELECT
    id,
    user_id,
    total_amount,
    status,
    order_date
FROM raw_orders
