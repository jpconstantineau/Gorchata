{{ config(materialized='table') }}

SELECT
    rs.sale_id,
    dc.customer_sk,
    rs.product_id,
    rs.sale_date,
    rs.sale_amount,
    rs.quantity
FROM {{ ref "raw_sales" }} rs
INNER JOIN {{ ref "dim_customers" }} dc 
    ON rs.customer_id = dc.customer_id 
    AND rs.sale_date >= dc.valid_from 
    AND rs.sale_date < dc.valid_to
INNER JOIN {{ ref "dim_products" }} dp 
    ON rs.product_id = dp.product_id
INNER JOIN {{ ref "dim_dates" }} dd 
    ON rs.sale_date = dd.sale_date
ORDER BY rs.sale_id
