{{ config "materialized" "table" }}

SELECT
    dd.sale_date,
    dd.year,
    dd.month,
    dp.product_category,
    dc.customer_state,
    SUM(fs.sale_amount) AS total_sales,
    SUM(fs.quantity) AS total_quantity,
    COUNT(*) AS sale_count,
    AVG(fs.sale_amount) AS avg_sale_amount
FROM {{ ref "fct_sales" }} fs
INNER JOIN {{ ref "dim_dates" }} dd 
    ON fs.sale_date = dd.sale_date
INNER JOIN {{ ref "dim_products" }} dp 
    ON fs.product_id = dp.product_id
INNER JOIN {{ ref "dim_customers" }} dc 
    ON fs.customer_sk = dc.customer_sk
GROUP BY
    dd.sale_date,
    dd.year,
    dd.month,
    dp.product_category,
    dc.customer_state
ORDER BY
    dd.sale_date,
    dp.product_category,
    dc.customer_state
