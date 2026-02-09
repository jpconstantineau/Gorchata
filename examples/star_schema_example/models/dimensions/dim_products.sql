{{ config "materialized" "table" }}

SELECT DISTINCT
    product_id,
    product_name,
    product_category,
    product_price
FROM {{ seed "raw_sales" }}
ORDER BY product_id
