{{ config "materialized" "table" }}

-- Alternative: Use incremental materialization for efficient processing
-- To enable incremental mode, uncomment the following line and comment out the config above:
-- {{ config(materialization="incremental", unique_key="sale_id") }}

SELECT
    rs.sale_id,
    dc.customer_sk,
    rs.product_id,
    rs.sale_date,
    rs.sale_amount,
    rs.quantity
FROM {{ seed "raw_sales" }} rs
INNER JOIN {{ ref "dim_customers" }} dc 
    ON rs.customer_id = dc.customer_id 
    AND rs.sale_date >= dc.valid_from 
    AND rs.sale_date < dc.valid_to
INNER JOIN {{ ref "dim_products" }} dp 
    ON rs.product_id = dp.product_id
INNER JOIN {{ ref "dim_dates" }} dd 
    ON rs.sale_date = dd.sale_date
-- Incremental mode: Only process new sales
-- When incremental is enabled, uncomment the following:
-- {{ if is_incremental }}
-- WHERE rs.sale_date > (SELECT COALESCE(MAX(sale_date), '1900-01-01') FROM {{ this }})
-- {{ end }}
ORDER BY rs.sale_id


