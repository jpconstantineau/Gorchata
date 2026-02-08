{{ config(materialized='table') }}

SELECT
    id,
    name,
    email,
    status,
    created_at
FROM raw_users
