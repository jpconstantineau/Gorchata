-- Stage clean users data
-- {{ config(materialized='view') }}

SELECT 
    id,
    name,
    email,
    created_at
FROM raw_users
WHERE deleted_at IS NULL
