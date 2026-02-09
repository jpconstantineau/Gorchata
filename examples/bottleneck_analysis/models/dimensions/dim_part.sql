{{ config "materialized" "table" }}

-- Part Dimension
-- Tracks unique parts manufactured with classification attributes

WITH distinct_parts AS (
    SELECT DISTINCT
        part_number
    FROM {{ seed "raw_work_orders" }}
),

transformed AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY part_number) AS part_key,
        part_number,
        -- Derived attributes (placeholder descriptions)
        'Manufacturing Part ' || part_number AS part_description,
        CASE 
            WHEN SUBSTR(part_number, 6, 2) = '00' THEN 'Family A'
            WHEN SUBSTR(part_number, 6, 2) = '01' THEN 'Family B'
            ELSE 'Family C'
        END AS part_family,
        CASE 
            WHEN part_number IN ('PART-001', 'PART-002', 'PART-003') THEN 'High'
            ELSE 'Standard'
        END AS routing_complexity,
        -- Metadata
        CURRENT_TIMESTAMP AS created_at
    FROM distinct_parts
)

SELECT * FROM transformed
ORDER BY part_key
