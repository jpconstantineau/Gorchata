{{ config "materialized" "table" }}

-- Train Dimension
-- Unit train formation records with trip-specific identifiers
-- Type 1 SCD (current state only)

WITH train_formations AS (
  -- Identify train formation events (when trains are formed at origin)
  -- Each train has multiple FORM_TRAIN events (one per car)
  SELECT
    train_id,
    MIN(CAST(event_timestamp AS TIMESTAMP)) AS formed_at,
    MIN(location_id) AS origin_location_id
  FROM {{ seed "raw_clm_events" }}
  WHERE event_type = 'FORM_TRAIN'
  GROUP BY train_id
),

train_completions AS (
  -- Identify train arrival at destination (completion)
  SELECT
    train_id,
    MIN(CAST(event_timestamp AS TIMESTAMP)) AS completed_at
  FROM {{ seed "raw_clm_events" }}
  WHERE event_type = 'ARRIVE_DESTINATION'
  GROUP BY train_id
),

train_car_counts AS (
  -- Count cars per train
  SELECT
    train_id,
    COUNT(DISTINCT car_id) AS num_cars
  FROM {{ seed "raw_clm_events" }}
  WHERE event_type = 'FORM_TRAIN'
  GROUP BY train_id
)

SELECT
  f.train_id,
  f.train_id AS train_name,  -- Use train_id as the name
  c.num_cars,
  f.formed_at,
  comp.completed_at
FROM train_formations f
LEFT JOIN train_completions comp
  ON f.train_id = comp.train_id
LEFT JOIN train_car_counts c
  ON f.train_id = c.train_id
WHERE f.train_id IS NOT NULL AND f.train_id != ''
ORDER BY f.formed_at, f.train_id
