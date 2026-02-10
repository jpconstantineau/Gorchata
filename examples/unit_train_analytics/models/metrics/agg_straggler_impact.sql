{{ config "materialized" "table" }}

-- Aggregated Straggler Impact Metrics
-- Analyzes straggler patterns, delay distribution, and impact
-- Grain: one row per corridor per week
-- Includes delay histogram buckets for distribution analysis

WITH straggler_details AS (
  -- Get straggler information with corridor context
  SELECT
    s.straggler_id,
    s.car_id,
    s.delay_hours,
    s.delay_category,
    s.set_out_timestamp,
    s.picked_up_timestamp,
    d.year,
    d.week,
    -- Get corridor from train trip if available
    t.corridor_id,
    -- Classify delay into buckets
    CASE
      WHEN s.delay_hours < 6 THEN 'delay_0_6_hours'
      WHEN s.delay_hours < 12 THEN 'delay_6_12_hours'
      WHEN s.delay_hours < 24 THEN 'delay_12_24_hours'
      ELSE 'delay_24_plus_hours'
    END AS delay_bucket
  FROM {{ ref "fact_straggler" }} s
  INNER JOIN {{ ref "dim_date" }} d
    ON s.set_out_date_key = d.date_key
  LEFT JOIN {{ ref "fact_train_trip" }} t
    ON s.original_train_id = t.train_id
    AND DATE(s.set_out_timestamp) = DATE(t.departure_timestamp)
  WHERE s.delay_hours IS NOT NULL
),

-- Calculate median separately using CTE to avoid correlated subquery scoping issues
median_calc AS (
  SELECT
    COALESCE(corridor_id, 'UNKNOWN') AS corridor_id,
    year,
    week,
    delay_hours,
    ROW_NUMBER() OVER (PARTITION BY COALESCE(corridor_id, 'UNKNOWN'), year, week ORDER BY delay_hours) AS rn,
    COUNT(*) OVER (PARTITION BY COALESCE(corridor_id, 'UNKNOWN'), year, week) AS cnt
  FROM straggler_details
)

-- Aggregate straggler impact by corridor and week
SELECT
  sd.corridor_id,
  sd.year,
  sd.week,
  
  -- Total straggler count
  COUNT(sd.straggler_id) AS straggler_count,
  
  -- Unique cars affected
  COUNT(DISTINCT sd.car_id) AS cars_affected,
  
  -- Average delay
  AVG(sd.delay_hours) AS avg_delay_hours,
  
  -- Median delay (from CTE, avoiding correlated subquery)
  MAX(mc.delay_hours) AS median_delay_hours,
  
  -- Maximum delay
  MAX(sd.delay_hours) AS max_delay_hours,
  
  -- Delay distribution histogram (count in each bucket)
  SUM(CASE WHEN sd.delay_bucket = 'delay_0_6_hours' THEN 1 ELSE 0 END) AS delay_0_6_hours,
  SUM(CASE WHEN sd.delay_bucket = 'delay_6_12_hours' THEN 1 ELSE 0 END) AS delay_6_12_hours,
  SUM(CASE WHEN sd.delay_bucket = 'delay_12_24_hours' THEN 1 ELSE 0 END) AS delay_12_24_hours,
  SUM(CASE WHEN sd.delay_bucket = 'delay_24_plus_hours' THEN 1 ELSE 0 END) AS delay_24_plus_hours,
  
  -- Rejoined count (stragglers that were picked up)
  SUM(CASE WHEN sd.picked_up_timestamp IS NOT NULL THEN 1 ELSE 0 END) AS rejoined_count,
  
  -- Still straggling count (not yet picked up)
  SUM(CASE WHEN sd.picked_up_timestamp IS NULL THEN 1 ELSE 0 END) AS still_straggling_count,
  
  -- Calculate straggler rate (per 100 opportunities)
  -- This will be refined when we know trip counts
  CAST(COUNT(sd.straggler_id) AS REAL) AS straggler_rate

FROM straggler_details sd
LEFT JOIN median_calc mc
  ON sd.corridor_id = mc.corridor_id
  AND sd.year = mc.year
  AND sd.week = mc.week
  AND mc.rn = (mc.cnt + 1) / 2
GROUP BY sd.corridor_id, sd.year, sd.week
ORDER BY sd.corridor_id, sd.year, sd.week
