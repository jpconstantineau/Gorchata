-- Shadow Yard Identification Analysis
-- Sophisticated detection logic for subtle shadow yard patterns
-- Combines dwell patterns, variance, and time clustering

{{ config "materialized" "table" }}

WITH location_dwell_stats AS (
  SELECT
    d.location_id,
    l.location_name,
    COUNT(*) AS dwell_event_count,
    AVG((julianday(d.dwell_end_timestamp) - julianday(d.dwell_start_timestamp)) * 24 * 60) AS avg_dwell_minutes,
    -- Calculate standard deviation using SQLite functions
    CASE 
      WHEN COUNT(*) > 1 THEN
        SQRT(
          AVG(
            ((julianday(d.dwell_end_timestamp) - julianday(d.dwell_start_timestamp)) * 24 * 60) * 
            ((julianday(d.dwell_end_timestamp) - julianday(d.dwell_start_timestamp)) * 24 * 60)
          ) - 
          AVG((julianday(d.dwell_end_timestamp) - julianday(d.dwell_start_timestamp)) * 24 * 60) *
          AVG((julianday(d.dwell_end_timestamp) - julianday(d.dwell_start_timestamp)) * 24 * 60)
        )
      ELSE 0
    END AS stddev_dwell_minutes,
    -- Time-of-day clustering: calculate variance in arrival hours
    CASE
      WHEN COUNT(*) > 1 THEN
        SQRT(
          AVG(CAST(strftime('%H', d.dwell_start_timestamp) AS INTEGER) * CAST(strftime('%H', d.dwell_start_timestamp) AS INTEGER)) -
          AVG(CAST(strftime('%H', d.dwell_start_timestamp) AS INTEGER)) * AVG(CAST(strftime('%H', d.dwell_start_timestamp) AS INTEGER))
        )
      ELSE 0
    END AS hour_variance
  FROM {{ ref "fact_dwell" }} d
  INNER JOIN {{ ref "dim_location" }} l ON d.location_id = l.location_id
  GROUP BY d.location_id, l.location_name
  HAVING COUNT(*) >= 2  -- Require multiple dwell events for pattern detection
),

enriched_stats AS (
  SELECT
    lds.*,
    -- Get shadow yard percentage from aggregation if available
    COALESCE(sy.shadow_yard_percentage, 0) AS shadow_yard_percentage,
    -- Calculate variance score (normalized 0-100)
    CASE 
      WHEN lds.avg_dwell_minutes > 0 THEN
        MIN(100, (lds.stddev_dwell_minutes / NULLIF(lds.avg_dwell_minutes, 0)) * 50)
      ELSE 0
    END AS variance_score,
    -- Calculate time clustering score (normalized 0-100)
    MIN(100, lds.hour_variance * 10) AS time_clustering_score
  FROM location_dwell_stats lds
  LEFT JOIN {{ ref "agg_shadow_yards" }} sy ON lds.location_id = sy.location_id
),

scored_locations AS (
  SELECT
    location_id,
    location_name,
    dwell_event_count,
    ROUND(avg_dwell_minutes, 2) AS avg_dwell_minutes,
    ROUND(stddev_dwell_minutes, 2) AS stddev_dwell_minutes,
    ROUND(shadow_yard_percentage, 2) AS shadow_yard_percentage,
    ROUND(variance_score, 2) AS variance_score,
    ROUND(time_clustering_score, 2) AS time_clustering_score,
    -- Composite score: weighted average of indicators
    ROUND(
      (shadow_yard_percentage * 0.5) +  -- 50% weight: primary indicator
      (variance_score * 0.3) +          -- 30% weight: dwell variance
      (time_clustering_score * 0.2),    -- 20% weight: time patterns
      2
    ) AS composite_score
  FROM enriched_stats
)

SELECT
  location_id,
  location_name,
  dwell_event_count,
  avg_dwell_minutes,
  stddev_dwell_minutes,
  shadow_yard_percentage,
  variance_score,
  time_clustering_score,
  composite_score,
  CASE WHEN composite_score > 60 THEN 1 ELSE 0 END AS shadow_yard_flag,
  ROW_NUMBER() OVER (ORDER BY composite_score DESC) AS ranking
FROM scored_locations
ORDER BY composite_score DESC
LIMIT 10  -- Top 10 suspicious locations
