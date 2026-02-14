-- Network Congestion Hotspots Analysis
-- Identifies bottlenecks in the rail network
-- Combines high dwell duration, variance, and traffic volume

{{ config "materialized" "table" }}

WITH location_congestion_stats AS (
  SELECT
    d.location_id,
    l.location_name,
    l.location_type,
    COUNT(*) AS dwell_event_count,
    AVG((julianday(d.dwell_end_timestamp) - julianday(d.dwell_start_timestamp)) * 24 * 60) AS avg_dwell_minutes,
    MAX((julianday(d.dwell_end_timestamp) - julianday(d.dwell_start_timestamp)) * 24 * 60) AS max_dwell_minutes,
    MIN((julianday(d.dwell_end_timestamp) - julianday(d.dwell_start_timestamp)) * 24 * 60) AS min_dwell_minutes,
    -- Calculate standard deviation of dwell duration
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
    END AS stddev_dwell
  FROM {{ ref "fact_dwell" }} d
  INNER JOIN {{ ref "dim_location" }} l ON d.location_id = l.location_id
  GROUP BY d.location_id, l.location_name, l.location_type
  HAVING COUNT(*) >= 3  -- Require multiple events for congestion pattern
),

scored_hotspots AS (
  SELECT
    location_id,
    location_name,
    location_type,
    dwell_event_count,
    avg_dwell_minutes,
    max_dwell_minutes,
    min_dwell_minutes,
    stddev_dwell,
    -- Calculate congestion score (composite metric)
    -- Higher score = more congested
    (
      -- Component 1: High average dwell (weight 40%)
      CASE WHEN avg_dwell_minutes > 180 THEN 40 * (avg_dwell_minutes / 360.0) ELSE 0 END +
      -- Component 2: High variance/unpredictability (weight 30%)
      CASE WHEN stddev_dwell > 60 THEN 30 * (stddev_dwell / 120.0) ELSE 0 END +
      -- Component 3: High traffic volume (weight 30%)
      CASE WHEN dwell_event_count > 10 THEN 30 * (CAST(dwell_event_count AS REAL) / 20.0) ELSE 0 END
    ) AS congestion_score
  FROM location_congestion_stats
  WHERE avg_dwell_minutes > 90  -- Focus on locations with significant dwell
     OR stddev_dwell > 45       -- or high variance
     OR dwell_event_count > 8   -- or high traffic
)

SELECT
  location_id,
  location_name,
  location_type,
  dwell_event_count,
  ROUND(avg_dwell_minutes, 2) AS avg_dwell_minutes,
  ROUND(max_dwell_minutes, 2) AS max_dwell_minutes,
  ROUND(min_dwell_minutes, 2) AS min_dwell_minutes,
  ROUND(stddev_dwell, 2) AS stddev_dwell,
  ROUND(MIN(100, congestion_score), 2) AS congestion_score,  -- Cap at 100
  ROW_NUMBER() OVER (ORDER BY congestion_score DESC) AS congestion_rank,
  -- Classify congestion severity
  CASE
    WHEN congestion_score >= 75 THEN 'Critical'
    WHEN congestion_score >= 50 THEN 'High'
    WHEN congestion_score >= 25 THEN 'Moderate'
    ELSE 'Low'
  END AS congestion_severity
FROM scored_hotspots
ORDER BY congestion_score DESC
LIMIT 15  -- Top 15 congestion hotspots
