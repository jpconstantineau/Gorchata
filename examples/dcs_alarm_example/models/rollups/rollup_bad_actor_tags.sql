{{ config "materialized" "table" }}

-- Bad Actor Tags Analysis
-- Identifies high-frequency alarm tags using Pareto analysis and composite scoring
-- Grain: One row per tag ranked by alarm contribution

WITH tag_activations AS (
  -- Count total activations per tag
  SELECT
    tag_key,
    area_key,
    COUNT(*) AS total_activations,
    -- Calculate daily average (assuming 2-day analysis period from test data)
    COUNT(*) * 1.0 / 2.0 AS avg_activations_per_day
  FROM {{ ref "fct_alarm_occurrence" }}
  GROUP BY tag_key, area_key
),

ranked_tags AS (
  -- Calculate Pareto metrics (rank, contribution %, cumulative %)
  SELECT
    tag_key,
    area_key,
    total_activations,
    avg_activations_per_day,
    -- Rank by descending activation count
    ROW_NUMBER() OVER (ORDER BY total_activations DESC) AS alarm_rank,
    -- Percentage contribution to total alarms
    total_activations * 100.0 / SUM(total_activations) OVER () AS contribution_pct,
    -- Total count for later calculations
    COUNT(*) OVER () AS total_tags,
    SUM(total_activations) OVER () AS grand_total
  FROM tag_activations
),

cumulative_pct AS (
  -- Calculate cumulative percentage in separate CTE to avoid nested window functions
  SELECT
    tag_key,
    area_key,
    total_activations,
    avg_activations_per_day,
    alarm_rank,
    contribution_pct,
    total_tags,
    SUM(contribution_pct) OVER (ORDER BY alarm_rank ROWS UNBOUNDED PRECEDING) AS cumulative_pct
  FROM ranked_tags
),

composite_scores AS (
  -- Calculate bad actor composite score (0-100)
  -- Weights: frequency 40%, standing 30%, chattering 30%
  SELECT
    rt.tag_key,
    rt.area_key,
    rt.total_activations,
    rt.avg_activations_per_day,
    rt.alarm_rank,
    rt.contribution_pct,
    rt.cumulative_pct,
    CASE 
      WHEN rt.alarm_rank * 100.0 / rt.total_tags <= 10 THEN 1 
      ELSE 0 
    END AS is_top_10_pct,
    -- Composite score calculation
    ROUND(
      -- Frequency component (40%): normalized to 0-1, capped at 10 activations = full score
      (MIN(rt.total_activations / 10.0, 1.0) * 40) +
      -- Standing alarm component (30%): normalized to 0-1, capped at 5 occurrences = full score
      (MIN(COALESCE(sa.standing_alarm_count, 0) / 5.0, 1.0) * 30) +
      -- Chattering component (30%): binary - has chattering or not
      (CASE WHEN ca.chattering_episode_count > 0 THEN 30 ELSE 0 END)
    , 1) AS bad_actor_score
  FROM cumulative_pct rt
  LEFT JOIN {{ ref "rollup_standing_alarms" }} sa ON rt.tag_key = sa.tag_key
  LEFT JOIN {{ ref "rollup_chattering_alarms" }} ca ON rt.tag_key = ca.tag_key
)

SELECT
  ROW_NUMBER() OVER (ORDER BY alarm_rank) AS bad_actor_key,
  tag_key,
  area_key,
  CAST(total_activations AS INTEGER) AS total_activations,
  CAST(avg_activations_per_day AS REAL) AS avg_activations_per_day,
  CAST(alarm_rank AS INTEGER) AS alarm_rank,
  CAST(contribution_pct AS REAL) AS contribution_pct,
  CAST(cumulative_pct AS REAL) AS cumulative_pct,
  CAST(is_top_10_pct AS INTEGER) AS is_top_10_pct,
  CAST(bad_actor_score AS REAL) AS bad_actor_score,
  CASE
    WHEN bad_actor_score >= 70 THEN 'CRITICAL'
    WHEN bad_actor_score >= 50 THEN 'SIGNIFICANT'
    WHEN bad_actor_score >= 30 THEN 'MODERATE'
    ELSE 'NORMAL'
  END AS bad_actor_category
FROM composite_scores
ORDER BY alarm_rank
