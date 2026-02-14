-- PSR Strategy Shifts Analysis
-- Detects operational model changes across PSR adoption periods
-- Compares pre-PSR, transition, and mature PSR performance

{{ config "materialized" "table" }}

WITH psr_period_performance AS (
  SELECT
    psr_period,
    AVG(avg_velocity_mph) AS avg_velocity_mph,
    AVG(avg_trip_duration_minutes) AS avg_duration_minutes,
    AVG(avg_dwell_count_per_trip) AS avg_dwell_count,
    SUM(total_trips) AS trip_count,
    SUM(total_trips) AS car_count,  -- Using total_trips as proxy for cars (1 car per trip in this dataset)
    -- Calculate asset utilization (trips per car) - simplified to 1.0 since it's 1:1
    1.0 AS trips_per_car,
    ROW_NUMBER() OVER (ORDER BY 
      CASE 
        WHEN psr_period = 'pre-PSR' THEN 1
        WHEN psr_period = 'transition' THEN 2
        WHEN psr_period = 'mature' THEN 3
        ELSE 4
      END
    ) AS period_order
  FROM {{ ref "agg_psr_evolution" }}
  WHERE total_trips > 0  -- Only include periods with actual trip data
  GROUP BY psr_period
),

baseline_metrics AS (
  -- Use first available period as baseline
  SELECT
    avg_velocity_mph AS pre_psr_velocity,
    avg_duration_minutes AS pre_psr_duration,
    avg_dwell_count AS pre_psr_dwell_count,
    trips_per_car AS pre_psr_trips_per_car
  FROM psr_period_performance
  WHERE period_order = 1
)

SELECT
  pp.psr_period,
  ROUND(pp.avg_velocity_mph, 2) AS avg_velocity_mph,
  ROUND(pp.avg_duration_minutes, 2) AS avg_duration_minutes,
  ROUND(pp.avg_dwell_count, 2) AS avg_dwell_count,
  pp.trip_count,
  pp.car_count AS railcar_count,
  ROUND(pp.trips_per_car, 3) AS trips_per_car,
  -- Velocity change vs pre-PSR baseline
  ROUND(pp.avg_velocity_mph - bm.pre_psr_velocity, 2) AS velocity_delta_vs_pre_psr,
  ROUND(
    ((pp.avg_velocity_mph - bm.pre_psr_velocity) / NULLIF(bm.pre_psr_velocity, 0)) * 100,
    2
  ) AS velocity_change_pct,
  -- Duration change vs pre-PSR baseline (negative = improvement)
  ROUND(pp.avg_duration_minutes - bm.pre_psr_duration, 2) AS duration_delta_vs_pre_psr,
  ROUND(
    ((pp.avg_duration_minutes - bm.pre_psr_duration) / NULLIF(bm.pre_psr_duration, 0)) * 100,
    2
  ) AS duration_change_pct,
  -- Dwell count change vs pre-PSR baseline (negative = fewer stops)
  ROUND(pp.avg_dwell_count - bm.pre_psr_dwell_count, 2) AS dwell_count_delta_vs_pre_psr,
  ROUND(
    ((pp.avg_dwell_count - bm.pre_psr_dwell_count) / NULLIF(bm.pre_psr_dwell_count, 0)) * 100,
    2
  ) AS dwell_count_change_pct,
  -- Asset utilization change
  ROUND(pp.trips_per_car - bm.pre_psr_trips_per_car, 3) AS utilization_delta_vs_pre_psr,
  ROUND(
    ((pp.trips_per_car - bm.pre_psr_trips_per_car) / NULLIF(bm.pre_psr_trips_per_car, 0)) * 100,
    2
  ) AS utilization_change_pct
FROM psr_period_performance pp
CROSS JOIN baseline_metrics bm
ORDER BY
  CASE pp.psr_period
    WHEN 'pre-PSR' THEN 1
    WHEN 'transition' THEN 2
    WHEN 'mature' THEN 3
  END
