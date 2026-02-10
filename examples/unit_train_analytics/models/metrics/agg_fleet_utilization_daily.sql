{{ config "materialized" "table" }}

-- Aggregated Fleet Utilization Daily
-- Daily fleet status metrics across all 228 cars
-- Grain: one row per day
-- Tracks how many cars are on trains, stragglers, or idle

WITH daily_car_status AS (
  -- For each date, determine status of each car
  SELECT
    d.date_key,
    d.full_date,
    c.car_id,
    -- Determine if car is on a train on this date
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM {{ ref "fact_car_location_event" }} f
        WHERE f.car_id = c.car_id
          AND DATE(f.event_timestamp) = d.full_date
          AND f.train_id IS NOT NULL
          AND f.event_type IN ('departed_origin', 'departed_destination', 'in_transit')
      ) THEN 1
      ELSE 0
    END AS is_on_train,
    -- Determine if car is a straggler on this date
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM {{ ref "fact_straggler" }} s
        WHERE s.car_id = c.car_id
          AND DATE(s.set_out_timestamp) <= d.full_date
          AND (s.picked_up_timestamp IS NULL OR DATE(s.picked_up_timestamp) >= d.full_date)
      ) THEN 1
      ELSE 0
    END AS is_straggler
  FROM {{ ref "dim_date" }} d
  CROSS JOIN {{ ref "dim_car" }} c
  WHERE d.full_date >= '2024-01-01'
    AND d.full_date <= '2024-03-31'
)

-- Aggregate fleet status by date
SELECT
  date_key,
  
  -- Total cars in fleet (should be 228)
  COUNT(DISTINCT car_id) AS total_cars,
  
  -- Cars actively on trains
  SUM(is_on_train) AS cars_on_trains,
  
  -- Cars traveling as stragglers
  SUM(is_straggler) AS cars_as_stragglers,
  
  -- Idle cars (not on trains and not stragglers)
  SUM(
    CASE 
      WHEN is_on_train = 0 AND is_straggler = 0 THEN 1
      ELSE 0
    END
  ) AS cars_idle,
  
  -- Fleet utilization percentage (cars on trains or as stragglers / total cars * 100)
  CAST(
    (SUM(is_on_train) + SUM(is_straggler)) AS REAL
  ) / NULLIF(COUNT(DISTINCT car_id), 0) * 100 AS utilization_pct

FROM daily_car_status
GROUP BY date_key
ORDER BY date_key
