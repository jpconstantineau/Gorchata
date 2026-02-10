{{ config "materialized" "table" }}

-- Aggregated Power Efficiency Metrics
-- Analyzes inferred locomotive power transfer patterns and efficiency
-- Grain: one row per corridor per week
-- Tracks same-power vs. different-power consecutive trips

WITH power_transfers AS (
  -- Get power transfer information from fact table
  SELECT
    pt.transfer_id,
    pt.train_id,
    pt.location_id,
    pt.arrival_timestamp,
    pt.departure_timestamp,
    pt.turnaround_hours,
    pt.inferred_same_power,
    pt.inferred_power_change,
    d.year,
    d.week,
    l.location_type,
    -- Get corridor context from train trips
    t.corridor_id
  FROM {{ ref "fact_inferred_power_transfer" }} pt
  INNER JOIN {{ ref "dim_location" }} l ON pt.location_id = l.location_id
  INNER JOIN {{ ref "dim_date" }} d ON pt.transfer_date_key = d.date_key
  LEFT JOIN {{ ref "fact_train_trip" }} t
    ON pt.train_id = t.train_id
    AND DATE(pt.arrival_timestamp) = DATE(t.departure_timestamp)
)

-- Aggregate power efficiency metrics by corridor and week
SELECT
  COALESCE(corridor_id, 'UNKNOWN') AS corridor_id,
  year,
  week,
  
  -- Total potential power transfer events
  COUNT(transfer_id) AS power_transfer_count,
  
  -- Same power consecutive trips (fast turnaround < 1 hour)
  SUM(inferred_same_power) AS same_power_trips,
  
  -- Different power trips (repower, turnaround > 1 hour)
  SUM(inferred_power_change) AS repower_trips,
  
  -- Repower frequency (percentage of trips requiring repower)
  CAST(SUM(inferred_power_change) AS REAL) / NULLIF(COUNT(transfer_id), 0) * 100 AS repower_frequency_pct,
  
  -- Average turnaround time for same power
  AVG(
    CASE WHEN inferred_same_power = 1 THEN turnaround_hours ELSE NULL END
  ) AS avg_same_power_turnaround_hours,
  
  -- Average turnaround time for repower
  AVG(
    CASE WHEN inferred_power_change = 1 THEN turnaround_hours ELSE NULL END
  ) AS avg_repower_turnaround_hours,
  
  -- Power efficiency score (higher is better)
  -- Score = (same_power_trips / total_trips) * 100
  CAST(SUM(inferred_same_power) AS REAL) / NULLIF(COUNT(transfer_id), 0) * 100 AS power_efficiency_score,
  
  -- Break down by location type
  SUM(CASE WHEN location_type = 'ORIGIN' THEN 1 ELSE 0 END) AS transfers_at_origin,
  SUM(CASE WHEN location_type = 'DESTINATION' THEN 1 ELSE 0 END) AS transfers_at_destination

FROM power_transfers
GROUP BY corridor_id, year, week
ORDER BY corridor_id, year, week
