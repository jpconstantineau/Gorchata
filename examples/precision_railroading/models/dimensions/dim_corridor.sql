{{ config "materialized" "table" }}

-- Corridor Dimension
-- Major rail corridors connecting key locations
-- 30-50 corridors representing high-traffic origin-destination pairs
-- Includes distance, lane type, traffic volume, and congestion metrics

WITH location_pairs AS (
  -- Identify frequently used origin-destination pairs from CLM events
  SELECT
    e1.splc_code AS origin_splc,
    e2.splc_code AS destination_splc,
    COUNT(*) AS trip_count
  FROM {{ seed "raw_clm_events" }} e1
  JOIN {{ seed "raw_clm_events" }} e2
    ON e1.car_number = e2.car_number
    AND e1.event_type = 'DEPA'
    AND e2.event_type = 'ARRI'
    AND e2.timestamp > e1.timestamp
    AND e2.timestamp = (
      SELECT MIN(timestamp)
      FROM {{ seed "raw_clm_events" }} e3
      WHERE e3.car_number = e1.car_number
        AND e3.event_type = 'ARRI'
        AND e3.timestamp > e1.timestamp
    )
  WHERE e1.splc_code != e2.splc_code
  GROUP BY e1.splc_code, e2.splc_code
  HAVING COUNT(*) > 10  -- Only corridors with significant traffic
),

top_corridors AS (
  -- Select top 40 corridors by traffic volume
  SELECT
    origin_splc,
    destination_splc,
    trip_count,
    ROW_NUMBER() OVER (ORDER BY trip_count DESC) AS corridor_rank
  FROM location_pairs
  ORDER BY trip_count DESC
  LIMIT 40
),

corridor_enrichment AS (
  -- Add corridor attributes
  SELECT
    'C' || SUBSTR('00' || CAST(corridor_rank AS TEXT), -3) AS corridor_code,
    tc.origin_splc,
    tc.destination_splc,
    
    -- Calculate distance based on lat/long difference
    -- Using simplified Euclidean distance scaled to miles (1 degree ≈ 69 miles)
    CAST(
      SQRT(
        POWER((lo.latitude - ld.latitude) * 69, 2) +
        POWER((lo.longitude - ld.longitude) * 69 * COS(RADIANS((lo.latitude + ld.latitude) / 2)), 2)
      ) AS INTEGER
    ) AS distance_miles,
    
    -- Lane type (mainline for high traffic, branch for medium, shortline for low)
    CASE
      WHEN tc.trip_count > 100 THEN 'mainline'
      WHEN tc.trip_count > 50 THEN 'branch'
      ELSE 'shortline'
    END AS lane_type,
    
    -- Traffic volume class
    CASE
      WHEN tc.trip_count > 100 THEN 'high'
      WHEN tc.trip_count > 50 THEN 'medium'
      ELSE 'low'
    END AS traffic_volume_class,
    
    -- Congestion level (0-100, based on traffic volume)
    CASE
      WHEN tc.trip_count > 100 THEN
        LEAST(100, CAST(50 + (tc.trip_count / 10) AS INTEGER))
      WHEN tc.trip_count > 50 THEN
        CAST(30 + (tc.trip_count / 5) AS INTEGER)
      ELSE
        CAST(10 + (tc.trip_count / 2) AS INTEGER)
    END AS congestion_level
    
  FROM top_corridors tc
  INNER JOIN {{ ref "dim_location" }} lo ON tc.origin_splc = lo.splc_code
  INNER JOIN {{ ref "dim_location" }} ld ON tc.destination_splc = ld.splc_code
  WHERE lo.location_id IS NOT NULL AND ld.location_id IS NOT NULL
)

SELECT
  ROW_NUMBER() OVER (ORDER BY corridor_code) AS corridor_id,
  corridor_code,
  origin_splc,
  destination_splc,
  distance_miles,
  lane_type,
  traffic_volume_class,
  congestion_level
FROM corridor_enrichment
ORDER BY corridor_code
