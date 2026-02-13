{{ config "materialized" "table" }}

-- Intermediate: Velocity Vectors
-- Purpose: Calculate speed (miles/hour) between sequential locations
-- Grain: One row per trip segment with velocity calculations
-- Source: int_trip_segments + dim_corridor

WITH trip_segments AS (
  SELECT
    trip_segment_id,
    car_number,
    railcar_id,
    trip_start_timestamp,
    trip_end_timestamp,
    trip_duration_minutes,
    origin_location_id,
    origin_splc_code,
    destination_location_id,
    destination_splc_code,
    is_loaded_trip,
    psr_period
  FROM {{ ref "int_trip_segments" }}
  WHERE origin_location_id != destination_location_id  -- Only actual movement trips
),

-- Join to dim_corridor to get distance between origin and destination
trip_corridors AS (
  SELECT
    ts.*,
    c.corridor_id,
    c.distance_miles
  FROM trip_segments ts
  LEFT JOIN dim_corridor c
    ON ts.origin_splc_code = c.origin_splc
    AND ts.destination_splc_code = c.destination_splc
),

-- Get location details for fallback distance calculation
trips_with_locations AS (
  SELECT
    tc.*,
    lo.latitude AS origin_lat,
    lo.longitude AS origin_lon,
    ld.latitude AS dest_lat,
    ld.longitude AS dest_lon
  FROM trip_corridors tc
  LEFT JOIN dim_location lo ON tc.origin_location_id = lo.location_id
  LEFT JOIN dim_location ld ON tc.destination_location_id = ld.location_id
),

-- Calculate velocity vectors
velocity_calculations AS (
  SELECT
    trip_segment_id,
    railcar_id,
    origin_location_id,
    destination_location_id,
    
    -- Distance (use corridor distance if available, otherwise estimate from lat/long)
    -- Ensure minimum 1 mile for all trips
    CASE
      WHEN distance_miles IS NOT NULL AND distance_miles > 0 THEN distance_miles
      WHEN origin_lat IS NOT NULL AND dest_lat IS NOT NULL THEN
        -- Calculate distance from lat/long
        CASE
          WHEN CAST(
            SQRT(
              POWER((origin_lat - dest_lat) * 69, 2) +
              POWER((origin_lon - dest_lon) * 69 * COS(RADIANS((origin_lat + dest_lat) / 2)), 2)
            ) AS INTEGER
          ) < 1 THEN 1
          ELSE CAST(
            SQRT(
              POWER((origin_lat - dest_lat) * 69, 2) +
              POWER((origin_lon - dest_lon) * 69 * COS(RADIANS((origin_lat + dest_lat) / 2)), 2)
            ) AS INTEGER
          )
        END
      ELSE 1  -- Default to 1 mile if all else fails
    END AS distance_miles,
    
    -- Duration in minutes (minute precision using julianday)
    CAST((julianday(trip_end_timestamp) - julianday(trip_start_timestamp)) * 24 * 60 AS INTEGER) AS duration_minutes,
    
    trip_start_timestamp,
    trip_end_timestamp,
    psr_period
  FROM trips_with_locations
  WHERE trip_end_timestamp IS NOT NULL  -- Only complete trips
),

-- Calculate velocity
final AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY trip_segment_id) AS velocity_vector_id,
    railcar_id,
    trip_segment_id,
    origin_location_id,
    destination_location_id,
    distance_miles,
    duration_minutes,
    
    -- Velocity calculation: (distance_miles / duration_minutes) * 60 = mph
    -- Handle edge cases: zero duration (shouldn't happen but be safe)
    CASE
      WHEN duration_minutes <= 0 THEN 0
      ELSE ROUND((CAST(distance_miles AS REAL) / CAST(duration_minutes AS REAL)) * 60.0, 2)
    END AS velocity_mph,
    
    trip_start_timestamp,
    trip_end_timestamp,
    psr_period
  FROM velocity_calculations
  WHERE duration_minutes > 0  -- Only include trips with measurable duration
)

SELECT * FROM final
