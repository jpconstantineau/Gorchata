{{ config "materialized" "table" }}

-- Location Dimension
-- 200 locations enriched with SPLC codes, coordinates, and shadow yard risk scores
-- Extracted from raw CLM events and classified by facility type
-- Shadow yards detected through dwell time pattern analysis

WITH distinct_locations AS (
  -- Get all unique locations from raw CLM events
  SELECT DISTINCT
    splc_code,
    location_name
  FROM {{ seed "raw_clm_events" }}
),

location_classification AS (
  -- Classify locations by type based on SPLC code patterns
  SELECT
    splc_code,
    location_name,
    CASE
      -- Terminals: T prefix (SPLC codes starting with T)
      WHEN splc_code LIKE 'T%' THEN 'terminal'
      -- Interchanges: I prefix
      WHEN splc_code LIKE 'I%' THEN 'interchange'
      -- Yards: Y prefix
      WHEN splc_code LIKE 'Y%' THEN 'yard'
      -- Customer sites: C prefix
      WHEN splc_code LIKE 'C%' THEN 'customer_site'
      -- Sidings: S prefix
      WHEN splc_code LIKE 'S%' THEN 'siding'
      ELSE 'yard'  -- Default to yard for any unclassified
    END AS location_type
  FROM distinct_locations
),

dwell_analysis AS (
  -- Analyze dwell times to identify shadow yards
  -- Shadow yards have suspiciously low dwell times for their facility type
  SELECT
    splc_code,
    AVG(CAST((JULIANDAY(pull_time) - JULIANDAY(place_time)) * 24 AS REAL)) AS avg_dwell_hours,
    COUNT(*) AS event_count
  FROM (
    SELECT
      splc_code,
      car_number,
      CASE 
        WHEN event_type = 'PLAC' THEN timestamp
      END AS place_time,
      LEAD(CASE WHEN event_type = 'PULL' THEN timestamp END) 
        OVER (PARTITION BY car_number, splc_code ORDER BY timestamp) AS pull_time
    FROM {{ seed "raw_clm_events" }}
    WHERE event_type IN ('PLAC', 'PULL')
  )
  WHERE place_time IS NOT NULL AND pull_time IS NOT NULL
  GROUP BY splc_code
),

location_enrichment AS (
  -- Add geographic coordinates, capacity, and shadow yard risk
  SELECT
    lc.splc_code,
    lc.location_name,
    lc.location_type,
    
    -- Latitude (US railroad corridor range: 25°N to 50°N)
    -- Derive from SPLC code hash for deterministic coordinates
    ROUND(25.0 + (ABS(
      CAST(
        (CAST(SUBSTR(lc.splc_code, 2) AS INTEGER) * 982451653) % 100000 AS REAL
      ) / 100000.0
    ) * 25.0), 6) AS latitude,
    
    -- Longitude (US railroad corridor range: -125°W to -70°W)
    ROUND(-125.0 + (ABS(
      CAST(
        (CAST(SUBSTR(lc.splc_code, 2) AS INTEGER) * 765432191) % 100000 AS REAL
      ) / 100000.0
    ) * 55.0), 6) AS longitude,
    
    -- State code (derive from longitude buckets)
    CASE
      WHEN -125.0 + (ABS(
        CAST((CAST(SUBSTR(lc.splc_code, 2) AS INTEGER) * 765432191) % 100000 AS REAL) / 100000.0
      ) * 55.0) < -110.0 THEN 'WA'
      WHEN -125.0 + (ABS(
        CAST((CAST(SUBSTR(lc.splc_code, 2) AS INTEGER) * 765432191) % 100000 AS REAL) / 100000.0
      ) * 55.0) < -100.0 THEN 'WY'
      WHEN -125.0 + (ABS(
        CAST((CAST(SUBSTR(lc.splc_code, 2) AS INTEGER) * 765432191) % 100000 AS REAL) / 100000.0
      ) * 55.0) < -95.0 THEN 'NE'
      WHEN -125.0 + (ABS(
        CAST((CAST(SUBSTR(lc.splc_code, 2) AS INTEGER) * 765432191) % 100000 AS REAL) / 100000.0
      ) * 55.0) < -88.0 THEN 'IL'
      WHEN -125.0 + (ABS(
        CAST((CAST(SUBSTR(lc.splc_code, 2) AS INTEGER) * 765432191) % 100000 AS REAL) / 100000.0
      ) * 55.0) < -80.0 THEN 'OH'
      ELSE 'NY'
    END AS state_code,
    
    -- Capacity classification by facility type
    CASE lc.location_type
      WHEN 'terminal' THEN 'high'
      WHEN 'interchange' THEN 'high'
      WHEN 'yard' THEN 'medium'
      WHEN 'customer_site' THEN 'medium'
      WHEN 'siding' THEN 'low'
    END AS capacity_classification,
    
    -- Shadow yard risk score (0-100)
    -- Based on dwell time analysis - lower dwell time = higher risk
    CASE
      WHEN da.avg_dwell_hours IS NULL THEN 0
      WHEN lc.location_type = 'yard' THEN
        -- Yards should have 24-48 hour dwell typically
        -- If dwell < 12 hours, high risk; 12-24 hours medium; >24 hours low
        CASE
          WHEN da.avg_dwell_hours < 8 THEN 
            LEAST(100, CAST(100 - (da.avg_dwell_hours * 10) AS INTEGER))
          WHEN da.avg_dwell_hours < 12 THEN 75
          WHEN da.avg_dwell_hours < 18 THEN 50
          WHEN da.avg_dwell_hours < 24 THEN 25
          ELSE 10
        END
      WHEN lc.location_type = 'terminal' THEN
        -- Terminals should have significant dwell
        CASE
          WHEN da.avg_dwell_hours < 6 THEN 85
          WHEN da.avg_dwell_hours < 12 THEN 60
          ELSE 20
        END
      ELSE 0
    END AS shadow_yard_risk_score,
    
    -- Region (based on longitude)
    CASE
      WHEN -125.0 + (ABS(
        CAST((CAST(SUBSTR(lc.splc_code, 2) AS INTEGER) * 765432191) % 100000 AS REAL) / 100000.0
      ) * 55.0) < -105.0 THEN 'West'
      WHEN -125.0 + (ABS(
        CAST((CAST(SUBSTR(lc.splc_code, 2) AS INTEGER) * 765432191) % 100000 AS REAL) / 100000.0
      ) * 55.0) < -95.0 THEN 'Southwest'
      WHEN -125.0 + (ABS(
        CAST((CAST(SUBSTR(lc.splc_code, 2) AS INTEGER) * 765432191) % 100000 AS REAL) / 100000.0
      ) * 55.0) < -85.0 THEN 'Midwest'
      WHEN 25.0 + (ABS(
        CAST((CAST(SUBSTR(lc.splc_code, 2) AS INTEGER) * 982451653) % 100000 AS REAL) / 100000.0
      ) * 25.0) < 35.0 THEN 'Southeast'
      ELSE 'Northeast'
    END AS region
    
  FROM location_classification lc
  LEFT JOIN dwell_analysis da ON lc.splc_code = da.splc_code
)

SELECT
  ROW_NUMBER() OVER (ORDER BY splc_code) AS location_id,
  splc_code,
  location_name,
  location_type,
  latitude,
  longitude,
  state_code,
  capacity_classification,
  shadow_yard_risk_score,
  region
FROM location_enrichment
ORDER BY splc_code
