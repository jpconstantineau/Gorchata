{{ config "materialized" "table" }}

-- Intermediate: Dwell Classification
-- Purpose: Classify dwell events by operational signature
-- Grain: One row per dwell event with classification
-- Source: int_nodal_dwell + dim_location

WITH nodal_dwell AS (
  SELECT
    dwell_id,
    railcar_id,
    location_id,
    dwell_start_timestamp,
    dwell_end_timestamp,
    dwell_duration_minutes,
    event_type_at_arrival,
    event_type_at_departure,
    is_loaded
  FROM {{ ref "int_nodal_dwell" }}
),

-- Join to dim_location for facility metadata
dwell_with_location AS (
  SELECT
    nd.*,
    l.location_type,
    l.shadow_yard_risk_score,
    l.location_name,
    l.capacity_classification
  FROM nodal_dwell nd
  INNER JOIN dim_location l ON nd.location_id = l.location_id
),

-- Apply classification rules (hierarchical, first match wins)
classified_dwell AS (
  SELECT
    dwell_id,
    railcar_id,
    location_id,
    dwell_start_timestamp,
    dwell_end_timestamp,
    dwell_duration_minutes,
    event_type_at_arrival,
    event_type_at_departure,
    is_loaded,
    location_type AS facility_type,
    shadow_yard_risk_score,
    
    -- Shadow yard detection (highest priority)
    CASE 
      WHEN shadow_yard_risk_score > 50
        AND dwell_duration_minutes BETWEEN 120 AND 1440
      THEN 1
      ELSE 0
    END AS shadow_yard_flag,
    
    -- Classification hierarchy (shadow yard takes precedence)
    CASE
      -- 1. Shadow yard holds (highest priority)
      WHEN shadow_yard_risk_score > 50
        AND dwell_duration_minutes BETWEEN 120 AND 1440
      THEN 'shadow_yard_hold'
      
      -- 2. Terminal operations (8-48 hours at terminals)
      WHEN location_type = 'terminal'
        AND dwell_duration_minutes BETWEEN 480 AND 2880
      THEN 'terminal'
      
      -- 3. Crew changes (1-4 hours at crew bases)
      -- Note: dim_location uses 'interchange' for crew change locations
      WHEN location_type = 'interchange'
        AND dwell_duration_minutes BETWEEN 60 AND 240
      THEN 'crew_change'
      
      -- 4. Mainline holds (0.5-6 hours at sidings)
      WHEN location_type = 'siding'
        AND dwell_duration_minutes BETWEEN 30 AND 360
      THEN 'mainline_hold'
      
      -- 5. Maintenance (>6 hours at repair facilities)
      -- Note: dim_location doesn't have 'repair_facility', using 'yard' with long dwell
      WHEN location_type = 'yard'
        AND dwell_duration_minutes > 360
      THEN 'maintenance'
      
      -- 6. Unclassified (everything else)
      ELSE 'unclassified'
    END AS dwell_classification
    
  FROM dwell_with_location
)

SELECT
  dwell_id,
  railcar_id,
  location_id,
  dwell_start_timestamp,
  dwell_end_timestamp,
  dwell_duration_minutes,
  event_type_at_arrival,
  event_type_at_departure,
  is_loaded,
  dwell_classification,
  shadow_yard_flag,
  facility_type
FROM classified_dwell
