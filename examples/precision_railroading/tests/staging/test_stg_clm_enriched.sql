-- Test: stg_clm_enriched Enrichment Validation
-- Description: Validates enriched staging layer with dimension lookups and derived fields
-- Expected Result: All tests should return 0 violations

WITH all_tests AS (
  -- Test 1: All SPLC codes resolve to locations
  SELECT
    'splc_codes_resolve' AS test_name,
    COUNT(*) AS violation_count,
    'All SPLC codes must resolve to dim_location' AS description
  FROM {{ ref "stg_clm_enriched" }}
  WHERE location_id IS NULL
  
  UNION ALL
  
  -- Test 2: All car numbers resolve to railcars
  SELECT
    'car_numbers_resolve' AS test_name,
    COUNT(*) AS violation_count,
    'All car numbers must resolve to dim_railcar' AS description
  FROM {{ ref "stg_clm_enriched" }}
  WHERE railcar_id IS NULL
  
  UNION ALL
  
  -- Test 3: All dates resolve to date dimension
  SELECT
    'dates_resolve' AS test_name,
    COUNT(*) AS violation_count,
    'All event dates must resolve to dim_date' AS description
  FROM {{ ref "stg_clm_enriched" }}
  WHERE date_id IS NULL
  
  UNION ALL
  
  -- Test 4: Temporal ordering per car (event_sequence matches timestamp order)
  SELECT
    'temporal_ordering' AS test_name,
    COUNT(*) AS violation_count,
    'Event sequence must match timestamp order per car' AS description
  FROM (
    SELECT
      car_number,
      timestamp,
      event_sequence,
      LAG(timestamp) OVER (PARTITION BY car_number ORDER BY event_sequence) AS prev_timestamp
    FROM {{ ref "stg_clm_enriched" }}
  )
  WHERE prev_timestamp IS NOT NULL 
    AND timestamp < prev_timestamp
  
  UNION ALL
  
  -- Test 5: is_loaded_event flag logic for PLAC
  SELECT
    'loaded_flag_plac' AS test_name,
    COUNT(*) AS violation_count,
    'PLAC events must have is_loaded_event = TRUE' AS description
  FROM {{ ref "stg_clm_enriched" }}
  WHERE event_type = 'PLAC' 
    AND (is_loaded_event IS NULL OR is_loaded_event != 1)
  
  UNION ALL
  
  -- Test 6: is_loaded_event flag logic for PULL
  SELECT
    'loaded_flag_pull' AS test_name,
    COUNT(*) AS violation_count,
    'PULL events must have is_loaded_event = FALSE' AS description
  FROM {{ ref "stg_clm_enriched" }}
  WHERE event_type = 'PULL' 
    AND (is_loaded_event IS NULL OR is_loaded_event != 0)
  
  UNION ALL
  
  -- Test 7: is_loaded_event NULL for movement events
  SELECT
    'loaded_flag_movement' AS test_name,
    COUNT(*) AS violation_count,
    'DEPA/ARRI events should have is_loaded_event = NULL' AS description
  FROM {{ ref "stg_clm_enriched" }}
  WHERE event_type IN ('DEPA', 'ARRI')
    AND is_loaded_event IS NOT NULL
  
  UNION ALL
  
  -- Test 8: is_movement_event flag for DEPA/ARRI
  SELECT
    'movement_flag_depa_arri' AS test_name,
    COUNT(*) AS violation_count,
    'DEPA/ARRI events must have is_movement_event = TRUE' AS description
  FROM {{ ref "stg_clm_enriched" }}
  WHERE event_type IN ('DEPA', 'ARRI')
    AND (is_movement_event IS NULL OR is_movement_event != 1)
  
  UNION ALL
  
  -- Test 9: is_movement_event flag for PLAC/PULL
  SELECT
    'movement_flag_plac_pull' AS test_name,
    COUNT(*) AS violation_count,
    'PLAC/PULL events must have is_movement_event = FALSE' AS description
  FROM {{ ref "stg_clm_enriched" }}
  WHERE event_type IN ('PLAC', 'PULL')
    AND (is_movement_event IS NULL OR is_movement_event != 0)
  
  UNION ALL
  
  -- Test 10: Event sequence starts at 1 per car
  SELECT
    'event_sequence_starts_at_one' AS test_name,
    COUNT(*) AS violation_count,
    'First event per car must have event_sequence = 1' AS description
  FROM (
    SELECT car_number, MIN(event_sequence) AS min_seq
    FROM {{ ref "stg_clm_enriched" }}
    GROUP BY car_number
  )
  WHERE min_seq != 1
  
  UNION ALL
  
  -- Test 11: Event sequence is contiguous per car
  SELECT
    'event_sequence_contiguous' AS test_name,
    COUNT(*) AS violation_count,
    'Event sequences must be contiguous without gaps per car' AS description
  FROM (
    SELECT
      car_number,
      event_sequence,
      LAG(event_sequence) OVER (PARTITION BY car_number ORDER BY event_sequence) AS prev_seq
    FROM {{ ref "stg_clm_enriched" }}
  )
  WHERE prev_seq IS NOT NULL 
    AND event_sequence != prev_seq + 1
  
  UNION ALL
  
  -- Test 12: Location type is populated when location_id exists
  SELECT
    'location_type_populated' AS test_name,
    COUNT(*) AS violation_count,
    'Location type must be populated when location_id exists' AS description
  FROM {{ ref "stg_clm_enriched" }}
  WHERE location_id IS NOT NULL 
    AND (location_type IS NULL OR TRIM(location_type) = '')
  
  UNION ALL
  
  -- Test 13: Railroad owner is populated when railcar_id exists
  SELECT
    'railroad_owner_populated' AS test_name,
    COUNT(*) AS violation_count,
    'Railroad owner must be populated when railcar_id exists' AS description
  FROM {{ ref "stg_clm_enriched" }}
  WHERE railcar_id IS NOT NULL 
    AND (railroad_owner IS NULL OR TRIM(railroad_owner) = '')
  
  UNION ALL
  
  -- Test 14: PSR period is populated when date_id exists
  SELECT
    'psr_period_populated' AS test_name,
    COUNT(*) AS violation_count,
    'PSR period must be populated when date_id exists' AS description
  FROM {{ ref "stg_clm_enriched" }}
  WHERE date_id IS NOT NULL 
    AND (psr_period IS NULL OR TRIM(psr_period) = '')
  
  UNION ALL
  
  -- Test 15: Latitude/longitude valid ranges
  SELECT
    'valid_coordinates' AS test_name,
    COUNT(*) AS violation_count,
    'Coordinates must be within valid ranges when populated' AS description
  FROM {{ ref "stg_clm_enriched" }}
  WHERE (latitude IS NOT NULL AND (latitude < -90 OR latitude > 90))
     OR (longitude IS NOT NULL AND (longitude < -180 OR longitude > 180))
)

-- Return all violations
SELECT
  test_name,
  violation_count,
  description
FROM all_tests
WHERE violation_count > 0
ORDER BY test_name
