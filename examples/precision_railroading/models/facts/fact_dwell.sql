{{ config "materialized" "table" }}

-- Fact: Dwell
-- Purpose: Grain-level fact table for stop events with classifications
-- Grain: One row per dwell event
-- Source: int_dwell_classification with dimension joins

WITH dwell_classification AS (
  SELECT
    dwell_id,
    railcar_id,
    location_id,
    dwell_start_timestamp,
    dwell_end_timestamp,
    dwell_duration_minutes,
    facility_type,
    dwell_classification,
    shadow_yard_flag,
    is_loaded,
    event_type_at_arrival,
    event_type_at_departure
  FROM {{ ref "int_dwell_classification" }}
)

-- Final fact table assembly with all dimensions and measures
SELECT
  dwell_id,
  railcar_id,
  location_id,
  -- Extract date key from timestamp for dim_date FK
  CAST(STRFTIME('%Y%m%d', dwell_start_timestamp) AS INTEGER) AS dwell_start_date_id,
  dwell_start_timestamp,
  dwell_end_timestamp,
  dwell_duration_minutes,
  facility_type,
  dwell_classification,
  shadow_yard_flag,
  is_loaded,
  event_type_at_arrival,
  event_type_at_departure,
  -- Derive PSR period from timestamp
  CASE
    WHEN CAST(STRFTIME('%Y', dwell_start_timestamp) AS INTEGER) <= 2017 THEN 'pre-PSR'
    WHEN CAST(STRFTIME('%Y', dwell_start_timestamp) AS INTEGER) BETWEEN 2018 AND 2020 THEN 'transition'
    ELSE 'mature'
  END AS psr_period
FROM dwell_classification
