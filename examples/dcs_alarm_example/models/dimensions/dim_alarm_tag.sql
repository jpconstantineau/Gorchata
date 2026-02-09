{{ config "materialized" "table" }}

-- SCD Type 2 Alarm Tag Dimension
-- Tracks alarm tag configurations with version history support

WITH source_data AS (
  SELECT
    tag_id,
    tag_name,
    tag_description,
    alarm_type,
    equipment_id,
    area_code,
    is_safety_critical,
    is_active,
    1 AS version_num,
    '2026-01-01' AS valid_from,
    '9999-12-31' AS valid_to,
    1 AS is_current,
    ROW_NUMBER() OVER (ORDER BY tag_id) AS row_num
  FROM {{ seed "raw_alarm_config" }}
)
SELECT
  (row_num * 1000 + version_num) AS tag_key,
  tag_id,
  tag_name,
  tag_description,
  alarm_type,
  equipment_id,
  area_code,
  is_safety_critical,
  is_active,
  valid_from,
  valid_to,
  is_current
FROM source_data
ORDER BY tag_id
