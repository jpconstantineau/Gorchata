-- Custom Generic Test: Valid Timestamp
-- Purpose: Verify timestamps are within valid range (not in future, not before system epoch)
-- Usage in schema.yml:
--   data_tests:
--     - valid_timestamp:
--         min_date: '2020-01-01'
--         max_date: '2030-12-31'

-- This test checks that timestamp values are within a reasonable range
-- Default range: 2020-01-01 to 2030-12-31 (configurable via min_date/max_date)

SELECT
  {{ .ModelName }}.{{ .ColumnName }},
  COUNT(*) as invalid_count
FROM {{ .ModelName }}
WHERE 
  -- Timestamp is before minimum valid date
  {{ .ColumnName }} < '{{ .Config.Args.min_date | default "2020-01-01" }}'
  -- Timestamp is after maximum valid date (future)
  OR {{ .ColumnName }} > '{{ .Config.Args.max_date | default "2030-12-31" }}'
  -- Timestamp is NULL (should be caught by not_null test, but checking here too)
  OR {{ .ColumnName }} IS NULL
GROUP BY {{ .ModelName }}.{{ .ColumnName }}
ORDER BY {{ .ModelName }}.{{ .ColumnName }}
