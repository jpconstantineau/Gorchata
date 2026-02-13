{{ config "materialized" "table" }}

-- Train Dimension
-- All unique train consists extracted from CLM events
-- Classified by train type with priority levels and typical car counts
-- PSR-optimized flag indicates trains operating under mature PSR principles

WITH distinct_trains AS (
  -- Get all unique trains from raw CLM events
  SELECT DISTINCT
    train_id
  FROM {{ seed "raw_clm_events" }}
),

train_classification AS (
  -- Classify trains by type and attributes based on train ID patterns
  SELECT
    train_id,
    train_id AS train_number,  -- Use train_id as the natural key
    
    -- Train type (derive from hash-based distribution)
    -- Manifest: 40%, Intermodal: 30%, Unit: 20%, Autorack: 10%
    CASE
      WHEN (ABS(CAST(SUBSTR(train_id, -4) AS INTEGER) * 982451653) % 100) < 40 THEN 'manifest'
      WHEN (ABS(CAST(SUBSTR(train_id, -4) AS INTEGER) * 982451653) % 100) < 70 THEN 'intermodal'
      WHEN (ABS(CAST(SUBSTR(train_id, -4) AS INTEGER) * 982451653) % 100) < 90 THEN 'unit'
      ELSE 'autorack'
    END AS train_type,
    
    -- Priority level (1-5, where 1 is highest)
    -- Intermodal and autorack typically higher priority (1-2)
    -- Manifest medium priority (2-4)
    -- Unit trains lower priority (3-5)
    CASE
      WHEN (ABS(CAST(SUBSTR(train_id, -4) AS INTEGER) * 982451653) % 100) < 40 THEN
        -- Manifest: priority 2-4
        2 + (ABS(CAST(SUBSTR(train_id, -4) AS INTEGER) * 765432191) % 3)
      WHEN (ABS(CAST(SUBSTR(train_id, -4) AS INTEGER) * 982451653) % 100) < 70 THEN
        -- Intermodal: priority 1-2
        1 + (ABS(CAST(SUBSTR(train_id, -4) AS INTEGER) * 765432191) % 2)
      WHEN (ABS(CAST(SUBSTR(train_id, -4) AS INTEGER) * 982451653) % 100) < 90 THEN
        -- Unit: priority 3-5
        3 + (ABS(CAST(SUBSTR(train_id, -4) AS INTEGER) * 765432191) % 3)
      ELSE
        -- Autorack: priority 1-2
        1 + (ABS(CAST(SUBSTR(train_id, -4) AS INTEGER) * 765432191) % 2)
    END AS priority_level,
    
    -- Typical car count by train type
    CASE
      WHEN (ABS(CAST(SUBSTR(train_id, -4) AS INTEGER) * 982451653) % 100) < 40 THEN
        -- Manifest: 70-120 cars
        70 + (ABS(CAST(SUBSTR(train_id, -4) AS INTEGER) * 123456789) % 51)
      WHEN (ABS(CAST(SUBSTR(train_id, -4) AS INTEGER) * 982451653) % 100) < 70 THEN
        -- Intermodal: 100-150 cars (double-stack)
        100 + (ABS(CAST(SUBSTR(train_id, -4) AS INTEGER) * 123456789) % 51)
      WHEN (ABS(CAST(SUBSTR(train_id, -4) AS INTEGER) * 982451653) % 100) < 90 THEN
        -- Unit: 75-125 cars
        75 + (ABS(CAST(SUBSTR(train_id, -4) AS INTEGER) * 123456789) % 51)
      ELSE
        -- Autorack: 50-80 cars
        50 + (ABS(CAST(SUBSTR(train_id, -4) AS INTEGER) * 123456789) % 31)
    END AS typical_car_count,
    
    -- PSR optimized flag
    -- Higher priority trains (1-2) more likely to be PSR optimized
    -- Use hash to distribute, but favor high priority
    CASE
      WHEN (1 + (ABS(CAST(SUBSTR(train_id, -4) AS INTEGER) * 765432191) % 2)) <= 2 THEN
        -- High priority: 80% PSR optimized
        CASE WHEN (ABS(CAST(SUBSTR(train_id, -4) AS INTEGER) * 111111111) % 100) < 80 THEN 1 ELSE 0 END
      ELSE
        -- Lower priority: 40% PSR optimized
        CASE WHEN (ABS(CAST(SUBSTR(train_id, -4) AS INTEGER) * 111111111) % 100) < 40 THEN 1 ELSE 0 END
    END AS psr_optimized
    
  FROM distinct_trains
)

SELECT
  ROW_NUMBER() OVER (ORDER BY train_id) AS train_id,
  train_number,
  train_type,
  priority_level,
  typical_car_count,
  psr_optimized
FROM train_classification
ORDER BY train_number
