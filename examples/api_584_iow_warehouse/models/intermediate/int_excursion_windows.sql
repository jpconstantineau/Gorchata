-- Intermediate model: Excursion Window Grouping
-- Purpose: Group consecutive excursions into events using 15-minute gap tolerance
-- Logic: Use LAG window function to detect time gaps, assign event IDs, aggregate metrics
-- Output: One record per excursion event with start/end times, duration, and peak magnitude

WITH excursions AS (
    SELECT 
        excursion_id,
        reading_id,
        timestamp,
        tag_id,
        asset_key,
        parameter_type,
        measured_value,
        breached_limit_value,
        breach_type,
        excursion_magnitude,
        criticality_level,
        limit_key
    FROM {{ ref "int_iow_excursions" }}
),

-- Calculate time gaps between consecutive readings for same sensor
gap_detection AS (
    SELECT 
        *,
        LAG(timestamp) OVER (
            PARTITION BY tag_id, parameter_type 
            ORDER BY timestamp
        ) AS prev_timestamp,
        
        -- Calculate minutes since last excursion for same sensor/parameter
        CASE
            WHEN LAG(timestamp) OVER (PARTITION BY tag_id, parameter_type ORDER BY timestamp) IS NULL THEN NULL
            ELSE (julianday(timestamp) - julianday(LAG(timestamp) OVER (PARTITION BY tag_id, parameter_type ORDER BY timestamp))) * 24 * 60
        END AS minutes_since_prev
        
    FROM excursions
),

-- Identify event boundaries (gap > 15 minutes or first reading)
event_boundaries AS (
    SELECT 
        *,
        CASE
            WHEN prev_timestamp IS NULL THEN 1  -- First reading for this sensor
            WHEN minutes_since_prev > 15 THEN 1  -- Gap exceeds tolerance
            ELSE 0  -- Part of ongoing event
        END AS new_event_flag
    FROM gap_detection
),

-- Assign event IDs using cumulative sum of boundary flags
event_assignment AS (
    SELECT 
        *,
        SUM(new_event_flag) OVER (
            PARTITION BY tag_id, parameter_type 
            ORDER BY timestamp 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS event_sequence
    FROM event_boundaries
),

-- Generate unique event ID combining sensor and sequence
event_ids AS (
    SELECT 
        *,
        tag_id || '_' || parameter_type || '_' || CAST(event_sequence AS TEXT) AS excursion_event_id
    FROM event_assignment
),

-- Calculate criticality ranking for aggregation
criticality_ranked AS (
    SELECT 
        *,
        CASE criticality_level
            WHEN 'Critical' THEN 1
            WHEN 'Standard' THEN 2
            WHEN 'Informational' THEN 3
            ELSE 4
        END AS criticality_rank
    FROM event_ids
)

-- Aggregate by event: start/end times, duration, peak magnitude, reading count
SELECT 
    excursion_event_id,
    tag_id,
    asset_key,
    parameter_type,
    
    -- Use most restrictive criticality level in event
    (SELECT criticality_level 
     FROM criticality_ranked cr2 
     WHERE cr2.excursion_event_id = cr.excursion_event_id 
     ORDER BY criticality_rank ASC 
     LIMIT 1) AS criticality_level,
    
    MIN(timestamp) AS excursion_start_time,
    MAX(timestamp) AS excursion_end_time,
    
    -- Duration in minutes
    CAST((julianday(MAX(timestamp)) - julianday(MIN(timestamp))) * 24 * 60 AS INTEGER) AS duration_minutes,
    
    COUNT(*) AS reading_count,
    MAX(excursion_magnitude) AS peak_magnitude,
    AVG(excursion_magnitude) AS average_magnitude,
    
    -- Most common breach type (if tied, pick first alphabetically)
    (SELECT breach_type 
     FROM criticality_ranked cr2 
     WHERE cr2.excursion_event_id = cr.excursion_event_id 
     GROUP BY breach_type 
     ORDER BY COUNT(*) DESC, breach_type ASC 
     LIMIT 1) AS breach_type,
    
    -- Limit key from most restrictive excursion
    (SELECT limit_key 
     FROM criticality_ranked cr2 
     WHERE cr2.excursion_event_id = cr.excursion_event_id 
     ORDER BY criticality_rank ASC, excursion_magnitude DESC 
     LIMIT 1) AS limit_key

FROM criticality_ranked cr
GROUP BY excursion_event_id, tag_id, asset_key, parameter_type
ORDER BY excursion_start_time, tag_id
