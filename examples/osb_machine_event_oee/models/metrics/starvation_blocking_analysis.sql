{{ config "materialized" "table" }}

-- Starvation and Blocking Analysis
-- Purpose: Identify equipment starvation (no material) and blocking (no space) events with root cause analysis
-- Dependencies: stg_equipment_state_history, dim_equipment, dim_production_area
-- Output: Equipment-level starvation/blocking metrics with upstream/downstream causal relationships

WITH equipment_area_mapping AS (
    -- Map equipment to production areas and flow relationships
    SELECT 
        e.equipment_id,
        e.equipment_name,
        e.production_area,
        pa.upstream_area_id,
        pa.downstream_area_id,
        pa.sequence_order
    FROM dim_equipment e
    INNER JOIN dim_production_area pa 
        ON e.production_area = pa.area_id
),

starvation_events AS (
    -- Identify starvation events (downstream waiting for material from upstream)
    SELECT 
        s.equipment_id,
        s.date_id,
        COUNT(*) AS event_count,
        SUM(s.state_duration_min) AS total_duration_min,
        MIN(s.state_start_timestamp) AS first_event_time,
        MAX(s.state_end_timestamp) AS last_event_time
    FROM stg_equipment_state_history s
    WHERE s.machine_state = 'Starved'
    GROUP BY 
        s.equipment_id,
        s.date_id
),

blocking_events AS (
    -- Identify blocking events (upstream waiting for space in downstream)
    SELECT 
        s.equipment_id,
        s.date_id,
        COUNT(*) AS event_count,
        SUM(s.state_duration_min) AS total_duration_min,
        MIN(s.state_start_timestamp) AS first_event_time,
        MAX(s.state_end_timestamp) AS last_event_time
    FROM stg_equipment_state_history s
    WHERE s.machine_state = 'Blocked'
    GROUP BY 
        s.equipment_id,
        s.date_id
),

upstream_downtime AS (
    -- Find upstream equipment downtime that could cause starvation
    SELECT 
        downstream.equipment_id AS affected_equipment,
        upstream_equip.equipment_id AS causing_equipment,
        s.date_id,
        SUM(s.state_duration_min) AS upstream_downtime_min
    FROM equipment_area_mapping downstream
    INNER JOIN equipment_area_mapping upstream_equip 
        ON downstream.upstream_area_id = upstream_equip.production_area
    INNER JOIN stg_equipment_state_history s 
        ON upstream_equip.equipment_id = s.equipment_id
    WHERE s.machine_state IN ('Unplanned Downtime', 'Planned Downtime')
    GROUP BY 
        downstream.equipment_id,
        upstream_equip.equipment_id,
        s.date_id
),

downstream_downtime AS (
    -- Find downstream equipment downtime that could cause blocking
    SELECT 
        upstream.equipment_id AS affected_equipment,
        downstream_equip.equipment_id AS causing_equipment,
        s.date_id,
        SUM(s.state_duration_min) AS downstream_downtime_min
    FROM equipment_area_mapping upstream
    INNER JOIN equipment_area_mapping downstream_equip 
        ON upstream.downstream_area_id = downstream_equip.production_area
    INNER JOIN stg_equipment_state_history s 
        ON downstream_equip.equipment_id = s.equipment_id
    WHERE s.machine_state IN ('Unplanned Downtime', 'Planned Downtime')
    GROUP BY 
        upstream.equipment_id,
        downstream_equip.equipment_id,
        s.date_id
),

starvation_with_cause AS (
    -- Correlate starvation events with upstream downtime
    SELECT 
        se.equipment_id,
        se.date_id,
        se.event_count AS starved_event_count,
        se.total_duration_min AS total_starved_time_min,
        ud.causing_equipment AS upstream_equipment_causing_starvation,
        ud.upstream_downtime_min
    FROM starvation_events se
    LEFT JOIN upstream_downtime ud 
        ON se.equipment_id = ud.affected_equipment
        AND se.date_id = ud.date_id
),

blocking_with_cause AS (
    -- Correlate blocking events with downstream downtime
    SELECT 
        be.equipment_id,
        be.date_id,
        be.event_count AS blocked_event_count,
        be.total_duration_min AS total_blocked_time_min,
        dd.causing_equipment AS downstream_equipment_causing_blocking,
        dd.downstream_downtime_min
    FROM blocking_events be
    LEFT JOIN downstream_downtime dd 
        ON be.equipment_id = dd.affected_equipment
        AND be.date_id = dd.date_id
),

combined_analysis AS (
    -- Combine starvation and blocking analysis
    SELECT 
        COALESCE(sc.equipment_id, bc.equipment_id) AS equipment_id,
        COALESCE(sc.date_id, bc.date_id) AS analysis_date,
        COALESCE(sc.starved_event_count, 0) AS starved_event_count,
        COALESCE(sc.total_starved_time_min, 0.0) AS total_starved_time_min,
        sc.upstream_equipment_causing_starvation,
        COALESCE(bc.blocked_event_count, 0) AS blocked_event_count,
        COALESCE(bc.total_blocked_time_min, 0.0) AS total_blocked_time_min,
        bc.downstream_equipment_causing_blocking,
        -- Identify root cause (equipment causing the most impact)
        CASE 
            WHEN COALESCE(sc.total_starved_time_min, 0) > COALESCE(bc.total_blocked_time_min, 0) 
            THEN sc.upstream_equipment_causing_starvation
            WHEN COALESCE(bc.total_blocked_time_min, 0) > 0 
            THEN bc.downstream_equipment_causing_blocking
            ELSE NULL
        END AS root_cause_equipment
    FROM starvation_with_cause sc
    FULL OUTER JOIN blocking_with_cause bc 
        ON sc.equipment_id = bc.equipment_id
        AND sc.date_id = bc.date_id
)

SELECT 
    equipment_id,
    SUM(starved_event_count) AS starved_event_count,
    SUM(blocked_event_count) AS blocked_event_count,
    SUM(total_starved_time_min) AS total_starved_time_min,
    SUM(total_blocked_time_min) AS total_blocked_time_min,
    -- Use most frequent upstream cause
    (SELECT upstream_equipment_causing_starvation 
     FROM combined_analysis ca2 
     WHERE ca2.equipment_id = ca.equipment_id 
       AND ca2.upstream_equipment_causing_starvation IS NOT NULL
     GROUP BY upstream_equipment_causing_starvation
     ORDER BY SUM(total_starved_time_min) DESC
     LIMIT 1) AS upstream_equipment_causing_starvation,
    -- Use most frequent downstream cause
    (SELECT downstream_equipment_causing_blocking 
     FROM combined_analysis ca3 
     WHERE ca3.equipment_id = ca.equipment_id 
       AND ca3.downstream_equipment_causing_blocking IS NOT NULL
     GROUP BY downstream_equipment_causing_blocking
     ORDER BY SUM(total_blocked_time_min) DESC
     LIMIT 1) AS downstream_equipment_causing_blocking,
    -- Use most frequent root cause
    (SELECT root_cause_equipment 
     FROM combined_analysis ca4 
     WHERE ca4.equipment_id = ca.equipment_id 
       AND ca4.root_cause_equipment IS NOT NULL
     GROUP BY root_cause_equipment
     ORDER BY COUNT(*) DESC
     LIMIT 1) AS root_cause_equipment
FROM combined_analysis ca
GROUP BY equipment_id
ORDER BY equipment_id
