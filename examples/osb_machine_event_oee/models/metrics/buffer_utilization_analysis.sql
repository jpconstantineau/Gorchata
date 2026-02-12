{{ config "materialized" "table" }}

-- Buffer Utilization Analysis
-- Purpose: Track buffer inventory levels over time and identify periods of high/low utilization
-- Dependencies: dim_production_area, dim_equipment, stg_equipment_state_history
-- Output: Buffer-level metrics for capacity planning and starvation/blocking prevention

WITH production_flow AS (
    -- Define material flow relationships between production areas
    SELECT 
        p1.area_id AS upstream_area_id,
        p1.area_name AS upstream_area_name,
        p2.area_id AS downstream_area_id,
        p2.area_name AS downstream_area_name,
        p1.area_name || ' to ' || p2.area_name AS buffer_name,
        p2.buffer_capacity_hours
    FROM dim_production_area p1
    INNER JOIN dim_production_area p2 
        ON p1.downstream_area_id = p2.area_id
    WHERE p2.buffer_capacity_hours > 0
),

equipment_rates AS (
    -- Get rated capacity for each equipment
    SELECT 
        e.equipment_id,
        e.production_area,
        e.rated_capacity_units_hr
    FROM dim_equipment e
),

time_periods AS (
    -- Generate hourly time periods for buffer level simulation
    SELECT DISTINCT
        date_id,
        CAST((julianday(state_end_timestamp) - julianday(state_start_timestamp)) * 24 AS INTEGER) AS hours_in_period
    FROM stg_equipment_state_history
),

upstream_flow AS (
    -- Calculate inflow rate from upstream equipment
    SELECT 
        pf.buffer_name,
        pf.upstream_area_id,
        pf.downstream_area_id,
        pf.buffer_capacity_hours,
        s.date_id,
        SUM(CASE WHEN s.machine_state = 'Running' 
            THEN s.state_duration_min / 60.0 * er.rated_capacity_units_hr 
            ELSE 0 
        END) AS total_inflow_tons,
        SUM(CASE WHEN s.machine_state = 'Running' 
            THEN s.state_duration_min / 60.0 
            ELSE 0 
        END) AS total_inflow_hours
    FROM production_flow pf
    INNER JOIN equipment_rates er 
        ON pf.upstream_area_id = er.production_area
    INNER JOIN stg_equipment_state_history s 
        ON er.equipment_id = s.equipment_id
    GROUP BY 
        pf.buffer_name,
        pf.upstream_area_id,
        pf.downstream_area_id,
        pf.buffer_capacity_hours,
        s.date_id
),

downstream_flow AS (
    -- Calculate outflow rate to downstream equipment
    SELECT 
        pf.buffer_name,
        s.date_id,
        SUM(CASE WHEN s.machine_state = 'Running' 
            THEN s.state_duration_min / 60.0 * er.rated_capacity_units_hr 
            ELSE 0 
        END) AS total_outflow_tons,
        SUM(CASE WHEN s.machine_state = 'Running' 
            THEN s.state_duration_min / 60.0 
            ELSE 0 
        END) AS total_outflow_hours
    FROM production_flow pf
    INNER JOIN equipment_rates er 
        ON pf.downstream_area_id = er.production_area
    INNER JOIN stg_equipment_state_history s 
        ON er.equipment_id = s.equipment_id
    GROUP BY 
        pf.buffer_name,
        s.date_id
),

buffer_capacity_calc AS (
    -- Calculate buffer capacity in tons based on average upstream rated capacity
    SELECT 
        pf.buffer_name,
        pf.buffer_capacity_hours,
        pf.buffer_capacity_hours * AVG(er.rated_capacity_units_hr) AS buffer_capacity_tons
    FROM production_flow pf
    INNER JOIN equipment_rates er 
        ON pf.upstream_area_id = er.production_area
    GROUP BY 
        pf.buffer_name,
        pf.buffer_capacity_hours
),

state_transitions AS (
    -- Get all state change times to create buffer level observation points
    SELECT DISTINCT
        pf.buffer_name,
        s.date_id,
        s.state_start_timestamp AS transition_time
    FROM production_flow pf
    INNER JOIN equipment_rates er 
        ON pf.upstream_area_id = er.production_area 
            OR pf.downstream_area_id = er.production_area
    INNER JOIN stg_equipment_state_history s 
        ON er.equipment_id = s.equipment_id
    
    UNION
    
    SELECT DISTINCT
        pf.buffer_name,
        s.date_id,
        s.state_end_timestamp AS transition_time
    FROM production_flow pf
    INNER JOIN equipment_rates er 
        ON pf.upstream_area_id = er.production_area 
            OR pf.downstream_area_id = er.production_area
    INNER JOIN stg_equipment_state_history s 
        ON er.equipment_id = s.equipment_id
),

buffer_level_at_transitions AS (
    -- Calculate cumulative inflow/outflow up to each transition point
    SELECT 
        st.buffer_name,
        st.date_id,
        st.transition_time,
        bc.buffer_capacity_hours,
        bc.buffer_capacity_tons,
        -- Cumulative inflow up to this point
        COALESCE(SUM(CASE 
            WHEN s_up.machine_state = 'Running' 
            AND s_up.state_end_timestamp <= st.transition_time
            THEN (s_up.state_duration_min / 60.0) * er_up.rated_capacity_units_hr 
            ELSE 0 
        END), 0.0) AS cumulative_inflow_tons,
        -- Cumulative outflow up to this point
        COALESCE(SUM(CASE 
            WHEN s_down.machine_state = 'Running' 
            AND s_down.state_end_timestamp <= st.transition_time
            THEN (s_down.state_duration_min / 60.0) * er_down.rated_capacity_units_hr 
            ELSE 0 
        END), 0.0) AS cumulative_outflow_tons
    FROM state_transitions st
    INNER JOIN production_flow pf 
        ON st.buffer_name = pf.buffer_name
    INNER JOIN buffer_capacity_calc bc 
        ON pf.buffer_name = bc.buffer_name
    LEFT JOIN equipment_rates er_up 
        ON pf.upstream_area_id = er_up.production_area
    LEFT JOIN stg_equipment_state_history s_up 
        ON er_up.equipment_id = s_up.equipment_id 
        AND st.date_id = s_up.date_id
    LEFT JOIN equipment_rates er_down 
        ON pf.downstream_area_id = er_down.production_area
    LEFT JOIN stg_equipment_state_history s_down 
        ON er_down.equipment_id = s_down.equipment_id 
        AND st.date_id = s_down.date_id
    GROUP BY 
        st.buffer_name,
        st.date_id,
        st.transition_time,
        bc.buffer_capacity_hours,
        bc.buffer_capacity_tons
),

buffer_level_pct AS (
    -- Calculate buffer level percentage at each transition
    SELECT 
        buffer_name,
        buffer_capacity_hours,
        date_id,
        transition_time,
        buffer_capacity_tons,
        cumulative_inflow_tons - cumulative_outflow_tons AS net_change_tons,
        -- Start at 50%, adjust by net change
        MIN(150.0, MAX(0.0, 
            50.0 + ((cumulative_inflow_tons - cumulative_outflow_tons) / NULLIF(buffer_capacity_tons, 0) * 100.0)
        )) AS buffer_level_pct
    FROM buffer_level_at_transitions
    WHERE buffer_capacity_tons > 0
),

buffer_utilization AS (
    -- Aggregate buffer metrics per day
    SELECT 
        buffer_name,
        date_id AS analysis_date,
        ROUND(AVG(buffer_level_pct), 1) AS avg_buffer_level_pct,
        ROUND(MIN(buffer_level_pct), 1) AS min_buffer_level_pct,
        ROUND(MAX(buffer_level_pct), 1) AS max_buffer_level_pct,
        ROUND(SUM(CASE WHEN buffer_level_pct > 90.0 THEN 1.0 ELSE 0 END) * 
            (MAX(julianday(transition_time)) - MIN(julianday(transition_time))) * 24.0 / COUNT(*), 1) AS hours_above_90_pct,
        ROUND(SUM(CASE WHEN buffer_level_pct < 10.0 THEN 1.0 ELSE 0 END) * 
            (MAX(julianday(transition_time)) - MIN(julianday(transition_time))) * 24.0 / COUNT(*), 1) AS hours_below_10_pct,
        ROUND((MAX(julianday(transition_time)) - MIN(julianday(transition_time))) * 24.0, 1) AS total_hours_analyzed,
        ROUND(AVG(buffer_capacity_hours), 1) AS buffer_capacity_hours
    FROM buffer_level_pct
    GROUP BY 
        buffer_name,
        date_id
)

SELECT * FROM buffer_utilization
ORDER BY analysis_date, buffer_name
