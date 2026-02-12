{{ config "materialized" "table" }}

-- Constraint Analysis (Theory of Constraints / Bottleneck Identification)
-- Purpose: Identify system constraints, analyze throughput, quantify capacity gaps, and recommend buffer sizing
-- Dependencies: stg_equipment_state_history, dim_equipment, dim_production_area
-- Output: Multi-analysis view supporting constraint identification, throughput calculation, capacity gap analysis, and buffer sizing

WITH equipment_utilization AS (
    -- Calculate utilization for each equipment (% of time spent running)
    SELECT 
        s.equipment_id,
        s.date_id,
        SUM(CASE WHEN s.machine_state = 'Running' THEN s.state_duration_min ELSE 0 END) AS operating_time_min,
        SUM(s.state_duration_min) AS total_time_min,
        CAST(SUM(CASE WHEN s.machine_state = 'Running' THEN s.state_duration_min ELSE 0 END) AS REAL) / 
            CAST(SUM(s.state_duration_min) AS REAL) * 100.0 AS utilization_pct
    FROM stg_equipment_state_history s
    GROUP BY 
        s.equipment_id,
        s.date_id
),

downstream_starvation AS (
    -- Calculate starvation caused in downstream equipment
    SELECT 
        upstream_map.equipment_id AS causing_equipment_id,
        COUNT(DISTINCT s.equipment_id) AS equipment_starved_count,
        SUM(s.state_duration_min) / 60.0 AS downstream_starvation_hours
    FROM stg_equipment_state_history s
    INNER JOIN dim_equipment e 
        ON s.equipment_id = e.equipment_id
    INNER JOIN dim_production_area pa 
        ON e.production_area = pa.area_id
    INNER JOIN dim_equipment upstream_map 
        ON pa.upstream_area_id = upstream_map.production_area
    WHERE s.machine_state = 'Starved'
    GROUP BY upstream_map.equipment_id
),

constraint_scoring AS (
    -- Score equipment as potential constraint
    -- Constraint = high utilization + causing downstream starvation
    SELECT 
        eu.equipment_id,
        e.equipment_name,
        e.criticality_level,
        eu.utilization_pct,
        COALESCE(ds.downstream_starvation_hours, 0.0) AS downstream_starvation_hours,
        COALESCE(ds.equipment_starved_count, 0) AS equipment_starved_count,
        -- Constraint score = utilization × (1 + downstream_impact)
        eu.utilization_pct * (1.0 + COALESCE(ds.downstream_starvation_hours, 0.0) / 100.0) AS constraint_score
    FROM equipment_utilization eu
    INNER JOIN dim_equipment e 
        ON eu.equipment_id = e.equipment_id
    LEFT JOIN downstream_starvation ds 
        ON eu.equipment_id = ds.causing_equipment_id
    WHERE eu.date_id = (SELECT MAX(date_id) FROM equipment_utilization)
),

constraint_identification AS (
    -- Identify system constraint (highest constraint score)
    SELECT 
        'Constraint Identification' AS analysis_type,
        NULL AS analysis_date,
        equipment_id,
        equipment_name,
        utilization_pct,
        downstream_starvation_hours,
        constraint_score,
        CASE 
            WHEN constraint_score = (SELECT MAX(constraint_score) FROM constraint_scoring) 
            THEN 1 
            ELSE 0 
        END AS is_system_constraint,
        NULL AS buffer_name,
        NULL AS current_capacity_hours,
        NULL AS blocked_hours_observed,
        NULL AS recommended_capacity_hours,
        NULL AS estimated_blocking_reduction_hours,
        NULL AS plant_throughput_tons,
        NULL AS constraint_equipment,
        NULL AS constraint_capacity_tons_hr,
        NULL AS constraint_utilization_pct,
        NULL AS demand_tons,
        NULL AS actual_throughput_tons,
        NULL AS capacity_gap_tons,
        NULL AS capacity_gap_pct,
        NULL AS estimated_revenue_loss_usd
    FROM constraint_scoring
),

blocking_analysis AS (
    -- Find equipment with significant blocking time for buffer sizing recommendations
    SELECT 
        s.equipment_id,
        e.production_area,
        pa.downstream_area_id,
        pa_downstream.area_name AS downstream_area_name,
        pa_downstream.buffer_capacity_hours AS current_buffer_capacity,
        SUM(s.state_duration_min) / 60.0 AS total_blocked_hours
    FROM stg_equipment_state_history s
    INNER JOIN dim_equipment e 
        ON s.equipment_id = e.equipment_id
    INNER JOIN dim_production_area pa 
        ON e.production_area = pa.area_id
    LEFT JOIN dim_production_area pa_downstream 
        ON pa.downstream_area_id = pa_downstream.area_id
    WHERE s.machine_state = 'Blocked'
    GROUP BY 
        s.equipment_id,
        e.production_area,
        pa.downstream_area_id,
        pa_downstream.area_name,
        pa_downstream.buffer_capacity_hours
    HAVING total_blocked_hours > 1.0
),

buffer_sizing_recommendations AS (
    -- Recommend buffer capacity increases to reduce blocking
    SELECT 
        'Buffer Sizing' AS analysis_type,
        NULL AS analysis_date,
        NULL AS equipment_id,
        NULL AS equipment_name,
        NULL AS utilization_pct,
        NULL AS downstream_starvation_hours,
        NULL AS constraint_score,
        NULL AS is_system_constraint,
        ba.production_area || ' to ' || ba.downstream_area_name AS buffer_name,
        ba.current_buffer_capacity AS current_capacity_hours,
        ba.total_blocked_hours AS blocked_hours_observed,
        -- Recommend capacity = current + (blocked_hours / 2)
        ba.current_buffer_capacity + (ba.total_blocked_hours / 2.0) AS recommended_capacity_hours,
        ba.total_blocked_hours * 0.6 AS estimated_blocking_reduction_hours,
        NULL AS plant_throughput_tons,
        NULL AS constraint_equipment,
        NULL AS constraint_capacity_tons_hr,
        NULL AS constraint_utilization_pct,
        NULL AS demand_tons,
        NULL AS actual_throughput_tons,
        NULL AS capacity_gap_tons,
        NULL AS capacity_gap_pct,
        NULL AS estimated_revenue_loss_usd
    FROM blocking_analysis ba
),

throughput_calculation AS (
    -- Calculate plant throughput limited by constraint
    SELECT 
        'Throughput Calculation' AS analysis_type,
        eu.date_id AS analysis_date,
        NULL AS equipment_id,
        NULL AS equipment_name,
        NULL AS utilization_pct,
        NULL AS downstream_starvation_hours,
        NULL AS constraint_score,
        NULL AS is_system_constraint,
        NULL AS buffer_name,
        NULL AS current_capacity_hours,
        NULL AS blocked_hours_observed,
        NULL AS recommended_capacity_hours,
        NULL AS estimated_blocking_reduction_hours,
        -- Throughput = constraint capacity × constraint utilization × operating hours
        e.rated_capacity_units_hr * (eu.operating_time_min / 60.0) AS plant_throughput_tons,
        cs.equipment_id AS constraint_equipment,
        e.rated_capacity_units_hr AS constraint_capacity_tons_hr,
        cs.utilization_pct AS constraint_utilization_pct,
        NULL AS demand_tons,
        NULL AS actual_throughput_tons,
        NULL AS capacity_gap_tons,
        NULL AS capacity_gap_pct,
        NULL AS estimated_revenue_loss_usd
    FROM equipment_utilization eu
    INNER JOIN constraint_scoring cs 
        ON eu.equipment_id = cs.equipment_id
    INNER JOIN dim_equipment e 
        ON eu.equipment_id = e.equipment_id
    WHERE cs.constraint_score = (SELECT MAX(constraint_score) FROM constraint_scoring)
),

capacity_gap_analysis AS (
    -- Quantify gap between demand and actual throughput
    -- Note: Only returns rows when forecast_demand table exists with matching data
    SELECT 
        'Capacity Gap' AS analysis_type,
        tc.analysis_date,
        NULL AS equipment_id,
        NULL AS equipment_name,
        NULL AS utilization_pct,
        NULL AS downstream_starvation_hours,
        NULL AS constraint_score,
        NULL AS is_system_constraint,
        NULL AS buffer_name,
        NULL AS current_capacity_hours,
        NULL AS blocked_hours_observed,
        NULL AS recommended_capacity_hours,
        NULL AS estimated_blocking_reduction_hours,
        NULL AS plant_throughput_tons,
        NULL AS constraint_equipment,
        NULL AS constraint_capacity_tons_hr,
        NULL AS constraint_utilization_pct,
        fd.demand_tons_per_day AS demand_tons,
        tc.plant_throughput_tons AS actual_throughput_tons,
        (fd.demand_tons_per_day - tc.plant_throughput_tons) AS capacity_gap_tons,
        (fd.demand_tons_per_day - tc.plant_throughput_tons) / NULLIF(fd.demand_tons_per_day, 0) * 100.0 AS capacity_gap_pct,
        (fd.demand_tons_per_day - tc.plant_throughput_tons) * 250.0 AS estimated_revenue_loss_usd
    FROM throughput_calculation tc
    LEFT JOIN (
        SELECT * FROM forecast_demand WHERE 1=1
    ) fd ON tc.analysis_date = fd.date_id
    WHERE fd.demand_tons_per_day IS NOT NULL
)

-- Union all analysis types
-- Note: capacity_gap_analysis will be empty if forecast_demand doesn't exist
SELECT * FROM constraint_identification
UNION ALL
SELECT * FROM buffer_sizing_recommendations
UNION ALL
SELECT * FROM throughput_calculation
UNION ALL
SELECT * FROM capacity_gap_analysis
ORDER BY 1, 7 DESC NULLS LAST
