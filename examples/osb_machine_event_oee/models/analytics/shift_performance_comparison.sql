-- Shift Performance Comparison
-- Compares OEE, availability, and downtime metrics across shifts to identify training needs
--
-- Business Logic:
-- - Aggregates equipment state by shift (Day, Swing, Night)
-- - Calculates availability = operating_time / (operating_time + downtime)
-- - Identifies worst-performing shifts for targeted training/supervision
-- - Helps diagnose handover effectiveness and shift-specific issues
--
-- Dependencies:
-- - stg_equipment_state_history: Time-series equipment states
-- - dim_shift: Shift definitions and schedules
--
-- Output Schema:
-- - shift_id: Shift identifier (DAY, SWING, NIGHT)
-- - shift_name: Human-readable shift name
-- - total_operating_time_hours: Hours equipment was running
-- - total_downtime_hours: Hours equipment was down
-- - availability_pct: Operating time as % of total time
-- - shift_rank: Performance rank (1=best availability)

WITH shift_aggregates AS (
    SELECT 
        s.shift_id,
        sh.shift_name,
        SUM(CASE WHEN s.machine_state = 'Running' THEN s.state_duration_min ELSE 0 END) / 60.0 AS total_operating_time_hours,
        SUM(CASE WHEN s.machine_state = 'Unplanned Downtime' THEN s.state_duration_min ELSE 0 END) / 60.0 AS total_downtime_hours,
        SUM(CASE WHEN s.machine_state IN ('Running', 'Unplanned Downtime') THEN s.state_duration_min ELSE 0 END) / 60.0 AS total_time_hours
    FROM stg_equipment_state_history s
    INNER JOIN dim_shift sh ON s.shift_id = sh.shift_id
    GROUP BY s.shift_id, sh.shift_name
),
shift_metrics AS (
    SELECT 
        shift_id,
        shift_name,
        total_operating_time_hours,
        total_downtime_hours,
        total_time_hours,
        -- Calculate availability percentage
        CASE 
            WHEN total_time_hours > 0 
            THEN (total_operating_time_hours / total_time_hours) * 100.0
            ELSE 0 
        END AS availability_pct
    FROM shift_aggregates
)
SELECT 
    shift_id,
    shift_name,
    ROUND(total_operating_time_hours, 1) AS total_operating_time_hours,
    ROUND(total_downtime_hours, 1) AS total_downtime_hours,
    ROUND(availability_pct, 1) AS availability_pct,
    ROW_NUMBER() OVER (ORDER BY availability_pct DESC) AS shift_rank
FROM shift_metrics
ORDER BY shift_rank
