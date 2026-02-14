-- ===================================================================
-- Excursion Root Cause Analysis
-- Purpose: Group excursions by operational patterns to identify root causes
-- Business Value: Identify systemic operational issues driving excursions
-- ===================================================================

WITH excursion_events AS (
    SELECT
        e.excursion_id,
        e.asset_key,
        e.parameter_type,
        e.excursion_start_timestamp,
        e.duration_minutes,
        e.cumulative_damage_from_excursion,
        e.severity_level,
        a.unit_name,
        d.day_of_week,
        d.is_weekend,
        d.is_summer_spec,
        d.is_winter_spec,
        d.is_turnaround,
        CAST(strftime('%H', e.excursion_start_timestamp) AS INTEGER) AS hour_of_day
    FROM {{ ref "fact_excursion_events" }} e
    INNER JOIN {{ ref "dim_asset" }} a
        ON e.asset_key = a.asset_key
    INNER JOIN {{ ref "dim_date" }} d
        ON date(e.excursion_start_timestamp) = d.full_date
),

classified_excursions AS (
    SELECT
        excursion_id,
        asset_key,
        parameter_type,
        duration_minutes,
        cumulative_damage_from_excursion,
        severity_level,
        unit_name,
        
        -- Classify excursions by operational patterns (proxies for root causes)
        CASE
            -- Unit startup (Monday mornings 6am-8am)
            WHEN day_of_week = 'Monday' AND hour_of_day >= 6 AND hour_of_day < 8 
                THEN 'Unit_Startup'
            
            -- Feedstock change (midnight-2am typical transition time)
            WHEN hour_of_day >= 0 AND hour_of_day < 2 
                THEN 'Feedstock_Change'
            
            -- Summer spec operations
            WHEN is_summer_spec = 1 
                THEN 'Summer_Spec'
            
            -- Winter spec operations
            WHEN is_winter_spec = 1 
                THEN 'Winter_Spec'
            
            -- Turnaround period (unit coming back online)
            WHEN is_turnaround = 1 
                THEN 'Turnaround_Period'
            
            -- Weekend operations (reduced staffing)
            WHEN is_weekend = 1 
                THEN 'Weekend_Operations'
            
            -- Night shift (10pm-6am, operator behavior differences)
            WHEN hour_of_day >= 22 OR hour_of_day < 6 
                THEN 'Night_Shift'
            
            ELSE 'Unknown'
        END AS root_cause_category
        
    FROM excursion_events
),

root_cause_aggregates AS (
    SELECT
        root_cause_category,
        COUNT(*) AS excursion_count,
        SUM(CASE WHEN severity_level = 'Critical' THEN 1 ELSE 0 END) AS critical_excursion_count,
        SUM(cumulative_damage_from_excursion) AS total_damage_accumulated,
        AVG(cumulative_damage_from_excursion) AS avg_damage_per_event,
        AVG(duration_minutes) AS avg_duration_minutes,
        MAX(duration_minutes) AS max_duration_minutes,
        COUNT(DISTINCT asset_key) AS affected_asset_count,
        COUNT(DISTINCT unit_name) AS affected_unit_count,
        -- Identify most common parameter type for this root cause
        MAX(parameter_type) AS most_common_parameter_type  -- Simplified - could use mode
    FROM classified_excursions
    GROUP BY root_cause_category
)

SELECT
    root_cause_category,
    excursion_count,
    critical_excursion_count,
    total_damage_accumulated,
    avg_damage_per_event,
    avg_duration_minutes,
    max_duration_minutes,
    affected_asset_count,
    affected_unit_count,
    most_common_parameter_type,
    
    -- Provide actionable recommendations based on root cause
    CASE
        WHEN root_cause_category = 'Unit_Startup' 
            THEN 'Review startup procedures - standardize warm-up rates and timing'
        WHEN root_cause_category = 'Feedstock_Change' 
            THEN 'Improve feedstock transition procedures - gradual changeover protocols'
        WHEN root_cause_category = 'Summer_Spec' 
            THEN 'Review summer spec product slate - may need tighter IOW limits'
        WHEN root_cause_category = 'Winter_Spec' 
            THEN 'Review winter spec operations - may need seasonal IOW adjustments'
        WHEN root_cause_category = 'Turnaround_Period' 
            THEN 'Improve post-turnaround procedures - extended break-in period'
        WHEN root_cause_category = 'Weekend_Operations' 
            THEN 'Review weekend staffing and supervision levels'
        WHEN root_cause_category = 'Night_Shift' 
            THEN 'Increase night shift training - review operator competency'
        ELSE 'Investigate further - no clear operational pattern identified'
    END AS recommended_action,
    
    -- Calculate severity ratio
    CAST(critical_excursion_count AS REAL) / NULLIF(excursion_count, 0) AS critical_excursion_ratio
    
FROM root_cause_aggregates
ORDER BY excursion_count DESC, total_damage_accumulated DESC;
