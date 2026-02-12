-- Bad Actor Prioritization
-- Scores equipment by impact (downtime × frequency × criticality) to prioritize maintenance investments
--
-- Business Logic:
-- - Impact Score = (total_downtime_hours × failure_count × criticality_multiplier)
-- - Criticality Multipliers: Critical=3, Important=2, Standard=1
-- - Higher scores indicate equipment requiring urgent reliability improvements
--
-- Dependencies:
-- - equipment_reliability_metrics: MTBF, MTTR, failure counts
-- - dim_equipment: Equipment criticality level
--
-- Output Schema:
-- - equipment_id: Unique equipment identifier
-- - equipment_name: Equipment name
-- - criticality_level: Critical, Important, or Standard
-- - total_failures: Number of failures in analysis period
-- - total_downtime_hours: Total hours of downtime
-- - mtbf_hours: Mean Time Between Failures
-- - mttr_hours: Mean Time To Repair
-- - impact_score: Composite score for prioritization
-- - priority_rank: Rank by impact (1=highest priority)

WITH equipment_impact AS (
    SELECT 
        r.equipment_id,
        r.equipment_name,
        e.criticality_level,
        r.failure_count AS total_failures,
        r.total_downtime_min / 60.0 AS total_downtime_hours,
        r.mtbf_hours,
        r.mttr_hours,
        r.failures_per_week,
        -- Criticality multiplier
        CASE e.criticality_level
            WHEN 'Critical' THEN 3.0
            WHEN 'Important' THEN 2.0
            WHEN 'Standard' THEN 1.0
            ELSE 1.0
        END AS criticality_multiplier
    FROM equipment_reliability_metrics r
    INNER JOIN dim_equipment e ON r.equipment_id = e.equipment_id
),
scored_equipment AS (
    SELECT 
        equipment_id,
        equipment_name,
        criticality_level,
        total_failures,
        total_downtime_hours,
        mtbf_hours,
        mttr_hours,
        failures_per_week,
        criticality_multiplier,
        -- Calculate impact score: downtime × frequency × criticality
        (total_downtime_hours * total_failures * criticality_multiplier) AS impact_score
    FROM equipment_impact
)
SELECT 
    equipment_id,
    equipment_name,
    criticality_level,
    total_failures,
    ROUND(total_downtime_hours, 1) AS total_downtime_hours,
    ROUND(mtbf_hours, 1) AS mtbf_hours,
    ROUND(mttr_hours, 1) AS mttr_hours,
    ROUND(failures_per_week, 1) AS failures_per_week,
    ROUND(impact_score, 1) AS impact_score,
    ROW_NUMBER() OVER (ORDER BY impact_score DESC) AS priority_rank
FROM scored_equipment
WHERE impact_score > 0
ORDER BY priority_rank
