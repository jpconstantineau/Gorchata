{{ config "materialized" "table" }}

-- Fact: Equipment Daily OEE
-- Calculate Overall Equipment Effectiveness (OEE) = Availability × Performance × Quality
-- Aggregated by equipment and date, with optional shift-level detail

WITH calendar_time AS (
  -- Define calendar time per date (24 hours = 1440 minutes)
  SELECT 
    date_id,
    1440.0 AS calendar_time_min
  FROM dim_date
),

planned_downtime_summary AS (
  -- Sum planned downtime per equipment per date
  SELECT 
    equipment_id,
    date_id,
    SUM(state_duration_min) AS planned_downtime_min
  FROM stg_equipment_state_history
  WHERE machine_state = 'Planned Downtime'
  GROUP BY equipment_id, date_id
),

unplanned_downtime_summary AS (
  -- Sum unplanned downtime per equipment per date
  SELECT 
    equipment_id,
    date_id,
    SUM(state_duration_min) AS unplanned_downtime_min
  FROM stg_equipment_state_history
  WHERE machine_state IN ('Unplanned Downtime', 'Breakdown')
  GROUP BY equipment_id, date_id
),

operating_time_summary AS (
  -- Sum operating time (Running state) per equipment per date
  SELECT 
    equipment_id,
    date_id,
    SUM(state_duration_min) AS operating_time_min
  FROM stg_equipment_state_history
  WHERE machine_state = 'Running'
  GROUP BY equipment_id, date_id
),

production_output_summary AS (
  -- Aggregate production output per equipment per date
  SELECT 
    p.equipment_id,
    p.date_id,
    COUNT(*) AS actual_output_qty,
    SUM(CASE WHEN p.pass_fail = 'Pass' THEN 1 ELSE 0 END) AS good_output_qty,
    SUM(CASE WHEN p.pass_fail = 'Fail' THEN 1 ELSE 0 END) AS defect_qty,
    -- Get ideal cycle time from product spec (assuming single product per day for simplicity)
    MAX(ps.ideal_cycle_time_min) AS ideal_cycle_time_min
  FROM fact_production_output p
  INNER JOIN dim_product_spec ps ON p.product_id = ps.product_id
  GROUP BY p.equipment_id, p.date_id
),

six_big_losses AS (
  -- Categorize downtime into Six Big Losses categories
  SELECT 
    s.equipment_id,
    s.date_id,
    SUM(CASE WHEN r.six_big_losses_category = 'Equipment Failure' THEN s.state_duration_min ELSE 0 END) AS equipment_failure_loss_min,
    SUM(CASE WHEN r.six_big_losses_category = 'Setup & Adjustment' THEN s.state_duration_min ELSE 0 END) AS setup_adjustment_loss_min,
    SUM(CASE WHEN r.six_big_losses_category = 'Small Stops' THEN s.state_duration_min ELSE 0 END) AS small_stops_loss_min,
    SUM(CASE WHEN r.six_big_losses_category = 'Reduced Speed' THEN s.state_duration_min ELSE 0 END) AS reduced_speed_loss_min,
    SUM(CASE WHEN r.six_big_losses_category = 'Startup Rejects' THEN s.state_duration_min ELSE 0 END) AS startup_rejects_loss_min,
    SUM(CASE WHEN r.six_big_losses_category = 'Production Rejects' THEN s.state_duration_min ELSE 0 END) AS production_rejects_loss_min
  FROM stg_equipment_state_history s
  LEFT JOIN dim_reason_code r ON s.reason_code_id = r.reason_code_id
  GROUP BY s.equipment_id, s.date_id
),

oee_base_metrics AS (
  -- Calculate base OEE metrics
  SELECT 
    e.equipment_id,
    e.equipment_name,
    e.equipment_type,
    ct.date_id,
    ct.calendar_time_min,
    COALESCE(pd.planned_downtime_min, 0) AS planned_downtime_min,
    -- Planned Production Time = Calendar Time - Planned Downtime
    ct.calendar_time_min - COALESCE(pd.planned_downtime_min, 0) AS planned_production_time_min,
    COALESCE(ud.unplanned_downtime_min, 0) AS unplanned_downtime_min,
    COALESCE(ot.operating_time_min, 0) AS operating_time_min,
    COALESCE(po.actual_output_qty, 0) AS actual_output_qty,
    COALESCE(po.good_output_qty, 0) AS good_output_qty,
    COALESCE(po.defect_qty, 0) AS defect_qty,
    COALESCE(po.ideal_cycle_time_min, 8.0) AS ideal_cycle_time_min -- Default to 8 min if no production
  FROM dim_equipment e
  CROSS JOIN calendar_time ct
  LEFT JOIN planned_downtime_summary pd ON e.equipment_id = pd.equipment_id AND ct.date_id = pd.date_id
  LEFT JOIN unplanned_downtime_summary ud ON e.equipment_id = ud.equipment_id AND ct.date_id = ud.date_id
  LEFT JOIN operating_time_summary ot ON e.equipment_id = ot.equipment_id AND ct.date_id = ot.date_id
  LEFT JOIN production_output_summary po ON e.equipment_id = po.equipment_id AND ct.date_id = po.date_id
  -- Only include equipment with state history OR production output
  WHERE EXISTS (
    SELECT 1 FROM stg_equipment_state_history s 
    WHERE s.equipment_id = e.equipment_id AND s.date_id = ct.date_id
  )
  OR EXISTS (
    SELECT 1 FROM fact_production_output p
    WHERE p.equipment_id = e.equipment_id AND p.date_id = ct.date_id
  )
),

oee_calculated AS (
  -- Calculate OEE components
  SELECT 
    b.*,
    -- Ideal Output = Operating Time / Ideal Cycle Time
    CASE 
      WHEN b.operating_time_min > 0 AND b.ideal_cycle_time_min > 0 
      THEN b.operating_time_min / b.ideal_cycle_time_min 
      ELSE 0 
    END AS ideal_output_qty,
    -- Availability (%) = Operating Time / Planned Production Time
    CASE 
      WHEN b.planned_production_time_min > 0 
      THEN (CAST(b.operating_time_min AS REAL) / CAST(b.planned_production_time_min AS REAL)) * 100.0 
      ELSE 0 
    END AS availability_pct,
    -- Performance (%) = Actual Output / Ideal Output
    CASE 
      WHEN b.operating_time_min > 0 AND b.ideal_cycle_time_min > 0 
      THEN (CAST(b.actual_output_qty AS REAL) / (b.operating_time_min / b.ideal_cycle_time_min)) * 100.0 
      ELSE 0 
    END AS performance_pct,
    -- Quality (%) = Good Output / Total Output
    CASE 
      WHEN b.actual_output_qty > 0 
      THEN (CAST(b.good_output_qty AS REAL) / CAST(b.actual_output_qty AS REAL)) * 100.0 
      ELSE 100.0  -- No defects if no production
    END AS quality_pct
  FROM oee_base_metrics b
)

SELECT 
  o.equipment_id,
  o.equipment_name,
  o.equipment_type,
  o.date_id,
  NULL AS shift_id,  -- Daily aggregate (no shift breakdown)
  o.calendar_time_min,
  o.planned_downtime_min,
  o.planned_production_time_min,
  o.unplanned_downtime_min,
  o.operating_time_min,
  o.actual_output_qty,
  o.good_output_qty,
  o.defect_qty,
  o.ideal_cycle_time_min,
  o.ideal_output_qty,
  o.availability_pct,
  o.performance_pct,
  o.quality_pct,
  -- OEE (%) = Availability × Performance × Quality
  (o.availability_pct / 100.0) * (o.performance_pct / 100.0) * (o.quality_pct / 100.0) * 100.0 AS oee_pct,
  -- Six Big Losses breakdown
  COALESCE(l.equipment_failure_loss_min, 0) AS equipment_failure_loss_min,
  COALESCE(l.setup_adjustment_loss_min, 0) AS setup_adjustment_loss_min,
  COALESCE(l.small_stops_loss_min, 0) AS small_stops_loss_min,
  COALESCE(l.reduced_speed_loss_min, 0) AS reduced_speed_loss_min,
  COALESCE(l.startup_rejects_loss_min, 0) AS startup_rejects_loss_min,
  COALESCE(l.production_rejects_loss_min, 0) AS production_rejects_loss_min
FROM oee_calculated o
LEFT JOIN six_big_losses l ON o.equipment_id = l.equipment_id AND o.date_id = l.date_id
ORDER BY o.date_id, o.equipment_id
