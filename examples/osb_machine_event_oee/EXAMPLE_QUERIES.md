# Example Queries - OSB Machine Event to OEE Analytics

Common analytics queries for daily operations, maintenance prioritization, and continuous improvement decisions.

## Table of Contents
1. [Equipment OEE by Day and Shift](#1-equipment-oee-by-day-and-shift)
2. [Top 10 Downtime Reasons (Pareto)](#2-top-10-downtime-reasons-pareto)
3. [Bad Actor Equipment Identification](#3-bad-actor-equipment-identification)
4. [Buffer Utilization Over Time](#4-buffer-utilization-over-time)
5. [Constraint Analysis](#5-constraint-analysis)
6. [Quality Defect Correlation](#6-quality-defect-correlation)
7. [Shift Performance Comparison](#7-shift-performance-comparison)
8. [Maintenance Strategy Effectiveness](#8-maintenance-strategy-effectiveness)
9. [Lost Production Quantification](#9-lost-production-quantification)
10. [Rolling 30-Day OEE Trend](#10-rolling-30-day-oee-trend)

---

## 1. Equipment OEE by Day and Shift

**Business Question:** What is the OEE performance for each equipment by date and shift?

**Use Case:** Daily operations review, shift handover meetings, performance tracking against targets.

**Query:**
```sql
SELECT 
  f.date_id,
  f.equipment_id,
  e.equipment_name,
  s.shift_name,
  f.oee_pct,
  f.availability_pct,
  f.performance_pct,
  f.quality_pct,
  CASE 
    WHEN f.oee_pct >= 85 THEN 'World-Class'
    WHEN f.oee_pct >= 70 THEN 'Good'
    ELSE 'Needs Improvement'
  END AS performance_rating
FROM fact_equipment_daily_oee f
JOIN dim_equipment e ON f.equipment_id = e.equipment_id
JOIN dim_shift s ON f.shift_id = s.shift_id
WHERE f.date_id >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY f.date_id DESC, f.equipment_id, s.shift_name;
```

**Expected Output:**
| date_id | equipment_id | equipment_name | shift_name | oee_pct | availability_pct | performance_pct | quality_pct | performance_rating |
|---------|--------------|----------------|------------|---------|------------------|-----------------|-------------|-------------------|
| 2024-03-15 | DRYER-01 | Primary Dryer | Day | 78.5 | 85.0 | 95.0 | 97.2 | Good |
| 2024-03-15 | DRYER-01 | Primary Dryer | Swing | 65.2 | 72.0 | 93.0 | 97.4 | Needs Improvement |
| 2024-03-15 | PRESS-01 | Continuous Press | Day | 89.3 | 93.0 | 97.0 | 99.0 | World-Class |

**Interpretation:**
- **OEE ≥85%**: World-class performance, maintain current practices
- **OEE 70-84%**: Good performance, opportunity for optimization
- **OEE <70%**: Needs improvement, investigate root causes (A×P×Q breakdown)

---

## 2. Top 10 Downtime Reasons (Pareto)

**Business Question:** What are the most significant causes of downtime (80/20 rule)?

**Use Case:** Maintenance prioritization, reliability improvement projects, budget allocation.

**Query:**
```sql
SELECT 
  f.pareto_rank,
  f.reason_code_id,
  r.reason_description,
  r.oee_loss_category,
  f.total_downtime_hours,
  f.percent_of_total_downtime,
  f.cumulative_percent,
  CASE 
    WHEN f.cumulative_percent <= 80 THEN 'Vital Few (80%)'
    ELSE 'Trivial Many (20%)'
  END AS pareto_classification
FROM failure_mode_pareto f
JOIN dim_reason_code r ON f.reason_code_id = r.reason_code_id
ORDER BY f.pareto_rank
LIMIT 10;
```

**Expected Output:**
| pareto_rank | reason_code_id | reason_description | oee_loss_category | total_downtime_hours | percent_of_total_downtime | cumulative_percent | pareto_classification |
|-------------|----------------|-------------------|-------------------|---------------------|--------------------------|-------------------|----------------------|
| 1 | RC003 | Bearing failure (dryer) | Equipment Failure | 48.0 | 32.5 | 32.5 | Vital Few (80%) |
| 2 | RC007 | Hydraulic leak (press) | Equipment Failure | 24.0 | 16.3 | 48.8 | Vital Few (80%) |
| 3 | RC012 | Motor overheating (strander) | Equipment Failure | 18.0 | 12.2 | 61.0 | Vital Few (80%) |
| 4 | RC015 | Resin system blockage | Equipment Failure | 15.0 | 10.2 | 71.2 | Vital Few (80%) |
| 5 | RC021 | Belt misalignment (former) | Equipment Failure | 12.0 | 8.1 | 79.3 | Vital Few (80%) |

**Interpretation:**
- **Pareto Principle**: ~80% of downtime caused by ~20% of failure modes
- **Focus on Top 5**: Prioritize these for maximum impact
- **Example**: Dryer bearing failure (Rank 1, 32.5%) → Implement condition monitoring

---

## 3. Bad Actor Equipment Identification

**Business Question:** Which equipment should we invest in to maximize downtime reduction?

**Use Case:** Capital budgeting, maintenance strategy development, ROI analysis for reliability improvements.

**Query:**
```sql
SELECT 
  b.priority_rank,
  b.equipment_id,
  e.equipment_name,
  e.criticality_level,
  b.total_downtime_hours,
  b.total_failures,
  r.mtbf_hours,
  r.mttr_hours,
  b.impact_score,
  -- ROI calculation: Double MTBF reduces failures by 50%
  (b.total_downtime_hours * 0.5) AS avoided_downtime_hours_per_year,
  (b.total_downtime_hours * 0.5 * 1000) AS annual_value_usd,
  CASE 
    WHEN (b.total_downtime_hours * 0.5 * 1000) > 100000 THEN 'High ROI Investment'
    WHEN (b.total_downtime_hours * 0.5 * 1000) > 50000 THEN 'Medium ROI Investment'
    ELSE 'Low ROI Investment'
  END AS investment_recommendation
FROM bad_actor_prioritization b
JOIN dim_equipment e ON b.equipment_id = e.equipment_id
JOIN equipment_reliability_metrics r ON b.equipment_id = r.equipment_id
ORDER BY b.priority_rank
LIMIT 5;
```

**Expected Output:**
| priority_rank | equipment_id | equipment_name | criticality_level | total_downtime_hours | total_failures | mtbf_hours | mttr_hours | impact_score | avoided_downtime_hours | annual_value_usd | investment_recommendation |
|--------------|-------------|----------------|-------------------|---------------------|----------------|-----------|-----------|-------------|----------------------|-----------------|--------------------------|
| 1 | DRYER-01 | Primary Dryer | Critical | 60.0 | 5 | 48.0 | 12.0 | 180.0 | 30.0 | $30,000 | High ROI Investment |
| 2 | PRESS-01 | Continuous Press | Critical | 24.0 | 3 | 140.0 | 8.0 | 36.0 | 12.0 | $12,000 | Medium ROI Investment |
| 3 | STRAND-01 | Strander #1 | Important | 18.0 | 4 | 90.0 | 4.5 | 18.0 | 9.0 | $9,000 | Low ROI Investment |

**Interpretation:**
- **Impact Score = Downtime × Failures × Criticality Multiplier** (Critical=3, Important=2, Standard=1)
- **DRYER-01 (Rank 1)**: $30K annual savings potential, justifies $100K investment (3.3-year payback)
- **Actions**: Bearing monitoring, increase PM frequency, spare parts stocking

---

## 4. Buffer Utilization Over Time

**Business Question:** How are buffers being utilized, and when do starvation/blocking risks occur?

**Use Case:** Buffer sizing optimization, predictive alerts, downtime propagation prevention.

**Query:**
```sql
SELECT 
  b.hour_timestamp,
  p.area_name,
  p.buffer_capacity_hours,
  b.buffer_level_hours,
  b.buffer_utilization_pct,
  b.inflow_rate_tons_per_hour,
  b.outflow_rate_tons_per_hour,
  b.is_starved,
  b.is_blocked,
  CASE 
    WHEN b.buffer_utilization_pct < 20 THEN 'Low (Risk: Starvation)'
    WHEN b.buffer_utilization_pct > 80 THEN 'High (Risk: Blocking)'
    ELSE 'Optimal'
  END AS buffer_status
FROM buffer_utilization_analysis b
JOIN dim_production_area p ON b.production_area_id = p.production_area_id
WHERE b.hour_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
  AND p.area_name IN ('DRYING', 'PRESSING')  -- Green bins, Dry silos
ORDER BY b.hour_timestamp DESC, p.area_name;
```

**Expected Output:**
| hour_timestamp | area_name | buffer_capacity_hours | buffer_level_hours | buffer_utilization_pct | inflow_rate | outflow_rate | is_starved | is_blocked | buffer_status |
|---------------|-----------|----------------------|-------------------|------------------------|------------|-------------|-----------|-----------|--------------|
| 2024-03-15 14:00 | DRYING | 4.0 | 0.8 | 20.0 | 12.0 | 10.0 | FALSE | FALSE | Low (Risk: Starvation) |
| 2024-03-15 14:00 | PRESSING | 0.5 | 0.42 | 84.0 | 10.0 | 8.0 | FALSE | FALSE | High (Risk: Blocking) |
| 2024-03-15 13:00 | DRYING | 4.0 | 0.0 | 0.0 | 0.0 | 0.0 | TRUE | FALSE | Low (Risk: Starvation) |

**Interpretation:**
- **Optimal Range**: 40-60% buffer utilization
- **Starvation (0%)**: Downstream equipment stops
- **Blocking (100%)**: Upstream equipment must stop
- **Actions**: Alert when <20% (impending starvation), increase buffer capacity if frequent blocking

---

## 5. Constraint Analysis

**Business Question:** What equipment limits plant throughput? Where should we invest capacity?

**Use Case:** Capacity planning, Theory of Constraints (TOC) analysis, capital project justification.

**Query:**
```sql
SELECT 
  c.equipment_id,
  e.equipment_name,
  e.capacity_tons_per_hour,
  c.average_utilization_pct,
  c.starvation_frequency_percent,
  c.blocking_frequency_percent,
  c.constraint_score,
  c.throughput_tons_per_day,
  c.capacity_gap_tons_per_day,
  -- Economic impact calculation
  c.capacity_gap_tons_per_day * 450 AS daily_revenue_loss_usd,
  (c.capacity_gap_tons_per_day * 450 * 250) AS annual_revenue_loss_usd,
  CASE 
    WHEN c.constraint_score = (SELECT MAX(constraint_score) FROM constraint_analysis) 
    THEN 'BOTTLENECK - Primary Constraint'
    WHEN c.average_utilization_pct > 85 
    THEN 'Near Constraint - Monitor'
    ELSE 'Non-Constraint'
  END AS constraint_status
FROM constraint_analysis c
JOIN dim_equipment e ON c.equipment_id = e.equipment_id
WHERE c.analysis_type = 'Constraint Identification'
ORDER BY c.constraint_score DESC;
```

**Expected Output:**
| equipment_id | equipment_name | capacity_tons_per_hour | average_utilization_pct | starvation_freq_pct | blocking_freq_pct | constraint_score | throughput_tons_per_day | capacity_gap_tons_per_day | daily_revenue_loss_usd | annual_revenue_loss_usd | constraint_status |
|-------------|---------------|----------------------|------------------------|--------------------|--------------------|-----------------|------------------------|--------------------------|----------------------|------------------------|------------------|
| DRYER-01 | Primary Dryer | 10.0 | 100.0 | 0.0 | 0.0 | 100.0 | 240.0 | 30.0 | $13,500 | $3,375,000 | BOTTLENECK - Primary Constraint |
| PRESS-01 | Continuous Press | 18.0 | 56.0 | 5.0 | 0.0 | 58.8 | 240.0 | 0.0 | $0 | $0 | Non-Constraint |
| STRAND-01 | Strander #1 | 6.0 | 95.0 | 0.0 | 12.0 | 95.0 | 240.0 | 0.0 | $0 | $0 | Near Constraint - Monitor |

**Interpretation:**
- **Constraint Score = Utilization × (1 + Starvation Frequency / 100)**
- **DRYER-01**: Bottleneck at 100% utilization, limits plant to 240 t/day ($3.4M annual revenue loss)
- **Investment**: Add 2nd dryer ($500K) → increase capacity 80% → 5-month payback
- **TOC Principle**: Improve bottleneck first (greatest impact), then address near-constraints

---

## 6. Quality Defect Correlation

**Business Question:** What process conditions correlate with quality defects?

**Use Case:** Quality root cause analysis, process optimization, SPC (Statistical Process Control).

**Query:**
```sql
WITH defect_events AS (
  SELECT 
    equipment_id,
    defect_type,
    defect_timestamp,
    thickness_mm,
    density_kg_m3,
    press_temperature_c,
    press_pressure_bar,
    resin_content_pct
  FROM quality_defects
  WHERE defect_timestamp >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT 
  defect_type,
  COUNT(*) AS defect_count,
  AVG(thickness_mm) AS avg_thickness_mm,
  AVG(density_kg_m3) AS avg_density_kg_m3,
  AVG(press_temperature_c) AS avg_press_temp_c,
  AVG(press_pressure_bar) AS avg_press_pressure_bar,
  AVG(resin_content_pct) AS avg_resin_pct,
  -- Compare to specification limits
  CASE 
    WHEN AVG(press_temperature_c) < 150 THEN 'Low Temperature (< 150°C spec)'
    WHEN AVG(press_temperature_c) > 180 THEN 'High Temperature (> 180°C spec)'
    ELSE 'Within Spec (150-180°C)'
  END AS temperature_assessment,
  CASE 
    WHEN AVG(density_kg_m3) < 620 THEN 'Low Density (< 620 kg/m³ spec)'
    WHEN AVG(density_kg_m3) > 680 THEN 'High Density (> 680 kg/m³ spec)'
    ELSE 'Within Spec (620-680 kg/m³)'
  END AS density_assessment
FROM defect_events
GROUP BY defect_type
ORDER BY defect_count DESC;
```

**Expected Output:**
| defect_type | defect_count | avg_thickness_mm | avg_density_kg_m3 | avg_press_temp_c | avg_press_pressure_bar | avg_resin_pct | temperature_assessment | density_assessment |
|-------------|--------------|-----------------|------------------|-----------------|----------------------|--------------|----------------------|------------------|
| Thickness Deviation | 15 | 10.3 | 655.0 | 143.3 | 4.8 | 5.2 | Low Temperature (< 150°C spec) | Within Spec |
| Delamination | 5 | 11.0 | 608.0 | 165.0 | 5.0 | 4.8 | Within Spec | Low Density (< 620 kg/m³ spec) |
| Surface Roughness | 3 | 11.1 | 662.0 | 172.0 | 5.2 | 5.5 | Within Spec | Within Spec |

**Interpretation:**
- **Thickness Deviation (83% of defects)**: Strongly correlated with low press temperature (143°C vs 165°C spec)
- **Root Cause**: Press temperature controller malfunction
- **Actions**: Replace temperature controller ($5K), update SOPs with temperature alarms
- **Expected Impact**: Reduce defects 5% → 2%, improve quality component 95% → 98% ($8K/day value)

---

## 7. Shift Performance Comparison

**Business Question:** How does performance vary by shift? Are there training needs?

**Use Case:** Shift management, training program development, operator performance standardization.

**Query:**
```sql
SELECT 
  s.shift_name,
  s.avg_availability_pct,
  s.total_operating_hours,
  s.total_downtime_hours,
  s.shift_performance_rank,
  -- Calculate performance gap from best shift
  (SELECT MAX(avg_availability_pct) FROM shift_performance_comparison) - s.avg_availability_pct 
    AS gap_from_best_shift_pct,
  -- Opportunity calculation
  (s.total_operating_hours + s.total_downtime_hours) * 
    ((SELECT MAX(avg_availability_pct) FROM shift_performance_comparison) - s.avg_availability_pct) / 100 * 
    10 * 450 AS potential_additional_value_usd
FROM shift_performance_comparison s
ORDER BY s.shift_performance_rank;
```

**Expected Output:**
| shift_name | avg_availability_pct | total_operating_hours | total_downtime_hours | shift_performance_rank | gap_from_best_shift_pct | potential_additional_value_usd |
|-----------|---------------------|----------------------|--------------------|----------------------|------------------------|------------------------------|
| Night | 93.8 | 1,500 | 99  | 1 | 0.0 | $0 |
| Day | 87.5 | 1,400 | 200 | 2 | 6.3 | $45,360 |
| Swing | 75.0 | 1,200 | 400 | 3 | 18.8 | $135,360 |

**Interpretation:**
- **Night Shift (Best)**: 93.8% availability, most experienced crew
- **Swing Shift (Worst)**: 75.0% availability, 18.8% gap from Night (-$135K annual opportunity)
- **Root Cause**: Less experienced operators on Swing shift
- **Actions**: 
  - Cross-train Swing crew with Night shift veterans
  - Target 88% availability for Swing (13% capacity gain = +31 hours/month operating time)
  - Expected value: $11K/month additional production

---

## 8. Maintenance Strategy Effectiveness

**Business Question:** Is our preventive maintenance improving reliability?

**Use Case:** Maintenance strategy evaluation, PM schedule optimization, budget justification.

**Query:**
```sql
WITH maintenance_metrics AS (
  SELECT 
    equipment_id,
    COUNT(CASE WHEN maintenance_type = 'PM' THEN 1 END) AS pm_count,
    COUNT(CASE WHEN maintenance_type = 'Breakdown' THEN 1 END) AS breakdown_count,
    SUM(CASE WHEN maintenance_type = 'PM' THEN maintenance_cost_usd ELSE 0 END) AS pm_cost,
    SUM(CASE WHEN maintenance_type = 'Breakdown' THEN maintenance_cost_usd ELSE 0 END) AS breakdown_cost
  FROM maintenance_work_orders
  WHERE work_order_date >= CURRENT_DATE - INTERVAL '90 days'
  GROUP BY equipment_id
)
SELECT 
  e.equipment_id,
  e.equipment_name,
  e.criticality_level,
  m.pm_count,
  m.breakdown_count,
  -- PM ratio (target >50% for proactive strategy)
  ROUND(m.pm_count * 100.0 / NULLIF(m.pm_count + m.breakdown_count, 0), 1) AS pm_ratio_pct,
  m.pm_cost,
  m.breakdown_cost,
  -- Cost ratio (breakdown typically 3-10x PM cost)
  ROUND(m.breakdown_cost / NULLIF(m.pm_cost, 0), 1) AS breakdown_to_pm_cost_ratio,
  CASE 
    WHEN (m.pm_count * 100.0 / NULLIF(m.pm_count + m.breakdown_count, 0)) > 50 
    THEN 'Proactive Strategy'
    WHEN (m.pm_count * 100.0 / NULLIF(m.pm_count + m.breakdown_count, 0)) > 30 
    THEN 'Mixed Strategy'
    ELSE 'Reactive Strategy - Increase PM'
  END AS maintenance_strategy_assessment
FROM dim_equipment e
JOIN maintenance_metrics m ON e.equipment_id = m.equipment_id
WHERE e.criticality_level IN ('Critical', 'Important')
ORDER BY (m.pm_count * 100.0 / NULLIF(m.pm_count + m.breakdown_count, 0)) ASC;
```

**Expected Output:**
| equipment_id | equipment_name | criticality_level | pm_count | breakdown_count | pm_ratio_pct | pm_cost | breakdown_cost | breakdown_to_pm_cost_ratio | maintenance_strategy_assessment |
|-------------|---------------|-------------------|---------|----------------|-------------|---------|---------------|---------------------------|------------------------------|
| DRYER-01 | Primary Dryer | Critical | 2 | 5 | 28.6 | $2,000 | $15,000 | 7.5 | Reactive Strategy - Increase PM |
| PRESS-01 | Continuous Press | Critical | 3 | 3 | 50.0 | $3,000 | $9,000 | 3.0 | Proactive Strategy |
| STRAND-01 | Strander #1 | Important | 4 | 4 | 50.0 | $1,600 | $4,800 | 3.0 | Proactive Strategy |

**Interpretation:**
- **DRYER-01 (Reactive)**: 28.6% PM ratio << 50% target, breakdowns cost 7.5× PM costs
- **Problem**: Insufficient preventive maintenance, run-to-failure approach
- **Actions**: 
  - Increase PM frequency: Quarterly → Monthly
  - Implement condition monitoring (vibration, temperature)
  - Expected impact: Reduce breakdowns 60%, lower total maintenance costs 40% ($5,200 annual savings)

---

## 9. Lost Production Quantification

**Business Question:** How much production are we losing to downtime and constraints?

**Use Case:** Business case development, capital project justification, continuous improvement ROI.

**Query:**
```sql
WITH production_losses AS (
  SELECT 
    d.date_id,
    SUM(e.capacity_tons_per_hour * f.total_downtime_minutes / 60.0) AS downtime_loss_tons,
    SUM(e.capacity_tons_per_hour * 
        (f.operating_minutes - f.operating_minutes * f.performance_pct / 100.0) / 60.0
    ) AS speed_loss_tons,
    SUM(f.actual_output_tons - f.good_output_tons) AS quality_loss_tons
  FROM fact_equipment_daily_oee f
  JOIN dim_equipment e ON f.equipment_id = e.equipment_id
  JOIN dim_date d ON f.date_id = d.date_id
  WHERE d.date_id >= CURRENT_DATE - INTERVAL '30 days'
    AND e.criticality_level IN ('Critical', 'Important')
  GROUP BY d.date_id
)
SELECT 
  'Last 30 Days' AS period,
  SUM(downtime_loss_tons) AS total_downtime_loss_tons,
  SUM(speed_loss_tons) AS total_speed_loss_tons,
  SUM(quality_loss_tons) AS total_quality_loss_tons,
  SUM(downtime_loss_tons + speed_loss_tons + quality_loss_tons) AS total_loss_tons,
  -- Economic impact (assume $450/ton revenue)
  SUM(downtime_loss_tons) * 450 AS downtime_loss_value_usd,
  SUM(speed_loss_tons) * 450 AS speed_loss_value_usd,
  SUM(quality_loss_tons) * 450 AS quality_loss_value_usd,
  SUM(downtime_loss_tons + speed_loss_tons + quality_loss_tons) * 450 AS total_loss_value_usd,
  -- Annualized projection
  SUM(downtime_loss_tons + speed_loss_tons + quality_loss_tons) * 450 * 12 AS annual_loss_projection_usd
FROM production_losses;
```

**Expected Output:**
| period | total_downtime_loss_tons | total_speed_loss_tons | total_quality_loss_tons | total_loss_tons | downtime_loss_value_usd | speed_loss_value_usd | quality_loss_value_usd | total_loss_value_usd | annual_loss_projection_usd |
|--------|------------------------|---------------------|----------------------|----------------|------------------------|--------------------|--------------------|-------------------|---------------------------|
| Last 30 Days | 120.0 | 45.0 | 12.0 | 177.0 | $54,000 | $20,250 | $5,400 | $79,650 | $955,800 |

**Interpretation:**
- **Downtime Losses (68%)**: $54K/month, $648K/year - **Highest priority**
- **Speed Losses (25%)**: $20K/month, $245K/year
- **Quality Losses (7%)**: $5K/month, $64K/year
- **Total Opportunity**: $80K/month, ~$1M/year
- **Improvement Targets**:
  - Reduce downtime 50% through reliability improvements → $324K/year value
  - Improve performance 5% through minor stop elimination → $12K/year value
  - Reduce defects 50% through process control → $32K/year value

---

## 10. Rolling 30-Day OEE Trend

**Business Question:** Is our OEE improving over time? Are initiatives working?

**Use Case:** Continuous improvement tracking, management reporting, initiative effectiveness assessment.

**Query:**
```sql
WITH daily_plant_oee AS (
  SELECT 
    f.date_id,
    AVG(f.oee_pct) AS daily_plant_oee_pct,
    SUM(f.good_output_tons) AS daily_production_tons
  FROM fact_equipment_daily_oee f
  JOIN dim_equipment e ON f.equipment_id = e.equipment_id
  WHERE e.criticality_level IN ('Critical', 'Important')
  GROUP BY f.date_id
)
SELECT 
  date_id,
  daily_plant_oee_pct,
  daily_production_tons,
  -- Rolling 7-day average
  AVG(daily_plant_oee_pct) OVER (
    ORDER BY date_id 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS rolling_7day_oee_pct,
  -- Rolling 30-day average
  AVG(daily_plant_oee_pct) OVER (
    ORDER BY date_id 
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS rolling_30day_oee_pct,
  -- Trend direction
  daily_plant_oee_pct - LAG(daily_plant_oee_pct, 30) OVER (ORDER BY date_id) AS oee_change_vs_30days_ago_pct,
  CASE 
    WHEN daily_plant_oee_pct - LAG(daily_plant_oee_pct, 30) OVER (ORDER BY date_id) > 2 
    THEN 'Improving Trend'
    WHEN daily_plant_oee_pct - LAG(daily_plant_oee_pct, 30) OVER (ORDER BY date_id) < -2 
    THEN 'Declining Trend'
    ELSE 'Stable'
  END AS trend_assessment
FROM daily_plant_oee
WHERE date_id >= CURRENT_DATE - INTERVAL '90 days'
ORDER BY date_id DESC;
```

**Expected Output:**
| date_id | daily_plant_oee_pct | daily_production_tons | rolling_7day_oee_pct | rolling_30day_oee_pct | oee_change_vs_30days_ago_pct | trend_assessment |
|---------|-------------------|---------------------|---------------------|----------------------|------------------------------|-----------------|
| 2024-03-15 | 82.5 | 240.0 | 81.8 | 79.2 | +5.3 | Improving Trend |
| 2024-03-14 | 80.2 | 235.0 | 81.5 | 78.8 | +4.8 | Improving Trend |
| 2024-03-13 | 83.1 | 245.0 | 81.2 | 78.5 | +5.1 | Improving Trend |

**Interpretation:**
- **Current OEE**: 82.5% (approaching 85% world-class target)
- **30-Day Trend**: +5.3% improvement (from 77.2% to 82.5%)
- **Assessment**: "Improving Trend" - Reliability initiatives showing results
- **Actions**: 
  - Continue current improvement trajectory
  - Sustain gains through standardized work
  - Target 85% OEE by Q2 (current pace achievable)

---

## Usage Tips

### Query Performance
- Add indexes on frequently filtered columns: `date_id`, `equipment_id`, `shift_id`
- Use `EXPLAIN` to analyze query plans
- Consider materialized views for complex calculations (OEE, MTBF, constraint scores)

### Parameterization
- Replace `CURRENT_DATE - INTERVAL 'X days'` with parameters for flexible date ranges
- Add `WHERE` clauses to filter by specific equipment, shifts, or production areas

### Visualization
- Query 1, 7, 10: Time-series line charts (trend over time)
- Query 2, 9: Pareto charts (bar + cumulative line)
- Query 3, 5: Tables with conditional formatting (heat maps)
- Query 4: Multi-line time-series (buffer levels over time)
- Query 6: Scatter plots (defect correlation analysis)

### Automation
- Schedule daily execution for Queries 1, 3, 9 (operational KPIs)
- Weekly execution for Queries 2, 7, 8 (tactical improvement)
- Monthly execution for Queries 5, 6, 10 (strategic planning)

---

## Additional Resources

- See [README.md](README.md) for project overview and business context
- See [DATA_DICTIONARY.md](DATA_DICTIONARY.md) for complete table/column documentation
- See [VISUALIZATION_GUIDE.md](VISUALIZATION_GUIDE.md) for dashboard design recommendations
