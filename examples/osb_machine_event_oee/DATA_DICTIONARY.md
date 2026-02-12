# Data Dictionary - OSB Machine Event to OEE Analytics

Complete documentation of all tables, columns, and business definitions for the OSB Machine Event to OEE Analytics data model.

## Table of Contents
1. [Dimension Tables](#dimension-tables)
2. [Staging Tables](#staging-tables)
3. [Fact Tables](#fact-tables)
4. [Metric Tables](#metric-tables)
5. [Analytics Tables](#analytics-tables)
6. [Calculated Fields](#calculated-fields)

---

## Dimension Tables

### dim_equipment

**Purpose:** Equipment catalog with technical specifications, criticality levels, and capacity ratings.

**Grain:** One row per piece of equipment in the OSB manufacturing facility.

**Key Columns:**
- `equipment_id` (PK) - Unique equipment identifier (e.g., DRYER-01)
- `production_area_id` (FK) - Link to production area
- `criticality_level` - Impact classification (Critical/Important/Standard)

| Column | Type | Description | Business Rules |
|--------|------|-------------|----------------|
| equipment_id | VARCHAR(50) | Unique equipment identifier | Format: EQUIP-NAME-## (e.g., DRYER-01) |
| equipment_name | VARCHAR(100) | Descriptive equipment name | Human-readable equipment type |
| production_area_id | VARCHAR(50) | Production area FK | Links to dim_production_area |
| equipment_type | VARCHAR(50) | Equipment category | Strander, Dryer, Press, Former, Saw, etc. |
| criticality_level | VARCHAR(20) | Impact on production | Critical (bottleneck), Important (high capacity), Standard |
| capacity_tons_per_hour | NUMERIC(10,2) | Rated throughput capacity | Dryer: 10 t/hr (bottleneck), Press: 18 t/hr, Strander: 6 t/hr |
| install_date | DATE | Equipment commissioning date | Used to calculate equipment age |

**Sample Query:**
```sql
-- Critical equipment with capacity constraints
SELECT equipment_id, equipment_name, capacity_tons_per_hour, criticality_level
FROM dim_equipment
WHERE criticality_level = 'Critical'
ORDER BY capacity_tons_per_hour;
```

---

### dim_production_area

**Purpose:** Manufacturing process areas with buffer capacities and process stage sequencing.

**Grain:** One row per production area/stage in the OSB manufacturing process.

**Key Columns:**
- `production_area_id` (PK) - Unique area identifier
- `process_order` - Sequence in production flow (1=upstream, 8=downstream)
- `buffer_capacity_hours` - Maximum buffer storage time

| Column | Type | Description | Business Rules |
|--------|------|-------------|----------------|
| production_area_id | VARCHAR(50) | Unique area identifier | Format: AREA-NAME (e.g., DRYING, PRESSING) |
| area_name | VARCHAR(100) | Production area name | Human-readable stage name |
| process_order | INTEGER | Sequence in production flow | 1=Stranding (upstream), 8=Packaging (downstream) |
| buffer_capacity_hours | NUMERIC(10,2) | Max buffer storage time | Green bins: 4h, Dry silos: 8h, Mat buffer: 0.5h |
| upstream_dependency | VARCHAR(50) | Upstream area ID | NULL for first stage, area_id for others |
| downstream_dependency | VARCHAR(50) | Downstream area ID | NULL for last stage, area_id for others |

**Sample Query:**
```sql
-- Production flow with buffer capacities
SELECT area_name, process_order, buffer_capacity_hours, 
       upstream_dependency, downstream_dependency
FROM dim_production_area
ORDER BY process_order;
```

---

### dim_reason_code

**Purpose:** Standardized downtime reason codes mapped to the Six Big Losses OEE model.

**Grain:** One row per downtime reason code.

**Key Columns:**
- `reason_code_id` (PK) - Unique reason code
- `oee_loss_category` - Maps to Six Big Losses (Equipment Failure, Setup, Small Stops, etc.)
- `is_planned` - Distinguishes planned vs unplanned downtime

| Column | Type | Description | Business Rules |
|--------|------|-------------|----------------|
| reason_code_id | VARCHAR(50) | Unique reason code | Format: RC### (e.g., RC001) |
| reason_description | VARCHAR(200) | Detailed downtime reason | Bearing failure, Hydraulic leak, PM, etc. |
| oee_loss_category | VARCHAR(50) | OEE loss classification | Equipment Failure, Setup, Small Stops, Reduced Speed, Startup/Production Rejects |
| loss_type | VARCHAR(20) | Primary loss impact | Availability, Performance, or Quality |
| is_planned | BOOLEAN | Planned vs unplanned | TRUE = PM/breaks, FALSE = breakdowns |
| typical_duration_minutes | INTEGER | Expected event duration | Used for anomaly detection |

**Sample Query:**
```sql
-- Unplanned downtime reasons (Availability losses)
SELECT reason_code_id, reason_description, oee_loss_category
FROM dim_reason_code
WHERE is_planned = FALSE AND loss_type = 'Availability'
ORDER BY oee_loss_category;
```

---

### dim_shift

**Purpose:** Shift schedule definitions for workforce performance analysis.

**Grain:** One row per shift.

**Key Columns:**
- `shift_id` (PK) - Unique shift identifier
- `shift_start_time` / `shift_end_time` - Shift hours

| Column | Type | Description | Business Rules |
|--------|------|-------------|----------------|
| shift_id | VARCHAR(20) | Unique shift identifier | Day, Swing, Night |
| shift_name | VARCHAR(50) | Shift name | Day (06:00-14:00), Swing (14:00-22:00), Night (22:00-06:00) |
| shift_start_time | TIME | Shift start time | 24-hour format |
| shift_end_time | TIME | Shift end time | 24-hour format |
| is_weekend_shift | BOOLEAN | Weekend indicator | TRUE for weekend shifts, FALSE for weekdays |

**Sample Query:**
```sql
-- Shift schedule
SELECT shift_id, shift_name, shift_start_time, shift_end_time
FROM dim_shift
ORDER BY shift_start_time;
```

---

### dim_product_spec

**Purpose:** OSB panel product specifications for quality compliance tracking.

**Grain:** One row per product specification/grade.

**Key Columns:**
- `product_spec_id` (PK) - Unique product identifier
- `thickness_mm` / `density_kg_m3` - Key quality parameters

| Column | Type | Description | Business Rules |
|--------|------|-------------|----------------|
| product_spec_id | VARCHAR(50) | Unique product identifier | Format: OSB-THICKNESS-GRADE (e.g., OSB-11-STRUCTURAL) |
| product_name | VARCHAR(100) | Product name | OSB 11mm Structural, OSB 15mm Sheathing, OSB 18mm Industrial |
| thickness_mm | NUMERIC(10,2) | Target thickness | 11mm, 15mm, 18mm |
| thickness_tolerance_mm | NUMERIC(10,2) | Allowable thickness variance | ±0.5mm for structural, ±1.0mm for industrial |
| density_kg_m3 | NUMERIC(10,2) | Target density | 650 kg/m³ typical |
| density_tolerance_kg_m3 | NUMERIC(10,2) | Allowable density variance | ±30 kg/m³ |

**Sample Query:**
```sql
-- Product specifications with tolerances
SELECT product_spec_id, product_name, thickness_mm, thickness_tolerance_mm, 
       density_kg_m3, density_tolerance_kg_m3
FROM dim_product_spec
ORDER BY thickness_mm;
```

---

### dim_date

**Purpose:** Date dimension for time-based analysis and reporting.

**Grain:** One row per calendar date.

**Key Columns:**
- `date_id` (PK) - Date in YYYY-MM-DD format
- `is_weekend`, `is_holiday` - Non-production days

| Column | Type | Description | Business Rules |
|--------|------|-------------|----------------|
| date_id | DATE | Calendar date (PK) | YYYY-MM-DD format |
| day_of_week | INTEGER | Day number (1=Monday, 7=Sunday) | 1-7 |
| day_of_week_name | VARCHAR(20) | Day name | Monday, Tuesday, etc. |
| week_of_year | INTEGER | ISO week number | 1-53 |
| month | INTEGER | Month number | 1-12 |
| month_name | VARCHAR(20) | Month name | January, February, etc. |
| quarter | INTEGER | Quarter number | 1-4 |
| year | INTEGER | Calendar year | YYYY |
| is_weekend | BOOLEAN | Weekend indicator | TRUE for Sat/Sun |
| is_holiday | BOOLEAN | Holiday indicator | TRUE for plant holidays |

**Sample Query:**
```sql
-- Production days (exclude weekends/holidays)
SELECT date_id, day_of_week_name
FROM dim_date
WHERE is_weekend = FALSE AND is_holiday = FALSE
ORDER BY date_id;
```

---

## Staging Tables

### stg_equipment_state_history

**Purpose:** Calculated state durations from raw equipment state change events using window functions.

**Grain:** One row per equipment state change event with calculated duration until next state change.

**Key Columns:**
- `equipment_id` (FK) - Equipment identifier
- `state_start_datetime` - Event timestamp
- `duration_seconds` - Calculated time in state (via LEAD window function)

| Column | Type | Description | Business Rules |
|--------|------|-------------|----------------|
| equipment_id | VARCHAR(50) | Equipment FK | Links to dim_equipment |
| state_start_datetime | TIMESTAMP | Event timestamp | When state change occurred |
| state | VARCHAR(50) | Equipment state | Running, Breakdown, Idle, Changeover, PM |
| reason_code_id | VARCHAR(50) | Downtime reason FK | NULL for Running state, required for Breakdown/PM |
| next_state_datetime | TIMESTAMP | Next event timestamp | Calculated via LEAD() OVER (PARTITION BY equipment_id ORDER BY state_start_datetime) |
| duration_seconds | INTEGER | Time in state (seconds) | next_state_datetime - state_start_datetime |
| duration_hours | NUMERIC(10,2) | Time in state (hours) | duration_seconds / 3600 |

**Calculation Logic:**
```sql
-- Calculate state durations using window function
SELECT 
  equipment_id,
  state_start_datetime,
  state,
  reason_code_id,
  LEAD(state_start_datetime) OVER (
    PARTITION BY equipment_id 
    ORDER BY state_start_datetime
  ) AS next_state_datetime,
  TIMESTAMPDIFF(
    SECOND, 
    state_start_datetime, 
    LEAD(state_start_datetime) OVER (...)
  ) AS duration_seconds
FROM raw_equipment_events;
```

**Sample Query:**
```sql
-- DRYER-01 downtime events with durations
SELECT state_start_datetime, state, reason_code_id, duration_hours
FROM stg_equipment_state_history
WHERE equipment_id = 'DRYER-01' AND state = 'Breakdown'
ORDER BY state_start_datetime;
```

---

## Fact Tables

### fact_equipment_daily_oee

**Purpose:** Daily OEE calculation by equipment with component breakdown (Availability × Performance × Quality).

**Grain:** One row per equipment per calendar date.

**Key Columns:**
- `equipment_id` (FK) - Equipment identifier
- `date_id` (FK) - Calendar date
- `oee_pct` - Overall Equipment Effectiveness (target ≥85%)

| Column | Type | Description | Business Rules |
|--------|------|-------------|----------------|
| equipment_id | VARCHAR(50) | Equipment FK | Links to dim_equipment |
| date_id | DATE | Date FK | Links to dim_date |
| planned_production_minutes | INTEGER | Scheduled production time | 24h × 60 - planned downtime (PM, breaks) |
| operating_minutes | INTEGER | Actual operating time | planned_production_minutes - unplanned downtime |
| availability_pct | NUMERIC(5,2) | Availability % | (operating_minutes / planned_production_minutes) × 100, Target >90% |
| actual_output_tons | NUMERIC(10,2) | Actual production | Measured output in tons |
| ideal_output_tons | NUMERIC(10,2) | Theoretical max output | operating_minutes × (capacity_tons_per_hour / 60) |
| performance_pct | NUMERIC(5,2) | Performance % | (actual_output_tons / ideal_output_tons) × 100, Target >95% |
| good_output_tons | NUMERIC(10,2) | Quality-compliant output | actual_output_tons - scrap - rework |
| quality_pct | NUMERIC(5,2) | Quality % | (good_output_tons / actual_output_tons) × 100, Target >99% |
| oee_pct | NUMERIC(5,2) | Overall Equipment Effectiveness | availability_pct × performance_pct × quality_pct, Target ≥85% |
| total_downtime_minutes | INTEGER | Sum of all downtime | Sum of Breakdown + PM + Changeover durations |

**Calculation Logic:**
```sql
-- OEE calculation
OEE = Availability × Performance × Quality

WHERE:
  Availability = Operating Time / Planned Production Time
  Performance = Actual Output / Ideal Output @ Capacity
  Quality = Good Output / Total Output

TARGETS:
  World-Class OEE ≥ 85%
  Availability > 90%
  Performance > 95%
  Quality > 99%
```

**Sample Query:**
```sql
-- Daily OEE for DRYER-01
SELECT date_id, oee_pct, availability_pct, performance_pct, quality_pct
FROM fact_equipment_daily_oee
WHERE equipment_id = 'DRYER-01'
ORDER BY date_id;
```

---

## Metric Tables

### equipment_reliability_metrics

**Purpose:** Equipment reliability KPIs (MTBF, MTTR, failure frequency) for maintenance planning.

**Grain:** One row per equipment for the analysis period.

**Key Columns:**
- `equipment_id` (FK) - Equipment identifier
- `mtbf_hours` - Mean Time Between Failures (target varies by equipment)
- `mttr_hours` - Mean Time To Repair (target <2 hours)

| Column | Type | Description | Business Rules |
|--------|------|-------------|----------------|
| equipment_id | VARCHAR(50) | Equipment FK | Links to dim_equipment |
| analysis_start_date | DATE | Period start date | First date in analysis window |
| analysis_end_date | DATE | Period end date | Last date in analysis window |
| total_operating_hours | NUMERIC(10,2) | Total running time | Sum of state='Running' durations |
| total_failure_count | INTEGER | Number of breakdowns | Count of state='Breakdown' events |
| mtbf_hours | NUMERIC(10,2) | Mean Time Between Failures | total_operating_hours / total_failure_count |
| total_downtime_hours | NUMERIC(10,2) | Sum of breakdown durations | Sum of Breakdown state durations |
| mttr_hours | NUMERIC(10,2) | Mean Time To Repair | total_downtime_hours / total_failure_count |
| availability_pct | NUMERIC(5,2) | Availability % | (total_operating_hours / (total_operating_hours + total_downtime_hours)) × 100 |

**Sample Query:**
```sql
-- Equipment ranked by MTBF (lowest = least reliable)
SELECT equipment_id, mtbf_hours, mttr_hours, total_failure_count
FROM equipment_reliability_metrics
ORDER BY mtbf_hours ASC;
```

---

### equipment_downtime_analysis

**Purpose:** Downtime aggregated by equipment and reason code for Pareto analysis.

**Grain:** One row per equipment per reason code for the analysis period.

**Key Columns:**
- `equipment_id` (FK) - Equipment identifier
- `reason_code_id` (FK) - Downtime reason
- `total_downtime_hours` - Sum of downtime for this equipment-reason combination

| Column | Type | Description | Business Rules |
|--------|------|-------------|----------------|
| equipment_id | VARCHAR(50) | Equipment FK | Links to dim_equipment |
| reason_code_id | VARCHAR(50) | Reason code FK | Links to dim_reason_code |
| total_downtime_hours | NUMERIC(10,2) | Sum of downtime | Sum of state durations for this reason |
| downtime_event_count | INTEGER | Number of occurrences | Count of breakdown events |
| avg_downtime_hours | NUMERIC(10,2) | Average event duration | total_downtime_hours / downtime_event_count |
| percent_of_equipment_downtime | NUMERIC(5,2) | Proportion of equipment's total downtime | (total_downtime_hours / SUM(total_downtime_hours) OVER equipment) × 100 |

**Sample Query:**
```sql
-- DRYER-01 downtime by reason (Pareto)
SELECT reason_code_id, total_downtime_hours, downtime_event_count, 
       percent_of_equipment_downtime
FROM equipment_downtime_analysis
WHERE equipment_id = 'DRYER-01'
ORDER BY total_downtime_hours DESC;
```

---

### failure_mode_pareto

**Purpose:** Pareto ranking of failure modes to identify the "vital few" causes of downtime.

**Grain:** One row per reason code, ranked by downtime impact across all equipment.

**Key Columns:**
- `reason_code_id` (FK) - Downtime reason
- `cumulative_percent` - Running total of downtime contribution (80/20 rule)

| Column | Type | Description | Business Rules |
|--------|------|-------------|----------------|
| reason_code_id | VARCHAR(50) | Reason code FK | Links to dim_reason_code |
| total_downtime_hours | NUMERIC(10,2) | Plant-wide downtime for this reason | Sum across all equipment |
| percent_of_total_downtime | NUMERIC(5,2) | Proportion of plant downtime | (total_downtime_hours / plant_total_downtime) × 100 |
| cumulative_percent | NUMERIC(5,2) | Running total percentage | SUM(percent_of_total_downtime) OVER (ORDER BY total_downtime_hours DESC) |
| pareto_rank | INTEGER | Rank by impact | ROW_NUMBER() OVER (ORDER BY total_downtime_hours DESC) |

**Sample Query:**
```sql
-- Top 10 failure modes causing 80% of downtime (Pareto)
SELECT reason_code_id, total_downtime_hours, percent_of_total_downtime, cumulative_percent
FROM failure_mode_pareto
WHERE cumulative_percent <= 80
ORDER BY pareto_rank;
```

---

### buffer_utilization_analysis

**Purpose:** Time-series simulation of buffer inventory levels for starvation/blocking prediction.

**Grain:** One row per production area per hour.

**Key Columns:**
- `production_area_id` (FK) - Production area
- `hour_timestamp` - Hourly timestamp
- `buffer_level_hours` - Estimated inventory in buffer (hours of supply)

| Column | Type | Description | Business Rules |
|--------|------|-------------|----------------|
| production_area_id | VARCHAR(50) | Area FK | Links to dim_production_area |
| hour_timestamp | TIMESTAMP | Hour start time | Hourly granularity for trending |
| buffer_level_hours | NUMERIC(10,2) | Current buffer inventory | Hours of downstream supply, range 0 to buffer_capacity_hours |
| buffer_utilization_pct | NUMERIC(5,2) | Utilization % | (buffer_level_hours / buffer_capacity_hours) × 100 |
| inflow_rate_tons_per_hour | NUMERIC(10,2) | Upstream throughput | Tons/hr feeding into buffer |
| outflow_rate_tons_per_hour | NUMERIC(10,2) | Downstream consumption | Tons/hr depleting buffer |
| is_starved | BOOLEAN | Starvation indicator | TRUE when buffer_level_hours = 0 |
| is_blocked | BOOLEAN | Blocking indicator | TRUE when buffer_level_hours = buffer_capacity_hours |

**Sample Query:**
```sql
-- Green bins buffer (between Stranding and Drying) utilization over time
SELECT hour_timestamp, buffer_level_hours, buffer_utilization_pct, is_starved, is_blocked
FROM buffer_utilization_analysis
WHERE production_area_id = 'DRYING'
ORDER BY hour_timestamp;
```

---

### starvation_blocking_analysis

**Purpose:** Root cause analysis of when equipment stopped due to upstream failures (starvation) or downstream failures (blocking).

**Grain:** One row per equipment per starvation/blocking event.

**Key Columns:**
- `equipment_id` (FK) - Affected equipment
- `root_cause_equipment_id` (FK) - Equipment that caused starvation/blocking
- `event_type` - Starvation or Blocking

| Column | Type | Description | Business Rules |
|--------|------|-------------|----------------|
| equipment_id | VARCHAR(50) | Affected equipment FK | Equipment that stopped |
| event_start_datetime | TIMESTAMP | Event start time | When starvation/blocking began |
| event_type | VARCHAR(20) | Event classification | Starvation (upstream failure), Blocking (downstream failure) |
| root_cause_equipment_id | VARCHAR(50) | Root cause equipment FK | Equipment failure that caused propagation |
| propagation_time_hours | NUMERIC(10,2) | Time from root cause to impact | Buffer depletion duration |
| lost_production_tons | NUMERIC(10,2) | Production impact | capacity_tons_per_hour × event_duration_hours |

**Sample Query:**
```sql
-- Starvation events with root causes
SELECT equipment_id, root_cause_equipment_id, event_start_datetime, 
       propagation_time_hours, lost_production_tons
FROM starvation_blocking_analysis
WHERE event_type = 'Starvation'
ORDER BY lost_production_tons DESC;
```

---

### constraint_analysis

**Purpose:** Theory of Constraints (TOC) analysis to identify the bottleneck limiting plant throughput.

**Grain:** One row per equipment with constraint scoring.

**Key Columns:**
- `equipment_id` (FK) - Equipment identifier
- `constraint_score` - TOC constraint rating (higher = more constraining)

| Column | Type | Description | Business Rules |
|--------|------|-------------|----------------|
| equipment_id | VARCHAR(50) | Equipment FK | Links to dim_equipment |
| average_utilization_pct | NUMERIC(5,2) | Average equipment utilization | (operating_hours / available_hours) × 100 |
| starvation_frequency_percent | NUMERIC(5,2) | Frequency of starvation events | (starvation_hours / total_hours) × 100 |
| blocking_frequency_percent | NUMERIC(5,2) | Frequency of blocking events | (blocking_hours / total_hours) × 100 |
| constraint_score | NUMERIC(10,2) | TOC constraint rating | utilization × (1 + starvation_frequency / 100), higher = bottleneck |
| throughput_tons_per_day | NUMERIC(10,2) | Average daily throughput | Plant-wide throughput limited by this equipment |
| capacity_gap_tons_per_day | NUMERIC(10,2) | Capacity shortfall | (demand - throughput) if constraint |

**Sample Query:**
```sql
-- Identify plant constraint (highest constraint score)
SELECT equipment_id, average_utilization_pct, constraint_score, 
       throughput_tons_per_day, capacity_gap_tons_per_day
FROM constraint_analysis
ORDER BY constraint_score DESC
LIMIT 1;
```

---

## Analytics Tables

### bad_actor_prioritization

**Purpose:** Equipment prioritization for maintenance investment decisions using impact scoring.

**Grain:** One row per equipment, ranked by impact score.

**Key Columns:**
- `equipment_id` (FK) - Equipment identifier
- `impact_score` - Weighted score (downtime × frequency × criticality)
- `priority_rank` - Investment priority ranking

| Column | Type | Description | Business Rules |
|--------|------|-------------|----------------|
| equipment_id | VARCHAR(50) | Equipment FK | Links to dim_equipment |
| criticality_level | VARCHAR(20) | Equipment criticality | Critical, Important, Standard |
| total_downtime_hours | NUMERIC(10,2) | Total breakdown time | Sum of Breakdown state durations |
| total_failures | INTEGER | Number of breakdowns | Count of Breakdown events |
| mtbf_hours | NUMERIC(10,2) | Mean Time Between Failures | From equipment_reliability_metrics |
| criticality_multiplier | INTEGER | Weight factor | Critical=3, Important=2, Standard=1 | 
| impact_score | NUMERIC(10,2) | Prioritization score | total_downtime_hours × total_failures × criticality_multiplier |
| priority_rank | INTEGER | Investment priority | ROW_NUMBER() OVER (ORDER BY impact_score DESC) |

**Calculation Logic:**
```sql
-- Impact score formula
impact_score = total_downtime_hours × total_failures × criticality_multiplier

WHERE criticality_multiplier:
  Critical = 3 (bottleneck, high capacity, safety-critical)
  Important = 2 (high capacity, not bottleneck)
  Standard = 1 (redundant capacity, low impact)
```

**Sample Query:**
```sql
-- Top 5 bad actors for maintenance investment
SELECT equipment_id, criticality_level, total_downtime_hours, total_failures, 
       impact_score, priority_rank
FROM bad_actor_prioritization
ORDER BY priority_rank
LIMIT 5;
```

---

### shift_performance_comparison

**Purpose:** Compare OEE and availability across shifts to identify training needs and performance gaps.

**Grain:** One row per shift with aggregated performance metrics.

**Key Columns:**
- `shift_id` (FK) - Shift identifier
- `avg_availability_pct` - Average availability across all equipment for this shift

| Column | Type | Description | Business Rules |
|--------|------|-------------|----------------|
| shift_id | VARCHAR(20) | Shift FK | Links to dim_shift |
| shift_name | VARCHAR(50) | Shift name | Day, Swing, Night |
| avg_availability_pct | NUMERIC(5,2) | Shift availability % | AVG(availability_pct) across equipment |
| total_operating_hours | NUMERIC(10,2) | Sum of operating time | Sum across all equipment |
| total_downtime_hours | NUMERIC(10,2) | Sum of downtime | Sum of Breakdown + PM durations |
| shift_performance_rank | INTEGER | Ranking | ROW_NUMBER() OVER (ORDER BY avg_availability_pct DESC) |

**Sample Query:**
```sql
-- Shift performance comparison
SELECT shift_name, avg_availability_pct, total_operating_hours, total_downtime_hours, 
       shift_performance_rank
FROM shift_performance_comparison
ORDER BY shift_performance_rank;
```

---

## Calculated Fields

### OEE Components

**Availability:**
```
Availability = Operating Time / Planned Production Time
            = (Planned Time - Unplanned Downtime) / Planned Time
            = (24h - PM - Breaks - Breakdowns) / (24h - PM - Breaks)

Target: >90%
```

**Performance:**
```
Performance = Actual Output / Ideal Output
           = Actual Tons / (Operating Hours × Capacity Tons/Hr)

Target: >95%
Losses: Minor stops, reduced speed
```

**Quality:**
```
Quality = Good Output / Total Output
       = (Total Output - Scrap - Rework) / Total Output

Target: >99%
Losses: Startup rejects, production rejects
```

**OEE:**
```
OEE = Availability × Performance × Quality

World-Class Target: ≥85%

Example:
  Availability 90% × Performance 95% × Quality 99% = 84.7% OEE
```

### Reliability Metrics

**MTBF (Mean Time Between Failures):**
```
MTBF = Total Operating Hours / Total Failure Count

Example: 
  2,160 operating hours / 45 failures = 48 hour MTBF
  
Interpretation: On average, equipment runs 48 hours between breakdowns
```

**MTTR (Mean Time To Repair):**
```
MTTR = Total Downtime Hours / Total Failure Count

Example:
  108 downtime hours / 45 failures = 2.4 hour MTTR
  
Interpretation: On average, repairs take 2.4 hours
Target: <2 hours for Critical equipment
```

**Impact Score (Bad Actor):**
```
Impact Score = Downtime Hours × Failure Count × Criticality Multiplier

WHERE Criticality Multiplier:
  Critical = 3
  Important = 2
  Standard = 1
  
Example (DRYER-01):
  12 downtime hours × 5 failures × 3 (Critical) = 180 impact score
  
Interpretation: Higher score = higher priority for maintenance investment
```

### Constraint Analysis

**Constraint Score (TOC):**
```
Constraint Score = Utilization % × (1 + Starvation Frequency % / 100)

Example (DRYER-01):
  100% utilization × (1 + 0% starvation / 100) = 100 constraint score
  
Example (PRESS-01):
  56% utilization × (1 + 5% starvation / 100) = 58.8 constraint score
  
Interpretation: Highest score = bottleneck (constraint)
```

**Capacity Gap:**
```
Capacity Gap = Demand - Actual Throughput

Example:
  Demand: 270 tons/day
  Actual: 240 tons/day (limited by Dryer @ 10 t/hr)
  Gap: 30 tons/day
  
Economic Impact: 30 tons/day × $450/ton = $13,500/day revenue loss
```

### Buffer Utilization

**Buffer Level (Hours of Supply):**
```
Buffer Level[t] = Buffer Level[t-1] + (Inflow - Outflow) × Δt / Capacity

Example (Green Bins):
  Initial: 2 hours supply
  Inflow: 12 t/hr (Stranders running)
  Outflow: 0 t/hr (Dryer breakdown)
  After 2 hours: 2h + (12 - 0) × 2 / 6 = 6 hours supply (blocked)
  
Interpretation: 
  0 hours = Starvation (downstream stopped)
  Max capacity = Blocking (upstream must stop)
  Optimal: 40-60% of capacity
```

---

## Usage Examples

### Daily Operations Report
```sql
-- Plant-wide OEE summary
SELECT 
  d.date_id,
  AVG(f.oee_pct) AS plant_oee_pct,
  SUM(f.good_output_tons) AS total_production_tons
FROM fact_equipment_daily_oee f
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.date_id = CURRENT_DATE - 1
GROUP BY d.date_id;
```

### Maintenance Prioritization
```sql
-- Top 3 bad actors with ROI recommendations
SELECT 
  b.equipment_id,
  e.equipment_name,
  b.impact_score,
  r.mtbf_hours,
  'Double MTBF from ' || r.mtbf_hours || 'h to ' || (r.mtbf_hours * 2) || 'h' AS recommendation
FROM bad_actor_prioritization b
JOIN dim_equipment e ON b.equipment_id = e.equipment_id
JOIN equipment_reliability_metrics r ON b.equipment_id = r.equipment_id
ORDER BY b.priority_rank
LIMIT 3;
```

### Constraint Identification
```sql
-- Current bottleneck with economic impact
SELECT 
  c.equipment_id,
  e.equipment_name,
  e.capacity_tons_per_hour,
  c.constraint_score,
  c.capacity_gap_tons_per_day,
  c.capacity_gap_tons_per_day * 450 AS daily_revenue_loss_usd
FROM constraint_analysis c
JOIN dim_equipment e ON c.equipment_id = e.equipment_id
WHERE c.analysis_type = 'Constraint Identification'
ORDER BY c.constraint_score DESC
LIMIT 1;
```

---

## Additional Resources

- See [README.md](README.md) for project overview and business context
- See [EXAMPLE_QUERIES.md](EXAMPLE_QUERIES.md) for 10 common analytics queries
- See [VISUALIZATION_GUIDE.md](VISUALIZATION_GUIDE.md) for dashboard design recommendations
