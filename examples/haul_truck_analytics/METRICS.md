# Haul Truck Analytics Metrics Catalog

## Overview

This document describes the **key performance indicators (KPIs)** tracked in the Haul Truck Analytics data warehouse. For each metric, we provide:

- **Business definition** and purpose
- **Calculation formula** with SQL reference
- **Industry benchmarks** or typical ranges
- **Interpretation guidance** (what's good/bad)
- **Actionable insights** (what to do with the metric)

All metrics are calculated from the `fact_haul_cycle` fact table and aggregated into materialized metrics tables for fast query performance.

## Metrics Categories

1. [Productivity Metrics](#productivity-metrics) - Material moved, cycle times, throughput
2. [Utilization Metrics](#utilization-metrics) - Asset and resource utilization
3. [Queue Metrics](#queue-metrics) - Bottleneck analysis and wait times
4. [Efficiency Metrics](#efficiency-metrics) - Fuel, speed, distance optimization
5. [Quality Metrics](#quality-metrics) - Payload compliance, cycle completeness
6. [Operator Metrics](#operator-metrics) - Performance by driver

---

## Productivity Metrics

### 1. Tons per Hour (TPH)

**Business Purpose**: Primary productivity measure - material moved per unit time

**Definition**: Total payload tons moved divided by productive operating hours

**Calculation**:
```sql
SELECT
  truck_id,
  date_id,
  SUM(payload_tons) AS total_tons_moved,
  SUM((julianday(cycle_end) - julianday(cycle_start)) * 24) AS total_productive_hours,
  SUM(payload_tons) / SUM((julianday(cycle_end) - julianday(cycle_start)) * 24) AS tons_per_hour
FROM fact_haul_cycle
GROUP BY truck_id, date_id;
```

**SQL Reference**: `models/metrics/truck_daily_productivity.sql`

**Benchmarks by Fleet Class**:
| Fleet Class | Target TPH | Good Performance | Poor Performance |
|-------------|------------|------------------|------------------|
| 100-ton     | 80-120     | >110             | <70              |
| 200-ton     | 150-200    | >180             | <130             |
| 400-ton     | 280-350    | >320             | <250             |

**Factors Affecting TPH**:
- Haul distance (longer distance = lower TPH)
- Queue times (more queuing = lower TPH)
- Cycle time efficiency (faster cycles = higher TPH)
- Payload utilization (fuller loads = higher TPH)

**Interpretation**:
- **Above Target**: Truck is performing well, efficient operations
- **Below Target**: Investigate causes (mechanical issues, operator performance, route problems, excessive queuing)
- **High Variability**: Indicates inconsistent operations (dispatch issues, variable conditions)

**Actionable Insights**:
- Compare TPH across trucks to identify underperformers
- Track TPH trends over time to detect degradation (maintenance needed)
- Compare TPH by shift to identify operational differences
- Calculate cost per ton using TPH and operating costs

---

### 2. Cycle Time

**Business Purpose**: Measure efficiency of complete haul cycle (loading → dumping → return)

**Definition**: Duration of complete cycle from loading start to next loading start

**Calculation**:
```sql
SELECT
  cycle_id,
  truck_id,
  (julianday(cycle_end) - julianday(cycle_start)) * 24 * 60 AS cycle_duration_min,
  -- Component breakdown
  duration_loading_min,
  duration_hauling_loaded_min,
  duration_queue_crusher_min,
  duration_dumping_min,
  duration_returning_min,
  duration_queue_shovel_min,
  duration_spot_delays_min
FROM fact_haul_cycle;
```

**SQL Reference**: `models/facts/fact_haul_cycle.sql`

**Benchmarks by Haul Distance**:
| Haul Distance | Target Cycle Time | Good    | Poor       |
|---------------|-------------------|---------|------------|
| Short (3-5 km)| 25-35 minutes     | <30 min | >40 min    |
| Medium (5-8 km)| 35-50 minutes    | <45 min | >55 min    |
| Long (8-12 km)| 50-75 minutes     | <65 min | >80 min    |

**Component Targets** (for medium haul):
- Loading: 4-8 minutes (depends on truck-shovel match)
- Hauling Loaded: 12-25 minutes (depends on distance, grade, conditions)
- Queue at Crusher: <8 minutes (if crusher is NOT bottleneck)
- Dumping: 1-2 minutes
- Returning Empty: 8-18 minutes (faster than loaded)
- Queue at Shovel: <3 minutes (if shovel is NOT bottleneck)
- Spot Delays: <5 minutes (should be minimal)

**Interpretation**:
- **Low Cycle Time**: Efficient operations, good productivity
- **High Cycle Time**: Investigate specific components:
  - High loading time → shovel efficiency issue
  - High haul/return time → road conditions, operator speed
  - High queue times → bottleneck (see Queue Metrics)
  - High spot delays → maintenance or operational issues

**Actionable Insights**:
- Break down cycle time by component to identify problem areas
- Compare cycle times by operator to identify training needs
- Track cycle time trends to detect degradation
- Use cycle time to estimate required fleet size for target throughput

---

### 3. Cycles per Shift

**Business Purpose**: Measure truck productivity over standard work period (12-hour shift)

**Definition**: Number of complete haul cycles completed in a 12-hour shift

**Calculation**:
```sql
SELECT
  truck_id,
  shift_id,
  date_id,
  COUNT(*) AS cycles_completed,
  AVG((julianday(cycle_end) - julianday(cycle_start)) * 24 * 60) AS avg_cycle_time_min
FROM fact_haul_cycle
GROUP BY truck_id, shift_id, date_id;
```

**SQL Reference**: `models/metrics/truck_daily_productivity.sql`

**Benchmarks**:
| Haul Distance | Target Cycles/Shift | Good  | Poor  |
|---------------|---------------------|-------|-------|
| Short haul    | 14-18               | >16   | <12   |
| Medium haul   | 10-14               | >12   | <9    |
| Long haul     | 7-10                | >9    | <6    |

**Factors Affecting Cycles/Shift**:
- Average cycle time (primary driver)
- Shift delays (pre-start inspections, breaks, refueling)
- Equipment availability (downtime reduces cycles)
- Dispatch efficiency (minimize empty travel, optimize assignments)

**Interpretation**:
- **Above Target**: High productivity, efficient operations
- **Below Target**: Investigate causes (long cycle times, excessive downtime, dispatch inefficiency)
- **Variability**: Compare day vs night shifts to identify shift-specific issues

**Actionable Insights**:
- Set daily production targets based on cycles/shift
- Compare actual vs target to identify shortfalls
- Use for capacity planning (trucks needed = target tons / (cycles per shift × avg payload))
- Benchmark operators and trucks against fleet average

---

## Utilization Metrics

### 4. Truck Utilization

**Business Purpose**: Measure proportion of available time spent productively hauling

**Definition**: Productive operating time as percentage of scheduled shift time

**Calculation**:
```sql
WITH shift_time AS (
  SELECT 12 AS scheduled_hours_per_shift  -- 12-hour shifts
),
productive_time AS (
  SELECT
    truck_id,
    shift_id,
    date_id,
    SUM((julianday(cycle_end) - julianday(cycle_start)) * 24) AS productive_hours
  FROM fact_haul_cycle
  GROUP BY truck_id, shift_id, date_id
)
SELECT
  pt.truck_id,
  pt.shift_id,
  pt.date_id,
  pt.productive_hours,
  st.scheduled_hours_per_shift,
  (pt.productive_hours / st.scheduled_hours_per_shift) * 100 AS utilization_pct
FROM productive_time pt
CROSS JOIN shift_time st;
```

**SQL Reference**: `models/metrics/truck_daily_productivity.sql`

**Benchmarks**:
| Utilization % | Rating     | Interpretation            |
|---------------|------------|---------------------------|
| >85%          | Excellent  | Minimal downtime          |
| 75-85%        | Good       | Normal operations         |
| 65-75%        | Fair       | Some inefficiencies       |
| <65%          | Poor       | Significant issues        |

**Reasons for Low Utilization**:
- Mechanical downtime (breakdowns, repairs)
- Excessive queue times (bottlenecks)
- Shift delays (pre-start inspections, refueling, operator changes)
- Dispatch inefficiency (trucks waiting for assignments)
- Weather or operational constraints

**Interpretation**:
- **High Utilization**: Truck is productive, minimal wasted time
- **Low Utilization**: Investigate downtime causes (maintenance logs, dispatch records, operator feedback)

**Actionable Insights**:
- Track utilization trends to detect declining performance
- Compare utilization across fleet to identify problem assets
- Calculate cost impact of low utilization (opportunity cost)
- Use for fleet sizing decisions (high utilization may indicate fleet shortage)

---

### 5. Payload Utilization

**Business Purpose**: Ensure trucks are loaded to optimal capacity (not underloaded or overloaded)

**Definition**: Actual payload as percentage of rated truck capacity

**Calculation**:
```sql
SELECT
  fc.cycle_id,
  fc.truck_id,
  fc.payload_tons,
  dt.payload_capacity_tons,
  (fc.payload_tons / dt.payload_capacity_tons) * 100 AS payload_utilization_pct,
  CASE
    WHEN (fc.payload_tons / dt.payload_capacity_tons) * 100 < 85 THEN 'underload'
    WHEN (fc.payload_tons / dt.payload_capacity_tons) * 100 >= 85 
      AND (fc.payload_tons / dt.payload_capacity_tons) * 100 < 95 THEN 'suboptimal'
    WHEN (fc.payload_tons / dt.payload_capacity_tons) * 100 >= 95
      AND (fc.payload_tons / dt.payload_capacity_tons) * 100 <= 105 THEN 'optimal'
    WHEN (fc.payload_tons / dt.payload_capacity_tons) * 100 > 105 THEN 'overload'
    ELSE 'unknown'
  END AS payload_band
FROM fact_haul_cycle fc
JOIN dim_truck dt ON fc.truck_id = dt.truck_id;
```

**SQL Reference**: `models/analytics/payload_compliance.sql`

**Payload Bands**:
| Band       | Range        | Interpretation                          | Action Required    |
|------------|--------------|-----------------------------------------|--------------------|
| Underload  | <85%         | Inefficient, wasted cycles              | Retrain, optimize  |
| Suboptimal | 85-95%       | Acceptable but not ideal                | Minor optimization |
| Optimal    | 95-105%      | Target range, maximizes productivity    | None               |
| Overload   | >105%        | Safety concern, equipment stress        | Enforce limits     |

**Industry Best Practices**:
- Target: 95-105% of rated capacity
- Consistent overloading (>105%) indicates shovel-truck mismatch
- Frequent underloading (<85%) indicates operator training issue or poor bucket fill factors

**Interpretation**:
- **Optimal Range (95-105%)**: Good truck-shovel matching, proper loading technique
- **Underload (<85%)**: Wasted cycles, operator may be conservative or shovel underfilling
- **Overload (>105%)**: Safety risk (suspension, brakes), equipment wear, potential regulation violation

**Actionable Insights**:
- Analyze payload distribution by shovel to identify mismatch (e.g., 100-ton truck at 400-ton shovel)
- Track payload by operator to identify training needs
- Investigate consistent overloading (may need smaller trucks or larger shovel)
- Calculate productivity impact of underloading (e.g., 10% underload = 10% fewer tons per cycle)

---

### 6. Shovel Utilization

**Business Purpose**: Measure efficiency of loading equipment (minimize idle time)

**Definition**: Time shovel spends loading trucks vs time available

**Calculation**:
```sql
WITH shovel_states AS (
  SELECT
    shovel_id,
    date_id,
    SUM(CASE WHEN operational_state = 'loading' THEN duration_min / 60.0 ELSE 0 END) AS loading_hours,
    -- Idle time when trucks are queued (shovel could load but trucks not positioned)
    SUM(CASE WHEN operational_state = 'queued_at_shovel' THEN duration_min / 60.0 ELSE 0 END) AS idle_hours_with_queue
  FROM stg_truck_states
  WHERE location_zone LIKE 'Shovel_%'
  GROUP BY shovel_id, date_id
),
shovel_metrics AS (
  SELECT
    shovel_id,
    date_id,
    loading_hours,
    -- Available hours (assuming 24-hour operation, 2 shifts × 12 hours)
    24 AS available_hours,
    (loading_hours / 24) * 100 AS utilization_pct
  FROM shovel_states
)
SELECT * FROM shovel_metrics;
```

**SQL Reference**: `models/metrics/shovel_utilization.sql`

**Benchmarks**:
| Utilization % | Rating     | Interpretation               |
|---------------|------------|------------------------------|
| >75%          | Excellent  | High truck demand, efficient |
| 65-75%        | Good       | Normal operations            |
| 55-65%        | Fair       | Some idle time               |
| <55%          | Poor       | Underutilized, bottleneck elsewhere |

**Interpretation**:
- **High Utilization (>75%)**: Shovel is in demand, may be constraint
- **Low Utilization (<55%)**: Shovel waiting for trucks, truck fleet may be undersized or crusher is bottleneck

**Actionable Insights**:
- Compare shovel utilization to crusher utilization to identify system constraint
- High shovel utilization + low crusher utilization = crusher is bottleneck
- Low shovel utilization + high crusher utilization = not enough trucks or shovel is constraint
- Track utilization by shift to identify dispatch efficiency differences

---

### 7. Crusher Utilization

**Business Purpose**: Measure primary crushing equipment efficiency and identify bottleneck

**Definition**: Time crusher spends receiving material vs capacity

**Calculation**:
```sql
SELECT
  crusher_id,
  date_id,
  -- Actual throughput
  SUM(payload_tons) AS total_tons_received,
  -- Actual operating time (dumping + queue time)
  SUM(duration_dumping_min + duration_queue_crusher_min) / 60.0 AS total_operating_hours,
  -- Actual TPH
  SUM(payload_tons) / (SUM(duration_dumping_min + duration_queue_crusher_min) / 60.0) AS actual_tph,
  -- Crusher capacity (from dim_crusher)
  dc.capacity_tph,
  -- Utilization
  (SUM(payload_tons) / (SUM(duration_dumping_min + duration_queue_crusher_min) / 60.0) / dc.capacity_tph) * 100 AS utilization_pct,
  -- Queue indicator (high queue time suggests crusher is constraint)
  AVG(duration_queue_crusher_min) AS avg_queue_time_min
FROM fact_haul_cycle fc
JOIN dim_crusher dc ON fc.crusher_id = dc.crusher_id
GROUP BY crusher_id, date_id, dc.capacity_tph;
```

**SQL Reference**: `models/metrics/crusher_throughput.sql`

**Benchmarks**:
| Utilization % | Queue Time  | Interpretation                      |
|---------------|-------------|-------------------------------------|
| >90%          | >10 min     | Crusher is bottleneck (at capacity) |
| 75-90%        | 5-10 min    | High utilization, manageable        |
| 60-75%        | <5 min      | Normal operations                   |
| <60%          | <3 min      | Underutilized, not a constraint     |

**Interpretation**:
- **High Utilization (>90%) + High Queue Time**: Crusher is system bottleneck, consider capacity expansion
- **High Utilization + Low Queue Time**: Efficient operations, dispatch smoothing arrivals
- **Low Utilization**: Crusher is not constraint, look upstream (shovel/truck capacity)

**Actionable Insights**:
- Compare crusher utilization to shovel utilization to identify constraint
- If crusher is bottleneck: optimize dispatch to smooth arrivals, consider capacity upgrade
- Track utilization by shift to identify crew performance differences
- Calculate cost impact of crusher downtime (entire system stops when crusher unavailable)

---

## Queue Metrics

### 8. Queue Time

**Business Purpose**: Identify bottlenecks in the haul system (where trucks wait)

**Definition**: Time trucks spend waiting at shovel or crusher before loading/dumping

**Calculation**:
```sql
SELECT
  CASE
    WHEN operational_state = 'queued_at_shovel' THEN 'SHOVEL'
    WHEN operational_state = 'queued_at_crusher' THEN 'CRUSHER'
  END AS queue_location,
  location_zone,
  shift_id,
  date_id,
  -- Queue metrics
  COUNT(*) AS queue_events,
  AVG(duration_min) AS avg_queue_time_min,
  MAX(duration_min) AS max_queue_time_min,
  SUM(duration_min) / 60.0 AS total_queue_hours,
  COUNT(DISTINCT truck_id) AS trucks_affected
FROM stg_truck_states
WHERE operational_state IN ('queued_at_shovel', 'queued_at_crusher')
GROUP BY queue_location, location_zone, shift_id, date_id;
```

**SQL Reference**: `models/metrics/queue_analysis.sql`

**Benchmarks**:
| Location | Target Queue Time | Acceptable  | Concerning    |
|----------|-------------------|-------------|---------------|
| Shovel   | <3 minutes        | <5 minutes  | >8 minutes    |
| Crusher  | <8 minutes        | <12 minutes | >15 minutes   |

**Interpretation**:
- **Low Queue Times (<target)**: Balanced system, no major bottleneck
- **High Crusher Queue, Low Shovel Queue**: Crusher is bottleneck
- **High Shovel Queue, Low Crusher Queue**: Shovel is bottleneck or truck fleet oversized
- **High Queue Times Both**: Dispatch inefficiency, poor scheduling

**Constraint Identification**:
```
If AVG(queue_crusher) > AVG(queue_shovel) × 2:
    → Crusher is constraint (bottleneck)
If AVG(queue_shovel) > AVG(queue_crusher) × 2:
    → Shovel is constraint (bottleneck)
If both queues high:
    → Dispatch inefficiency or fleet mismatch
```

**Actionable Insights**:
- Focus improvement efforts on location with highest queue time
- Calculate cost of queue time (fuel burned while idle, opportunity cost)
- Optimize dispatch to smooth arrivals at constraint
- Consider capacity expansion at bottleneck location

---

### 9. Queue Hours Lost

**Business Purpose**: Quantify productivity impact of queuing (cost driver)

**Definition**: Total hours fleet spends queuing (waiting) vs productive hauling

**Calculation**:
```sql
SELECT
  date_id,
  shift_id,
  -- Total queue hours across fleet
  SUM(duration_queue_shovel_min + duration_queue_crusher_min) / 60.0 AS total_queue_hours,
  -- Productive hours
  SUM(duration_loading_min + duration_hauling_loaded_min + duration_dumping_min + duration_returning_min) / 60.0 AS total_productive_hours,
  -- Queue as % of total time
  (SUM(duration_queue_shovel_min + duration_queue_crusher_min) / 
   (SUM(duration_queue_shovel_min + duration_queue_crusher_min + 
        duration_loading_min + duration_hauling_loaded_min + 
        duration_dumping_min + duration_returning_min))) * 100 AS queue_time_pct
FROM fact_haul_cycle
GROUP BY date_id, shift_id;
```

**SQL Reference**: `models/metrics/queue_analysis.sql`

**Benchmarks**:
| Queue Time % | Rating     | Interpretation          |
|--------------|------------|-------------------------|
| <5%          | Excellent  | Minimal waste           |
| 5-10%        | Good       | Acceptable              |
| 10-15%       | Fair       | Some inefficiency       |
| >15%         | Poor       | Significant bottleneck  |

**Cost Impact Calculation**:
```
Queue Hours Lost × Truck Operating Cost/Hour = Direct Queue Cost

Example:
- 100 queue hours/day
- $500/hour operating cost
- Daily queue cost = $50,000
- Annual queue cost = $18.25 million

Opportunity Cost:
- Queue hours could have been productive
- Lost tons = Queue Hours × Target TPH
- Lost revenue = Lost Tons × Material Value/Ton
```

**Actionable Insights**:
- Present queue cost in dollars to justify capacity investments
- Track queue hours trend to measure improvement initiatives
- Compare queue hours by shift to identify operational differences
- Use for capacity planning ROI analysis

---

## Efficiency Metrics

### 10. Fuel Efficiency

**Business Purpose**: Optimize fuel consumption, identify mechanical issues or poor driving practices

**Definition**: Fuel consumed per ton of material hauled (liters per ton)

**Calculation**:
```sql
SELECT
  fc.truck_id,
  dt.fleet_class,
  fc.date_id,
  SUM(fc.fuel_consumed_liters) AS total_fuel_liters,
  SUM(fc.payload_tons) AS total_tons_hauled,
  SUM(fc.fuel_consumed_liters) / SUM(fc.payload_tons) AS fuel_per_ton,
  -- Distance-adjusted metric
  SUM(fc.fuel_consumed_liters) / 
    (SUM(fc.payload_tons) * (SUM(fc.distance_loaded_km) + SUM(fc.distance_empty_km))) 
    AS fuel_per_ton_mile
FROM fact_haul_cycle fc
JOIN dim_truck dt ON fc.truck_id = dt.truck_id
GROUP BY fc.truck_id, dt.fleet_class, fc.date_id;
```

**SQL Reference**: `models/analytics/fuel_efficiency.sql`

**Benchmarks by Fleet Class**:
| Fleet Class | Target (L/ton) | Good      | Poor        |
|-------------|----------------|-----------|-------------|
| 100-ton     | 1.0-1.5        | <1.2      | >1.7        |
| 200-ton     | 0.8-1.2        | <1.0      | >1.4        |
| 400-ton     | 0.6-0.9        | <0.7      | >1.1        |

**Factors Affecting Fuel Efficiency**:
- Truck class (larger trucks more efficient per ton)
- Haul distance and grade (longer distance, steeper grade = more fuel)
- Load factor (heavier loads = more fuel per ton, but fewer cycles needed)
- Operator driving style (aggressive acceleration, excessive idling = more fuel)
- Mechanical condition (worn engine, transmission issues = higher consumption)
- Road conditions (poor roads = higher rolling resistance)

**Interpretation**:
- **Better than Benchmark**: Efficient operations, good mechanical condition, skilled operators
- **Worse than Benchmark**: Investigate causes (mechanical issues, operator behavior, route conditions)
- **High Variability**: Inconsistent operations (operator differences, varying conditions)

**Actionable Insights**:
- Compare fuel efficiency by operator to identify training needs
- Track trends to detect mechanical degradation (schedule maintenance)
- Calculate fuel cost impact (fuel consumption × fuel price = daily fuel cost)
- Use for route optimization (compare fuel per ton-mile across different routes)

---

### 11. Speed Metrics

**Business Purpose**: Ensure safe and efficient travel speeds (loaded vs empty)

**Definition**: Average speed when hauling loaded vs returning empty

**Calculation**:
```sql
SELECT
  truck_id,
  date_id,
  -- Loaded speed
  AVG(speed_avg_loaded_kmh) AS avg_loaded_speed_kmh,
  -- Empty speed
  AVG(speed_avg_empty_kmh) AS avg_empty_speed_kmh,
  -- Speed ratio (empty should be faster)
  AVG(speed_avg_empty_kmh) / AVG(speed_avg_loaded_kmh) AS empty_to_loaded_ratio
FROM fact_haul_cycle
GROUP BY truck_id, date_id;
```

**SQL Reference**: `models/facts/fact_haul_cycle.sql`

**Benchmarks**:
| Speed Type  | Target Range  | Interpretation                        |
|-------------|---------------|---------------------------------------|
| Loaded      | 20-35 km/h    | Limited by weight, grade, conditions  |
| Empty       | 30-50 km/h    | Faster, but still safe                |
| Ratio       | 1.3-1.7       | Empty should be 30-70% faster         |

**Speed Targets by Conditions**:
- Flat haul road, good conditions: Loaded 30-35 km/h, Empty 45-50 km/h
- Moderate grade: Loaded 20-28 km/h, Empty 35-45 km/h
- Steep grade or poor road: Loaded 15-25 km/h, Empty 25-35 km/h

**Interpretation**:
- **Loaded Speed Too High (>40 km/h)**: Safety concern, check operator behavior
- **Loaded Speed Too Low (<15 km/h)**: Mechanical issue (engine power, transmission) or operator overly conservative
- **Empty Speed Too Low (<25 km/h)**: Operator inefficiency or mechanical issue
- **Low Ratio (<1.2)**: Operator not taking advantage of empty speed or mechanical constraint

**Actionable Insights**:
- Compare speeds by operator to identify coaching opportunities or unsafe behavior
- Track speed trends to detect mechanical degradation
- Use speed data to refine cycle time estimates
- Analyze speed vs fuel efficiency to find optimal speed (too fast = excessive fuel)

---

### 12. Distance Metrics

**Business Purpose**: Optimize haul routes and validate GPS accuracy

**Definition**: Distance traveled loaded (shovel to crusher) and empty (crusher to shovel)

**Calculation**:
```sql
SELECT
  truck_id,
  shovel_id,
  crusher_id,
  AVG(distance_loaded_km) AS avg_distance_loaded_km,
  AVG(distance_empty_km) AS avg_distance_empty_km,
  AVG(distance_loaded_km + distance_empty_km) AS avg_total_round_trip_km
FROM fact_haul_cycle
GROUP BY truck_id, shovel_id, crusher_id;
```

**SQL Reference**: `models/facts/fact_haul_cycle.sql`

**Benchmarks**:
- **Expected Haul Distance**: Based on mine layout (e.g., 5-8 km one-way for medium haul)
- **Distance Ratio**: Loaded distance ≈ Empty distance (unless different routes)
- **Variability**: Low variability expected for same shovel-crusher pair

**Interpretation**:
- **Distance Higher than Expected**: GPS error, route deviation, or longer route taken
- **High Variability**: Inconsistent routes, operator taking different paths
- **Distance Loaded ≠ Distance Empty**: Different routes used (e.g., one-way haul roads)

**Actionable Insights**:
- Validate GPS accuracy (compare calculated distance to surveyed haul road distance)
- Identify route optimization opportunities (are all trucks taking shortest path?)
- Calculate ton-miles for productivity analysis
- Use for haul road maintenance planning (tonnage × distance = wear)

---

## Quality Metrics

### 13. Payload Compliance Rate

**Business Purpose**: Measure adherence to payload targets (minimize underloading and overloading)

**Definition**: Percentage of cycles with payload in optimal range (95-105% of capacity)

**Calculation**:
```sql
WITH payload_bands AS (
  SELECT
    truck_id,
    payload_tons,
    dt.payload_capacity_tons,
    (payload_tons / payload_capacity_tons) * 100 AS payload_pct,
    CASE
      WHEN (payload_tons / payload_capacity_tons) * 100 < 85 THEN 'underload'
      WHEN (payload_tons / payload_capacity_tons) * 100 >= 85 AND (payload_tons / payload_capacity_tons) * 100 < 95 THEN 'suboptimal'
      WHEN (payload_tons / payload_capacity_tons) * 100 >= 95 AND (payload_tons / payload_capacity_tons) * 100 <= 105 THEN 'optimal'
      WHEN (payload_tons / payload_capacity_tons) * 100 > 105 THEN 'overload'
    END AS payload_band
  FROM fact_haul_cycle fc
  JOIN dim_truck dt ON fc.truck_id = dt.truck_id
)
SELECT
  truck_id,
  COUNT(*) AS total_cycles,
  SUM(CASE WHEN payload_band = 'optimal' THEN 1 ELSE 0 END) AS optimal_cycles,
  SUM(CASE WHEN payload_band = 'underload' THEN 1 ELSE 0 END) AS underload_cycles,
  SUM(CASE WHEN payload_band = 'suboptimal' THEN 1 ELSE 0 END) AS suboptimal_cycles,
  SUM(CASE WHEN payload_band = 'overload' THEN 1 ELSE 0 END) AS overload_cycles,
  (SUM(CASE WHEN payload_band = 'optimal' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS compliance_rate_pct
FROM payload_bands
GROUP BY truck_id;
```

**SQL Reference**: `models/analytics/payload_compliance.sql`

**Benchmarks**:
| Compliance Rate | Rating     | Interpretation           |
|-----------------|------------|--------------------------|
| >80%            | Excellent  | Consistent optimal loading|
| 70-80%          | Good       | Mostly compliant         |
| 60-70%          | Fair       | Some deviation           |
| <60%            | Poor       | Significant issues       |

**Interpretation**:
- **High Compliance (>80%)**: Good truck-shovel matching, well-trained operators
- **High Underload Rate**: Operator conservative or shovel underfilling
- **High Overload Rate**: Safety concern, need to enforce limits

**Actionable Insights**:
- Identify trucks/operators with low compliance for training
- Analyze payload patterns by shovel to detect mismatch
- Calculate productivity impact of underloading (10% underload = 11% more cycles needed)
- Investigate safety implications of overloading

---

### 14. Cycle Completeness

**Business Purpose**: Ensure cycles have all required states (data quality check)

**Definition**: Percentage of cycles with all expected states present

**Calculation**:
```sql
WITH cycle_states AS (
  SELECT
    fc.cycle_id,
    fc.truck_id,
    -- Required states
    MAX(CASE WHEN s.operational_state = 'loading' THEN 1 ELSE 0 END) AS has_loading,
    MAX(CASE WHEN s.operational_state = 'hauling_loaded' THEN 1 ELSE 0 END) AS has_hauling,
    MAX(CASE WHEN s.operational_state = 'dumping' THEN 1 ELSE 0 END) AS has_dumping,
    MAX(CASE WHEN s.operational_state = 'returning_empty' THEN 1 ELSE 0 END) AS has_returning
  FROM fact_haul_cycle fc
  JOIN stg_truck_states s 
    ON s.truck_id = fc.truck_id
    AND s.state_start >= fc.cycle_start
    AND s.state_end <= fc.cycle_end
  GROUP BY fc.cycle_id, fc.truck_id
)
SELECT
  COUNT(*) AS total_cycles,
  SUM(CASE WHEN has_loading = 1 AND has_hauling = 1 AND has_dumping = 1 AND has_returning = 1 
           THEN 1 ELSE 0 END) AS complete_cycles,
  (SUM(CASE WHEN has_loading = 1 AND has_hauling = 1 AND has_dumping = 1 AND has_returning = 1 
            THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS completeness_rate_pct
FROM cycle_states;
```

**SQL Reference**: `tests/test_business_rules.sql`

**Benchmarks**:
- **Target**: 100% (all cycles should have required states)
- **Acceptable**: >95% (some edge cases at shift boundaries)
- **Concerning**: <90% (data quality or state detection issues)

**Interpretation**:
- **100% Completeness**: State detection working correctly
- **Missing States**: State detection rule gaps or incomplete telemetry

**Actionable Insights**:
- Investigate incomplete cycles (state detection tuning needed)
- Check for telemetry gaps (GPS signal loss, sensor failures)
- Validate state transition logic
- Exclude incomplete cycles from metrics (or flag for review)

---

## Operator Metrics

### 15. Operator Efficiency Score

**Business Purpose**: Rank operators by performance, identify training needs and top performers

**Definition**: Composite score based on cycle time efficiency, payload utilization, and fuel efficiency

**Calculation**:
```sql
WITH operator_metrics AS (
  SELECT
    operator_id,
    -- Cycle time performance (relative to fleet average)
    AVG((julianday(cycle_end) - julianday(cycle_start)) * 24 * 60) AS avg_cycle_time_min,
    -- Payload utilization
    AVG(payload_tons / dt.payload_capacity_tons * 100) AS avg_payload_utilization_pct,
    -- Fuel efficiency
    SUM(fuel_consumed_liters) / SUM(payload_tons) AS fuel_per_ton,
    -- Spot delay frequency
    AVG(duration_spot_delays_min) AS avg_spot_delay_min
  FROM fact_haul_cycle fc
  JOIN dim_truck dt ON fc.truck_id = dt.truck_id
  GROUP BY operator_id
),
fleet_benchmarks AS (
  SELECT
    AVG(avg_cycle_time_min) AS fleet_avg_cycle_time,
    AVG(avg_payload_utilization_pct) AS fleet_avg_payload_util,
    AVG(fuel_per_ton) AS fleet_avg_fuel_per_ton
  FROM operator_metrics
)
SELECT
  om.operator_id,
  do.experience_level,
  om.avg_cycle_time_min,
  om.avg_payload_utilization_pct,
  om.fuel_per_ton,
  om.avg_spot_delay_min,
  -- Efficiency score (lower cycle time = better, higher payload = better, lower fuel = better)
  ((fb.fleet_avg_cycle_time / om.avg_cycle_time_min) * 0.4 +
   (om.avg_payload_utilization_pct / fb.fleet_avg_payload_util) * 0.4 +
   (fb.fleet_avg_fuel_per_ton / om.fuel_per_ton) * 0.2) * 100 AS efficiency_score
FROM operator_metrics om
CROSS JOIN fleet_benchmarks fb
JOIN dim_operator do ON om.operator_id = do.operator_id
ORDER BY efficiency_score DESC;
```

**SQL Reference**: `models/analytics/operator_performance.sql`

**Efficiency Score Interpretation**:
| Score    | Rating     | Interpretation              |
|----------|------------|-----------------------------|
| >110     | Excellent  | Top 10% performer           |
| 100-110  | Good       | Above average               |
| 90-100   | Average    | Fleet benchmark             |
| 80-90    | Fair       | Below average, coaching needed |
| <80      | Poor       | Significant improvement needed |

**Score Components** (weights):
- **Cycle Time Efficiency (40%)**: Faster cycles = higher score
- **Payload Utilization (40%)**: Closer to optimal (100%) = higher score
- **Fuel Efficiency (20%)**: Lower fuel per ton = higher score

**Interpretation**:
- **High Efficiency Score**: Skilled operator, model for others
- **Low Efficiency Score**: Training opportunity, pair with mentor
- **By Experience Level**: Compare within level (don't expect junior = senior)

**Actionable Insights**:
- Recognize and reward top performers
- Create mentorship programs (top operators coach bottom quartile)
- Track improvement over time (effectiveness of training programs)
- Use for scheduling (assign high-efficiency operators to critical shifts)
- Identify unsafe behavior (excessive speed, frequent overloading)

---

### 16. Operator Performance Ranking

**Business Purpose**: Compare operators within experience levels, fair evaluation

**Definition**: Percentile rank within experience level (Junior, Intermediate, Senior)

**Calculation**:
```sql
WITH operator_efficiency AS (
  -- Use efficiency score from previous metric
  SELECT 
    operator_id,
    do.experience_level,
    efficiency_score,
    PERCENT_RANK() OVER (PARTITION BY do.experience_level ORDER BY efficiency_score DESC) AS percentile_rank
  FROM operator_performance op
  JOIN dim_operator do ON op.operator_id = do.operator_id
)
SELECT
  operator_id,
  experience_level,
  efficiency_score,
  -- Convert to percentile (0-100)
  ROUND(percentile_rank * 100, 0) AS percentile_within_level,
  CASE
    WHEN percentile_rank <= 0.25 THEN 'Top 25%'
    WHEN percentile_rank <= 0.50 THEN 'Top 50%'
    WHEN percentile_rank <= 0.75 THEN 'Bottom 50%'
    ELSE 'Bottom 25%'
  END AS performance_quartile
FROM operator_efficiency
ORDER BY experience_level, efficiency_score DESC;
```

**SQL Reference**: `models/analytics/operator_performance.sql`

**Interpretation**:
- **Top 25%**: High performers within level, candidates for promotion (Junior → Intermediate)
- **Top 50%**: Solid performers, meeting expectations
- **Bottom 50%**: Need improvement, monitor progress
- **Bottom 25%**: Priority for training, consider reassignment if no improvement

**Actionable Insights**:
- Use for fair performance reviews (compare within experience level)
- Identify promotion candidates (Junior operators in Top 25% → promote to Intermediate)
- Target training resources to bottom quartile
- Track movement between quartiles to measure training effectiveness

---

## Summary

### Metrics Priority Matrix

| Metric Category | Frequency     | Audience           | Priority |
|-----------------|---------------|--------------------|----------|
| Productivity    | Daily         | Operations, Mgmt   | High     |
| Utilization     | Daily/Weekly  | Operations         | High     |
| Queue           | Real-time/Daily| Dispatch, Ops     | Critical |
| Efficiency      | Weekly        | Maintenance, Ops   | Medium   |
| Quality         | Weekly/Monthly| Ops, Safety        | Medium   |
| Operator        | Monthly       | HR, Training       | Low      |

### Metric Relationships

```
Productivity (TPH) = f(Cycle Time, Payload Utilization, Truck Utilization)

Cycle Time = Loading + Hauling + Queue + Dumping + Returning + Spot Delays

Utilization = f(Uptime, Queue Time, Dispatch Efficiency)

Queue Time → identifies Bottleneck (Shovel vs Crusher)

Fuel Efficiency = f(Mechanical Condition, Operator Behavior, Route)

Operator Performance = f(Cycle Time, Payload, Fuel, Safety)
```

### Using Metrics for Decision-Making

1. **Daily Operations**: Monitor productivity, queue times, utilization
2. **Weekly Reviews**: Analyze trends, identify problem trucks/operators
3. **Monthly Planning**: Capacity planning, maintenance scheduling
4. **Quarterly Strategy**: Capital investments (new trucks, crusher upgrade)
5. **Annual Benchmarking**: Compare to industry benchmarks, set targets

---

## References

- **SME Mining Engineering Handbook** - Haulage system performance benchmarks
- **CRC Mining** - Australian mining productivity standards
- **Caterpillar Performance Handbook** - Equipment specifications and cycle time estimates
- **Modular Mining** - Dispatch system KPI definitions

---

**Note**: All formulas and benchmarks in this document are based on industry best practices. Actual performance will vary based on site-specific conditions (haul distance, road grade, material density, climate, etc.). Calibrate benchmarks to your operation for accurate performance evaluation.
