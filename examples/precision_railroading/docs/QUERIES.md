# Analytical Query Cookbook: Precision Scheduled Railroading

## Introduction

This document provides **12 practical analytical queries** for exploring the PSR data warehouse. Each query includes business context, SQL code, sample output, and interpretation guidance.

## Table of Contents
1. [Shadow Yard Identification](#1-shadow-yard-identification)
2. [Network Congestion Hotspots](#2-network-congestion-hotspots)
3. [Worst Performing Corridors](#3-worst-performing-corridors)
4. [Seasonal Performance Trends](#4-seasonal-performance-trends)
5. [PSR Adoption Impact](#5-psr-adoption-impact)
6. [Dwell Time Distribution](#6-dwell-time-distribution)
7. [Trip Cycle Analysis](#7-trip-cycle-analysis)
8. [Directional Efficiency](#8-directional-efficiency)
9. [Weekly Performance Trends](#9-weekly-performance-trends)
10. [Asset Utilization by Railcar Type](#10-asset-utilization-by-railcar-type)
11. [Buffer Consumption Patterns](#11-buffer-consumption-patterns)
12. [Temporal PSR Progression](#12-temporal-psr-progression)

---

## 1. Shadow Yard Identification

### Business Question
**Which locations function as unofficial staging areas (shadow yards) and what is their operational impact?**

Shadow yards emerge when PSR schedule pressure creates persistent dwell patterns at non-yard locations. Identifying them reveals hidden inefficiencies and potential service failure points.

### SQL Query
```sql
SELECT 
  location_name,
  splc_code,
  shadow_yard_risk_score,
  avg_dwell_minutes,
  ROUND(avg_dwell_minutes / 60.0, 1) AS avg_dwell_hours,
  dwell_event_count,
  railcar_count,
  CASE 
    WHEN shadow_yard_risk_score >= 70 THEN 'High Risk'
    WHEN shadow_yard_risk_score >= 50 THEN 'Moderate Risk'
    ELSE 'Low Risk'
  END AS risk_category
FROM shadow_yard_identification
WHERE shadow_yard_risk_score >= 50
ORDER BY shadow_yard_risk_score DESC;
```

### Sample Output
```
location_name        | splc_code | risk_score | avg_dwell_min | avg_dwell_hrs | event_count | railcar_count | risk_category
---------------------|-----------|------------|---------------|---------------|-------------|---------------|---------------
Chicago Junction     | 041506    | 83         | 1,847         | 30.8          | 12,453      | 8,921         | High Risk
Memphis Staging      | 454212    | 71         | 1,623         | 27.1          | 9,876       | 6,543         | High Risk
Kansas City Buffer   | 484901    | 67         | 1,512         | 25.2          | 8,234       | 5,789         | Moderate Risk
Dallas Interchange   | 756301    | 58         | 1,389         | 23.2          | 7,123       | 4,892         | Moderate Risk
St. Louis Junction   | 475916    | 52         | 1,256         | 20.9          | 6,234       | 4,123         | Moderate Risk
```

### Interpretation
- **5 shadow yards identified** with risk scores 52-83
- Average dwell: **21-31 hours** (should be <12 hours at non-yards)
- **40K+ dwell events** involving **30K+ unique railcars**
- **High Risk locations** (score ≥70): Require immediate operational review
- **Moderate Risk locations** (score 50-69): Monitor for deterioration

### Business Impact
These locations absorb 30K railcars for 20-30 hours each - effectively **750K-900K railcar-hours of "hidden" inventory** annually. Reducing this by 50% could free ~15K railcars for productive service.

---

## 2. Network Congestion Hotspots

### Business Question
**Which corridors experience the worst network fluidity and where should we invest in capacity?**

Network fluidity measures congestion-free flow. Scores <50 indicate severe bottlenecks requiring infrastructure investment or operational changes.

### SQL Query
```sql
SELECT
  corridor_name,
  origin_name,
  destination_name,
  ROUND(avg_fluidity_score, 1) AS fluidity_score,
  ROUND(avg_velocity_mph, 1) AS velocity_mph,
  trip_count,
  ROUND((SELECT AVG(fluidity_score) FROM agg_network_fluidity), 1) AS network_avg_fluidity,
  ROUND(avg_fluidity_score - (SELECT AVG(fluidity_score) FROM agg_network_fluidity), 1) AS variance_from_avg
FROM network_congestion_hotspots
WHERE avg_fluidity_score < 50
ORDER BY avg_fluidity_score ASC
LIMIT 10;
```

### Sample Output
```
corridor_name         | origin_name       | destination_name  | fluidity | velocity | trips  | network_avg | variance
----------------------|-------------------|-------------------|----------|----------|--------|-------------|----------
Chicago-Memphis       | Chicago Junction  | Memphis Staging   | 33.2     | 12.4     | 45,678 | 68.5        | -35.3
KC-Dallas             | Kansas City       | Dallas Yard       | 36.7     | 14.1     | 38,234 | 68.5        | -31.8
St. Louis-Nashville   | St. Louis Junct   | Nashville Term    | 38.9     | 15.3     | 32,567 | 68.5        | -29.6
Memphis-Atlanta       | Memphis Staging   | Atlanta Interch   | 41.2     | 16.8     | 28,945 | 68.5        | -27.3
Dallas-Houston        | Dallas Yard       | Houston Port      | 43.5     | 17.9     | 26,123 | 68.5        | -25.0
```

### Interpretation
- **5 corridors** with fluidity <50 (severe congestion)
- Average velocity: **12-18 mph** vs. network average **~28 mph**
- **30-35 points below network average** - significant underperformance
- High trip volumes (26K-46K trips) indicate strategic importance

### Business Impact
These 5 corridors represent **171K trips** moving at **42-56% of network average speed**. Improving fluidity to network average would save approximately:
- **~8M railcar-hours** annually
- **Equivalent to ~900 fewer railcars needed** system-wide
- **$30-40M annual operating cost reduction** potential

---

## 3. Worst Performing Corridors

### Business Question
**Which corridors have the lowest asset velocity and should be targeted for PSR improvement initiatives?**

### SQL Query
```sql
SELECT
  corridor_name,
  origin_name,
  destination_name,
  distance_miles,
  ROUND(avg_velocity_mph, 1) AS velocity_mph,
  ROUND(avg_trip_duration_hours, 1) AS avg_duration_hrs,
  trip_count,
  ROUND(distance_miles / NULLIF(avg_velocity_mph, 0), 1) AS theoretical_duration_hrs,
  ROUND(avg_trip_duration_hours - (distance_miles / NULLIF(avg_velocity_mph, 0)), 1) AS excess_duration_hrs
FROM worst_performing_corridors
ORDER BY avg_velocity_mph ASC
LIMIT 10;
```

### Sample Output
```
corridor_name         | origin_name      | destination_name | distance | velocity | duration | trips  | theoretical | excess_hrs
----------------------|------------------|------------------|----------|----------|----------|--------|-------------|------------
Chicago-Memphis       | Chicago Junction | Memphis Staging  | 534      | 11.8     | 67.2     | 45,678 | 45.3        | 21.9
KC-Dallas             | Kansas City      | Dallas Yard      | 498      | 13.6     | 52.4     | 38,234 | 36.6        | 15.8
Memphis-Atlanta       | Memphis Staging  | Atlanta Interch  | 378      | 14.2     | 38.7     | 28,945 | 26.6        | 12.1
St. Louis-Nashville   | St. Louis Junct  | Nashville Term   | 312      | 15.1     | 29.3     | 32,567 | 20.7        | 8.6
Dallas-Houston        | Dallas Yard      | Houston Port     | 245      | 16.4     | 21.8     | 26,123 | 14.9        | 6.9
```

### Interpretation
- **Worst corridor**: Chicago-Memphis at **11.8 mph** (should be 25-30 mph)
- **21.9 hours excess duration** per trip (67.2 actual vs. 45.3 theoretical)
- **Top 5 corridors**: 12-16 mph (42-57% of network average ~28 mph)
- **7-22 hours excess dwell/delay** per trip

### Business Impact
For Chicago-Memphis alone (45,678 trips):
- **1.0M excess railcar-hours** annually (45,678 trips × 21.9 hrs)
- **Equivalent to ~114 dedicated railcars** stuck in this corridor
- Achieving network average velocity would free these assets

---

## 4. Seasonal Performance Trends

### Business Question
**How do velocity and dwell metrics vary by season, and when should we expect operational challenges?**

25% seasonal variance is built into the data (summer peak, winter trough). Understanding patterns enables proactive resource allocation.

### SQL Query
```sql
SELECT
  year,
  quarter,
  CASE quarter
    WHEN 1 THEN 'Winter (Jan-Mar)'
    WHEN 2 THEN 'Spring (Apr-Jun)'
    WHEN 3 THEN 'Summer (Jul-Sep)'
    WHEN 4 THEN 'Fall (Oct-Dec)'
  END AS season,
  ROUND(avg_velocity_mph, 1) AS velocity_mph,
  ROUND(avg_dwell_minutes / 60.0, 1) AS dwell_hours,
  trip_count,
  ROUND(avg_velocity_mph / AVG(avg_velocity_mph) OVER (PARTITION BY year) * 100 - 100, 1) AS velocity_variance_pct,
  ROUND((avg_dwell_minutes - AVG(avg_dwell_minutes) OVER (PARTITION BY year)) / AVG(avg_dwell_minutes) OVER (PARTITION BY year) * 100, 1) AS dwell_variance_pct
FROM seasonal_performance_trends
WHERE year BETWEEN 2023 AND 2025
ORDER BY year, quarter;
```

### Sample Output
```
year | quarter | season            | velocity | dwell_hrs | trips   | velocity_var% | dwell_var%
-----|---------|-------------------|----------|-----------|---------|---------------|------------
2023 | 1       | Winter (Jan-Mar)  | 25.3     | 13.8      | 890,234 | -7.3%         | +8.7%
2023 | 2       | Spring (Apr-Jun)  | 28.1     | 11.2      | 945,678 | +2.9%         | -11.5%
2023 | 3       | Summer (Jul-Sep)  | 29.8     | 10.4      | 978,123 | +9.2%         | -17.7%
2023 | 4       | Fall (Oct-Dec)    | 26.7     | 12.9      | 901,456 | -2.2%         | +1.6%
2024 | 1       | Winter (Jan-Mar)  | 25.8     | 13.5      | 902,345 | -6.8%         | +7.9%
2024 | 2       | Spring (Apr-Jun)  | 28.4     | 11.1      | 958,234 | +2.5%         | -11.1%
2024 | 3       | Summer (Jul-Sep)  | 30.1     | 10.3      | 989,567 | +8.6%         | -17.6%
2024 | 4       | Fall (Oct-Dec)    | 27.1     | 12.6      | 912,789 | -2.2%         | +0.8%
```

### Interpretation
- **Summer (Q3)**: **+8-9% velocity**, **-18% dwell** (best performance)
- **Winter (Q1)**: **-7% velocity**, **+8% dwell** (worst performance)
- **~25% seasonal swing** in both velocity and dwell
- **Spring/Fall**: Transitional performance, closer to annual average

### Business Impact
**Seasonal staffing/equipment implications**:
- **Winter**: Need +8-10% more assets to maintain service levels
- **Summer**: Can operate with 8-10% fewer assets
- **~1,200-1,400 railcar equivalent swing** between seasons
- **Plan maintenance for Q3** (summer) when excess capacity available

---

## 5. PSR Adoption Impact

### Business Question
**How has PSR transformed our operations from 2016 to 2025?**

### SQL Query
```sql
SELECT
  psr_period,
  CASE psr_period
    WHEN 'pre-PSR' THEN '2016-2017'
    WHEN 'transition' THEN '2018-2020'
    WHEN 'mature' THEN '2021-2025'
  END AS years,
  ROUND(avg_velocity_mph, 1) AS velocity_mph,
  ROUND(avg_dwell_minutes / 60.0, 1) AS dwell_hours,
  ROUND(avg_trip_duration_minutes / 60.0, 1) AS trip_duration_hours,
  shadow_yard_count,
  trip_count,
  -- Calculate improvements vs. pre-PSR
  ROUND((avg_velocity_mph - FIRST_VALUE(avg_velocity_mph) OVER (ORDER BY psr_period)) / 
        FIRST_VALUE(avg_velocity_mph) OVER (ORDER BY psr_period) * 100, 1) AS velocity_improvement_pct,
  ROUND((FIRST_VALUE(avg_dwell_minutes) OVER (ORDER BY psr_period) - avg_dwell_minutes) / 
        FIRST_VALUE(avg_dwell_minutes) OVER (ORDER BY psr_period) * 100, 1) AS dwell_reduction_pct
FROM psr_strategy_shifts
ORDER BY 
  CASE psr_period 
    WHEN 'pre-PSR' THEN 1 
    WHEN 'transition' THEN 2 
    WHEN 'mature' THEN 3 
  END;
```

### Sample Output
```
psr_period  | years     | velocity | dwell_hrs | trip_hrs | shadow_yards | trips     | velocity_improv% | dwell_reduction%
------------|-----------|----------|-----------|----------|--------------|-----------|------------------|------------------
pre-PSR     | 2016-2017 | 18.2     | 20.8      | 14.8     | 0            | 22,145,678| 0.0%             | 0.0%
transition  | 2018-2020 | 22.7     | 16.4      | 11.5     | 3            | 33,234,567| +24.7%           | -21.2%
mature      | 2021-2025 | 27.4     | 12.1      | 8.7      | 7            | 55,412,345| +50.5%           | -41.8%
```

### Interpretation
**Pre-PSR to Mature PSR Transformation**:
- **Velocity**: +50.5% increase (18.2 → 27.4 mph)
- **Dwell**: -41.8% reduction (20.8 → 12.1 hours)
- **Trip Duration**: -41.2% improvement (14.8 → 8.7 hours)
- **Shadow Yards**: 0 → 7 (emergence as byproduct of schedule pressure)

**Transition Period** (2018-2020):
- Halfway point in all metrics
- Shadow yards beginning to emerge (3 locations)
- Mixed operational patterns

### Business Impact
**Mature PSR achievements**:
- **50% velocity increase** = ~50% fewer railcars needed for same traffic
- **42% dwell reduction** = faster service, better asset turns
- **Trade-off**: 7 shadow yards indicate schedule pressure side-effects
- **Net**: Major operational efficiency gain despite shadow yard emergence

---

## 6. Dwell Time Distribution

### Business Question
**What is the distribution of dwell times across location types, and where are the outliers?**

### SQL Query
```sql
WITH dwell_stats AS (
  SELECT
    l.facility_type,
    COUNT(*) AS dwell_event_count,
    ROUND(AVG(d.dwell_duration_minutes) / 60.0, 1) AS avg_dwell_hours,
    ROUND(MIN(d.dwell_duration_minutes) / 60.0, 1) AS min_dwell_hours,
    ROUND(MAX(d.dwell_duration_minutes) / 60.0, 1) AS max_dwell_hours,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY d.dwell_duration_minutes) / 60.0, 1) AS median_dwell_hours,
    ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY d.dwell_duration_minutes) / 60.0, 1) AS p90_dwell_hours
  FROM fact_dwell d
  JOIN dim_location l ON d.location_id = l.location_id
  GROUP BY l.facility_type
)
SELECT
  facility_type,
  dwell_event_count,
  avg_dwell_hours,
  median_dwell_hours,
  p90_dwell_hours,
  max_dwell_hours,
  CASE
    WHEN facility_type IN ('terminal', 'yard') THEN '12-24 hours'
    WHEN facility_type = 'interchange' THEN '6-12 hours'
    WHEN facility_type = 'customer_site' THEN '24-72 hours'
    WHEN facility_type = 'siding' THEN '<6 hours'
  END AS expected_dwell
FROM dwell_stats
ORDER BY avg_dwell_hours DESC;
```

### Sample Output
```
facility_type  | events    | avg_hrs | median_hrs | p90_hrs | max_hrs | expected_dwell
---------------|-----------|---------|------------|---------|---------|----------------
customer_site  | 4,234,567 | 42.3    | 36.8       | 89.2    | 156.7   | 24-72 hours
yard           | 6,789,234 | 18.7    | 15.2       | 34.5    | 89.3    | 12-24 hours
terminal       | 5,456,123 | 16.4    | 13.7       | 29.8    | 67.4    | 12-24 hours
interchange    | 2,345,678 | 9.8     | 7.9        | 18.6    | 45.2    | 6-12 hours
siding         | 1,234,567 | 4.2     | 3.1        | 9.7     | 31.5    | <6 hours
```

### Interpretation
- **Customer sites**: Longest dwell (42 hours avg) - expected for loading/unloading
- **Yards/Terminals**: 16-19 hours (within PSR targets of <24 hours)
- **Interchanges**: 9.8 hours (good performance, <12 hour target)
- **Sidings**: 4.2 hours (expected for short-term holds)
- **P90 values**: 90% of dwells complete within these times

### Business Impact
Terminal/yard dwell within PSR targets indicates successful implementation. Customer site dwell (42 hrs) is operational reality of loading/unloading operations.

---

## 7. Trip Cycle Analysis

### Business Question
**How balanced are loaded vs. empty trips, and what is the cycle efficiency?**

### SQL Query
```sql
WITH cycle_stats AS (
  SELECT
    rc.car_type,
    COUNT(DISTINCT t.trip_segment_id) AS total_trips,
    SUM(CASE WHEN t.trip_type = 'loaded' THEN 1 ELSE 0 END) AS loaded_trips,
    SUM(CASE WHEN t.trip_type = 'empty' THEN 1 ELSE 0 END) AS empty_trips,
    ROUND(AVG(CASE WHEN t.trip_type = 'loaded' THEN t.distance_miles END), 1) AS avg_loaded_distance,
    ROUND(AVG(CASE WHEN t.trip_type = 'empty' THEN t.distance_miles END), 1) AS avg_empty_distance,
    ROUND(AVG(CASE WHEN t.trip_type = 'loaded' THEN t.duration_minutes / 60.0 END), 1) AS avg_loaded_duration_hrs,
    ROUND(AVG(CASE WHEN t.trip_type = 'empty' THEN t.duration_minutes / 60.0 END), 1) AS avg_empty_duration_hrs
  FROM fact_trip t
  JOIN dim_railcar rc ON t.railcar_id = rc.railcar_id
  GROUP BY rc.car_type
)
SELECT
  car_type,
  total_trips,
  loaded_trips,
  empty_trips,
  ROUND(CAST(loaded_trips AS REAL) / total_trips * 100, 1) AS loaded_pct,
  ROUND(CAST(empty_trips AS REAL) / total_trips * 100, 1) AS empty_pct,
  avg_loaded_distance,
  avg_empty_distance,
  avg_loaded_duration_hrs,
  avg_empty_duration_hrs,
  ROUND((avg_loaded_distance + avg_empty_distance) / 2.0, 1) AS avg_cycle_distance,
  ROUND((avg_loaded_duration_hrs + avg_empty_duration_hrs) / 2.0, 1) AS avg_cycle_duration_hrs
FROM cycle_stats
ORDER BY total_trips DESC;
```

### Sample Output
```
car_type   | total_trips | loaded | empty   | loaded% | empty% | load_dist | empty_dist | load_hrs | empty_hrs | cycle_dist | cycle_hrs
-----------|-------------|--------|---------|---------|--------|-----------|------------|----------|-----------|------------|----------
hopper     | 12,345,678  | 6.2M   | 6.1M    | 50.4%   | 49.6%  | 287.3     | 295.4      | 11.2     | 10.8      | 291.4      | 11.0
tank       | 9,876,543   | 4.9M   | 4.9M    | 49.8%   | 50.2%  | 312.6     | 318.9      | 12.1     | 11.7      | 315.8      | 11.9
box        | 7,654,321   | 3.8M   | 3.8M    | 50.1%   | 49.9%  | 298.7     | 303.2      | 11.6     | 11.2      | 301.0      | 11.4
gondola    | 6,543,210   | 3.3M   | 3.2M    | 50.3%   | 49.7%  | 276.4     | 281.9      | 10.8     | 10.4      | 279.2      | 10.6
intermodal | 3,210,987   | 1.6M   | 1.6M    | 49.9%   | 50.1%  | 425.7     | 431.2      | 15.8     | 15.3      | 428.5      | 15.6
```

### Interpretation
- **Excellent balance**: 49-51% loaded/empty across all car types
- **Symmetric distances**: Loaded and empty trips similar distance (within 2-3%)
- **Intermodal longest**: 428 miles average cycle (box/hopper: 279-301 miles)
- **Duration consistent**: 10-16 hours per trip depending on car type

### Business Impact
Near-perfect 50/50 loaded/empty split indicates:
- Efficient bidirectional traffic flows
- Minimal repositioning waste
- Good commodity/lane balance
- PSR schedules aligned with demand patterns

---

## 8. Directional Efficiency

### Business Question
**Which corridors have severe directional imbalances requiring repositioning strategies?**

### SQL Query
```sql
SELECT
  corridor_name,
  origin_name,
  destination_name,
  ROUND(asymmetry_ratio, 2) AS asymmetry_ratio,
  loaded_trip_count,
  empty_trip_count,
  total_trip_count,
  ROUND(CAST(loaded_trip_count AS REAL) / total_trip_count * 100, 1) AS loaded_pct,
  ROUND(CAST(empty_trip_count AS REAL) / total_trip_count * 100, 1) AS empty_pct,
  CASE
    WHEN asymmetry_ratio >= 2.5 THEN 'Severe Imbalance'
    WHEN asymmetry_ratio >= 1.5 THEN 'Moderate Imbalance'
    ELSE 'Balanced'
  END AS imbalance_category
FROM directional_efficiency_analysis
WHERE asymmetry_ratio >= 1.5
ORDER BY asymmetry_ratio DESC
LIMIT 15;
```

### Sample Output
```
corridor_name      | origin_name    | destination_name | ratio | loaded | empty | total  | load% | empty% | category
-------------------|----------------|------------------|-------|--------|-------|--------|-------|--------|------------------
Coal Mine-Port     | Mine Alpha     | Port Houston     | 3.42  | 38,567 | 11,234| 49,801 | 77.4% | 22.6%  | Severe Imbalance
Grain-Export       | Grain Belt     | Pacific Coast    | 2.87  | 32,456 | 11,289| 43,745 | 74.2% | 25.8%  | Severe Imbalance
Oil-Refinery       | Shale Fields   | Gulf Refineries  | 2.64  | 28,234 | 10,689| 38,923 | 72.5% | 27.5%  | Severe Imbalance
Lumber-Market      | Forest Zone    | Urban Centers    | 1.98  | 24,123 | 12,189| 36,312 | 66.4% | 33.6%  | Moderate Imbalance
Container-Import   | Coast Ports    | Midwest Hubs     | 1.87  | 22,567 | 12,078| 34,645 | 65.1% | 34.9%  | Moderate Imbalance
```

### Interpretation
- **3 corridors with severe imbalance** (ratio >2.5): **72-77% loaded** in dominant direction
- **Commodity-specific patterns**: Coal, grain, oil show one-way loaded flows
- **12 corridors with moderate imbalance** (ratio 1.5-2.5)
- **Repositioning required**: ~65-78% of return trips empty

### Business Impact
For **Coal Mine-Port corridor** (49,801 trips):
- **27,333 empty repositioning moves** (49,801 trips × 54.9% imbalance)
- **At $400/empty move**: **$10.9M annual repositioning cost**
- **Across 15 imbalanced corridors**: **$45-60M potential exposure**

**Mitigation strategies**:
- Backhaul business development (find loads for return trips)
- Equipment pooling/sharing arrangements
- Strategic location of reload facilities

---

## 9. Weekly Performance Trends

### Business Question
**How does corridor performance vary week-by-week, and are there consistent patterns?**

### SQL Query
```sql
WITH weekly_trends AS (
  SELECT
    week_period,
    CAST(SUBSTR(week_period, 1, 4) AS INTEGER) AS year,
    CAST(SUBSTR(week_period, 7) AS INTEGER) AS week_num,
    COUNT(DISTINCT corridor_id) AS corridor_count,
    ROUND(AVG(average_velocity_mph), 1) AS avg_velocity_mph,
    ROUND(AVG(average_dwell_count), 1) AS avg_dwell_count,
    SUM(trip_count) AS total_trips
  FROM agg_corridor_weekly_performance
  GROUP BY week_period
)
SELECT
  year,
  week_num,
  week_period,
  corridor_count,
  avg_velocity_mph,
  avg_dwell_count,
  total_trips,
  ROUND(avg_velocity_mph - AVG(avg_velocity_mph) OVER (PARTITION BY year), 1) AS velocity_vs_year_avg,
  CASE
    WHEN week_num BETWEEN 1 AND 13 THEN 'Q1'
    WHEN week_num BETWEEN 14 AND 26 THEN 'Q2'
    WHEN week_num BETWEEN 27 AND 39 THEN 'Q3'
    ELSE 'Q4'
  END AS quarter
FROM weekly_trends
WHERE year = 2025
ORDER BY week_num;
```

**Sample Output** (2025, selected weeks):
```
year | week | week_period | corridors | velocity | dwell_count | trips  | vs_year_avg | quarter
-----|------|-------------|-----------|----------|-------------|--------|-------------|--------
2025 | 1    | 2025-W01    | 947       | 25.1     | 2.3         | 18,234 | -2.8        | Q1
2025 | 5    | 2025-W05    | 948       | 24.7     | 2.4         | 17,892 | -3.2        | Q1
2025 | 14   | 2025-W14    | 949       | 27.3     | 2.1         | 19,567 | -0.6        | Q2
2025 | 20   | 2025-W20    | 950       | 28.9     | 1.9         | 20,234 | +1.0        | Q2
2025 | 27   | 2025-W27    | 951       | 30.2     | 1.7         | 21,123 | +2.3        | Q3
2025 | 35   | 2025-W35    | 950       | 29.8     | 1.8         | 20,987 | +1.9        | Q3
2025 | 40   | 2025-W40    | 949       | 27.1     | 2.1         | 19,456 | -0.8        | Q4
2025 | 48   | 2025-W48    | 948       | 25.6     | 2.3         | 18,567 | -2.3        | Q4
```

### Interpretation
- **Winter (Q1, Q4)**: 2-3 mph below year average (25-26 mph)
- **Summer (Q2, Q3)**: 1-2 mph above year average (29-30 mph)
- **Dwell count**: Inverse relationship (higher in winter: 2.3-2.4, lower in summer: 1.7-1.9)
- **Trip volume**: +15% in summer vs. winter (demand seasonality)
- **Consistent pattern**: Repeats annually

---

## 10. Asset Utilization by Railcar Type

### Business Question
**Which railcar types are most efficiently utilized under PSR operations?**

### SQL Query
```sql
WITH utilization_stats AS (
  SELECT
    rc.car_type,
    COUNT(DISTINCT rc.railcar_id) AS car_count,
    COUNT(DISTINCT t.trip_segment_id) AS trip_count,
    SUM(t.distance_miles) AS total_distance_miles,
    SUM(t.duration_minutes) AS total_duration_minutes,
    ROUND(AVG(t.average_velocity_mph), 1) AS avg_velocity_mph,
    ROUND(AVG(t.duration_minutes) / 60.0, 1) AS avg_trip_duration_hrs
  FROM dim_railcar rc
  JOIN fact_trip t ON rc.railcar_id = t.railcar_id
  GROUP BY rc.car_type
)
SELECT
  car_type,
  car_count,
  trip_count,
  ROUND(CAST(trip_count AS REAL) / car_count, 0) AS trips_per_car,
  ROUND(total_distance_miles / car_count, 0) AS miles_per_car,
  ROUND(total_duration_minutes / car_count / 60.0, 0) AS hours_per_car,
  avg_velocity_mph,
  avg_trip_duration_hrs,
  ROUND(miles_per_car / NULLIF(hours_per_car, 0), 1) AS utilization_velocity_mph,
  ROUND((hours_per_car / (365 * 24.0 * 10)) * 100, 1) AS time_utilization_pct
FROM utilization_stats
ORDER BY trips_per_car DESC;
```

### Sample Output
```
car_type   | cars  | trips      | trips/car | miles/car | hrs/car | velocity | trip_hrs | util_velocity | time_util%
-----------|-------|------------|-----------|-----------|---------|----------|----------|---------------|------------
intermodal | 1,234 | 3,210,987  | 2,603     | 745,234   | 32,456  | 28.9     | 15.6     | 23.0          | 37.0%
hopper     | 4,567 | 12,345,678 | 2,703     | 698,123   | 29,876  | 26.7     | 11.0     | 23.4          | 34.1%
box        | 2,890 | 7,654,321  | 2,649     | 712,345   | 30,234  | 27.4     | 11.4     | 23.6          | 34.5%
tank       | 2,234 | 9,876,543  | 4,420     | 823,456   | 34,123  | 28.1     | 11.9     | 24.1          | 38.9%
gondola    | 1,075 | 6,543,210  | 6,086     | 892,123   | 36,789  | 26.2     | 10.6     | 24.3          | 42.0%
```

### Interpretation
- **Gondola most utilized**: 6,086 trips/car, 42% time utilization
- **Intermodal/hopper**: 2,600-2,700 trips/car, 34-37% time utilization
- **Tank mid-range**: 4,420 trips/car, 39% time utilization
- **Utilization velocity**: 23-24 mph across all types (consistent with PSR)
- **Time utilization**: 34-42% (remainder is dwell/idle time)

### Business Impact
**58-66% idle time** indicates opportunity for further PSR improvements. Each 5% improvement in time utilization = ~600-1,200 fewer railcars needed across fleet.

---

## 11. Buffer Consumption Patterns

### Business Question
**Where are we consuming schedule buffers, and which corridors are at risk of schedule failure?**

### SQL Query
```sql
SELECT
  c.corridor_name,
  c.origin_splc,
  c.destination_splc,
  c.distance_miles,
  bc.buffer_consumption_percentage,
  bc.schedule_adherence_score,
  bc.trip_count,
  CASE
    WHEN bc.buffer_consumption_percentage > 150 THEN 'Critical Risk'
    WHEN bc.buffer_consumption_percentage > 100 THEN 'High Risk'
    WHEN bc.buffer_consumption_percentage > 75 THEN 'Moderate Risk'
    ELSE 'Low Risk'
  END AS risk_level,
  ROUND(c.distance_miles / NULLIF(bc.buffer_consumption_percentage, 0) * 100, 1) AS effective_buffer_miles
FROM agg_buffer_consumption bc
JOIN dim_corridor c ON bc.corridor_id = c.corridor_id
WHERE bc.buffer_consumption_percentage > 75
ORDER BY bc.buffer_consumption_percentage DESC
LIMIT 15;
```

### Sample Output
```
corridor_name      | origin | dest  | distance | buffer% | adherence | trips | risk_level    | buffer_miles
-------------------|--------|-------|----------|---------|-----------|-------|---------------|-------------
Chicago-Memphis    | 041506 | 45421 | 534      | 178.3   | 42.1      | 45K   | Critical Risk | 299.5
KC-Dallas          | 484901 | 75630 | 498      | 156.7   | 51.3      | 38K   | Critical Risk | 317.7
Memphis-Atlanta    | 454212 | 12345 | 378      | 134.2   | 58.9      | 29K   | High Risk     | 281.7
St. Louis-Nash     | 475916 | 54321 | 312      | 121.5   | 63.4      | 33K   | High Risk     | 256.8
Dallas-Houston     | 756301 | 67890 | 245      | 108.9   | 68.7      | 26K   | High Risk     | 225.0
```

### Interpretation
- **2 corridors critical risk** (buffer >150%): Schedule buffers exceeded, frequent delays
- **3 corridors high risk** (buffer 100-150%): No remaining buffer, any delay propagates
- **adherence_score 42-69**: Reflects buffer over-consumption
- **effective_buffer_miles**: How much route distance buffer provides (lower = tighter)

### Business Impact
**Chicago-Memphis**: 178% buffer consumption with 42% adherence means:
- **Schedules failing 58% of the time**
- **78% over-consumed buffers** (178% - 100%)
- Requires **+30-40% additional buffer** or **infrastructure capacity improvement**

**System-wide**: 15 corridors with buffer >75% represent **~300K trips** operating with insufficient slack, affecting **~25% of network traffic**.

---

## 12. Temporal PSR Progression

### Business Question
**How have PSR metrics evolved quarter-by-quarter, showing the gradual transformation?**

### SQL Query
```sql
SELECT
  year,
  quarter,
  psr_period,
  ROUND(avg_velocity_mph, 1) AS velocity_mph,
  ROUND(avg_dwell_minutes / 60.0, 1) AS dwell_hours,
  trip_count,
  ROUND(avg_velocity_mph - LAG(avg_velocity_mph) OVER (ORDER BY year, quarter), 1) AS velocity_change_qoq,
  ROUND((avg_dwell_minutes - LAG(avg_dwell_minutes) OVER (ORDER BY year, quarter)) / 60.0, 1) AS dwell_change_qoq,
  ROUND((avg_velocity_mph - AVG(avg_velocity_mph) OVER ()) / AVG(avg_velocity_mph) OVER () * 100, 1) AS velocity_vs_overall_avg_pct
FROM agg_psr_evolution
ORDER BY year, quarter;
```

### Sample Output (selected quarters):
```
year | qtr | psr_period | velocity | dwell_hrs | trips     | vel_chg_qoq | dwell_chg_qoq | vs_overall%
-----|-----|------------|----------|-----------|-----------|-------------|---------------|-------------
2016 | 1   | pre-PSR    | 17.8     | 21.4      | 5,423,456 | NULL        | NULL          | -26.2%
2017 | 4   | pre-PSR    | 18.6     | 20.2      | 5,678,234 | +0.2        | -0.3          | -22.9%
2018 | 1   | transition | 19.4     | 19.1      | 5,892,345 | +0.8        | -1.1          | -19.6%
2019 | 4   | transition | 23.1     | 15.8      | 6,234,567 | +0.5        | -0.6          | -4.1%
2020 | 4   | transition | 25.2     | 13.7      | 6,567,890 | +0.7        | -0.7          | +4.6%
2021 | 1   | mature     | 26.1     | 12.9      | 6,789,123 | +0.9        | -0.8          | +8.3%
2023 | 4   | mature     | 27.8     | 11.8      | 7,234,567 | +0.4        | -0.3          | +15.4%
2025 | 4   | mature     | 28.4     | 11.3      | 7,456,789 | +0.2        | -0.1          | +17.9%
```

### Interpretation
- **Steady improvement**: +0.2 to +0.9 mph quarter-over-quarter during transition
- **Dwell reduction**: -0.1 to -1.1 hours per quarter
- **Acceleration during transition** (2018-2020): Steepest improvements
- **Maturation slowdown** (2021-2025): Continued improvement but at slower rate
- **Pre-PSR baseline**: 17.8 mph, 21.4 hrs dwell
- **Mature PSR**: 28.4 mph, 11.3 hrs dwell (+59.6% velocity, -47.2% dwell)

### Business Impact
Gradual quarter-by-quarter improvement demonstrates **disciplined PSR rollout**, avoiding operational disruption from "big bang" changes. **2019-2020 acceleration** corresponds to industry-wide PSR adoption timing.

---

## Conclusion

These 12 queries demonstrate the analytical depth of the PSR warehouse:
- **Shadow yards**: Identifying hidden inefficiencies
- **Congestion**: Finding bottlenecks for investment targeting
- **Seasonal patterns**: Planning resources proactively
- **PSR impact**: Quantifying transformation value
- **Directional imbalance**: Measuring repositioning cost exposure
- **Buffer consumption**: Identifying schedule failure risks

Each query combines business context with actionable metrics, enabling data-driven PSR optimization.

## Additional Resources
- [README.md](../README.md): Warehouse overview and setup
- [BUSINESS_CONTEXT.md](BUSINESS_CONTEXT.md): PSR history and principles
- [PSR_EVOLUTION.md](PSR_EVOLUTION.md): Three-period transformation framework
- [SETUP.md](SETUP.md): Technical setup and troubleshooting
