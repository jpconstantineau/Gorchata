# Precision Scheduled Railroading - Metrics & KPI Documentation

## Table of Contents
1. [Introduction](#introduction)
2. [KPI Definitions](#kpi-definitions)
3. [PSR Evolution Framework](#psr-evolution-framework)
4. [Shadow Yard Detection Methodology](#shadow-yard-detection-methodology)
5. [Interpretation Guides](#interpretation-guides)
6. [Target Values & Benchmarks](#target-values--benchmarks)
7. [Business Use Cases](#business-use-cases)

---

## Introduction

This document provides comprehensive documentation for all Key Performance Indicators (KPIs) and metrics used in the Precision Scheduled Railroading (PSR) data warehouse. These metrics enable railroad operators to:

- Monitor network fluidity and operational efficiency
- Detect shadow yards and identify congestion hotspots
- Track PSR adoption progress across historical periods
- Analyze directional efficiency and asset utilization
- Identify seasonal performance patterns

All metrics use **minute-level precision** for temporal calculations, leveraging SQLite's `julianday()` function for accurate datetime arithmetic.

---

## KPI Definitions

### 1. Network Fluidity Index

**Formula**: `(distance_miles / duration_minutes) * 60`

**Unit**: Miles per hour (mph)

**Description**: Measures the average velocity of railcar movements across the network. Higher values indicate better network fluidity.

**Calculation Logic**:
```sql
AVG((distance_miles / NULLIF((julianday(trip_end_timestamp) - julianday(trip_start_timestamp)) * 24 * 60, 0)) * 60)
```

**Typical Range**: 10-50 mph (network average), 30-80 mph (corridor-specific)

**Interpretation**:
- **< 15 mph**: Severe congestion or operational issues
- **15-25 mph**: Below-average network performance
- **25-35 mph**: Average PSR network performance
- **35-50 mph**: High-performing PSR network
- **> 50 mph**: Exceptional fluidity (mainline corridors)

---

### 2. Slot Adherence Score

**Formula**: `100 - (temporal_variance_coefficient * 100)`

**Unit**: Score (0-100), where 100 = perfect schedule adherence

**Description**: Measures predictability of railcar arrivals based on temporal variance. Penalizes high variance in trip durations or dwell times.

**Calculation Logic**:
```sql
-- Variance coefficient = stddev / mean
100 - MIN(100, (SQRT(variance) / NULLIF(avg_duration, 0)) * 100)
```

**Typical Range**: 40-95

**Interpretation**:
- **< 50**: Unpredictable, high variance
- **50-70**: Moderate schedule adherence
- **70-85**: Good predictability (PSR target)
- **85-95**: Excellent adherence
- **> 95**: Near-perfect scheduling

---

### 3. Shadow Yard Detection Score

**Formula**: Composite weighted score combining multiple indicators

**Unit**: Score (0-100), where higher values indicate stronger shadow yard patterns

**Components**:
1. **Shadow Yard Percentage** (50% weight): Percentage of time spent at non-official yard locations
2. **Dwell Variance** (30% weight): High variance in dwell duration indicates inconsistent usage
3. **Time Clustering** (20% weight): Variance in arrival hour patterns

**Calculation Logic**:
```sql
composite_score = (shadow_yard_percentage * 0.5) + 
                  (variance_score * 0.3) + 
                  (time_clustering_score * 0.2)
                  
WHERE:
  variance_score = MIN(100, (stddev_dwell / avg_dwell) * 50)
  time_clustering_score = MIN(100, hour_variance * 10)
```

**Detection Threshold**: `composite_score > 60`

**Interpretation**:
- **0-30**: Normal operations, not a shadow yard
- **30-60**: Potential shadow yard, warrants investigation
- **60-80**: Likely shadow yard
- **> 80**: Confirmed shadow yard with strong patterns

---

###4. Buffer Consumption

**Formula**: `((actual_duration - baseline_duration) / baseline_duration) * 100`

**Unit**: Percentage (%)

**Description**: Measures how much of the planned schedule buffer has been consumed. Negative values indicate trips running faster than baseline.

**Typical Range**: -20% to +80%

**Interpretation**:
- **< -10%**: Running significantly ahead of schedule
- **-10% to +10%**: On schedule
- **+10% to +30%**: Moderate buffer consumption
- **+30% to +50%**: High buffer consumption
- **> +50%**: Critical buffer exhaustion, schedule risk

---

### 5. Directional Asymmetry Ratio

**Formula**: `loaded_velocity_mph / empty_velocity_mph`

**Unit**: Ratio (dimensionless)

**Description**: Compares velocity of loaded vs empty trips on the same corridor. Reveals operational prioritization.

**Calculation Logic**:
```sql
asymmetry_ratio = AVG(loaded_velocity) / NULLIF(AVG(empty_velocity), 0)
```

**Typical Range**: 0.5 to 2.0

**Interpretation**:
- **< 0.8**: Empty prioritized (railroad favors repositioning empty cars)
- **0.8 - 1.2**: Balanced (no clear priority)
- **> 1.2**: Loaded prioritized (railroad favors revenue freight)
- **Ideal**: Close to 1.0 (balanced operations)

---

### 6. Congestion Score

**Formula**: Composite metric combining dwell duration, variance, and traffic volume

**Unit**: Score (0-100), capped at 100

**Components**:
- **High Average Dwell** (40% weight): Locations with avg_dwell > 180 minutes
- **High Variance** (30% weight): Unpredictable dwell patterns (stddev > 60 minutes)
- **High Traffic Volume** (30% weight): Frequent dwell events (> 10 events)

**Calculation Logic**:
```sql
congestion_score = 
  CASE WHEN avg_dwell > 180 THEN 40 * (avg_dwell / 360) ELSE 0 END +
  CASE WHEN stddev_dwell > 60 THEN 30 * (stddev_dwell / 120) ELSE 0 END +
  CASE WHEN dwell_count > 10 THEN 30 * (dwell_count / 20) ELSE 0 END
```

**Severity Classification**:
- **0-25**: Low congestion
- **25-50**: Moderate congestion
- **50-75**: High congestion
- **75-100**: Critical congestion hotspot

---

## PSR Evolution Framework

### Three Historical Periods

**1. Pre-PSR (2016-2017)**
- Traditional railroad operations
- Hub-and-spoke network design
- Multiple intermediate yards
- Lower velocity, higher dwell counts

**2. Transition (2018-2020)**
- Active PSR implementation
- Network rationalization
- Yard closures and consolidations
- Increasing velocity, decreasing dwell

**3. Mature PSR (2021-2025)**
- Fully implemented PSR operations
- Point-to-point network design
- Minimized intermediate handling
- Peak velocity, minimal dwell

### Expected Performance Trajectory

| Metric | Pre-PSR | Transition | Mature |
|--------|---------|------------|--------|
| Avg Velocity | 18-22 mph | 25-30 mph | 32-40 mph |
| Avg Dwell Count | 4-6 stops | 3-4 stops | 2-3 stops |
| Trip Duration | Baseline | -10% to -20% | -25% to -35% |
| Asset Utilization | 0.6-0.8 | 0.8-1.0 | 1.0-1.3 |

---

## Shadow Yard Detection Methodology

### Definition
A **shadow yard** is an unofficial location where railcars accumulate, often indicating:
- Network congestion
- Operational bottlenecks
- Hidden capacity constraints
- PSR implementation challenges

### Detection Algorithm

**Step 1: Identify Candidate Locations**
- Locations with multiple dwell events (≥ 2)
- Non-official yard designations

**Step 2: Calculate Indicators**
```sql
shadow_yard_percentage = (dwell_duration / total_time) * 100
dwell_variance = STDDEV(dwell_duration) / AVG(dwell_duration)
time_clustering = STDDEV(arrival_hour)
```

**Step 3: Compute Composite Score**
```sql
composite_score = (shadow_yard_percentage * 0.5) +
                  (variance_score * 0.3) +
                  (time_clustering_score * 0.2)
```

**Step 4: Apply Threshold**
- `composite_score > 60` → Flag as shadow yard
- Rank by score (highest = most suspicious)

### Validation Criteria
- Minimum 2 dwell events for statistical validity
- Locations must not be official yards
- Shadow yard percentage > 30% (primary indicator)
- High dwell variance (secondary indicator)

---

## Interpretation Guides

### How to Read Each Metric

**Network Fluidity Index**
- **Context**: Compare to historical baseline and peer railroads
- **Action Thresholds**:
  - < 20 mph: Immediate investigation required
  - 20-30 mph: Monitor for degradation trends
  - > 30 mph: Healthy performance
  
**Shadow Yard Identification**
- **Context**: Analyze top 5-7 locations by composite score
- **Action Thresholds**:
  - Score > 80: Immediate operational review
  - Score 60-80: Plan mitigation strategies
  - Score < 60: Monitor quarterly

**PSR Strategy Shifts**
- **Context**: Compare periods sequentially (pre → transition → mature)
- **Action Thresholds**:
  - Velocity change < +5% year-over-year: PSR stalling
  - Velocity change 5-15%: On track
  - Velocity change > 15%: Exceeding targets

**Congestion Hotspots**
- **Context**: Rank locations by severity classification
- **Action Thresholds**:
  - Critical (75-100): Capacity expansion projects
  - High (50-75): Operational improvements
  - Moderate (25-50): Monitor trends

---

## Target Values & Benchmarks

### Industry Benchmarks (PSR Railroads)

| Metric | Class I Average | Top Quartile | Best in Class |
|--------|----------------|--------------|---------------|
| Network Velocity | 28 mph | 35 mph | 42 mph |
| Dwell Per Trip | 3.2 stops | 2.5 stops | 1.8 stops |
| Slot Adherence | 72 | 82 | 91 |
| Shadow Yards | 8-12 locations | 4-6 locations | 0-2 locations |
| Congestion Hotspots | 15-20 | 8-12 | 3-5 |

### PSR Maturity Targets

**Year 1-2 (Transition)**
- 15-20% velocity improvement vs baseline
- 20-30% reduction in dwell count
- Shadow yard count reduced by 30%

**Year 3-4 (Mature)**
- 30-40% velocity improvement vs baseline
- 40-50% reduction in dwell count
- Shadow yard count reduced by 60%+
- Top-quartile slot adherence achieved

---

## Business Use Cases

### 1. Executive Dashboard
**Question**: "How is our PSR transformation progressing?"

**Metrics Used**:
- PSR Strategy Shifts (velocity_delta_vs_pre_psr)
- Network Fluidity Index (trend over time)
- Shadow Yard Count (reduction achieved)

**Expected Insight**: "35% velocity improvement achieved in mature PSR period, with shadow yards reduced from 12 to 3 locations."

---

### 2. Capacity Planning
**Question**: "Where are our network bottlenecks?"

**Metrics Used**:
- Network Congestion Hotspots (congestion_score > 75)
- Shadow Yard Identification (composite_score > 60)
- Worst Performing Corridors (fluidity_rank in bottom 10)

**Expected Insight**: "5 critical congestion hotspots identified; Chicago terminal and Kansas City junction require immediate capacity expansion."

---

### 3. Operational Efficiency
**Question**: "Are we meeting PSR scheduling targets?"

**Metrics Used**:
- Slot Adherence Score (target: > 80)
- Buffer Consumption (target: < +20%)
- Seasonal Performance Trends (yoy_velocity_change_pct)

**Expected Insight**: "Slot adherence at 78%, below 80% target; winter Q1 shows 15% velocity degradation requiring mitigation."

---

### 4. Asset Utilization
**Question**: "Are we balancing loaded vs empty movements?"

**Metrics Used**:
- Directional Efficiency Analysis (asymmetry_ratio)
- Trips per car (from PSR evolution)

**Expected Insight**: "3 corridors show asymmetry_ratio > 1.5 (loaded heavily prioritized), indicating potential empty repositioning inefficiencies."

---

### 5. Strategic Planning
**Question**: "What operational model changes have delivered the most value?"

**Metrics Used**:
- PSR Strategy Shifts (all deltas vs pre-PSR baseline)
- Seasonal Performance Trends (historical patterns)

**Expected Insight**: "Transition period (2018-2020) delivered 18% velocity gain; mature period added incremental 12%, totaling 30% network improvement."

---

## Conclusion

These metrics provide a comprehensive view of railroad operational performance under Precision Scheduled Railroading. By monitoring KPIs across fluidity, congestion, asset utilization, and schedule adherence, operators can:

- Identify operational bottlenecks proactively
- Validate PSR transformation ROI
- Prioritize capital investment decisions
- Benchmark against industry peers
- Drive continuous operational improvement

For technical implementation details, see [ARCHITECTURE.md](./ARCHITECTURE.md).
