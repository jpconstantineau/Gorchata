# API 584 IOW Metrics Reference

## Overview

This document provides a **comprehensive reference guide** to all business metrics, health scores, damage calculations, and alert priorities in the API 584 Risk-Based Integrity Operating Window (IOW) monitoring system. It is intended for **integrity engineers, operations managers, and maintenance planners** who use these metrics to make decisions about inspections, maintenance, and operating envelope adjustments.

Each metric includes:
- **Business definition**: What the metric measures and why it matters
- **Formula**: Mathematical calculation with all variables defined
- **Example calculation**: Worked example with real numbers
- **Interpretation guidance**: How to read the metric value and what actions to take
- **Thresholds**: Specific cutoff values that trigger different responses

## Health Metrics

### Asset Integrity Health Index

**Business Definition**: A normalized 0-100 score that summarizes overall asset condition based on excursion history. Higher scores indicate better integrity health. This metric provides at-a-glance assessment comparable across all assets regardless of unit, parameter type, or damage mechanism.

**Formula**:
```
health_index = 100 - (weighted_excursion_score / 30.0) × 100
```

**Where**:
- **weighted_excursion_score** = (critical_excursion_count × 3) + (standard_excursion_count × 2) + (informational_excursion_count × 1)
- **critical_excursion_count** = Number of excursions violating Critical IOW limits
- **standard_excursion_count** = Number of excursions violating Standard IOW limits
- **informational_excursion_count** = Number of excursions violating Informational IOW limits
- **30.0** = Normalization factor calibrated so "typical" asset scores ~50

**Weighting Rationale**:
- **Critical excursions (3×)**: Most severe violations, highest damage potential, immediate safety risk
- **Standard excursions (2×)**: Moderate violations, accelerated degradation, production impact
- **Informational excursions (1×)**: Minor violations, awareness level, minimal immediate impact

**Example Calculation**:

**Asset**: CDU-015-TT (Reactor Inlet Temperature)

**Excursion History** (over analysis period):
- Critical excursions: 8
- Standard excursions: 15
- Informational excursions: 22

**Step 1: Calculate weighted excursion score**
```
weighted_excursion_score = (8 × 3) + (15 × 2) + (22 × 1)
                         = 24 + 30 + 22
                         = 76
```

**Step 2: Calculate health index**
```
health_index = 100 - (76 / 30.0) × 100
             = 100 - (2.533 × 100)
             = 100 - 253.3
             = -153.3  (floor at 0)
             = 0
```

**Note**: This example shows an extremely unhealthy asset. For more typical asset:

**Asset**: VDU-003-TT (Vacuum Column Top Temperature)

**Excursion History**:
- Critical excursions: 2
- Standard excursions: 8
- Informational excursions: 12

**Weighted score**: (2 × 3) + (8 × 2) + (12 × 1) = 6 + 16 + 12 = 34

**Health index**: 100 - (34 / 30.0) × 100 = 100 - 113.3 = **-13.3 → 0 (floored)**

**Better example** (healthier asset):

**Asset**: HCU-021-PT (Hydrocracker Pressure)

**Excursion History**:
- Critical excursions: 0
- Standard excursions: 3
- Informational excursions: 8

**Weighted score**: (0 × 3) + (3 × 2) + (8 × 1) = 0 + 6 + 8 = 14

**Health index**: 100 - (14 / 30.0) × 100 = 100 - 46.7 = **53.3** (Fair status)

**Status Tiers**:

| Health Index Range | Status | Color | Interpretation | Action Required |
|--------------------|--------|-------|----------------|-----------------|
| **>90** | Excellent | Green | Minimal excursions, asset within all IOW limits | Continue normal operation |
| **70-90** | Good | Green | Occasional minor excursions, normal wear patterns | Continue monitoring |
| **50-70** | Fair | Yellow | Moderate excursion frequency, increased attention warranted | Increase monitoring frequency |
| **30-50** | Poor | Orange | Frequent excursions across multiple criticality levels | Schedule inspection within 30 days |
| **<30** | Critical | Red | Severe excursion patterns, integrity at risk | Immediate inspection required |

**Interpretation Guidance**:

- **Health index = 95**: Asset is performing excellently with minimal excursions. May have 1-2 informational excursions but no critical or standard violations. Continue normal operation.

- **Health index = 75**: Good performance with occasional excursions. Might have 3-5 standard excursions and several informational. Asset is aging normally. Continue monitoring.

- **Health index = 55**: Fair performance indicating elevated concern. Asset likely has 1-2 critical excursions plus multiple standard excursions. Increase monitoring frequency, review operating procedures to minimize future excursions.

- **Health index = 35**: Poor performance requiring intervention. Asset has multiple critical excursions (3-5) and frequent standard excursions. Schedule inspection within 30 days, investigate root causes (operational practices, design limitations, material degradation).

- **Health index = 15**: Critical condition requiring immediate action. Asset has extensive excursion history (5+ critical, 10+ standard). Immediate inspection required, consider operating envelope restrictions until inspection confirms fitness-for-service.

**Trending**:
- **Health dropping over time**: Indicates progressive degradation, accelerating damage accumulation, or declining operational discipline
- **Health stable**: Normal aging pattern, excursion frequency consistent with historical baseline
- **Health improving**: Indicates successful intervention (operating procedure changes, process optimization, equipment repair)

### Health Trend (30-Day Change)

**Business Definition**: Change in health index over the past 30 days, indicating rate of deterioration or improvement.

**Formula**:
```
health_trend_30d = current_health_index - health_index_30_days_ago
```

**Example Calculation**:

**Asset**: FCC-008-PT (FCC Regenerator Pressure)
- Current health index: 42
- Health index 30 days ago: 67

**Health trend**: 42 - 67 = **-25** (deteriorating)

**Interpretation**:
- **health_trend_30d < -20**: Rapid deterioration, investigate immediately (triggers health_degradation alert)
- **health_trend_30d between -20 and -10**: Moderate deterioration, increased monitoring
- **health_trend_30d between -10 and +10**: Normal variation, stable condition
- **health_trend_30d > +10**: Improving (interventions working, operational discipline improving)

## Damage Metrics

### Cumulative Damage Index (Area Under Curve)

**Business Definition**: Quantifies total accumulated damage from all excursions using Area Under Curve (AUC) methodology. Represents the aggregate impact of operating outside IOW limits over time. Expressed in "damage-minutes" or "damage-hours" reflecting the product of excursion magnitude and duration.

**Formula**:
```
cumulative_damage_index = Σ (excursion_magnitude × duration_minutes)
```

**For each excursion window**:
```
excursion_damage = avg_magnitude × duration_minutes
```

**Where**:
- **excursion_magnitude** = Absolute deviation from IOW limit (°F, psig, pH units, bbl/day)
- **duration_minutes** = Length of excursion event from start to end
- **avg_magnitude** = Average magnitude across all readings in the excursion window
- **Σ** = Sum across all excursion events for the asset

**Example Calculation**:

**Scenario**: Asset CDU-015-TT has three excursion events:

**Event 1**: Temperature excursion above critical limit
- Critical upper limit: 950°F
- Readings: 975°F (5 readings over 25 minutes)
- Average excursion magnitude: 975 - 950 = 25°F
- Duration: 25 minutes
- **Event damage**: 25 × 25 = **625 damage-minutes**

**Event 2**: Temperature excursion above standard limit
- Standard upper limit: 925°F
- Readings average: 940°F (12 readings over 60 minutes)
- Average excursion magnitude: 940 - 925 = 15°F
- Duration: 60 minutes
- **Event damage**: 15 × 60 = **900 damage-minutes**

**Event 3**: Temperature below critical limit
- Critical lower limit: 550°F
- Readings average: 530°F (8 readings over 40 minutes)
- Average excursion magnitude: 550 - 530 = 20°F
- Duration: 40 minutes
- **Event damage**: 20 × 40 = **800 damage-minutes**

**Total cumulative damage index**: 625 + 900 + 800 = **2,325 damage-minutes** = **38.75 damage-hours**

**Rolling Window Aggregations**:

The cumulative damage index is calculated over multiple time windows to support different analysis purposes:

**30-Day Cumulative Damage** (`cumulative_damage_30d`):
- Sum of all excursion damage in last 30 days
- **Use case**: Recent trend analysis, operational discipline tracking
- **Example**: 1,200 damage-minutes → 20 damage-hours in last month

**90-Day Cumulative Damage** (`cumulative_damage_90d`):
- Sum of all excursion damage in last 90 days (quarterly)
- **Use case**: Quarterly performance reviews, turnaround planning
- **Example**: 4,500 damage-minutes → 75 damage-hours in last quarter

**365-Day Cumulative Damage** (`cumulative_damage_365d`):
- Sum of all excursion damage in last 365 days (annual)
- **Use case**: Annual damage budget tracking, RBI interval optimization
- **Example**: 18,000 damage-minutes → 300 damage-hours in last year

**Interpretation Guidance**:

**Absolute Damage Values**:
- **<5,000 damage-minutes**: Low damage accumulation, asset aging normally
- **5,000-15,000 damage-minutes**: Moderate damage, within typical operating envelope
- **15,000-30,000 damage-minutes**: Elevated damage, approaching inspection trigger
- **>30,000 damage-minutes**: High damage accumulation, inspection recommended

**Damage Rate (30-day vs 365-day comparison)**:
```
avg_daily_damage_30d = cumulative_damage_30d / 30
avg_daily_damage_365d = cumulative_damage_365d / 365

damage_acceleration_ratio = avg_daily_damage_30d / avg_daily_damage_365d
```

**Example**:
- 30-day damage: 3,000 damage-minutes → avg: 100 damage-minutes/day
- 365-day damage: 18,000 damage-minutes → avg: 49.3 damage-minutes/day
- **Acceleration ratio**: 100 / 49.3 = **2.03** (damage rate has doubled recently)

**Interpretation**:
- **Ratio > 1.5**: Damage accelerating, investigate recent operational changes
- **Ratio 0.8-1.2**: Stable damage rate, normal operations
- **Ratio < 0.8**: Damage rate decreasing, operational improvements working

### Aging Acceleration Factor

**Business Definition**: Compares the rate of actual damage accumulation to the expected aging rate based on design life assumptions. Indicates whether the asset is aging faster or slower than designed.

**Formula**:
```
aging_acceleration_factor = pct_design_life_consumed_by_damage / pct_design_life_elapsed
```

**Component Formulas**:
```
pct_design_life_elapsed = (years_in_service / design_life_years) × 100

pct_design_life_consumed_by_damage = (cumulative_damage_365d / design_damage_threshold) × 100
```

**Where**:
- **years_in_service** = (current_date - install_date) in years
- **design_life_years** = Expected asset lifespan from design specifications (typically 20-40 years)
- **cumulative_damage_365d** = Total damage accumulated over last 365 days
- **design_damage_threshold** = Mechanism-specific allowable damage over design life (engineering judgment)

**Example Calculation**:

**Asset**: CDU-015-TT (Reactor Inlet Temperature)

**Asset Details**:
- Install date: January 1, 2010
- Current date: February 1, 2026
- Years in service: 16.08 years
- Design life: 30 years
- Primary damage mechanism: High-Temperature Sulfidation
- Design damage threshold: 100,000 damage-minutes over 30 years

**Current Damage**:
- Cumulative damage (365-day): 28,500 damage-minutes

**Step 1: Calculate percent design life elapsed**
```
pct_design_life_elapsed = (16.08 / 30) × 100
                        = 53.6%
```

**Step 2: Calculate percent design life consumed by damage**
```
pct_design_life_consumed_by_damage = (28,500 / 100,000) × 100
                                    = 28.5%
```

**Step 3: Calculate aging acceleration factor**
```
aging_acceleration_factor = 28.5 / 53.6
                          = 0.53
```

**Interpretation**: This asset is aging **better than expected** (factor < 1.0). It has consumed 28.5% of its damage allowance while 53.6% of its design life has elapsed. At this rate, it will reach 100% damage allowance at 56 years (1.87× design life).

**Contrasting Example** (Accelerated Aging):

**Asset**: FCC-008-PT (FCC Regenerator Pressure)

**Asset Details**:
- Install date: January 1, 2015
- Current date: February 1, 2026
- Years in service: 11.08 years
- Design life: 25 years
- Primary damage mechanism: Thermal Fatigue
- Design damage threshold: 50,000 damage-minutes

**Current Damage**:
- Cumulative damage (365-day): 32,400 damage-minutes

**Calculations**:
- Percent design life elapsed: (11.08 / 25) × 100 = **44.3%**
- Percent consumed by damage: (32,400 / 50,000) × 100 = **64.8%**
- **Aging acceleration factor**: 64.8 / 44.3 = **1.46**

**Interpretation**: This asset is aging **faster than designed** (factor > 1.0). It has consumed 64.8% of its damage allowance while only 44.3% of its design life has elapsed. At this rate, it will reach 100% damage allowance at **17.1 years** (0.68× design life), indicating premature aging.

**Thresholds and Actions**:

| Aging Acceleration Factor | Lifecycle Status | Interpretation | Action Required |
|---------------------------|------------------|----------------|-----------------|
| **<0.8** | Better Than Expected | Asset aging slower than designed; conservative operation | Continue current practices; may extend intervals |
| **0.8-1.2** | Normal Aging | Asset aging at expected rate; on track with design | Continue monitoring; inspections per RBI plan |
| **1.2-1.5** | Slightly Accelerated | Asset aging faster than designed; elevated concern | Investigate operating conditions; increase monitoring |
| **1.5-2.0** | Accelerated Aging | Significant over-consumption of design margin | Immediate investigation; tighten operating envelope; schedule inspection |
| **>2.0** | Critical Acceleration | Severe over-consumption; potential early failure | Emergency intervention; evaluate fitness-for-service; consider replacement |

**Root Cause Investigation**:

When aging_acceleration_factor > 1.2, investigate:
1. **Operating severity**: Are actual temperatures/pressures higher than design assumptions?
2. **Cycle frequency**: More startups/shutdowns than designed for?
3. **Feedstock changes**: More corrosive crude slates than design basis?
4. **Maintenance practices**: Delayed repairs causing cascading damage?
5. **Design deficiencies**: Was original design life estimate optimistic?

### Average Daily Damage

**Business Definition**: Average damage accumulated per day, calculated over specified rolling window (30, 90, or 365 days). Enables trend analysis and comparison of damage rates across different time periods.

**Formula**:
```
avg_daily_damage_30d = cumulative_damage_30d / 30
avg_daily_damage_90d = cumulative_damage_90d / 90
avg_daily_damage_365d = cumulative_damage_365d / 365
```

**Example Calculation**:

**Asset**: VDU-003-TT

**Damage Data**:
- 30-day cumulative damage: 2,400 damage-minutes
- 90-day cumulative damage: 6,750 damage-minutes
- 365-day cumulative damage: 24,820 damage-minutes

**Calculations**:
- **avg_daily_damage_30d**: 2,400 / 30 = **80 damage-minutes/day**
- **avg_daily_damage_90d**: 6,750 / 90 = **75 damage-minutes/day**
- **avg_daily_damage_365d**: 24,820 / 365 = **68 damage-minutes/day**

**Interpretation**: Recent damage rate (80/day) is slightly higher than quarterly (75/day) and annual (68/day) averages, indicating mild acceleration. Monitor for continued trend.

## Alert Priorities

Alerts are classified into four priority levels with specific response time requirements:

### Priority Levels

| Priority Level | Response Time | Severity | Typical Triggers |
|----------------|---------------|----------|------------------|
| **Critical** | <4 hours | Immediate safety or equipment risk | Critical IOW excursions, emergency conditions |
| **High** | <24 hours | Significant integrity concern | Inspection due (damage >80% OR health <50), rapid health degradation |
| **Medium** | <7 days | Elevated monitoring required | Inspection due (90+ days since critical), damage threshold warnings |
| **Low** | <30 days | Awareness and planning | Informational excursions, routine inspection scheduling |

### Alert Types and Triggers

#### Alert Type 1: Critical Excursions

**Trigger Condition**: Any reading violates Critical IOW limit

**Priority**: Critical (response <4 hours)

**Example**:
- Asset: CDU-015-TT
- Parameter: Temperature
- Timestamp: 2026-02-14 08:45:00
- Reading: 975°F
- Critical upper limit: 950°F
- Excursion magnitude: 25°F
- **Action**: Immediate operator intervention to bring temperature below 950°F

#### Alert Type 2: Inspection Due

**Trigger Condition (multi-factor)**: 
- Cumulative damage >80% of design threshold, OR
- Health index <50, OR
- 90+ days since last critical excursion without inspection

**Priority**: 
- High if damage >80% OR health <50
- Medium if 90+ days since critical

**Example 1** (Damage threshold):
- Asset: FCC-008-PT
- Cumulative damage (365d): 42,500 damage-minutes
- Design threshold: 50,000 damage-minutes
- Percent consumed: 85%
- **Trigger**: Damage >80%
- **Action**: Schedule inspection within 7 days

**Example 2** (Poor health):
- Asset: VDU-003-TT
- Health index: 38
- **Trigger**: Health <50
- **Action**: Schedule inspection within 14 days

**Example 3** (Time since critical):
- Asset: HCU-021-PT
- Days since last critical excursion: 95 days
- Last inspection: None since critical event
- **Trigger**: 90+ days
- **Action**: Schedule inspection within 30 days

#### Alert Type 3: Health Degradation

**Trigger Condition**: Health index drops >20 points in 30-day period

**Priority**: High (response <24 hours for investigation)

**Example**:
- Asset: CDU-009-PT
- Current health index: 52
- Health index 30 days ago: 78
- Health drop: 26 points
- **Trigger**: Drop >20 points
- **Action**: Immediate investigation of root cause (recent process upsets, increased excursion frequency, operational changes)

#### Alert Type 4: Damage Threshold

**Trigger Condition**: Cumulative damage exceeds mechanism-specific safe operating limit

**Priority**: Medium to Critical depending on percentage over threshold

**Mechanism-Specific Thresholds** (damage-minutes):

| Damage Mechanism | Threshold | Alert at 80% | Alert at 100% | Alert at 120% |
|------------------|-----------|--------------|---------------|---------------|
| High-Temperature Sulfidation | 100,000 | 80,000 | 100,000 | 120,000 |
| HTHA | 80,000 | 64,000 | 80,000 | 96,000 |
| Naphthenic Acid Corrosion | 120,000 | 96,000 | 120,000 | 144,000 |
| Creep and Stress Rupture | 50,000 | 40,000 | 50,000 | 60,000 |
| Corrosion Under Insulation | 75,000 | 60,000 | 75,000 | 90,000 |
| Stress Corrosion Cracking | 60,000 | 48,000 | 60,000 | 72,000 |
| PASCC | 40,000 | 32,000 | 40,000 | 48,000 |
| Amine SCC | 55,000 | 44,000 | 55,000 | 66,000 |
| Carburization | 65,000 | 52,000 | 65,000 | 78,000 |
| Wet H2S Cracking | 70,000 | 56,000 | 70,000 | 84,000 |
| Thermal Fatigue | 85,000 | 68,000 | 85,000 | 102,000 |

**Example**:
- Asset: CDU-015-TT
- Primary damage mechanism: High-Temperature Sulfidation
- Threshold: 100,000 damage-minutes
- Cumulative damage (365d): 92,000 damage-minutes
- Percent of threshold: 92%
- **Trigger**: Damage >80% threshold
- **Priority**: High
- **Action**: Metallurgical assessment within 7 days; consider operating envelope tightening

## Query Metrics

### Inspection Priority Score

**Business Definition**: Composite score combining health, damage, and critical event frequency to rank assets for inspection scheduling. Higher scores indicate higher inspection urgency.

**Formula**:
```
priority_score = (100 - health_index) × 2.0 + 
                 (cumulative_damage_365d / 100) × 3.0 + 
                 (critical_excursion_count × 5.0)
```

**Component Weights**:
- **Health index contribution**: 2× weight (emphasizes current condition)
- **Cumulative damage contribution**: 3× weight (emphasizes damage accumulation)
- **Critical events contribution**: 5× weight (emphasizes most severe violations)

**Example Calculation**:

**Asset**: CDU-015-TT

**Metrics**:
- Health index: 22
- Cumulative damage (365d): 28,500 damage-minutes
- Critical excursion count: 15

**Calculation**:
```
priority_score = (100 - 22) × 2.0 + (28,500 / 100) × 3.0 + (15 × 5.0)
               = (78 × 2.0) + (285 × 3.0) + (75)
               = 156 + 855 + 75
               = 1,086
```

**Interpretation**: Very high priority for inspection (score >1000 indicates critical condition requiring immediate scheduling)

**Typical Score Ranges**:
- **<100**: Low priority (routine inspection schedule)
- **100-250**: Medium priority (schedule within 90 days)
- **250-500**: High priority (schedule within 30 days)
- **>500**: Critical priority (schedule within 7 days)

### Bad Actor Score

**Business Definition**: Composite ranking identifying worst-performing assets using multiple risk factors. Used to filter bottom 10% for focused attention.

**Formula**:
```
bad_actor_score = (critical_excursion_count × 0.30) +
                  (cumulative_damage_365d / 1000.0 × 0.25) +
                  (excursion_count_total × 0.20) +
                  ((100 - health_index) × 0.25)
```

**Component Weights**:
- **Critical events**: 30% (highest safety impact)
- **Cumulative damage**: 25% (long-term degradation)
- **Total excursion frequency**: 20% (chronic poor performance)
- **Inverted health index**: 25% (holistic condition)

**Example Calculation**:

**Asset**: FCC-008-PT

**Metrics**:
- Critical excursion count: 12
- Cumulative damage (365d): 32,400 damage-minutes
- Total excursion count: 87
- Health index: 35

**Calculation**:
```
bad_actor_score = (12 × 0.30) + (32,400 / 1000.0 × 0.25) + (87 × 0.20) + ((100 - 35) × 0.25)
                = (3.6) + (32.4 × 0.25) + (17.4) + (65 × 0.25)
                = 3.6 + 8.1 + 17.4 + 16.25
                = 45.35
```

**Interpretation**: High bad actor score. Asset ranks in bottom 10% and requires focused intervention.

**Percentile Ranking**:
- SQL uses `PERCENT_RANK()` to rank all assets by bad_actor_score
- Bottom 10% (percentile_rank ≤ 0.10) flagged as bad actors
- Typically ~10 assets out of 100 total

## Formulas Reference

### Quick Reference Table

| Metric | Formula | Units | Typical Range |
|--------|---------|-------|---------------|
| **Health Index** | `100 - (weighted_excursion_score / 30.0) × 100` | 0-100 scale | 0-100 |
| **Weighted Excursion Score** | `(critical × 3) + (standard × 2) + (informational × 1)` | Points | 0-500+ |
| **Cumulative Damage (AUC)** | `Σ(magnitude × duration)` | damage-minutes | 0-100,000+ |
| **Aging Acceleration Factor** | `pct_consumed_by_damage / pct_elapsed` | Ratio | 0.5-2.0+ |
| **Avg Daily Damage** | `cumulative_damage / days` | damage-min/day | 10-500 |
| **Inspection Priority Score** | `(100-health)×2 + (damage/100)×3 + (critical×5)` | Points | 0-2000+ |
| **Bad Actor Score** | `(crit×0.3) + (dmg/1000×0.25) + (freq×0.2) + ((100-hlth)×0.25)` | Points | 0-100+ |

### Variable Definitions

| Variable | Description | Typical Values |
|----------|-------------|----------------|
| `critical_excursion_count` | Number of critical IOW violations | 0-50 |
| `standard_excursion_count` | Number of standard IOW violations | 0-100 |
| `informational_excursion_count` | Number of informational IOW violations | 0-200 |
| `cumulative_damage_30d` | Damage accumulated in last 30 days | 0-10,000 damage-min |
| `cumulative_damage_365d` | Damage accumulated in last 365 days | 0-100,000 damage-min |
| `excursion_magnitude` | Deviation from IOW limit | 1-200 (units vary) |
| `duration_minutes` | Length of excursion event | 5-1000 minutes |
| `health_index` | 0-100 health score | 0-100 |
| `years_in_service` | Age of asset since install | 0-50 years |
| `design_life_years` | Expected asset lifespan | 20-40 years |

## Interpretation Guide

### Scenario 1: Asset with High Damage, Good Health

**Metrics**:
- Health index: 82
- Cumulative damage (365d): 45,000 damage-minutes
- Critical excursions: 0
- Standard excursions: 5
- Aging acceleration factor: 1.65

**Interpretation**: Asset has good health score (few recent excursions) but high historical damage accumulation. The aging acceleration factor of 1.65 indicates damage accumulated faster than designed in the past, but recent improvement in health suggests corrective actions are working (better operational discipline, process optimization).

**Action**: Continue current practices, schedule inspection based on cumulative damage (within 60 days), monitor health trend to confirm sustained improvement.

### Scenario 2: Asset with Moderate Damage, Poor Health

**Metrics**:
- Health index: 38
- Cumulative damage (365d): 18,000 damage-minutes
- Critical excursions: 8
- Standard excursions: 22
- Health trend (30d): -18 points

**Interpretation**: Asset has moderate cumulative damage but poor current health with rapid recent deterioration (18-point drop). The high frequency of recent critical excursions indicates worsening operational conditions or equipment degradation.

**Action**: Immediate investigation required. High likelihood of recent operational upset, equipment malfunction, or process control failure. Inspect within 7 days, implement temporary operating envelope restrictions until root cause identified.

### Scenario 3: Asset with Low Damage, Accelerated Aging

**Metrics**:
- Health index: 88
- Cumulative damage (365d): 8,500 damage-minutes
- Critical excursions: 1
- Aging acceleration factor: 1.85
- Years in service: 5
- Design life: 30 years

**Interpretation**: Asset appears healthy (health index 88, low cumulative damage) but aging acceleration factor of 1.85 is concerning. Only 5 years in service (16.7% of design life) but already consumed 31% of damage allowance (based on 8,500 / design threshold of ~27,000). At this rate, asset will reach damage limit at 16 years instead of 30 years.

**Action**: Review design basis assumptions. Asset may be experiencing more severe service conditions than originally designed for. Consider material upgrade at next turnaround, tighten IOW limits to slow damage accumulation, or accept reduced service life and adjust replacement capital plan.

### Scenario 4: Bad Actor Asset

**Metrics**:
- Health index: 28
- Cumulative damage (365d): 52,000 damage-minutes
- Critical excursions: 18
- Standard excursions: 45
- Bad actor score: 62.5 (ranked #2 out of 100 assets)
- Inspection priority score: 985

**Interpretation**: This is a top-tier bad actor requiring immediate, comprehensive intervention. Health is critical (<30), damage is near design limit, and excursion frequency is extreme.

**Action**: 
1. **Immediate** (within 24 hours): Restrict operating envelope to prevent further damage, implement continuous monitoring
2. **Short-term** (within 7 days): Emergency inspection with advanced NDE techniques (UT thickness, PT/MT cracking, metallography if possible)
3. **Medium-term** (within 30 days): Root cause analysis (operations, design, material, maintenance practices)
4. **Long-term**: Material upgrade, equipment redesign, or replacement depending on inspection findings and economic analysis

---

**Document Status**: Business metrics reference guide complete

**Last Updated**: February 2026

**Maintainer**: Integrity Engineering Team
