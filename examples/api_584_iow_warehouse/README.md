# API 584 IOW Data Warehouse

## Overview

This example demonstrates a comprehensive **Risk-Based Integrity Operating Window (IOW) monitoring system** for refinery static equipment, implementing the API 584 standard from the American Petroleum Institute. The system provides real-time integrity management through continuous analysis of high-frequency sensor telemetry, enabling proactive detection of operating conditions that accelerate asset degradation and damage accumulation.

The project showcases Gorchata's capabilities in building advanced time-series analytics for process safety management, featuring:

- **5 dimension tables** (date, asset, IOW limit, parameter type, criticality level)
- **1 staging table** (sensor readings at 5-minute intervals, ~1.3M test records)
- **3 intermediate tables** (excursion detection, windowing, severity scoring)
- **2 fact tables** (excursion events, damage accumulation with AUC methodology)
- **3 metrics tables** (asset health indices 0-100 scale, bad actors, unit summaries)
- **4 alert models** (critical excursions, inspection due, health degradation, damage threshold)
- **6 analytical queries** (inspection prioritization, trending, root cause, lifecycle analysis)

The example models **100 refinery assets** across 4 process units (CDU, VDU, FCC, HCU) tracking 11 damage mechanisms through 1 month of test data at 5-minute sensor sampling intervals, designed to scale to 5 years of production data (~75M readings per asset).

## Business Context

### API 584 and Risk-Based Inspection

**API 584** is the American Petroleum Institute's recommended practice for applying risk-based inspection (RBI) methodologies to pressure vessels, piping, and associated equipment in the petroleum and chemical industries. Published by API and periodically updated, this standard provides a systematic framework for optimizing inspection programs based on the actual risk profile of each asset rather than calendar-based schedules.

**Core Principles:**
- **Risk = Probability of Failure × Consequence of Failure**: Assets are ranked by both their likelihood of failure and the potential severity of that failure.
- **Damage Mechanism Focus**: Each asset is evaluated against specific deterioration mechanisms relevant to its service conditions (temperature, pressure, chemistry, metallurgy).
- **Inspection Frequency Optimization**: High-risk assets receive frequent inspections while low-risk assets are monitored less aggressively, optimizing resource allocation.
- **Living Program**: Risk assessments are continuously updated based on new inspection findings, operational changes, and incident learnings.

### Refinery Damage Mechanisms

Refinery static equipment operates under severe service conditions that drive multiple **damage mechanisms** simultaneously. Understanding these mechanisms is critical for setting appropriate operating windows and predicting remaining asset life. The eleven primary damage mechanisms modeled in this system are:

**1. High-Temperature Sulfidation**
- **Description**: Sulfur compounds in crude oil react with steel at temperatures above 450°F to form iron sulfide scale, causing progressive metal loss
- **Affected Units**: CDU atmospheric furnaces, VDU furnaces, FCC reactor cyclones, HCU fractionators
- **Key Parameters**: Temperature (>450°F), sulfur content in feedstock
- **Mitigation**: Material upgrades (5Cr-0.5Mo, 9Cr-1Mo), temperature control

**2. High-Temperature Hydrogen Attack (HTHA)**
- **Description**: Atomic hydrogen diffuses into steel at high temperature and pressure, reacting with carbides to form methane bubbles that cause embrittlement and fissuring
- **Affected Units**: Hydrocracker reactors, hydrotreaters, reformer reactors
- **Key Parameters**: Temperature (>450°F), hydrogen partial pressure (>200 psi)
- **Mitigation**: Material selection per Nelson curves (API 941), strict adherence to IOW

**3. Naphthenic Acid Corrosion**
- **Description**: Organic acids naturally present in certain crude oils cause aggressive high-temperature corrosion at 450-750°F
- **Affected Units**: CDU atmospheric column overhead, VDU vacuum column, crude preheat trains
- **Key Parameters**: Temperature (peak at 550-650°F), Total Acid Number (TAN) of crude slate
- **Mitigation**: Alloy upgrades (Type 316/317 stainless), neutralizers, crude slate management

**4. Creep and Stress Rupture**
- **Description**: Time-dependent deformation and eventual failure of metals under constant stress at elevated temperatures
- **Affected Units**: Furnace tubes, reformer reactors, FCC catalyst regenerators
- **Key Parameters**: Metal temperature (>800°F), stress level, time at temperature
- **Mitigation**: Conservative design margins, periodic thickness testing, metallurgical monitoring

**5. Corrosion Under Insulation (CUI)**
- **Description**: External corrosion of carbon steel and austenitic stainless steel beneath insulation due to water ingress
- **Affected Units**: All insulated piping and vessels operating in 25-250°F range
- **Key Parameters**: Surface temperature cycling through dew point, insulation condition
- **Mitigation**: Coating systems, moisture barriers, periodic removal of insulation for inspection

**6. Stress Corrosion Cracking (SCC)**
- **Description**: Brittle cracking in tensile-stressed metals exposed to specific corrosive environments
- **Affected Units**: Austenitic stainless steel in chloride-bearing services, caustic services
- **Key Parameters**: Chloride concentration, pH, temperature, tensile stress
- **Mitigation**: Material selection (duplex stainless), stress relief, chemistry control

**7. Polythionic Acid Stress Corrosion Cracking (PASCC)**
- **Description**: Cracking of sensitized austenitic stainless steels during shutdowns when air, moisture, and sulfide deposits combine
- **Affected Units**: Stainless steel piping/vessels in high-temperature sulfur service
- **Key Parameters**: Sensitization degree, sulfide deposits, oxygen exposure during shutdown
- **Mitigation**: Stabilized grades (321/347 SS), solution annealing, alkaline neutralization during turnarounds

**8. Amine Stress Corrosion Cracking**
- **Description**: Cracking of carbon steel in amine treating units caused by lean amine solutions under tensile stress
- **Affected Units**: Amine absorbers, strippers, regenerators, rich/lean amine piping
- **Key Parameters**: Amine concentration, temperature, heat-affected zones, hardness
- **Mitigation**: Post-weld heat treatment, hardness control (<200 BHN), amine chemistry control

**9. Carburization**
- **Description**: Carbon diffusion into steel at high temperatures in hydrocarbon-rich environments, causing embrittlement
- **Affected Units**: Reformer furnace tubes, ethylene pyrolysis furnaces, coker heaters
- **Key Parameters**: Metal temperature (>1000°F), hydrocarbon partial pressure, alloy composition
- **Mitigation**: High-nickel alloys (HP Mod, HK-40), regular tube thickness monitoring

**10. Wet H2S Cracking (Sulfide Stress Cracking)**
- **Description**: Hydrogen embrittlement of high-strength steels in aqueous H2S environments
- **Affected Units**: Sour water systems, hydrotreater exchangers, crude unit overhead
- **Key Parameters**: H2S concentration in water phase, pH (<4 critical), material hardness
- **Mitigation**: Material hardness limits per NACE MR0175, pH control, resistant alloys

**11. Thermal Fatigue**
- **Description**: Crack initiation and propagation from cyclic thermal stresses during startups, shutdowns, or feed/rate changes
- **Affected Units**: Reactor inlet/outlet nozzles, quench zones, equipment with large thermal gradients
- **Key Parameters**: Temperature cycling frequency and magnitude, stress concentrations
- **Mitigation**: Design for thermal gradients, operational discipline to minimize cycles

### Integrity Operating Windows (IOW)

An **Integrity Operating Window** defines the safe operating envelope for each asset, establishing clear boundaries between acceptable operation and conditions that accelerate damage. The IOW concept uses a **three-tier criticality structure**:

**Critical Limits** (Red Zone)
- Most restrictive boundaries representing imminent risk to asset integrity
- Excursions indicate high probability of accelerated damage accumulation
- Typical consequence: Unplanned shutdown risk, potential safety incident
- Response requirement: Immediate corrective action (<4 hours)
- Example: Reactor temperature >950°F when HTHA-susceptible material limit is 925°F

**Standard Limits** (Yellow Zone)
- Operating boundaries based on design specifications and normal control ranges
- Excursions indicate departure from optimal operation but manageable short-term
- Typical consequence: Accelerated aging, increased inspection frequency
- Response requirement: Corrective action within 24 hours
- Example: Column pressure 15% above design operating pressure

**Informational Limits** (Green Zone)
- Wide boundaries for awareness and trending purposes
- Excursions trigger documentation but do not require immediate response
- Typical consequence: Logged for pattern analysis, root cause investigations
- Response requirement: Review at next planning meeting (within 7 days)
- Example: Temperature 10°F outside typical seasonal range but within design

**IOW Limit Examples by Parameter Type:**

| Parameter | Critical | Standard | Informational |
|-----------|----------|----------|---------------|
| **Pressure** (psig) | <345 or >405 | <360 or >390 | <370 or >380 |
| **Temperature** (°F) | <550 or >950 | <575 or >925 | <600 or >900 |
| **pH** | <4.5 or >9.0 | <5.0 or >8.5 | <5.5 or >8.0 |
| **Flow** (bbl/day) | <8,000 or >62,000 | <10,000 or >60,000 | <12,000 or >58,000 |

### Area Under Curve (AUC) Damage Methodology

The **Area Under Curve** methodology quantifies cumulative process damage by integrating the magnitude and duration of operating excursions outside IOW limits. This physics-based approach recognizes that both the severity of the excursion and the time spent in that condition contribute to total damage accumulation.

**Formula:**
```
Cumulative Damage Index = Σ(Excursion Magnitude × Duration)
```

**Where:**
- **Excursion Magnitude**: Absolute value of the deviation outside the limit (e.g., 25°F over limit)
- **Duration**: Time spent in excursion state (minutes)
- **Result**: Damage expressed in "damage-minutes" or "damage-hours"

**Example Calculation:**
A reactor operates at 975°F for 3 hours when the critical limit is 950°F.
- Excursion magnitude: 975 - 950 = 25°F
- Duration: 3 hours × 60 = 180 minutes
- Cumulative damage: 25 × 180 = 4,500 damage-minutes

This value accumulates over time, enabling trend analysis of damage rates and prediction of when accumulated damage will consume design life margin. The methodology supports:
- **Damage Rate Trending**: Identify assets with accelerating damage accumulation
- **Remaining Life Estimation**: Compare cumulative damage to design allowances
- **Inspection Interval Optimization**: Schedule inspections based on actual damage rather than calendar
- **Root Cause Prioritization**: Focus investigations on highest damage-contribution events

## Architecture

### Star Schema Design

```
                    ┌─────────────────┐
                    │    dim_date     │
                    │  (1826 days)    │
                    │   2021-2025     │
                    └────────┬────────┘
                             │
         ┌───────────────────┼────────────────────┐
         │                   │                    │
    ┌────▼─────┐      ┌──────▼──────┐     ┌──────▼──────┐
    │dim_asset │      │  dim_iow_   │     │dim_parameter│
    │(100 assets)◄────┤   limit     ├────►│   _type     │
    │CDU:30    │      │ (12 limits) │     │ (4 types)   │
    │VDU:20    │      └─────────────┘     └─────────────┘
    │FCC:30    │
    │HCU:20    │              │
    └────┬─────┘              │
         │              ┌─────▼────────┐
         │              │dim_criticality│
         │              │  _level       │
         │              │  (3 levels)   │
         │              └───────────────┘
         │
         └──────────────────┐
                            │
                     ┌──────▼────────┐
                     │   STAGING     │
                     │stg_sensor_    │
                     │  readings     │
                     │ (1.3M test)   │
                     │ (75M prod)    │
                     └──────┬────────┘
                            │
                    ┌───────▼────────┐
                    │ INTERMEDIATE   │
                    │int_iow_        │
                    │  excursions    │
                    │      ↓         │
                    │int_excursion_  │
                    │   windows      │
                    │      ↓         │
                    │int_excursion_  │
                    │   severity     │
                    └───────┬────────┘
                            │
                    ┌───────▼────────┐
                    │     FACTS      │
                    │fact_excursion_ │
                    │    events      │
                    │       +        │
                    │fact_asset_     │
                    │damage_accum    │
                    └───────┬────────┘
                            │
                    ┌───────▼────────┐
                    │    METRICS     │
                    │metrics_asset_  │
                    │integrity_index │
                    │       +        │
                    │metrics_bad_    │
                    │    actors      │
                    │       +        │
                    │metrics_unit_   │
                    │health_summary  │
                    └───────┬────────┘
                            │
                ┌───────────┴────────────┐
                │                        │
         ┌──────▼──────┐         ┌──────▼────────┐
         │   ALERTS    │         │   QUERIES     │
         │  (4 types)  │         │ (6 analytical)│
         │             │         │               │
         │• Critical   │         │• Inspection   │
         │  Excursions │         │  Priority     │
         │• Inspection │         │• Parameter    │
         │  Due        │         │  Trending     │
         │• Health     │         │• Damage Mech  │
         │  Degradation│         │  Correlation  │
         │• Damage     │         │• Root Cause   │
         │  Threshold  │         │• Lifecycle    │
         │             │         │• Unit Compare │
         └─────────────┘         └───────────────┘
```

### Data Flow Pipeline

The IOW monitoring system implements a **layered analytics pipeline** that progressively transforms raw sensor telemetry into actionable integrity insights:

```
┌─────────────────────────────────────────────────────────────────────┐
│ LAYER 1: RAW DATA SOURCES                                           │
└─────────────────────────────────────────────────────────────────────┘
  • Sensor telemetry (Pressure, Temperature, pH, Flow)
  • Asset registry (equipment, units, materials, damage mechanisms)
  • IOW limits (critical/standard/informational boundaries)
  • Calendar (dates with turnaround/seasonal flags)
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│ LAYER 2: STAGING (stg_sensor_readings)                              │
│ Purpose: Filter and enrich sensor data                              │
└─────────────────────────────────────────────────────────────────────┘
  • Quality filtering (remove bad/suspect readings)
  • 5-minute interval enforcement
  • Dimension enrichment (join asset, parameter, date metadata)
  • Output: 1.3M clean sensor readings (1 month test data)
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│ LAYER 3: INTERMEDIATE (Excursion Detection)                         │
└─────────────────────────────────────────────────────────────────────┘
  
  ┌────────────────────────────────────┐
  │ int_iow_excursions                 │
  │ • Compare readings to IOW limits   │
  │ • Flag violations with magnitude   │
  │ • Classify by criticality tier     │
  └──────────────┬─────────────────────┘
                 │
                 ▼
  ┌────────────────────────────────────┐
  │ int_excursion_windows              │
  │ • Group consecutive excursions     │
  │ • Calculate start/end timestamps   │
  │ • Compute duration in minutes      │
  └──────────────┬─────────────────────┘
                 │
                 ▼
  ┌────────────────────────────────────┐
  │ int_excursion_severity             │
  │ • Apply AUC calculation            │
  │ • Classify severity (Minor/Mod/Maj)│
  │ • Enrich with asset/limit metadata │
  └──────────────┬─────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│ LAYER 4: FACTS (Aggregated Events & Damage)                         │
└─────────────────────────────────────────────────────────────────────┘
  
  ┌────────────────────────────────────┐
  │ fact_excursion_events              │
  │ • One row per excursion event      │
  │ • AUC damage index per event       │
  │ • Complete event attributes        │
  └──────────────┬─────────────────────┘
                 │
                 ▼
  ┌────────────────────────────────────┐
  │ fact_asset_damage_accumulation     │
  │ • Aggregate damage by asset        │
  │ • Rolling windows (30/90/365 days) │
  │ • Lifecycle metrics (design life)  │
  └──────────────┬─────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│ LAYER 5: METRICS (KPIs & Health Scores)                             │
└─────────────────────────────────────────────────────────────────────┘
  
  ┌────────────────────────────────────┐
  │ metrics_asset_integrity_index      │
  │ • Health score 0-100 per asset     │
  │ • Status categorization            │
  │ • Trend analysis (30-day changes)  │
  └────────────────────────────────────┘
  
  ┌────────────────────────────────────┐
  │ metrics_bad_actors                 │
  │ • Bottom 10% performers            │
  │ • Composite bad actor score        │
  │ • Prioritized attention list       │
  └────────────────────────────────────┘
  
  ┌────────────────────────────────────┐
  │ metrics_unit_health_summary        │
  │ • Rollup by unit (CDU/VDU/FCC/HCU) │
  │ • Fleet-wide KPIs                  │
  │ • Unit performance comparison      │
  └──────────────┬─────────────────────┘
                 │
         ┌───────┴────────┐
         │                │
         ▼                ▼
┌─────────────────┐  ┌──────────────────────┐
│ LAYER 6: ALERTS │  │ LAYER 7: QUERIES     │
│ (4 alert types) │  │ (6 analytical queries)│
└─────────────────┘  └──────────────────────┘
  • Critical          • Inspection Priority
    Excursions        • Parameter Trending
  • Inspection Due    • Damage Mechanism
  • Health              Correlation
    Degradation       • Root Cause Analysis
  • Damage Threshold  • Lifecycle Analysis
                      • Unit Performance
```

### Model Descriptions

**Dimensions (5 tables):**
- **dim_date**: 1,826 rows covering 2021-2025 with refinery-specific flags (turnaround periods, seasonal specs)
- **dim_asset**: 100 assets across 4 units with material grades, damage mechanisms, design life
- **dim_iow_limit**: 12 limit definitions (4 parameter types × 3 criticality levels)
- **dim_parameter_type**: 4 parameter types with normal operating ranges (Pressure, Temperature, pH, Flow)
- **dim_criticality_level**: 3 tiers with response time requirements (Critical: 1 hr, Standard: 24 hrs, Informational: 168 hrs)

**Staging (1 table):**
- **stg_sensor_readings**: Filtered sensor telemetry at 5-minute intervals (~1.3M test records, scales to ~75M production)

**Intermediate (3 tables):**
- **int_iow_excursions**: Point-in-time IOW limit violations with magnitude and criticality
- **int_excursion_windows**: Consecutive excursion periods grouped into discrete events with start/end timestamps
- **int_excursion_severity**: Severity classification (Minor/Moderate/Major/Severe) with AUC damage calculation

**Facts (2 tables):**
- **fact_excursion_events**: One row per excursion event with cumulative damage index and full dimensional context
- **fact_asset_damage_accumulation**: Asset-level damage aggregates with rolling windows (30/90/365 days) and lifecycle metrics

**Metrics (3 tables):**
- **metrics_asset_integrity_index**: Health score 0-100 per asset with status tiers (Excellent/Good/Fair/Poor/Critical)
- **metrics_bad_actors**: Bottom 10% performers by composite scoring (frequency, damage, criticality)
- **metrics_unit_health_summary**: Unit-level KPIs for CDU, VDU, FCC, HCU with fleet benchmarking

**Alerts (4 tables):**
- **alerts_critical_excursions**: Real-time notifications for all critical-level IOW breaches
- **alerts_inspection_due**: Automated inspection scheduling triggers based on damage >80% OR health <50 OR 90+ days since critical
- **alerts_health_degradation**: Detection of rapid deterioration (>20 point health drop in 30 days)
- **alerts_damage_threshold**: Mechanism-specific damage accumulation warnings (e.g., Sulfidation >1000, HTHA >800)

**Analytical Queries (6 files):**
- **inspection_priority_queue.sql**: Risk-ranked inspection schedule with priority scoring
- **parameter_trending.sql**: Statistical process control with 3-sigma limits for early drift detection
- **damage_mechanism_correlation.sql**: Link parameter excursions to damage mechanism activation
- **excursion_root_cause_analysis.sql**: Operational event correlation (startups, feedstock changes)
- **asset_lifecycle_analysis.sql**: Compare actual aging to design life consumption
- **unit_performance_comparison.sql**: Normalize metrics across units for relative performance

## Key Performance Indicators

### Asset Integrity Health Index
**Scale**: 0-100 (100 = perfect health, 0 = critical condition)

**Formula**: `100 - (weighted_excursion_score / 30.0) × 100`

**Weighting**: 
- Critical excursions: 3× weight
- Standard excursions: 2× weight  
- Informational excursions: 1× weight

**Status Tiers**:
- **Excellent** (>90): Minimal excursions, within all IOW limits
- **Good** (70-90): Occasional minor excursions, normal wear
- **Fair** (50-70): Moderate excursion frequency, increased monitoring warranted
- **Poor** (30-50): Frequent excursions, inspection recommended
- **Critical** (<30): Severe excursion patterns, immediate action required

**Business Value**: Provides at-a-glance asset condition assessment, enables comparison across fleet, triggers inspection workflows when health drops below thresholds.

### Cumulative Damage Index
**Definition**: Area Under Curve (AUC) calculation of excursion magnitude × duration

**Units**: damage-minutes or damage-hours

**Formula**: `Σ(|parameter_value - limit_value| × duration_minutes)` for all excursions

**Rolling Windows**:
- 30-day cumulative: Recent damage trend
- 90-day cumulative: Quarterly pattern analysis
- 365-day cumulative: Annual damage budget tracking

**Business Value**: Quantifies total damage accumulation for remaining life calculations, enables trend analysis to predict future damage rates, supports inspection interval optimization based on actual accumulated damage vs design allowances.

### Bad Actor Score
**Definition**: Composite ranking identifying worst-performing assets (bottom 10%)

**Formula**: Weighted combination of:
- Critical excursion count: 30%
- Cumulative damage (365-day): 25%
- Excursion frequency: 20%
- Inverted health index: 25%

**Ranking Method**: PERCENT_RANK() function, bottom 10% flagged as bad actors

**Business Value**: Focuses limited maintenance resources on highest-risk assets, enables proactive intervention before failure, identifies patterns for root cause investigation.

### Inspection Priority Score
**Formula**: 
```
(100 - health_index) × 2 + (cumulative_damage_365d / 100) × 3 + (critical_excursion_count × 5)
```

**Interpretation**: Higher score = higher inspection urgency

**Components**:
- Health index contribution: 2× weight on poor health
- Damage contribution: 3× weight on accumulated damage
- Critical event contribution: 5× weight on critical excursions

**Business Value**: Objective, data-driven inspection scheduling, replaces subjective prioritization, optimizes inspection resource allocation to highest-risk assets.

### Aging Acceleration Factor
**Formula**: `(pct_design_life_consumed_by_damage) / (pct_design_life_elapsed)`

**Interpretation**:
- **>1.0**: Aging faster than designed (accelerated degradation)
- **~1.0**: Aging on track with design expectations (normal)
- **<1.0**: Aging slower than designed (conservative operation)

**Lifecycle Status**:
- **Accelerated_Aging** (>1.2): Operating conditions exceeding design assumptions
- **Normal_Aging** (0.8-1.2): Within design envelope
- **Better_Than_Expected** (<0.8): Conservative operation preserving asset life

**Business Value**: Identifies assets being "over-consumed" by aggressive operation, enables life extension strategies through operating discipline, supports capital planning for early replacement.

## Data Model

### Model Summary Table

| Layer | Model Name | Grain | Row Count (Test) | Row Count (Production) |
|-------|------------|-------|------------------|------------------------|
| **Dimensions** | | | | |
| | dim_date | One row per day | 1,826 | 1,826 |
| | dim_asset | One row per asset | 100 | 100 |
| | dim_iow_limit | One row per parameter-criticality combo | 12 | 12 |
| | dim_parameter_type | One row per parameter type | 4 | 4 |
| | dim_criticality_level | One row per criticality level | 3 | 3 |
| **Staging** | | | | |
| | stg_sensor_readings | One row per sensor reading | ~1.3M | ~75M |
| **Intermediate** | | | | |
| | int_iow_excursions | One row per excursion data point | ~50K | ~2.5M |
| | int_excursion_windows | One row per excursion event | ~10K | ~500K |
| | int_excursion_severity | One row per classified event | ~10K | ~500K |
| **Facts** | | | | |
| | fact_excursion_events | One row per excursion event | ~10K | ~500K |
| | fact_asset_damage_accumulation | One row per asset (current state) | 100 | 100 |
| **Metrics** | | | | |
| | metrics_asset_integrity_index | One row per asset | 100 | 100 |
| | metrics_bad_actors | One row per bad actor asset (~10) | ~10 | ~10 |
| | metrics_unit_health_summary | One row per unit | 4 | 4 |
| **Alerts** | | | | |
| | alerts_critical_excursions | One row per critical excursion | ~2K | ~100K |
| | alerts_inspection_due | One row per asset needing inspection | ~15 | ~15 |
| | alerts_health_degradation | One row per degrading asset | ~5 | ~5 |
| | alerts_damage_threshold | One row per threshold breach | ~8 | ~8 |

### Asset Distribution

**By Unit:**
- CDU (Crude Distillation Unit): 30 assets
- VDU (Vacuum Distillation Unit): 20 assets
- FCC (Fluid Catalytic Cracking): 30 assets
- HCU (Hydrocracker Unit): 20 assets

**By Criticality Level:**
- Critical: 30 assets (potential safety incidents if failed)
- Standard: 50 assets (production impact if failed)
- Informational: 20 assets (monitoring-only classification)

**Primary Damage Mechanisms (11 total):**
- High-Temperature Sulfidation
- High-Temperature Hydrogen Attack (HTHA)
- Naphthenic Acid Corrosion
- Creep and Stress Rupture
- Corrosion Under Insulation (CUI)
- Stress Corrosion Cracking (SCC)
- Polythionic Acid SCC (PASCC)
- Amine Stress Corrosion Cracking
- Carburization
- Wet H2S Cracking (Sulfide Stress Cracking)
- Thermal Fatigue

## How to Run

### Prerequisites

- **Go 1.25 or higher**
- **Gorchata installed**: 
  ```powershell
  go install github.com/yourusername/gorchata/cmd/gorchata@latest
  ```
- **Terminal** (PowerShell on Windows, bash on Linux/Mac)
- **SQLite** (included with Gorchata)

### Running the Example

**1. Navigate to the example directory:**

```powershell
cd examples/api_584_iow_warehouse
```

**2. Verify seed data exists (pre-generated for test data):**

```powershell
# Check for seed files (should show 6 CSV files)
Get-ChildItem seeds/*.csv

# Expected files:
# - dim_date.csv (1,826 rows)
# - dim_asset.csv (100 rows)
# - dim_iow_limit.csv (12 rows)
# - dim_parameter_type.csv (4 rows)
# - dim_criticality_level.csv (3 rows)
# - stg_sensor_readings.csv (~1.3M rows, ~75MB)
```

**3. Run all models (complete pipeline):**

```powershell
gorchata run
```

This executes the full data pipeline:
- Loads dimension tables from seed data
- Loads staging sensor readings (~1.3M records)
- Detects IOW excursions (intermediate layer)
- Aggregates excursion events and damage (fact layer)
- Calculates health metrics and bad actors (metrics layer)
- Generates alerts for critical conditions

**Output**: Database file `gorchata.db` with 15 tables populated

**4. Run specific models or layers:**

```powershell
# Run only staging layer
gorchata run --models stg_sensor_readings

# Run intermediate excursion detection pipeline
gorchata run --models int_iow_excursions,int_excursion_windows,int_excursion_severity

# Run fact tables only
gorchata run --models fact_excursion_events,fact_asset_damage_accumulation

# Run metrics layer only
gorchata run --models metrics_asset_integrity_index,metrics_bad_actors,metrics_unit_health_summary

# Run alert models only
gorchata run --models alerts_critical_excursions,alerts_inspection_due,alerts_health_degradation,alerts_damage_threshold
```

**5. Run tests to validate implementation:**

```powershell
# Run all API 584 IOW tests (44+ tests)
go test -v

# Run with coverage
go test -v -cover

# Run specific test
go test -v -run TestAssetIntegrityHealthIndex
```

**6. Execute analytical queries:**

```powershell
# Open SQLite CLI
sqlite3 gorchata.db

# Run inspection priority query
.read queries/inspection_priority_queue.sql

# Run parameter trending query
.read queries/parameter_trending.sql

# Run damage mechanism correlation query
.read queries/damage_mechanism_correlation.sql

# Run root cause analysis query
.read queries/excursion_root_cause_analysis.sql

# Run lifecycle analysis query
.read queries/asset_lifecycle_analysis.sql

# Run unit performance comparison query
.read queries/unit_performance_comparison.sql

# Exit SQLite
.quit
```

**7. Inspect results:**

```powershell
# Connect to database
sqlite3 gorchata.db

# View asset health indices
SELECT 
    tag_id, 
    equipment_name, 
    health_index, 
    health_status 
FROM metrics_asset_integrity_index 
ORDER BY health_index ASC 
LIMIT 10;

# View bad actors
SELECT 
    tag_id, 
    equipment_name, 
    bad_actor_score,
    critical_excursion_count,
    cumulative_damage_365d
FROM metrics_bad_actors
ORDER BY bad_actor_score DESC;

# View critical alerts
SELECT 
    tag_id,
    excursion_start_timestamp,
    parameter_type,
    parameter_value,
    critical_upper_limit,
    excursion_magnitude,
    alert_priority
FROM alerts_critical_excursions
ORDER BY excursion_start_timestamp DESC
LIMIT 20;
```

### Regenerating Seed Data

The example includes pre-generated seed data for convenience. To regenerate with different parameters:

```powershell
# Navigate to transformations directory
cd transformations

# Run seed data generator (modify parameters inside file as needed)
go run generate_sensor_data.go

# Returns to example directory
cd ..

# Run pipeline with new data
gorchata run
```

**Note**: Seed data generation creates realistic sensor telemetry with:
- 5-minute sampling intervals
- Realistic parameter value distributions
- Intentional IOW excursions (10-15% of readings)
- Multiple damage mechanism scenarios
- Seasonal variation patterns

## Analytical Queries

The example includes six business-focused analytical queries that demonstrate the power of the IOW monitoring system for decision support:

### 1. Inspection Priority Queue
**File**: [queries/inspection_priority_queue.sql](queries/inspection_priority_queue.sql)

**Purpose**: Generate risk-ranked inspection schedule for maintenance planning

**Key Metrics**:
- Priority score (composite of health, damage, critical events)
- Consequence category (High/Medium/Low based on unit and criticality)
- Recommended inspection timing (Immediate/Within_7_Days/Within_30_Days/Routine)
- Last excursion date and days since critical event

**Business Value**: Replaces calendar-based inspection schedules with data-driven RBI prioritization, optimizes limited inspection resources, provides objective justification for inspection timing.

**Sample Output**:
```
Tag        | Equipment            | Priority | Health | Damage  | Recommendation
-----------|----------------------|----------|--------|---------|------------------
CDU-015-TT | Reactor Inlet Nozzle | 285      | 22     | 15,480  | Immediate
FCC-008-PT | Regenerator Cyclone  | 267      | 35     | 12,220  | Immediate
VDU-003-TT | Vacuum Column Top    | 189      | 58     | 8,940   | Within_7_Days
```

### 2. Parameter Trending
**File**: [queries/parameter_trending.sql](queries/parameter_trending.sql)

**Purpose**: Statistical process control for early detection of parameter drift before IOW limits are breached

**Key Metrics**:
- 30-day rolling average
- Standard deviation
- 3-sigma control limits (upper and lower)
- Trend direction (increasing/decreasing/stable)
- Proximity to IOW limits

**Business Value**: Enables proactive intervention before excursions occur, identifies gradual degradation patterns (catalyst aging, fouling, seal leaks), supports predictive maintenance strategies.

**Sample Output**:
```
Tag        | Parameter    | 30-Day Avg | Std Dev | Upper 3σ | IOW Critical | Trend
-----------|--------------|------------|---------|----------|--------------|----------
HCU-021-TT | Temperature  | 748.3°F    | 12.4    | 785.5    | 800.0        | Increasing
CDU-009-PT | Pressure     | 372.8 psig | 8.7     | 398.9    | 405.0        | Stable
```

### 3. Damage Mechanism Correlation
**File**: [queries/damage_mechanism_correlation.sql](queries/damage_mechanism_correlation.sql)

**Purpose**: Link parameter excursions to specific damage mechanism activation

**Key Metrics**:
- Damage mechanism type
- Associated parameter types
- Excursion frequency by mechanism
- Cumulative damage by mechanism
- Assets at risk for each mechanism

**Business Value**: Focuses metallurgical investigations on specific failure modes, validates operating envelope assumptions, identifies systematic issues affecting multiple assets with same damage mechanism.

**Sample Output**:
```
Damage Mechanism       | Parameter    | Asset Count | Excursions | Cumulative Damage
-----------------------|--------------|-------------|------------|------------------
HTHA                   | Temperature  | 15          | 1,245      | 458,900
                       | Pressure     |             | 892        | 
Sulfidation            | Temperature  | 22          | 2,187      | 672,450
Naphthenic Acid        | Temperature  | 8           | 734        | 215,680
```

### 4. Excursion Root Cause Analysis
**File**: [queries/excursion_root_cause_analysis.sql](queries/excursion_root_cause_analysis.sql)

**Purpose**: Correlate excursions with operational events (startups, shutdowns, feedstock changes, upset conditions)

**Key Metrics**:
- Excursion timestamp
- Recent operational events (within 4 hours before excursion)
- Event type (startup, feedstock change, upset, turnaround)
- Frequency of excursion-event correlation

**Business Value**: Identifies procedural improvements to reduce excursions during transient operations, quantifies impact of feedstock variability on integrity, supports operational discipline training.

**Sample Output**:
```
Tag        | Excursion Time    | Event Type       | Event Time        | Correlation
-----------|-------------------|------------------|-------------------|-------------
CDU-015-TT | 2021-05-15 14:23  | Unit_Startup     | 2021-05-15 12:10  | Yes
FCC-008-PT | 2021-06-03 08:45  | Feedstock_Change | 2021-06-03 07:20  | Yes
VDU-003-TT | 2021-07-22 22:15  | Upset_Condition  | 2021-07-22 21:50  | Yes
```

### 5. Asset Lifecycle Analysis
**File**: [queries/asset_lifecycle_analysis.sql](queries/asset_lifecycle_analysis.sql)

**Purpose**: Compare actual asset aging to design life expectations, identify assets being over-consumed

**Key Metrics**:
- Design life (years)
- Elapsed life (years since installation)
- Percentage of design life elapsed
- Percentage of design life consumed by damage
- Aging acceleration factor
- Lifecycle status (Accelerated_Aging/Normal_Aging/Better_Than_Expected)

**Business Value**: Supports capital planning for replacement timing, identifies operating conditions exceeding design assumptions, enables life extension strategies through operating discipline.

**Sample Output**:
```
Tag        | Equipment     | Design Life | Elapsed | % Elapsed | % Consumed | Acceleration | Status
-----------|---------------|-------------|---------|-----------|------------|--------------|--------
CDU-015-TT | Reactor Inlet | 25 years    | 11.2    | 44.8%     | 68.5%      | 1.53         | Accelerated
FCC-008-PT | Regen Cyclone | 30 years    | 8.5     | 28.3%     | 29.1%      | 1.03         | Normal
VDU-003-TT | Column Top    | 40 years    | 15.7    | 39.3%     | 22.8%      | 0.58         | Better
```

### 6. Unit Performance Comparison
**File**: [queries/unit_performance_comparison.sql](queries/unit_performance_comparison.sql)

**Purpose**: Normalize integrity metrics across process units (CDU/VDU/FCC/HCU) for comparative performance evaluation

**Key Metrics**:
- Unit name
- Asset count
- Average health index
- Total cumulative damage
- Critical excursion count
- Bad actor count
- Unit integrity ranking

**Business Value**: Identifies units requiring focused integrity improvement programs, enables benchmarking between similar units, supports resource allocation decisions for unit-level initiatives.

**Sample Output**:
```
Unit | Asset Count | Avg Health | Total Damage | Critical Events | Bad Actors | Ranking
-----|-------------|------------|--------------|-----------------|------------|--------
VDU  | 20          | 78.5       | 245,890      | 145             | 1          | 1
CDU  | 30          | 72.3       | 418,550      | 287             | 4          | 2
HCU  | 20          | 68.1       | 335,720      | 312             | 3          | 3
FCC  | 30          | 61.4       | 527,340      | 425             | 5          | 4
```

## Testing

The example implements comprehensive test coverage following TDD methodology, with **44+ tests** validating all phases of the implementation:

**Test Categories:**

1. **Schema Validation** (5 tests)
   - Schema file exists and is valid YAML
   - All 18 models defined in schema
   - Dimension table schemas are complete

2. **Seed Data** (6 tests)
   - All seed files exist and are readable
   - Seed data row counts match expectations
   - CSV format validation

3. **Staging Layer** (8 tests)
   - Sensor reading data quality filtering
   - 5-minute interval enforcement
   - Timestamp sequence validation
   - Parameter value range checks
   - Asset-tag referential integrity

4. **Intermediate Layer** (9 tests)
   - Excursion detection logic
   - Windowing algorithm correctness
   - Severity classification
   - AUC damage calculation accuracy

5. **Fact Layer** (8 tests)
   - Excursion event aggregation
   - Damage accumulation by asset
   - Rolling window calculations (30/90/365 days)
   - Lifecycle metric computation

6. **Metrics Layer** (10 tests)
   - Health index formula validation
   - Bad actor identification (bottom 10%)
   - Unit summary aggregations
   - Health status tier assignment

7. **Alert Layer** (8 tests)
   - Critical excursion alert generation
   - Inspection due logic (damage >80% OR health <50 OR 90+ days)
   - Health degradation detection (>20 point drop)
   - Damage threshold triggers by mechanism

**Running Tests:**

```powershell
# Run all tests
go test -v

# Run with coverage report
go test -v -cover

# Run specific test category
go test -v -run TestDim
go test -v -run TestStaging
go test -v -run TestIntermediate
go test -v -run TestFact
go test -v -run TestMetrics
go test -v -run TestAlert

# Run single test
go test -v -run TestAssetIntegrityHealthIndex
```

**Test Approach:**
- TDD workflow followed throughout all 8 phases
- Tests written before implementation
- Each model validated with schema, data quality, and business logic tests
- Tests use actual database with seed data (not mocks)
- All tests must pass before phase completion

## References

### API 584 and Risk-Based Inspection
- **API 584**: *Risk-Based Inspection Technology*, American Petroleum Institute (latest edition)
- **API 580**: *Risk-Based Inspection*, American Petroleum Institute
- **API 581**: *Risk-Based Inspection Methodology*, American Petroleum Institute

### Damage Mechanisms
- **API 571**: *Damage Mechanisms Affecting Fixed Equipment in the Refining Industry*, American Petroleum Institute
- **API 579-1/ASME FFS-1**: *Fitness-For-Service*, American Petroleum Institute / ASME
- **API 941**: *Steels for Hydrogen Service at Elevated Temperatures and Pressures in Petroleum Refineries and Petrochemical Plants*, American Petroleum Institute

### Integrity Operating Windows
- **API RP 584**: Section on establishing Integrity Operating Windows (IOW)
- **OSHA PSM**: Process Safety Management standard (29 CFR 1910.119) - Operating limits
- **EPA RMP**: Risk Management Program regulations (40 CFR Part 68) - Safe operating envelopes

### Area Under Curve Methodology
- Various internal refinery integrity management programs
- Process safety management literature on cumulative damage tracking
- Statistical process control references for AUC applications

### Additional Resources
- **ASME Boiler and Pressure Vessel Code**: Design standards for pressure equipment
- **NACE MR0175/ISO 15156**: Materials for H2S service
- **ISA-18.2**: Management of Alarm Systems for the Process Industries

---

**Project Status**: ✅ Complete - All phases implemented (February 2026)

**Total Implementation**: 8 phases completed
- Phase 1: Dimension tables
- Phase 2: Staging layer
- Phase 3: Intermediate layer (excursion detection)
- Phase 4: Fact tables
- Phase 5: Metrics layer
- Phase 6: Alert models
- Phase 7: Analytical queries
- Phase 8: Documentation

**Test Coverage**: 44+ tests passing

**Data Scale**: 
- Test: 1 month, 1.3M sensor readings, 100 assets
- Production capacity: 5 years, ~75M readings, 100 assets
