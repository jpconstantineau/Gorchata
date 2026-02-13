
# KPI Definitions - Oil Refinery Data Warehousing

## Overview

This document provides comprehensive definitions, formulas, benchmarks, and usage guidelines for all Key Performance Indicators (KPIs) in the oil refinery data warehouse. These KPIs enable data-driven decision-making for yield optimization, energy efficiency, and operational excellence.

**Document Version:** 1.0  
**Last Updated:** Phase 8 Implementation  
**Audience:** Operations Management, Engineering Teams, Data Analysts, Executive Leadership

---

## Table of Contents

1. [Yield & Product Slate KPIs](#yield--product-slate-kpis)
2. [Energy Efficiency KPIs](#energy-efficiency-kpis)
3. [Capacity & Utilization KPIs](#capacity--utilization-kpis)
4. [Reliability & Downtime KPIs](#reliability--downtime-kpis)
5. [Mass Balance & Process KPIs](#mass-balance--process-kpis)
6. [Data Quality KPIs](#data-quality-kpis)
7. [Unit-Specific KPIs](#unit-specific-kpis)
8. [Trending & Volatility KPIs](#trending--volatility-kpis)

---

## Yield & Product Slate KPIs

### 1.1 Gasoline Yield Percentage

**Definition:** The percentage of crude oil input that is converted into gasoline products.

**Formula:**
```
Gasoline Yield % = (Gasoline Production (bbl) / Crude Input (bbl)) × 100
```

**Components:**
- **Gasoline Production:** Sum of all gasoline grades (Regular, Premium, Midgrade)
- **Crude Input:** Total crude oil receipts for the period

**Target Benchmark:**
- **Industry Typical:** 45-55% for gasoline-optimized refineries
- **Simple Refineries:** 35-45%
- **Complex Refineries (Summer Mode):** 50-55%
- **Complex Refineries (Winter Mode):** 40-50% (shift to distillates)

**Business Rationale:**
- Gasoline is typically the highest-value petroleum product
- Seasonal optimization: Higher gasoline in summer (driving season), lower in winter
- Crude slate impacts yield: Lighter crudes produce more gasoline naturally
- Secondary conversion units (FCC, reformer) increase gasoline yield from heavier fractions

**Data Source:**
- `fact_unit_production.product_volume_bbl` (WHERE product_type IN gasoline categories)
- `fact_crude_receipts.receipt_volume_bbl`

**Calculation Frequency:** Daily, aggregated to monthly

**Investigation Thresholds:**
- **Below 40%:** Investigate crude slate, conversion unit performance, operating modes
- **Above 60%:** Verify measurement accuracy, check for operational anomalies
- **Sudden 5%+ change:** Review crude slate changes, unit turnarounds, operational shifts

**Related Metrics:**
- Distillate Yield %
- High-Value Product %
- FCC Conversion Efficiency

---

### 1.2 Distillate Yield Percentage

**Definition:** The percentage of crude oil input that is converted into distillate products (diesel, jet fuel, kerosene).

**Formula:**
```
Distillate Yield % = (Distillate Production (bbl) / Crude Input (bbl)) × 100
```

**Components:**
- **Distillate Production:** Sum of diesel, jet fuel (kerosene), heating oil
- **Crude Input:** Total crude oil receipts for the period

**Target Benchmark:**
- **Industry Typical:** 25-35%
- **Diesel-Optimized (Winter):** 35-40%
- **Gasoline-Optimized (Summer):** 20-30%

**Business Rationale:**
- Distillates are critical transportation fuels (diesel for trucks, jet fuel for aircraft)
- Winter heating oil demand increases distillate production
- Global shift toward diesel in passenger vehicles increases importance
- Middle distillate is natural crude fraction; less conversion required than gasoline

**Seasonal Patterns:**
- **Winter (Oct-Mar):** Higher distillate production for heating oil demand
- **Summer (Apr-Sep):** Lower distillate, shift capacity to gasoline

**Data Source:**
- `fact_unit_production.product_volume_bbl` (WHERE product_type IN distillate categories)
- `fact_crude_receipts.receipt_volume_bbl`

**Calculation Frequency:** Daily, aggregated to monthly

**Investigation Thresholds:**
- **Below 20%:** Review hydrotreating units, crude slate (too light?)
- **Above 45%:** Verify not over-producing at expense of higher-value gasoline
- **Seasonal mismatch:** Distillate high in summer or low in winter warrants review

---

### 1.3 High-Value Product Percentage

**Definition:** The combined yield of gasoline, distillates, and jet fuel as a percentage of crude input.

**Formula:**
```
High-Value Product % = ((Gasoline + Distillates + Jet Fuel) / Crude Input) × 100
```

**Target Benchmark:**
- **Complex Refineries:** 75-85%
- **Simple Refineries:** 60-75%
- **World-Class Refineries:** > 85%

**Business Rationale:**
- Maximizing high-value products is the primary goal of refinery optimization
- Remaining products (residual fuel, coke, etc.) have lower economic value
- Higher percentage indicates more complete crude conversion
- Measures effectiveness of secondary conversion units

**Data Source:**
- `fact_unit_production.product_volume_bbl` (filtered by product categories)
- `fact_crude_receipts.receipt_volume_bbl`

**Calculation Frequency:** Daily, aggregated to monthly

**Strategic Importance:**
- Directly impacts refinery profitability
- Key metric for crude slate optimization decisions
- Informs capital investment decisions (add conversion capacity?)

---

### 1.4 Yield Gap Percentage

**Definition:** The difference between theoretical yields (from crude assay data) and actual measured yields.

**Formula:**
```
Yield Gap % = ((Theoretical Yield % - Actual Yield %) / Theoretical Yield %) × 100
```

**Target Benchmark:**
- **Excellent:** < 2%
- **Acceptable:** 2-5%
- **Investigate:** > 5%
- **Action Required:** > 10%

**Business Rationale:**
- Theoretical yields come from laboratory crude assay distillation curves
- Gap indicates yield loss due to:
  - Operating conditions not optimized
  - Catalyst deactivation
  - Equipment performance degradation
  - Feedstock quality variation from assay
  - Measurement errors

**Data Source:**
- Theoretical: Crude assay laboratory data (external)
- Actual: `fact_unit_production` aggregates

**Calculation Frequency:** Weekly or monthly (requires assay data)

**Investigation Actions (> 5% gap):**
1. Review unit operating conditions vs. design
2. Check catalyst activity and age
3. Verify crude slate matches assay basis
4. Audit measurement instruments and calibration
5. Compare to historical performance

---

## Energy Efficiency KPIs

### 2.1 Energy Intensity Index (EII)

**Definition:** The amount of energy consumed per barrel of crude oil throughput, measuring overall refinery energy efficiency.

**Formula:**
```
EII = Energy Consumed (MMBtu) / Throughput (bbl)
```

**Components:**
- **Energy Consumed:** Refinery fuel gas, natural gas, electricity (converted to MMBtu)
- **Throughput:** Total barrels processed through all units

**Target Benchmark:**
- **Excellent (Complex Refinery):** < 0.70 MMBtu/bbl
- **Good (Complex Refinery):** 0.70-0.80 MMBtu/bbl
- **Acceptable (Complex Refinery):** 0.80-0.90 MMBtu/bbl
- **Poor:** > 0.90 MMBtu/bbl
- **Simple Refineries:** 0.40-0.60 MMBtu/bbl (fewer conversion units)

**Business Rationale:**
- Energy costs are 2nd largest refinery operating expense (after crude)
- Lower EII = better energy efficiency = lower operating costs
- Complex refineries have higher EII due to energy-intensive conversion units
- Environmental regulations increasingly focus on energy efficiency

**Assumptions:**
- Refinery fuel gas heating value: ~18 MMBtu/ton
- Natural gas: ~1.03 MMBtu/MCF
- Electricity: ~3,412 BTU/kWh (0.003412 MMBtu/kWh)

**Data Source:**
- `fact_unit_operations.refinery_fuel_consumed_tons`
- `fact_unit_operations.throughput_bbl`

**Calculation Frequency:** Daily, with 7-day and 30-day moving averages

**Improvement Opportunities:**
- Heat integration and heat exchanger network optimization
- Furnace and heater efficiency improvements
- Steam system optimization
- Power generation and cogeneration
- Energy management systems (EMS)
- Operational best practices (reduce flaring, optimize temperatures)

**Cost Savings Example:**
- Current EII: 0.85 MMBtu/bbl
- Target EII: 0.70 MMBtu/bbl
- Gap: 0.15 MMBtu/bbl
- Throughput: 300,000 bbl/day
- Daily savings: 0.15 × 300,000 = 45,000 MMBtu/day
- At $4/MMBtu: **$180,000/day = $65.7M/year**

---

### 2.2 Unit-Specific Energy Intensity

**Definition:** Energy consumed per barrel of throughput for an individual process unit.

**Formula:**
```
Unit EII = Unit Energy Consumed (MMBtu) / Unit Throughput (bbl)
```

**Typical Benchmarks by Unit Type:**
- **CDU (Crude Distillation Unit):** 0.05-0.10 MMBtu/bbl
- **VDU (Vacuum Distillation Unit):** 0.08-0.15 MMBtu/bbl
- **FCC (Fluid Catalytic Cracker):** 0.30-0.45 MMBtu/bbl
- **Hydrocracker:** 0.70-1.00 MMBtu/bbl
- **Reformer:** 0.40-0.60 MMBtu/bbl
- **Hydrotreater:** 0.15-0.25 MMBtu/bbl

**Business Rationale:**
- Identifies which units have efficiency opportunities
- Benchmark individual units against design or industry standards
- Track performance degradation over time

**Data Source:**
- `fact_unit_operations` (by unit_id)

**Calculation Frequency:** Daily

---

## Capacity & Utilization KPIs

### 3.1 Capacity Utilization Percentage

**Definition:** The percentage of design capacity being utilized by a process unit or refinery complex.

**Formula:**
```
Capacity Utilization % = (Actual Throughput / Design Capacity) × 100
```

**For Complex-Level (Weighted Average):**
```
Weighted Avg Utilization % = Σ(Throughput × Capacity) / Σ(Capacity²) × 100
```

**Target Benchmark:**
- **Excellent:** > 90%
- **Good:** 85-90%
- **Acceptable:** 80-85%
- **Low:** 70-80%
- **Very Low:** < 70%

**Business Rationale:**
- Higher utilization = better asset productivity and lower unit costs
- Planned turnarounds reduce utilization temporarily (every 3-5 years)
- Unplanned low utilization indicates operational constraints or market issues
- Operating at 100%+ capacity (above nameplate) possible with optimization

**Factors Affecting Utilization:**
- **Feedstock availability:** Crude supply constraints
- **Product demand:** Storage full, market weak
- **Mechanical constraints:** Equipment issues, fouling
- **Regulatory constraints:** Emission limits, flaring restrictions
- **Planned maintenance:** Turnarounds, catalyst changes
- **Economic optimization:** Margin too low to run unit

**Data Source:**
- `fact_unit_operations.throughput_bbl`
- `dim_unit.capacity_bbl_day`

**Calculation Frequency:** Daily, aggregated to monthly

**Investigation Thresholds:**
- **< 70%:** Identify and address constraints (mechanical, feedstock, market)
- **> 95%:** Monitor closely for over-capacity stress, but generally positive
- **Sudden 10%+ drop:** Investigate unit trip, feedstock issue, or constraint

---

### 3.2 Average Daily Throughput

**Definition:** The average barrels per day processed during a given period.

**Formula:**
```
Avg Daily Throughput = Total Throughput / Operating Days
```

**Data Source:**
- `fact_daily_kpi.total_throughput_bbl`

**Usage:**
- Basis for rate-based calculations (yields, energy intensity, etc.)
- Compare to prior periods to identify trends
- Input to economic models and margin calculations

---

## Reliability & Downtime KPIs

### 4.1 Reliability Factor Percentage

**Definition:** The percentage of available time that units are operating (uptime).

**Formula:**
```
Reliability Factor % = ((Available Hours - Downtime Hours) / Available Hours) × 100
```

**Alternative Formula:**
```
Reliability Factor % = (Operating Hours / (Operating Hours + Downtime Hours)) × 100
```

**Target Benchmark:**
- **Excellent:** > 95%
- **Good:** 90-95%
- **Acceptable:** 85-90%
- **Poor:** < 85%

**World-Class Reliability:** > 98%

**Business Rationale:**
- Directly impacts production volume and revenue
- Unplanned downtime is extremely costly (lost production + repair costs)
- Reliability is key driver of refinery profitability
- Differentiates best-in-class from average refineries

**Data Source:**
- `fact_unit_operations.operating_hours`
- `fact_unit_operations.downtime_hours`

**Calculation Frequency:** Daily, monthly, annual

**Industry Context:**
- Major turnarounds every 3-5 years reduce annual reliability
- Continuous Improvement programs target incremental reliability gains
- Predictive maintenance and reliability engineering increase uptime

---

### 4.2 Planned vs. Unplanned Downtime

**Planned Downtime:**
- Scheduled turnarounds (major overhauls every 3-5 years)
- Catalyst changes (FCC: 3-5 years, reformer: 2-3 years)
- Regulatory inspections
- Preventive maintenance

**Unplanned Downtime:**
- Equipment failures
- Process upsets
- Emergency shutdowns
- Utility failures (power, steam, cooling water)

**Target Benchmark:**
- **Planned Downtime:** 2-5% annually (varies by turnaround schedule)
- **Unplanned Downtime:** < 2% annually (< 7 days/year for 24/7 operation)

**Cost Impact Example:**
- Refinery margin: $15/bbl
- Throughput: 300,000 bbl/day
- 1 day unplanned downtime: $4.5M lost margin
- 1% annual unplanned downtime (3.65 days): **$16.4M/year**

**Data Source:**
- `fact_unit_operations.downtime_hours`
- `fact_unit_operations.downtime_type`

**Calculation Frequency:** Daily, monthly aggregate, year-to-date tracking

---

## Mass Balance & Process KPIs

### 5.1 Unaccounted For Loss (UFL) Percentage

**Definition:** The percentage of mass that cannot be accounted for in the mass balance.

**Formula:**
```
UFL % = ((Inputs - Outputs - Inventory Change) / Inputs) × 100
```

**Target Benchmark:**
- **Excellent:** < 0.3%
- **Good:** 0.3-0.5%
- **Acceptable:** 0.5-1.0%
- **Investigate:** > 1.0%
- **Critical:** > 2.0%

**Business Rationale:**
- Mass balance closure is fundamental to inventory management
- UFL represents:
  - Measurement errors (primary cause)
  - Fugitive emissions (small leaks)
  - Sampling and analysis errors
  - Tank calibration errors
  - Process losses (flaring, etc.)

**Investigation Priorities (> 1.0%):**
1. Review meter calibration and accuracy
2. Check tank gauging and strapping tables
3. Verify sampling and lab analysis procedures
4. Audit data entry and calculation logic
5. Inspect for leaks or fugitive emissions

**Data Source:**
- `fact_mass_balance.unaccounted_pct`

**Calculation Frequency:** Daily

**Regulatory Importance:**
- EPA and state regulations require mass balance documentation
- High UFL may indicate unreported emissions
- Inventory tax calculations depend on accurate mass balance

---

## Data Quality KPIs

### 6.1 Data Quality Pass Rate Percentage

**Definition:** The percentage of data quality checks that pass validation rules.

**Formula:**
```
Pass Rate % = (Checks Passed / Total Checks) × 100
```

**Target Benchmark:**
- **Excellent:** > 95%
- **Good:** 90-95%
- **Acceptable:** 85-90%
- **Action Required:** < 85%

**Business Rationale:**
- High-quality data is essential for decision-making confidence
- Automated data quality checks catch errors early
- Pass rate trends indicate data quality improvement or degradation
- Low pass rate may indicate:
  - Instrumentation issues
  - Data entry errors
  - Process anomalies
  - System integration problems

**Types of Data Quality Checks:**
- **Range Checks:** Value within expected bounds (e.g., API gravity 15-45)
- **Relationship Checks:** Logical consistency (e.g., throughput ≤ capacity)
- **Mass Balance:** Inputs ≈ Outputs + Inventory Change
- **Historical Comparison:** Value within ±2σ of historical average
- **Cross-Validation:** Same measurement from multiple sources agrees

**Data Source:**
- `fact_data_quality_checks.pass_fail`

**Calculation Frequency:** Daily

**Response Actions (< 90% pass rate):**
1. Review failed checks by entity type and rule
2. Investigate most common failure types
3. Calibrate instruments if range checks failing
4. Review data entry procedures if manual data
5. Update validation rules if too strict/loose

---

## Unit-Specific KPIs

### 7.1 FCC Conversion Efficiency

**Definition:** The percentage of FCC feed converted to light products (dry gas, LPG, gasoline).

**Formula:**
```
FCC Conversion % = (Light Products / Feed) × 100
```

**Components:**
- **Light Products:** Dry gas + LPG (C3/C4) + Gasoline (C5-221°C)
- **Feed:** Vacuum gas oil or other FCC feedstock

**Target Benchmark:**
- **On Target:** 72-78%
- **Below Target:** < 72% (investigate catalyst activity)
- **Above Target:** > 78% (check for over-cracking, coke yield)

**Business Rationale:**
- FCC is the heart of gasoline production in most refineries
- Conversion measures how effectively heavy oil is cracked to light products
- Catalyst age, activity, and formulation drive conversion
- Operating conditions (temperature, catalyst-to-oil ratio) impact conversion

**Factors Affecting Conversion:**
- **Catalyst age and activity** (major factor)
- **Feedstock quality** (lighter feeds convert more easily)
- **Reactor temperature** (higher = more conversion)
- **Catalyst-to-oil ratio** (higher = more conversion)
- **Regenerator conditions** (catalyst regeneration effectiveness)

**Investigation Triggers:**
- **< 72%:** Check catalyst age, activity, feedstock quality
- **> 78%:** Verify coke yield not excessive, check product quality
- **Sudden 3%+ drop:** Catalyst deactivation, feed quality change, or unit issue

**Data Source:**
- `fact_unit_production` (WHERE unit_type = 'FCC')

**Calculation Frequency:** Daily

---

### 7.2 Reformer Octane Gain

**Definition:** The increase in octane number from reformer feed (naphtha) to reformate product.

**Formula:**
```
Octane Gain = Reformate Octane - Feed Octane
```

**Typical Values:**
- **Feed Octane:** 50-60 (RON)
- **Reformate Octane:** 95-102 (RON)
- **Typical Gain:** 40-50 octane numbers

**Business Rationale:**
- Reformer produces high-octane gasoline blending component
- Higher octane gain = more valuable product
- Trade-off between octane level and reformer severity (and H2 production)

---

### 7.3 Hydrocracker Conversion

**Definition:** The percentage of heavy feedstock converted to lighter, more valuable products.

**Formula:**
```
HC Conversion % = ((Feed - Unconverted Oil) / Feed) × 100
```

**Target Benchmark:** 60-80% (depends on mode: mild vs. full conversion)

---

## Trending & Volatility KPIs

### 8.1 7-Day Moving Average

**Definition:** The average value of a metric over the most recent 7 days.

**Formula:**
```
7-Day MA = Σ(Daily Values for Last 7 Days) / 7
```

**Purpose:**
- Smooth out daily volatility
- Reveal short-term trends
- Useful for operational management

**Common Metrics with 7-Day MA:**
- Gasoline yield %
- Energy Intensity Index (EII)
- Capacity utilization %
- Data quality pass rate %

---

### 8.2 30-Day Moving Average

**Definition:** The average value of a metric over the most recent 30 days.

**Formula:**
```
30-Day MA = Σ(Daily Values for Last 30 Days) / 30
```

**Purpose:**
- Reveal longer-term trends
- Filter out short-term noise
- Useful for strategic planning

---

### 8.3 Throughput Volatility

**Definition:** The standard deviation of daily throughput within a month, measuring operational stability.

**Formula:**
```
Throughput Volatility = √(Σ(Throughputᵢ - Avg Throughput)² / (n-1))
```

**Interpretation:**
- **Low volatility:** Stable, consistent operations
- **High volatility:** Unstable operations, frequent rate changes
- Planned turnarounds increase volatility
- Unplanned upsets increase volatility

**Target:** Minimize volatility for stable, predictable operations

**Data Source:**
- `fact_daily_kpi.total_throughput_bbl`

**Calculation Frequency:** Monthly

---

### 8.4 Month-over-Month Change Percentage

**Definition:** The percentage change in a metric from one month to the next.

**Formula:**
```
MoM Change % = ((Current Month - Previous Month) / Previous Month) × 100
```

**Purpose:**
- Track performance trends
- Identify improvement or degradation
- Set context for executive dashboards

**Investigation Threshold:** > 10% change warrants explanation

**Common Metrics with MoM Tracking:**
- Throughput
- Gasoline yield
- Energy intensity
- Reliability factor

---

## Summary - Top 10 Refinery KPIs

| Rank | KPI | Target | Calculation Frequency |
|------|-----|--------|----------------------|
| 1 | **Gasoline Yield %** | 45-55% | Daily |
| 2 | **Energy Intensity Index (EII)** | < 0.70 MMBtu/bbl | Daily |
| 3 | **Capacity Utilization %** | > 90% | Daily |
| 4 | **Reliability Factor %** | > 95% | Daily |
| 5 | **High-Value Product %** | > 75% | Daily |
| 6 | **Unaccounted For Loss (UFL) %** | < 0.5% | Daily |
| 7 | **FCC Conversion %** | 72-78% | Daily |
| 8 | **Data Quality Pass Rate %** | > 95% | Daily |
| 9 | **Unplanned Downtime** | < 2% annually | Daily tracking |
| 10 | **Yield Gap %** | < 5% | Weekly |

---

## KPI Dashboard Recommendations

### **Executive Dashboard** (Daily Refresh)
- Throughput
- Gasoline Yield %
- Energy Intensity Index
- Capacity Utilization %
- Reliability Factor %
- MoM Change %

### **Operations Dashboard** (Real-time / Hourly)
- Unit-level utilization
- Downtime events
- Process upsets
- Data quality alerts

### **Optimization Dashboard** (Daily / Weekly)
- Yield gap analysis
- FCC conversion trends
- Product slate optimization
- Energy efficiency opportunities

### **Reliability Dashboard** (Daily / Monthly)
- Planned vs. unplanned downtime
- Year-to-date reliability
- Equipment failure modes
- Maintenance effectiveness

---

## Glossary

- **bbl:** Barrel (42 U.S. gallons)
- **MMBtu:** Million British Thermal Units (energy)
- **RON:** Research Octane Number
- **FCC:** Fluid Catalytic Cracker
- **CDU:** Crude Distillation Unit
- **VDU:** Vacuum Distillation Unit
- **UFL:** Unaccounted For Loss
- **EII:** Energy Intensity Index
- **KPI:** Key Performance Indicator
- **MoM:** Month-over-Month

---

## Document Control

**Prepared By:** Phase 8 Implementation Team  
**Approved By:** Operations Management  
**Review Frequency:** Annually or when major process changes occur  
**Next Review Date:** Annual review cycle

**Revision History:**
- Version 1.0 (Phase 8): Initial comprehensive KPI documentation

---

**END OF KPI DEFINITIONS DOCUMENT**
