# Mass Balance Principles in Oil Refining

## Overview

Mass balance is the cornerstone of refinery accounting, operations optimization, and regulatory compliance. This document covers fundamental principles, calculation methods, reconciliation techniques, and troubleshooting common variance issues.

## Fundamental Principles

### Conservation of Mass

The **Law of Conservation of Mass** states that matter cannot be created or destroyed in chemical and physical processes.

**Refinery Application**:
```
Total Mass IN = Total Mass OUT + Accumulation + Losses
```

Where:
- **Mass IN**: Crude oil, purchased intermediates, hydrogen
- **Mass OUT**: Products shipped, fuel consumed, emissions
- **Accumulation**: Inventory change (tanks, process units, pipelines)
- **Losses**: Evaporation, leaks, measurement errors, unaccounted-for

### Volume vs. Mass Balance

**Volume Balance** (operational view):
```
Volume IN ≠ Volume OUT
```
Volume changes through processing due to:
- Density changes (cracking reactions produce lighter products)
- Temperature differences (expansion/contraction)
- Measurement at different conditions

**Mass Balance** (accounting view):
```
Mass IN ≈ Mass OUT (within measurement error)
```
Mass is conserved, making it the preferred basis for accountability.

### Volume Gain/Loss in Processing

Typical refinery processing creates **volume gain** of 3-7%:

**Example** (100,000 BPD crude input):
- Crude receipts: 100,000 bbl/day @ 0.87 SG = 1,187 tons/day
- Products shipped: 105,000 bbl/day @ 0.78 SG = 1,118 tons/day
- Fuel consumed: 6,000 bbl/day @ 0.75 SG = 61 tons/day
- Losses: 0.5% = 6 tons/day
- Inventory change: +2 tons/day

**Volume balance**: 105,000 + 6,000 = 111,000 bbl out vs. 100,000 bbl in = **11% volume gain**
**Mass balance**: 1,118 + 61 + 6 + 2 = 1,187 tons (balanced!)

The volume gain occurs because:
- Cracking reactions split large molecules into smaller ones
- Lighter molecules have lower density (higher API gravity)
- Same mass occupies more volume at lower density

## Refinery-Wide Mass Balance

### Input Streams

#### Crude Oil Receipts
- **Sources**: Pipeline, marine tanker, rail, truck
- **Measurement**: Tank gauging (before/after delivery)
- **Temperature correction**: Observed volume → standard volume (60°F)
- **Weight calculation**: Volume × specific gravity × 0.1364 = tons

#### Purchased Intermediates
- **Examples**: Butane for blending, ethanol, MTBE, isobutane for alkylation
- **Measurement**: Typically by weight or temperature-corrected volume
- **Quality**: Specification verification at receipt

#### Hydrogen (If Purchased)
- **Measurement**: Mass flow meter or volume with P/T correction
- **Quality**: Purity (typically >99.9%)

### Output Streams

#### Products Shipped
- **Gasoline grades**: Regular 87, Mid-grade 89, Premium 91/93
- **Diesel grades**: ULSD #2, Off-road diesel, Winter diesel
- **Jet fuel**: Jet A, Jet A-1
- **Other products**: Propane, asphalt, petroleum coke, sulfur

**Measurement**:
- Tank gauging before/after loading
- Temperature correction to 60°F
- Weight calculation from volume and density

#### Refinery Fuel Gas
Products consumed internally as fuel for process heaters:
- **Sources**: Light ends from distillation, H₂-rich gas from reformer, FCC dry gas
- **Measurement**: Orifice plate flow meters with P/T compensation
- **Typical rate**: 5-7% of crude input (mass basis)

#### Emissions and Venting
- **Flare**: Safety releases, startup/shutdown venting
- **Vent stacks**: Equipment purging, tank breathing
- **Fugitive emissions**: Leaks from valves, seals
- **Typical**: 0.1-0.3% of crude input

### Inventory Change

#### Tank Inventory
Measured daily via:
- **Automatic tank gauging**: Continuous level monitoring
- **Manual gauging**: Daily measurements for custody transfer tanks
- **Temperature**: Representative tank temperature
- **Water draw-off**: Free water removed and measured

**Calculation**:
```
Opening Inventory (Day 1 8 AM) - Closing Inventory (Day 2 8 AM) 
= Net Inventory Change
```

**Reporting**:
- Positive change = Inventory buildup (subtracted from outputs)
- Negative change = Inventory drawdown (added to outputs)

#### In-Process Inventory
Material in pipes, vessels, columns:
- **Estimate**: Typically small (0.1-0.5% of total inventory)
- **Assumption**: Constant from day to day
- **Turnaround impact**: Major change during shutdown/startup

### Losses and Unaccounted For

#### Measured Losses
1. **Evaporation**: Tank breathing, loading operations
2. **Flare**: Measured via flow meters
3. **Wastewater**: Trace hydrocarbons in effluent
4. **Coke production**: Petroleum coke from coker unit (measured by weight)

#### Unmeasured Losses (Estimated)
1. **Fugitive emissions**: Statistical estimate based on component count
2. **Metering errors**: Systematic measurement biases
3. **Sampling losses**: Small losses during quality sampling
4. **Calculation errors**: Rounding, API gravity approximations

#### Target: <1% Unaccounted For

Acceptable loss range:
- **Excellent**: <0.5% of crude input
- **Good**: 0.5-1.0%
- **Fair**: 1.0-2.0%
- **Poor**: >2.0% (investigate)

## Unit-Level Mass Balance

### Crude Distillation Unit (CDU)

**Inputs**:
- Crude oil feed (single or blend of multiple crudes)
- Steam (stripping steam, desalter wash water)

**Outputs**:
- Light naphtha
- Heavy naphtha
- Kerosene
- Light gas oil
- Heavy gas oil
- Atmospheric residue
- Light ends (C3, C4, fuel gas)
- Wastewater (desalter brine)

**Mass Balance Formula**:
```
Crude Feed + Steam = Σ(Products) + Desalter Water + Losses
```

**Typical Yields** (Light sweet crude, 35° API):
| Stream | Yield (wt%) | API Gravity |
|--------|-------------|-------------|
| Fuel Gas | 2% | - |
| Propane/Butane | 3% | - |
| Light Naphtha | 8% | 70° |
| Heavy Naphtha | 12% | 55° |
| Kerosene | 10% | 45° |
| Light Gas Oil | 12% | 35° |
| Heavy Gas Oil | 15% | 28° |
| Atm. Residue | 38% | 18° |

**Key Measurements**:
- Feed rate: Orifice flow meter with density compensation
- Side draw rates: Individual flow meters on each product
- Temperature profile: Verify separation efficiency
- Bottom level: Control residue outlet flow

### Fluid Catalytic Cracking (FCC) Unit

**Inputs**:
- Vacuum gas oil (VGO) or heavy gas oil feed
- Fresh catalyst makeup

**Outputs**:
- Fuel gas (C1, C2)
- LPG (C3, C4)
- FCC gasoline
- Light cycle oil (LCO)
- Clarified slurry oil (CSO)
- Coke (burned in regenerator, exits as CO₂)
- Spent catalyst (withdrawn for disposal)

**Mass Balance Equations**:

**Reactor Section**:
```
Feed + Catalyst (hot) = Products + Coked Catalyst
```

**Regenerator Section**:
```
Coked Catalyst + Air = Regenerated Catalyst + CO₂ + CO + NOx
```

**Overall**:
```
Feed + Air + Fresh Catalyst = Liquid Products + Fuel Gas + CO₂ + Spent Catalyst
```

**Typical Yields** (VGO feed, 25° API):
| Stream | Yield (wt%) | API Gravity |
|--------|-------------|-------------|
| Fuel Gas | 4% | - |
| Propane | 5% | - |
| Butanes | 10% | - |
| FCC Gasoline | 48% | 58° |
| Light Cycle Oil | 18% | 22° |
| Slurry Oil | 8% | 10° |
| Coke | 5% | - |
| Loss (to CO₂) | 2% | - |

**Closure Considerations**:
- **Coke to CO₂**: Coke on catalyst is combusted (leaves as gas)
- **Catalyst fines**: Lost to atmosphere via stack (small but measurable)
- **CO slip**: Incomplete combustion (CO vs. CO₂ ratio affects heat balance)

### Hydrocracker

**Inputs**:
- Vacuum gas oil or deasphalted oil feed
- Hydrogen (high purity, 1500-2500 SCF/bbl)

**Outputs**:
- Light naphtha
- Heavy naphtha
- Jet fuel / Diesel
- Unconverted oil (recycled or sent to other units)
- Light ends (C1-C4)
- H₂S and NH₃ (to amine treating and sulfur recovery)

**Mass Balance Formula**:
```
Feed + Hydrogen = Σ(Liquid Products) + Light Gases + H₂S + NH₃ + Water
```

**Hydrogen Accounting**:
- **Consumed**: Incorporated into products (hydrogenation reactions)
- **Typical consumption**: 1500-2500 SCF H₂ per barrel of feed
- **Measurement**: Makeup hydrogen flow meter (mass or volumetric with P/T correction)

**Net Weight Increase**:
Hydrocracker is unique in that **mass out > mass in** due to hydrogen addition:
```
Example:
- Feed: 50,000 bbl/day @ 0.92 SG = 626 tons/day
- H₂ consumed: 2000 SCF/bbl × 50,000 bbl = 100 million SCF/day
  = 100M SCF × 0.0052 lb/SCF / 2000 lb/ton = 260 tons/day H₂
- Total input: 626 + 260 = 886 tons/day

Products: 
- Naphtha + Diesel: ~53,000 bbl/day @ 0.80 SG = 579 tons/day
- Light ends: 6,000 bbl/day @ 0.56 SG = 46 tons/day
- Unconverted: 10,000 bbl/day @ 0.91 SG = 124 tons/day
- H₂S/NH₃/H₂O: ~135 tons/day
- Total output: 579 + 46 + 124 + 135 = 884 tons/day

Balance: 886 in ≈ 884 out (0.2% variance – excellent)
```

### Catalytic Reformer

**Inputs**:
- Heavy naphtha feed (180-380°F boiling range)
- Hydrogen recycle

**Outputs**:
- Reformate (high-octane gasoline blendstock)
- Hydrogen (net producer: 1500-2500 SCF H₂ per barrel feed)
- Light ends (C1-C4)

**Mass Balance Formula**:
```
Naphtha Feed = Reformate + Hydrogen (net) + Light Ends
```

**Hydrogen Accounting**:
- **Produced**: Dehydrogenation of naphthenes to aromatics releases H₂
- **Consumed**: Hydrocracking side reactions
- **Net production**: 1500-2500 SCF per barrel (major refinery H₂ source)

**Net Weight Loss**:
Reformer loses mass to hydrogen production:
```
Example:
- Feed: 40,000 bbl/day @ 0.75 SG = 409 tons/day
- Reformate: 37,000 bbl/day @ 0.74 SG = 374 tons/day (87% vol yield, 91% wt yield)
- Light ends: 2,000 bbl/day @ 0.56 SG = 15 tons/day
- H₂ (net): 2000 SCF/bbl × 40,000 = 80M SCF/day = 208 tons H₂
  
Balance: 409 tons in = 374 + 15 + 20 = 409 tons out ✓
```

## Reconciliation Methods

### Daily Reconciliation

**Purpose**: Identify measurement errors and process upsets quickly.

**Process**:
1. Collect all meter readings at consistent time (e.g., 8 AM daily)
2. Calculate temperature-corrected volumes
3. Convert to mass using current densities
4. Compare inputs vs. outputs + inventory change
5. Calculate variance percentage

**Acceptable Daily Variance**: ±2% (wider tolerance due to measurement timing differences)

**Red Flags**:
- Consistent directional bias (always positive or always negative)
- Sudden large variance (>5%) without known cause
- Inventory change doesn't match known activities

### Weekly Reconciliation

**Purpose**: Smooth out daily variations and identify systematic issues.

**Process**:
1. Sum daily balances for the week
2. Recalculate using more accurate composite density (lab-tested samples)
3. Reconcile major inventory movements (tank cleanings, line fills, etc.)
4. Adjust for known measurement biases

**Acceptable Weekly Variance**: ±1%

### Monthly Reconciliation

**Purpose**: Official accounting, regulatory reporting, financial close.

**Process**:
1. Close out month at specific cutoff time
2. Perform physical inventory count (manual gauging of all active tanks)
3. Reconcile all receipts and shipments (Bill of Lading validation)
4. Investigate and document all variances >0.5%
5. Make accounting adjustments if necessary

**Acceptable Monthly Variance**: ±0.5% (most rigorous standard)

**Regulatory Reporting**:
- **EPA**: Emissions reporting
- **EIA**: Petroleum supply monthly (Forms EIA-810, EIA-811)
- **State agencies**: Tank farm reporting, groundwater monitoring

## Variance Troubleshooting

### Common Causes of Variance

#### 1. Measurement Errors

**Tank Gauging Issues**:
- **Incorrect tank tables**: Using wrong calibration chart
- **Water level not accounted for**: Free water measured as product
- **Temperature stratification**: Single temp probe not representative
- **Gauge float sticking**: Incorrect level reading

**Example**:
```
Tank with 100,000 bbl of diesel:
- Actual temperature: 65°F
- Measured temperature: 70°F (probe in warm layer near top)
- VCF difference: ~0.003
- Resulting error: 100,000 × 0.003 = 300 barrels (~$15,000 at $50/bbl)
```

**Flow Meter Issues**:
- **Calibration drift**: Meters out of calibration
- **Air/vapor in liquid**: Gas bubbles inflate volumetric readings
- **Density compensation error**: Wrong gravity setting
- **Meter bypass open**: Flow not measured

#### 2. Timing Differences

**Pipeline Inventory**:
- Crude receipts recorded when leaving supplier
- Product shipments recorded when leaving refinery
- Material in transit creates apparent shortage/surplus

**Solution**: Track pipeline linepack and adjust for known transit times.

#### 3. Process Upsets

**Examples**:
- Unit shutdown/startup: Inventory held up in process equipment
- Temperature excursions: Vapor losses increase
- Pressure relief: Material sent to flare

**Solution**: Document operational events and correlate with variance timing.

#### 4. Leaks and Spills

**Tank leaks**:
- Slow seep from bottom seal (0.1-1 bbl/day)
- Detection: Unexplained inventory loss from specific tank

**Pipeline leaks**:
- Corrosion failures
- Detection: Unbalanced delivery vs. receipt

**Solution**: Implement leak detection programs, regular inspections.

### Investigation Procedure

**Step 1: Verify Calculation**
- Re-check all data entry
- Verify formulas and conversion factors
- Confirm proper tank tables and API gravities used

**Step 2: Meter Validation**
- Review meter calibration dates
- Check for known meter issues (maintenance logs)
- Compare redundant measurements if available

**Step 3: Inventory Reconciliation**
- Re-gauge critical tanks manually
- Verify temperature readings representative
- Check for free water in tanks

**Step 4: Process Analysis**
- Review unit operations for upsets
- Check flare flow meters for relief events
- Correlate with operator logs

**Step 5: Systematic Error Search**
- Look for consistent patterns (time of day, specific tank, etc.)
- Compare current variance to historical trends
- Review recent procedure changes

### Documentation

**Variance Report** should include:
1. **Magnitude**: % variance and absolute quantity
2. **Direction**: Shortage or surplus
3. **Affected streams**: Which products/units show variance
4. **Investigation summary**: Steps taken, findings
5. **Root cause**: Identified or suspected cause
6. **Corrective actions**: Implemented or planned fixes
7. **Financial impact**: Value of unaccounted material

## Advanced Topics

### Statistical Process Control

Apply control charts to mass balance variance:
- **Center line**: Target variance = 0%
- **Control limits**: ±2 or 3 standard deviations
- **Out of control signals**: 
  - Single point outside control limits
  - 7 consecutive points on one side of center
  - Increasing trend over time

### Refinery Linear Program (LP) Integration

Mass balance data feeds optimization models:
- **Crude selection**: Optimize crude slate based on yields
- **Operating mode**: Maximize gasoline vs. distillate depending on margins
- **Hydrogen balance**: Ensure adequate H₂ for hydroprocessing units
- **Fuel gas balance**: Match refinery fuel with captive supply

### Sustainability Reporting

Mass balance enables carbon accounting:
- **Crude carbon content**: 83-87% carbon by weight
- **CO₂ from combustion**: C + O₂ → CO₂ (3.67 tons CO₂ per ton C)
- **Scope 1 emissions**: Refinery fuel consumption + process emissions
- **Scope 3 emissions**: Product sold (burned by end users)

**Example**:
```
Refinery processing 100,000 BPD crude (85% carbon):
- Crude input: 100,000 bbl × 0.87 SG × 0.1364 = 1,186 tons/day
- Carbon input: 1,186 × 0.85 = 1,008 tons C/day
- Potential CO₂: 1,008 × 3.67 = 3,699 tons CO₂/day

Distribution:
- Refinery fuel consumed (Scope 1): 6% = 222 tons CO₂/day
- Products sold (Scope 3): 94% = 3,477 tons CO₂/day
```

## Data Warehouse Implementation

The Oil Refinery Data Warehouse will use mass balance principles to:

### Phase 2-3: Fact Tables
- **fact_daily_unit_production**: Record inputs, outputs, inventory changes by unit
- **fact_crude_runs**: Crude receipts with blend ratios
- **fact_product_shipments**: Products shipped with quality

### Phase 4: Aggregations
- **Daily refinery-wide balance**: Roll-up all units
- **Product balance**: Track specific product through blending
- **Hydrogen balance**: Consumption vs. production

### Phase 5: Data Quality Validation
- **Variance tests**: Flag daily variances >2%, monthly >0.5%
- **Trend analysis**: Identify systematic biases
- **Reconciliation reporting**: Automated variance investigation triggers

### Phase 6: Analytics
- **Predictive variance**: Machine learning to predict expected variance
- **Anomaly detection**: Statistical outlier identification
- **Root cause analysis**: Correlate variance with operational events

## References

### Industry Standards
- **API MPMS Chapter 12**: Calculation of Petroleum Quantities
- **API MPMS Chapter 14**: Natural Gas Fluids Measurement
- **ISO 4267**: Petroleum and liquid petroleum products — Calculation of oil quantities

### Technical References
- Nelson, W.L. (1958). *Petroleum Refinery Engineering*, 4th Ed. (classic text on material balance)
- Gary, J.H., et al. (2007). *Petroleum Refining: Technology and Economics* (Chapter 3: Material Balance)

### Regulatory
- **EPA**: Petroleum Refinery Sector Risk and Technology Review (RTR)
- **EIA Forms**: Instructions for Forms EIA-810, 811, 812, 816, 817, 820
- **FERC**: Uniform System of Accounts for Natural Gas Companies

### Accounting Standards
- **FASB ASC 930**: Extractive Activities — Mining
- **IASB IFRS 6**: Exploration for and Evaluation of Mineral Resources
