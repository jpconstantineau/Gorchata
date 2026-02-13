# Oil Refinery Data Transformation and Warehousing

## Overview

This example demonstrates a comprehensive **data warehouse implementation** for oil refinery operations, showcasing Gorchata's capabilities in managing complex petroleum refining data analytics. The project models:

- **7 dimension tables** (date, unit, product, crude_grade, stream, location, catalyst_cycle)
- **Production tracking** across multiple process units with mass balance validation
- **Crude slate optimization** modeling 4-5 different crude grades
- **Product quality management** with seasonal specifications
- **Catalyst lifecycle tracking** correlating age with efficiency
- **Downtime analysis** distinguishing planned vs unplanned events
- **Multi-level aggregations** from daily operations to monthly summaries

The example provides a foundation for **refinery operations analytics**, enabling analysis of throughput, yields, efficiency, and optimization opportunities across the entire crude-to-products value chain.

## Business Context

### Oil Refinery Operations

An **oil refinery** is a complex industrial facility that transforms crude oil into valuable petroleum products through physical and chemical processes. Modern refineries operate 24/7 with multiple interdependent process units working in concert to maximize product value while meeting stringent quality specifications and environmental requirements.

**Key Characteristics:**
- **Capital intensive**: Multi-billion dollar facilities with 20-40 year lifespans
- **Continuous operations**: 24/7/365 operation with planned maintenance turnarounds every 3-5 years
- **Complex chemistry**: Hundreds of simultaneous reactions across multiple units
- **Mass balance critical**: Every molecule of feedstock must be accounted for in products
- **Quality constraints**: Strict product specifications driven by regulations and market requirements
- **Economic optimization**: Daily decisions on crude selection, operating modes, and product slate

### Refinery Process Units

#### 1. **Crude Distillation Unit (CDU)**
The heart of the refinery, the CDU is the first major processing step.

**Function**: Separates crude oil into fractions based on boiling point ranges through atmospheric distillation at ~350°F maximum temperature.

**Typical Products/Streams**:
- **Light Naphtha**: <180°F boiling range, for gasoline blending or reformer feed
- **Heavy Naphtha**: 180-380°F, for reformer feed
- **Kerosene**: 380-520°F, for jet fuel or diesel blending
- **Light Gas Oil (LGO)**: 520-650°F, for diesel blending
- **Heavy Gas Oil (HGO)**: 650-750°F, for FCC feed or hydrocracker feed
- **Atmospheric Residue**: 750°F+, sent to VDU for further processing

**Typical Capacity**: 100,000-400,000 barrels per day (BPD)

**Key Operations**:
- Multiple distillation trays (30-50) create temperature gradient
- Side draws extract specific boiling range cuts
- Reflux controls separation efficiency
- Desalter upstream removes salts/sediments from crude

#### 2. **Vacuum Distillation Unit (VDU)**
Processes the heaviest stream from the CDU under vacuum to avoid thermal cracking.

**Function**: Further separates atmospheric residue into lighter fractions under reduced pressure (~25-40 mmHg absolute), allowing distillation at lower temperatures.

**Typical Products/Streams**:
- **Light Vacuum Gas Oil (LVGO)**: Feed for FCC or hydrocracker
- **Heavy Vacuum Gas Oil (HVGO)**: Feed for FCC or hydrocracker
- **Vacuum Residue (VR)**: Sent to coker, asphalt production, or fuel oil

**Typical Capacity**: 40-60% of CDU capacity

**Key Operations**:
- Operates at 5-10% of atmospheric pressure
- Steam ejectors create vacuum
- Multiple cuts optimize downstream unit feeds

#### 3. **Fluid Catalytic Cracking (FCC)**
Converts heavy gas oils into gasoline and lighter products using catalyzed high-temperature cracking.

**Function**: Breaks large hydrocarbon molecules into smaller, more valuable molecules through catalytic cracking at ~900-1000°F.

**Typical Products**:
- **FCC Gasoline**: High octane gasoline blending component (45-50% yield)
- **Light Cycle Oil (LCO)**: Diesel blending component (15-20% yield)
- **Clarified Slurry Oil (CSO)**: Heavy fuel oil component (5-10% yield)
- **Light Gases**: Propylene, butylenes for alkylation (15-20% yield)
- **Coke**: Burned in regenerator to reheat catalyst (3-5% yield)

**Typical Capacity**: 50,000-150,000 BPD

**Key Operations**:
- Catalyst circulates between reactor and regenerator
- Catalyst activity degrades over time (2-4 week cycles)
- Air burns coke off catalyst in regenerator
- Heat balance between reactor and regenerator critical

#### 4. **Hydrocracker**
Converts heavy oils into lighter products using high-pressure hydrogen and catalyst.

**Function**: Saturates and cracks heavy molecules under hydrogen pressure (1200-2500 psi) and moderate temperature (650-800°F).

**Typical Products**:
- **Naphtha**: For reformer or gasoline blending (30-40% yield)
- **Diesel/Jet Fuel**: High-quality middle distillates (40-50% yield)
- **Unconverted Oil**: Recycled or sent to other units (10-20%)

**Typical Capacity**: 30,000-100,000 BPD

**Key Operations**:
- Two-stage process: hydrogenation then cracking
- Consumes large quantities of hydrogen (1500-2500 SCF/bbl)
- Catalyst life: 2-4 years between regenerations
- Produces ultra-low sulfur products

#### 5. **Catalytic Reformer**
Converts low-octane naphtha into high-octane gasoline blending components and produces hydrogen.

**Function**: Restructures naphtha molecules (dehydrogenation, isomerization, cyclization) at 850-950°F and 100-500 psi to increase octane.

**Typical Products**:
- **Reformate**: High-octane gasoline component (85-90% yield, 95-100 octane)
- **Hydrogen**: Byproduct consumed by hydrotreaters and hydrocracker (1000-2000 SCF/bbl)
- **Light Gases**: Propane, butanes (5-10% yield)

**Typical Capacity**: 20,000-80,000 BPD

**Key Operations**:
- Multiple reactors in series (typically 3-4)
- Catalyst deactivates from coke formation
- Continuous or cyclic regeneration modes
- Critical hydrogen source for refinery

#### 6. **Hydrotreaters (Multiple Types)**
Remove sulfur, nitrogen, and metals from various streams using hydrogen at moderate pressure.

**Types**:
- **Naphtha Hydrotreater**: Desulfurization for reformer feed or gasoline
- **Diesel Hydrotreater**: Produces ultra-low sulfur diesel (ULSD <15 ppm sulfur)
- **Gas Oil Hydrotreater**: Pretreatment for FCC or hydrocracker feed

**Function**: Catalytic reaction with hydrogen (300-800 psi, 600-750°F) converts sulfur compounds to H₂S, which is removed and sent to sulfur recovery.

**Typical Products**: Same boiling range as feed but with reduced sulfur, improved stability

**Typical Capacity**: 20,000-100,000 BPD per unit (most refineries have 3-6 hydrotreaters)

**Key Operations**:
- Hydrogen consumption: 200-800 SCF/bbl depending on sulfur content
- Catalyst life: 1-3 years
- H₂S scrubbing and sulfur recovery downstream

#### 7. **Alkylation Unit**
Combines light olefins with isobutane to create high-octane gasoline blending component.

**Function**: Acid-catalyzed reaction (sulfuric or hydrofluoric acid) at low temperature (30-70°F) combines C3-C5 olefins from FCC with isobutane.

**Typical Products**:
- **Alkylate**: Premium gasoline blending component (92-96 octane, zero sulfur, low vapor pressure)

**Typical Capacity**: 10,000-30,000 BPD

**Key Operations**:
- Requires refrigeration system
- Acid catalyst requires makeup and regeneration
- Critical for meeting gasoline specifications
- High octane without aromatics

#### 8. **Delayed Coker**
Thermally cracks vacuum residue into lighter products and solid petroleum coke.

**Function**: Severe thermal cracking at >900°F under pressure converts residue into lighter hydrocarbons and solid coke in large drums.

**Typical Products**:
- **Coker Naphtha**: Unstable, requires hydrotreating (15-25% yield)
- **Coker Gas Oil**: FCC feed or diesel blending after hydrotreating (35-45% yield)
- **Petroleum Coke**: Fuel-grade or anode-grade coke (25-35% yield)
- **Light Gases**: Fuel gas (5-10% yield)

**Typical Capacity**: 20,000-60,000 BPD

**Key Operations**:
- Batch process using two drums alternately
- 16-24 hour coking cycle per drum
- Hydraulic decoking to remove solid coke
- Environmentally challenging (air emissions)

### Mass Balance Fundamentals

**Mass balance** is the foundation of refinery accounting and optimization. Based on the law of conservation of mass, every molecule entering the refinery must be accounted for in products, losses, or inventory changes.

**Basic Principle**:
```
Inputs = Outputs + Losses + Inventory Change
```

**Refinery-Wide Mass Balance**:
```
Crude Input + Blendstocks Received = Products Shipped + Fuel Consumed + Losses + Inventory Change
```

**Typical Refinery Yields** (100,000 BPD light sweet crude):
- **Gasoline**: 45-50% (45,000-50,000 BPD)
- **Diesel/Jet**: 25-30% (25,000-30,000 BPD)
- **Fuel Oil**: 5-10% (5,000-10,000 BPD)
- **Asphalt/Heavy Products**: 5-8% (5,000-8,000 BPD)
- **Light Gases/Fuel**: 5-8% (5,000-8,000 BPD)
- **Refinery Fuel**: 5-7% (5,000-7,000 BPD)
- **Loss**: 1-2% (1,000-2,000 BPD)

**Key Mass Balance Challenges**:
1. **Measurement accuracy**: Tank gauging, flow meters, temperature corrections
2. **Volume vs. mass**: Must convert using density/API gravity
3. **Temperature corrections**: Standard conditions (60°F) for accounting
4. **Timing differences**: Continuous operations vs. batch accounting periods
5. **Multiple units**: Reconciling balances across interconnected process units

**Quality-Adjusted Balance**:
Not just volume/mass, but also property balancing:
- **Sulfur balance**: Track sulfur through process (regulatory requirement)
- **Octane balance**: Manage octane blending in gasoline
- **Cetane balance**: Manage cetane in diesel products
- **Density balance**: Volume expansion/contraction through processing

### Measurement Standards and Conversions

#### API Gravity
Primary density measurement for petroleum:

**Formula**: API Gravity = (141.5 / Specific Gravity at 60°F) - 131.5

**Classifications**:
- **Light Crude**: API > 31.1° (higher value = lighter)
- **Medium Crude**: API 22.3-31.1°
- **Heavy Crude**: API < 22.3°

**Example Crudes**:
- WTI (West Texas Intermediate): 39.6° API, 0.827 SG
- Brent: 38.3° API, 0.835 SG
- Maya (Mexico): 22.0° API, 0.920 SG (heavy sour)
- Dubai: 31.0° API, 0.871 SG
- Mars: 29.0° API, 0.882 SG

**Why It Matters**: 
- Lighter crudes (higher API) yield more gasoline/diesel
- Heavier crudes (lower API) require more complex refining
- Price differential: Light sweet commands premium vs. heavy sour

#### Volume to Weight Conversions
Essential for mass balance calculations.

**Standard Conditions**: 60°F and atmospheric pressure

**Conversion Factor**:
```
Weight (tons) = Volume (barrels) × Specific Gravity × 0.1364
Weight (pounds) = Volume (barrels) × Specific Gravity × 350
```

Where: Specific Gravity = 141.5 / (API + 131.5)

**Temperature Correction**:
Petroleum expands when heated, so volume measurements must be corrected to standard 60°F:

**Volume Correction Factor (VCF)**:
```
Volume at 60°F = Volume at T × VCF(T, API)
```

VCF tables provided by API/ASTM; typical correction ~0.05% per °F

**Example Calculation**:
- 10,000 barrels of 30° API crude at 80°F
- Specific Gravity = 141.5/(30+131.5) = 0.876
- VCF at 80°F for 30° API ≈ 0.989
- Volume at 60°F = 10,000 × 0.989 = 9,890 barrels
- Weight = 9,890 × 0.876 × 0.1364 = 1,181 tons

#### Sulfur Content
Major crude property affecting processing requirements.

**Classifications**:
- **Sweet Crude**: < 0.5% sulfur (easier to refine)
- **Sour Crude**: > 0.5% sulfur (requires more hydrotreating)

**Environmental Impact**:
- Gasoline: < 10 ppm sulfur (US Tier 3 standard)
- Diesel: < 15 ppm sulfur (ULSD standard)
- Marine fuel: < 0.5% sulfur (IMO 2020)

**Processing Implications**:
- High sulfur crude requires extensive hydrotreating
- Consumes more hydrogen
- Produces more H₂S requiring sulfur recovery
- Higher catalyst replacement costs

#### Energy Content
Measured in BTU (British Thermal Units) or heating value.

**Typical Values**:
- Light products (gasoline): 5.2-5.4 million BTU/barrel
- Middle distillates (diesel): 5.8-6.0 million BTU/barrel
- Heavy products (fuel oil): 6.2-6.4 million BTU/barrel

**Refinery Use**:
- Thermal efficiency calculations
- Fuel gas valuation
- Economic optimization

### Crude Slate and Operating Modes

#### Typical Crude Slate (Approved Design Decision)
A refinery processes **4-5 different crude grades** to optimize economics and manage supply:

**Example Slate** (100,000 BPD total):
1. **WTI** (39.6° API, 0.4% S): 30,000 BPD (30%)
2. **Brent** (38.3° API, 0.4% S): 25,000 BPD (25%)
3. **Maya** (22.0° API, 3.3% S): 20,000 BPD (20%)
4. **Dubai** (31.0° API, 2.0% S): 15,000 BPD (15%)
5. **Mars** (29.0° API, 1.9% S): 10,000 BPD (10%)

**Blend Properties**:
- Weighted Average API: 32.5°
- Weighted Average Sulfur: 1.4%

#### Operating Mode Shifts (Approved Design Decision)
Refineries adjust crude mix based on economics and demand:

**Baseline Mode**: Balanced crude slate (above example)

**Light Mode**: Shift to lighter crudes (+10% light, -10% heavy)
- Increases gasoline yield +3-5%
- Reduces fuel oil production -8-10%
- Increases profitability when gasoline margins strong

**Heavy Mode**: Shift to heavier crudes (+10% heavy, -10% light)
- Reduces crude cost (heavy crude discount)
- Requires more secondary processing (FCC, hydrocracker)
- Produces more fuel oil/asphalt

**Operating Mode Impact**:
- **Throughput**: ±5% depending on crude mix (lighter = higher throughput)
- **Yields**: 3-8% shift in gasoline vs. fuel oil
- **Energy Use**: Heavy modes consume more energy (10-15% higher)
- **Catalyst Cycle**: Heavy/sour crudes age catalyst faster (20-30% shorter life)

### Catalyst Lifecycle Management

#### Catalyst Degradation (Approved Design Decision)
Catalysts lose activity over time due to:
- **Coking**: Carbon deposits block active sites
- **Poisoning**: Metals (Ni, V) and sulfur compounds reduce activity
- **Sintering**: High temperatures reduce surface area
- **Attrition**: Physical breakdown in fluid systems (FCC)

#### Catalyst Cycle Stages
Tracking catalyst age enables predictive maintenance and optimization:

**Stage Definitions**:
1. **Fresh**: 0-90 days
   - Highest activity (100% efficiency)
   - May run "hot" initially
   - Learning optimal conditions

2. **Early-Mid**: 91-180 days
   - Stable high performance (95-98% efficiency)
   - Optimal operating window
   - Consistent yields

3. **Mid-Cycle**: 181-365 days
   - Gradual decline (90-95% efficiency)
   - Temperature increases to maintain conversion
   - Monitor regeneration frequency

4. **Late-Mid**: 366-540 days
   - Noticeable decline (85-90% efficiency)
   - Temperature near limits
   - Plan for regeneration/replacement

5. **End-of-Run**: 541+ days
   - Reduced performance (75-85% efficiency)
   - Operating at maximum temperature
   - Regeneration/replacement required soon

**Efficiency Correlation** (Approved Design Decision):
- Fresh catalyst: 100% baseline conversion
- Mid-cycle: 90-95% conversion (requires 20-30°F higher temperature)
- End-of-run: 75-85% conversion (approaching temperature limits)

**Economic Impact**:
- Premature replacement: Lost catalyst value ($1-5 million)
- Extended runs: Reduced yields (1-3% loss = $1-2 million/year)
- Optimal timing: Balance costs with planning turnarounds

### Seasonal Specifications and Product Quality

#### Reid Vapor Pressure (RVP) - Seasonal Gasoline Specs
RVP measures volatility of gasoline (evaporation tendency).

**Regulatory Limits**:
- **Summer Gasoline** (June 1 - September 15): 7.8-9.0 psi RVP
- **Winter Gasoline** (September 16 - May 31): 13.5-15.0 psi RVP

**Why It Matters**:
- High RVP in summer → evaporative emissions (ozone formation)
- High RVP in winter → improved cold-start performance

**Refinery Impact**:
- Summer: Reduce butane blending (butane has 52 psi RVP)
- Winter: Increase butane blending (cheaper, improves cold starts)
- Planning: Build summer-spec inventory in spring, winter-spec in fall

**Blending Example**:
```
Target: 9.0 psi RVP (summer)
Components:
- FCC Gasoline: 7.0 psi RVP (80%)
- Alkylate: 4.5 psi RVP (10%)
- Butane: 52 psi RVP (2%)
- Reformate: 3.5 psi RVP (8%)

Blended RVP = (0.80×7.0 + 0.10×4.5 + 0.02×52 + 0.08×3.5) = 7.33 psi

Need to adjust blend to reach 9.0 psi target
```

#### Cetane Number - Diesel Quality
Measures ignition quality of diesel fuel (higher = better combustion).

**Standards**:
- US Diesel: Minimum 40 cetane
- Premium Diesel: 45-50 cetane
- European Diesel: Minimum 51 cetane

**Refinery Impact**:
- Light cycle oil (from FCC): Low cetane (20-30)
- Hydrocracker diesel: High cetane (50-60)
- Blend optimization to meet specs

#### Pour Point - Cold Temperature Performance
Temperature at which fuel stops flowing.

**Standards**:
- Summer Diesel: 0°F pour point
- Winter Diesel (Arctic): -40°F pour point
- Fuel Oil: +30°F to -10°F depending on grade

**Refinery Impact**:
- Dewaxing units for winter diesel
- Kerosene blending to lower pour point
- Regional product specifications

### Downtime Management

#### Planned Downtime (Approved Design Decision)
Scheduled maintenance essential for safe, reliable operations:

**Types**:
1. **Turnaround**: Major maintenance every 3-5 years (30-60 days)
   - Catalyst replacement
   - Vessel inspection
   - Major equipment overhaul
   - Cost: $10-50 million

2. **Planned Maintenance**: Annual or bi-annual (5-15 days)
   - Routine inspections
   - Minor repairs
   - Catalyst regeneration
   - Cost: $1-5 million

3. **Catalyst Changeout**: Scheduled mid-cycle (3-10 days)
   - Replace spent catalyst
   - Typically 2-4 week notice
   - Cost: $1-3 million

**Planning Considerations**:
- Schedule during low-margin periods
- Coordinate with crude receipts
- Minimize lost production (opportunity cost)
- Pre-stage equipment and materials

#### Unplanned Downtime (Approved Design Decision)
Equipment failures and upsets that cause unexpected shutdowns:

**Types**:
1. **Equipment Failure**: Pumps, compressors, heat exchangers
   - Duration: Hours to days
   - Impact: Unit shutdown or reduced rate

2. **Process Upset**: Operating condition deviation
   - Duration: Hours
   - Impact: Rate reduction, product quality issues

3. **Utility Loss**: Power, steam, cooling water
   - Duration: Hours
   - Impact: Partial or total site shutdown

4. **Safety Incident**: Fires, releases, injuries
   - Duration: Days to weeks
   - Impact: Unit shutdown, regulatory scrutiny

**Tracking Requirements**:
- **Cause code**: Equipment type, failure mode
- **Duration**: Start time, end time, restart time
- **Impact**: Lost production, product quality impact
- **Cost**: Repairs + lost margin + potential fines

**Key Metrics**:
- **Availability**: Actual runtime / Scheduled runtime
- **Reliability**: Mean time between failures (MTBF)
- **Utilization**: Actual throughput / Design capacity

### Project Objectives

This data warehouse implementation will enable:

1. **Production Analytics**
   - Daily, weekly, monthly throughput tracking by unit
   - Yield analysis by crude type and operating mode
   - Mass balance reconciliation and variance analysis

2. **Quality Management**
   - Product specification tracking (RVP, sulfur, octane, cetane)
   - Seasonal specification transitions
   - Off-spec product identification and root cause

3. **Catalyst Management**
   - Lifecycle tracking with performance correlation
   - Efficiency trends and predictive replacement
   - Economic optimization of catalyst cycles

4. **Downtime Analysis**
   - Planned vs. unplanned downtime by unit
   - Root cause categorization
   - Reliability and availability metrics

5. **Crude Slate Optimization**
   - Crude cost vs. yield analysis
   - Optimal slate determination by market conditions
   - Operating mode profitability comparison

6. **Economic Analysis**
   - Margin analysis by product
   - Energy efficiency and consumption
   - Optimization opportunities identification

## Phase 1 Deliverables

### Dimension Tables

#### dim_date
Standard date dimension with calendar and fiscal attributes:
- **Primary Key**: date_key (YYYYMMDD)
- **Attributes**: year, quarter, month, week, day_of_week, day_name, month_name
- **Fiscal Calendar**: fiscal_year, fiscal_quarter
- **Flags**: is_weekend, is_holiday

#### dim_unit
Process unit hierarchy and capacity:
- **Primary Key**: unit_id
- **Hierarchy**: complex_name → unit_type → unit_name
- **Capacity**: capacity_bpd, design_capacity_bpd
- **Types**: CDU, VDU, FCC, Hydrocracker, Reformer, Hydrotreater, Alkylation, Coker
- **Metadata**: commissioned_date, feed_type, product_type

#### dim_product
Product hierarchy with typical properties:
- **Primary Key**: product_id
- **Hierarchy**: product_category → product_type → product_grade
- **Categories**: Light Distillates, Middle Distillates, Heavy Products, Specialty, Intermediate
- **Properties**: api_gravity, sulfur_pct, specific_gravity, flash_point, pour_point
- **Gasoline**: rvp_psi, octane_number
- **Diesel**: cetane_number

#### dim_crude_grade
Crude oil types and properties:
- **Primary Key**: crude_grade_id
- **Grades**: WTI, Brent, Maya, Dubai, Mars, Mixed
- **Properties**: api_gravity, sulfur_pct, specific_gravity, pour_point
- **Classification**: crude_type (Light Sweet, Light Sour, Heavy Sweet, Heavy Sour)
- **Yields**: typical_yield_light_pct, typical_yield_middle_pct, typical_yield_heavy_pct

#### dim_stream
Intermediate refinery streams:
- **Primary Key**: stream_id
- **Types**: Light, Medium, Heavy, Residue, Gas
- **Boiling Range**: boiling_range_min_f, boiling_range_max_f
- **Properties**: typical_api_gravity, typical_sulfur_pct
- **Examples**: Naphtha, Kerosene, Gas Oil, Atmospheric Residue, Light Cycle Oil

#### dim_location
Sources and destinations:
- **Primary Key**: location_id
- **Types**: Pipeline, Marine Terminal, Truck Terminal, Rail Terminal, Storage, Customer
- **Category**: Source, Destination, Both, Internal
- **Geography**: region, state_province, country

#### dim_catalyst_cycle
Catalyst lifecycle stages:
- **Primary Key**: catalyst_cycle_id
- **Stages**: Fresh (0-90 days), Early-Mid (91-180), Mid-Cycle (181-365), Late-Mid (366-540), End-of-Run (541+)
- **Performance**: typical_efficiency_pct, typical_activity_index
- **Age Range**: age_days_min, age_days_max
- **Alert**: requires_attention flag

### Schema Structure

The schema follows Gorchata conventions:
- `version: 2` for schema file format
- Comprehensive `data_tests` for data quality validation
- Referential integrity constraints via `relationships` tests (to be added in Phase 2)
- Rich descriptions for all models and key columns

### Testing

Comprehensive test suite validates:
- ✓ Schema file exists and parses correctly
- ✓ All required dimension tables present
- ✓ Each dimension has required fields
- ✓ Primary key constraints defined (unique + not_null)
- ✓ All models have descriptions

Run tests:
```bash
cd examples/oil_refinery_warehousing
go test -v
```

## Phase 2 Deliverables (✅ COMPLETE)

### Crude Oil Receipts Fact Table

**Implementation Date**: February 12, 2026  
**Status**: ✅ Complete with full TDD validation

Phase 2 implements crude oil receipt tracking with comprehensive petroleum measurement calculations.

#### fact_crude_receipts
Tracks crude oil receipts with volume/weight conversions:
- **Primary Key**: receipt_id
- **Foreign Keys**: 
  - date_key → dim_date
  - crude_grade_id → dim_crude_grade
  - source_location_id → dim_location
- **Measurements**:
  - gross_volume_bbl: Volume before BS&W deduction
  - net_volume_bbl: Volume after BS&W deduction
  - weight_short_tons: Calculated mass
- **Quality Metrics**:
  - observed_api_gravity, observed_temperature_f
  - api_gravity_60f: Temperature-corrected API
  - specific_gravity_60f: Calculated from API
  - bsw_pct: Basic Sediment & Water percentage
  - sulfur_wt_pct: Sulfur content
- **Operational**: receipt_mode (Pipeline, Marine, Truck, Rail)

#### stg_crude_receipts
Staging table for ETL processing (same structure as fact table).

### Conversion Formulas Implemented

All formulas are industry-standard petroleum measurements:

**1. API Gravity to Specific Gravity**
```
SG = 141.5 / (API + 131.5)
```
Example: WTI at 39.6° API = 0.827 SG

**2. Volume to Weight**
```
Weight (short tons) = Volume (bbl) × 0.1364 × SG
```
Example: 10,000 bbl WTI = 1,128 tons

**3. BS&W Deduction**
```
Net Volume = Gross Volume × (1 - BSW% / 100)
```
Example: 100,000 bbl with 0.1% BS&W = 99,900 bbl net

**4. Temperature Correction** (simplified)
```
Correction Factor = 1 - ((T - 60) × 0.0004)
```

### Seed Data

**File**: `seeds/seed_crude_receipts.yml`
- **30 transactions** over 10 days (Feb 1-10, 2026)
- **5 crude grades**: WTI (11), Brent (3), Maya (4), Dubai (5), Mars (7)
- **3 receipt modes**: Pipeline (16), Marine (11), Truck (3)
- **Total volume**: 3.25 million barrels gross
- **Realistic parameters**:
  - Temperatures: 52-88°F
  - BS&W: 0.05-0.45%
  - API gravity: 22-40°

### Transformations

**File**: `transformations/crude_receipts_transformations.sql`
- Complete SQL transformation logic
- Documented petroleum calculation formulas
- Quality checks and validation rules
- Production-ready with extensibility notes

### Testing

**9 new test functions** validating:
- ✓ fact_crude_receipts table structure (14 columns)
- ✓ stg_crude_receipts staging table structure
- ✓ Foreign key relationships (3 FKs)
- ✓ API gravity conversion (5 test cases, ±0.001 tolerance)
- ✓ Volume to weight conversion (4 test cases, ±1.0 ton tolerance)
- ✓ BS&W deduction (4 test cases, exact arithmetic)
- ✓ Temperature correction (4 test cases, ±100 bbl tolerance)

**Test Results**: 20/20 tests passing (100%)

Run Phase 2 tests:
```bash
cd examples/oil_refinery_warehousing
go test -v -run "TestFactCrudeReceipts|TestAPIGravity|TestVolumeToWeight|TestBSW|TestTemperature"
```

### Documentation

- **PHASE_2_SUMMARY.md**: Detailed implementation report with statistics
- **docs/MEASUREMENT_STANDARDS.md**: Petroleum measurement formulas (Phase 1)
- Inline SQL documentation in transformation files

## Next Phases

### Phase 3: Crude Distillation Unit (CDU) Production
### Phase 3: Crude Distillation Unit (CDU) Production
- `fact_cdu_production`: Daily CDU throughput and yields by cut point
- `stg_cdu_operations`: Staging for CDU operational data
- Crude slate tracking and yield analysis
- Cut point optimization metrics

### Phase 4: Product Blending and Shipments
- `fact_product_blends`: Gasoline/diesel blending recipes
- `fact_product_shipments`: Product shipments by grade and destination
- `fact_product_quality`: Quality test results by product and batch
- Blending optimization logic

### Phase 5: Unit Production and Downtime
- `fact_unit_production`: Daily production by unit with mass balance
- `fact_downtime_events`: Downtime analysis with planned vs. unplanned
- `stg_unit_production`: Staging for production data
- `stg_unit_downtime`: Staging for downtime events

### Phase 6: Metrics and Aggregations
- Monthly production summaries
- Yield analysis by crude type
- Catalyst efficiency tracking
- Downtime reliability metrics

### Phase 5: Data Quality Tests
- Mass balance validation (inputs = outputs + losses)
- Quality specification compliance
- Referential integrity across all dimensions
- Temporal sequence validation

### Phase 6: Analytical Models
- Crude slate optimization models
- Operating mode profitability analysis
- Catalyst lifecycle predictions
- Seasonal specification planning

### Phase 7: Advanced Analytics
- Seasonal gasoline RVP compliance modeling
- Crude assay-based yield predictions
- Energy efficiency analysis
- Predictive maintenance indicators

## Documentation

Additional documentation available in [docs/](docs/):
- **MEASUREMENT_STANDARDS.md**: Detailed API gravity, volume/mass conversions, temperature corrections
- **PROCESS_UNITS.md**: Deep dive into each refinery unit operation
- **MASS_BALANCE.md**: Mass balance principles and reconciliation methods
- **CRUDE_PROPERTIES.md**: Crude oil characteristics and selection criteria
- **PRODUCT_SPECIFICATIONS.md**: Market specifications and regulatory requirements
- **CATALYST_MANAGEMENT.md**: Catalyst lifecycle and optimization strategies

## References

### Industry Standards
- **API MPMS**: Manual of Petroleum Measurement Standards (temperature corrections, density calculations)
- **ASTM D4052**: Standard Test Method for Density and Relative Density of Liquids
- **ASTM D86**: Standard Test Method for Distillation of Petroleum Products
- **ASTM D2887**: Standard Test Method for Boiling Range Distribution (Simulated Distillation)

### Refinery Operations
- Gary, J.H., Handwerk, G.E., Kaiser, M.J. (2007). *Petroleum Refining: Technology and Economics*, 5th Edition
- Speight, J.G. (2014). *The Chemistry and Technology of Petroleum*, 5th Edition
- Jones, D.S.J., Pujado, P.R. (2006). *Handbook of Petroleum Processing*

### Crude Oil Properties
- EIA Crude Oil Categories: https://www.eia.gov/petroleum/
- Crude Assay Database: Various industry sources (confidential)

### Environmental Regulations
- EPA Tier 3 Gasoline Standards
- EPA ULSD Requirements (15 ppm sulfur)
- IMO 2020 Marine Fuel Sulfur Regulations

## Gorchata Project Integration

This example follows standard Gorchata project structure:

```bash
# Run the schema validation tests
cd examples/oil_refinery_warehousing
go test -v

# Future phases will add:
# - gorchata_project.yml configuration
# - profiles.yml for database settings
# - seed data for dimensions
# - SQL model implementations
```

## License

This example is part of the Gorchata project and follows the same license terms.
