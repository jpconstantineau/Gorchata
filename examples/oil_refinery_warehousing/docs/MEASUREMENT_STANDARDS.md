# Oil Refinery Measurement Standards

## Overview

This document provides detailed specifications for petroleum measurement standards used in refinery operations, including API gravity calculations, volume-to-mass conversions, temperature corrections, and quality specifications.

## API Gravity

### Definition
API (American Petroleum Institute) gravity is the industry-standard measurement of petroleum density relative to water.

### Formula
```
API Gravity = (141.5 / Specific Gravity at 60°F) - 131.5
```

where Specific Gravity is measured relative to water at 60°F (15.56°C).

### Relationship to Specific Gravity
```
Specific Gravity (60°F/60°F) = 141.5 / (API Gravity + 131.5)
```

### Classifications

#### Crude Oil Classifications
- **Light Crude**: API > 31.1°
  - Example: WTI at 39.6° API
  - Characteristics: Low viscosity, high gasoline yields, easier to refine
  
- **Medium Crude**: API 22.3° to 31.1°
  - Example: Mars at 29.0° API
  - Characteristics: Moderate viscosity, balanced yields
  
- **Heavy Crude**: API < 22.3°
  - Example: Maya at 22.0° API
  - Characteristics: High viscosity, low gasoline yields, requires complex refining

#### Product Classifications
- **Gasoline**: 57-63° API (typical)
- **Diesel/Jet Fuel**: 35-42° API (typical)
- **Fuel Oil**: 10-25° API (typical)
- **Asphalt**: 5-15° API (typical)

### Example Calculations

**Example 1: Calculate API from Specific Gravity**
```
Given: Specific Gravity = 0.850 at 60°F
API Gravity = (141.5 / 0.850) - 131.5 = 35.0°
Classification: Light crude
```

**Example 2: Calculate Specific Gravity from API**
```
Given: API Gravity = 30.0°
Specific Gravity = 141.5 / (30.0 + 131.5) = 0.876
```

## Volume and Mass Conversions

### Standard Conditions
All petroleum measurements are standardized to:
- Temperature: 60°F (15.56°C)
- Pressure: 1 atmosphere (14.7 psia)

### Basic Conversion Formulas

#### Barrels to Tons
```
Weight (short tons) = Volume (barrels) × Specific Gravity × 0.1364
```

Where:
- 1 barrel = 42 US gallons
- 1 short ton = 2000 pounds
- Conversion factor 0.1364 = (42 gal/bbl × 8.337 lb/gal) / 2000 lb/ton ÷ SG of water

#### Barrels to Pounds
```
Weight (pounds) = Volume (barrels) × Specific Gravity × 350
```

Where:
- 1 barrel = 42 gallons
- 1 gallon of water at 60°F = 8.337 pounds
- 42 × 8.337 ≈ 350

#### Barrels to Metric Tons
```
Weight (metric tons) = Volume (barrels) × Specific Gravity × 0.1364 × 0.907185
Weight (metric tons) = Volume (barrels) × Specific Gravity × 0.1237
```

### Worked Examples

**Example 1: Light Sweet Crude (WTI)**
```
Volume: 10,000 barrels
API Gravity: 39.6°
Specific Gravity: 141.5 / (39.6 + 131.5) = 0.827

Mass Calculation:
Weight (tons) = 10,000 × 0.827 × 0.1364 = 1,128 short tons
Weight (pounds) = 10,000 × 0.827 × 350 = 2,894,500 pounds
Weight (metric tons) = 10,000 × 0.827 × 0.1237 = 1,023 metric tons
```

**Example 2: Heavy Sour Crude (Maya)**
```
Volume: 10,000 barrels
API Gravity: 22.0°
Specific Gravity: 141.5 / (22.0 + 131.5) = 0.920

Mass Calculation:
Weight (tons) = 10,000 × 0.920 × 0.1364 = 1,255 short tons
Weight (pounds) = 10,000 × 0.920 × 350 = 3,220,000 pounds
Weight (metric tons) = 10,000 × 0.920 × 0.1237 = 1,138 metric tons

Note: 10,000 barrels of Maya weighs 11.3% more than 10,000 barrels of WTI
```

**Example 3: Gasoline Product**
```
Volume: 50,000 barrels
API Gravity: 60.0°
Specific Gravity: 141.5 / (60.0 + 131.5) = 0.739

Mass Calculation:
Weight (tons) = 50,000 × 0.739 × 0.1364 = 5,037 short tons
Weight (metric tons) = 50,000 × 0.739 × 0.1237 = 4,570 metric tons
```

## Temperature Corrections

### The Need for Temperature Correction
Petroleum products expand and contract significantly with temperature changes. Since custody transfer and accounting require standard conditions (60°F), observed volumes must be corrected.

### Thermal Expansion Coefficient
Typical expansion: **0.04% to 0.07% volume change per °F** depending on API gravity

### Volume Correction Factor (VCF)
The VCF adjusts observed volume at operating temperature to volume at 60°F:

```
Volume at 60°F = Volume observed × VCF(T, API)
```

### API MPMS Chapter 11.1 Tables
Industry standard tables (formerly ASTM D1250) provide VCF values based on:
- Observed temperature
- API gravity or density

### Simplified VCF Calculation
For estimation purposes (not for custody transfer):

```
VCF ≈ 1 + α × (60 - T)
```

Where:
- α = thermal expansion coefficient ≈ 0.0004 to 0.0007 per °F
- T = observed temperature in °F

### Worked Examples

**Example 1: Summer Gasoline Measurement**
```
Tank Measurement:
- Observed Volume: 100,000 barrels
- Tank Temperature: 85°F
- Product API: 60°

Temperature Correction:
Using API tables, VCF(85°F, 60°API) ≈ 0.987

Corrected Volume:
Volume at 60°F = 100,000 × 0.987 = 98,700 barrels

Net Effect: Lost 1,300 barrels (1.3%) due to thermal expansion
```

**Example 2: Winter Diesel Measurement**
```
Tank Measurement:
- Observed Volume: 75,000 barrels
- Tank Temperature: 40°F
- Product API: 38°

Temperature Correction:
Using API tables, VCF(40°F, 38°API) ≈ 1.011

Corrected Volume:
Volume at 60°F = 75,000 × 1.011 = 75,825 barrels

Net Effect: Gained 825 barrels (1.1%) due to thermal contraction
```

**Example 3: Heavy Fuel Oil in Pipeline**
```
Pipeline Measurement:
- Observed Volume: 50,000 barrels
- Pipeline Temperature: 110°F (heated for flow)
- Product API: 18°

Temperature Correction:
Using API tables, VCF(110°F, 18°API) ≈ 0.970

Corrected Volume:
Volume at 60°F = 50,000 × 0.970 = 48,500 barrels

Net Effect: Lost 1,500 barrels (3.0%) due to significant thermal expansion at low API
```

### Temperature Impact by Product Type
| Product Type | API Gravity | Expansion Coefficient | Impact at ±20°F |
|-------------|-------------|----------------------|-----------------|
| Gasoline | 60° | 0.00065/°F | ±1.3% |
| Jet Fuel | 45° | 0.00055/°F | ±1.1% |
| Diesel | 36° | 0.00050/°F | ±1.0% |
| Fuel Oil | 18° | 0.00045/°F | ±0.9% |

## Quality Specifications

### Sulfur Content

#### Measurement Units
- **Weight Percent (wt%)**: Standard reporting
- **Parts Per Million (ppm)**: 1 wt% = 10,000 ppm

#### Classifications
**Crude Oil**:
- Sweet: < 0.5% sulfur (< 5,000 ppm)
- Sour: > 0.5% sulfur (> 5,000 ppm)

**Products**:
- Gasoline (US Tier 3): < 10 ppm sulfur
- ULSD (Ultra-Low Sulfur Diesel): < 15 ppm sulfur
- Jet Fuel (Jet A): < 3,000 ppm sulfur
- Heating Oil: < 500 ppm sulfur
- Marine Fuel (IMO 2020): < 0.5% sulfur (< 5,000 ppm)

#### Test Methods
- ASTM D4294: X-Ray Fluorescence (XRF) for 0.015% to 5% sulfur
- ASTM D5453: UV Fluorescence for 1 ppm to 8,000 ppm sulfur
- ASTM D2622: X-Ray Fluorescence for petroleum products

### Reid Vapor Pressure (RVP)

#### Definition
Measurement of vapor pressure at 100°F, indicating fuel volatility.

#### Specifications
**Gasoline (US EPA)**:
- Class A (Summer): 7.8 psi RVP (June 1 - Sept 15)
- Class B (Transition): 9.0 psi RVP
- Class C (Winter): 13.5-15.0 psi RVP (Sept 16 - May 31)

**Regional Variations**:
- California: 6.9-7.0 psi RVP (summer)
- High Altitude: Higher limits allowed

#### Blending Components Impact
| Component | Typical RVP |
|-----------|-------------|
| Butane | 52 psi |
| FCC Gasoline | 7-10 psi |
| Reformate | 2-4 psi |
| Alkylate | 4-5 psi |
| Ethanol | 18 psi (blending RVP) |

#### Test Method
- ASTM D323: Reid Vapor Pressure test

### Octane Number

#### Measurement Types
1. **Research Octane Number (RON)**: Test at low speed/load
2. **Motor Octane Number (MON)**: Test at high speed/load
3. **Anti-Knock Index (AKI)**: Posted pump rating = (RON + MON) / 2

#### Typical Ratings
**US Gasoline**:
- Regular: 87 AKI (91 RON / 83 MON)
- Mid-Grade: 89 AKI (93 RON / 85 MON)
- Premium: 91-93 AKI (95-97 RON / 87-89 MON)

**Blending Components**:
| Component | RON | MON |
|-----------|-----|-----|
| Butane | 93 | 92 |
| FCC Gasoline | 92 | 80 |
| Reformate | 95-100 | 85-90 |
| Alkylate | 95 | 93 |
| Ethanol | 113 | 94 |

#### Test Methods
- ASTM D2699: Research Octane Number
- ASTM D2700: Motor Octane Number

### Cetane Number

#### Definition
Measure of diesel fuel's ignition quality (higher = better ignition).

#### Specifications
- US Diesel: Minimum 40 cetane
- Premium Diesel: 45-50 cetane
- European Diesel: Minimum 51 cetane
- Arctic Diesel: 45-55 cetane

#### Typical Values by Source
| Stream | Cetane Number |
|--------|---------------|
| Straight Run Gas Oil | 50-60 |
| Hydrocracker Diesel | 50-65 |
| Light Cycle Oil (FCC) | 20-30 |
| Coker Gas Oil | 25-35 |

#### Test Methods
- ASTM D613: Cetane Number (engine test)
- ASTM D6890: Cetane Index (calculated from density and distillation)
- ASTM D7668: Derived Cetane Number (combustion analyzer)

### Distillation Range

#### T10, T50, T90 Points
- **T10**: Temperature at which 10% of sample evaporates
- **T50**: 50% evaporation point (mid-boiling point)
- **T90**: 90% evaporation point (end point)

#### Product Specifications
**Gasoline**:
- T10: < 140°F (cold start, evaporation)
- T50: 200-220°F (drivability)
- T90: 330-375°F (deposits, emissions)

**Diesel**:
- T10: > 400°F (flash point)
- T50: 480-560°F (combustion quality)
- T90: < 640°F (emissions, deposits)

**Jet Fuel**:
- T10: > 360°F (flash point safety)
- T50: 400-450°F (combustion)
- Final Boiling Point: < 572°F

#### Test Methods
- ASTM D86: Distillation of Petroleum Products (manual)
- ASTM D2887: Simulated Distillation by Gas Chromatography

## Mass Balance Accounting Standards

### Inventory Measurement

#### Tank Gauging Methods
1. **Manual Gauging**: Tape and bob, accurate to ±1/8 inch
2. **Automatic Tank Gauge (ATG)**: Float or servo gauge, ±0.1 inch
3. **Radar Gauge**: Non-contact, ±1-2mm accuracy

#### Volume from Tank Tables
```
Gross Observed Volume (GOV) = Tank Height × Tank Factor

Net Standard Volume (NSV) = GOV × VCF(T, API) × Free Water Correction
```

### Measurement Uncertainty

#### Typical Uncertainty Sources
| Source | Uncertainty |
|--------|-------------|
| Tank gauge reading | ±0.1% to 0.3% |
| Temperature measurement | ±0.5°F → ±0.025% volume |
| API gravity | ±0.1° → ±0.1% density |
| Tank table interpolation | ±0.05% |
| Total combined | ±0.2% to 0.5% |

#### Acceptable Tolerance
- Single tank: ±0.5%
- Custody transfer: ±0.3%
- Refinery-wide balance: ±1.0%

### Reporting Standards

#### Daily Operations Report
- Crude receipts (by grade)
- Unit production (by stream)
- Product shipments (by grade)
- Inventory changes (opening, closing)
- Losses and gains

#### Monthly Reconciliation
- Total inputs vs. outputs
- Mass balance closure (target: ±0.5%)
- Variance investigation for >1% difference
- Inventory adjustments

## References

### API Standards
- API MPMS Chapter 11.1: Volume Correction Factors
- API MPMS Chapter 12: Calculation of Petroleum Quantities
- API MPMS Chapter 3: Tank Gauging

### ASTM Standards
- ASTM D4052: Density by Digital Density Meter
- ASTM D1298: Density by Hydrometer
- ASTM D323: Reid Vapor Pressure
- ASTM D86: Distillation of Petroleum Products
- ASTM D2699/D2700: Octane Number
- ASTM D613: Cetane Number

### Industry References
- API Technical Data Book: Petroleum Refining
- ASTM International: www.astm.org
- American Petroleum Institute: www.api.org
