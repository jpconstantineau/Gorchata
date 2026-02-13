# Oil Refinery Seed Data Configuration

## Overview

This document describes seed data that will be loaded into dimension tables to support the oil refinery data warehouse. Seed data provides reference data that is relatively static and supports operational fact data.

## Dimension: dim_crude_grade

Seed data for crude oil grades used in refinery operations.

### Records to Load

| crude_grade_id | crude_name | api_gravity | sulfur_pct | crude_type | origin | specific_gravity | pour_point_f | typical_yield_light_pct | typical_yield_middle_pct | typical_yield_heavy_pct |
|----------------|------------|-------------|------------|------------|--------|------------------|--------------|------------------------|-------------------------|------------------------|
| WTI | WTI | 39.6 | 0.24 | Light Sweet | West Texas, USA | 0.827 | -5 | 52 | 30 | 18 |
| BRENT | Brent | 38.3 | 0.37 | Light Sweet | North Sea, UK | 0.835 | -20 | 50 | 32 | 18 |
| MAYA | Maya | 22.0 | 3.30 | Heavy Sour | Mexico | 0.920 | 30 | 28 | 35 | 37 |
| DUBAI | Dubai | 31.0 | 2.00 | Medium | UAE | 0.871 | 15 | 42 | 35 | 23 |
| MARS | Mars | 29.0 | 1.90 | Medium | Gulf of Mexico, USA | 0.882 | 20 | 38 | 36 | 26 |
| MIXED | Mixed | 32.0 | 1.50 | Medium | Blend | 0.864 | 10 | 44 | 34 | 22 |

### Seed File: seeds/dim_crude_grade.csv

```csv
crude_grade_id,crude_name,api_gravity,sulfur_pct,crude_type,origin,specific_gravity,pour_point_f,typical_yield_light_pct,typical_yield_middle_pct,typical_yield_heavy_pct
WTI,WTI,39.6,0.24,Light Sweet,West Texas USA,0.827,-5,52,30,18
BRENT,Brent,38.3,0.37,Light Sweet,North Sea UK,0.835,-20,50,32,18
MAYA,Maya,22.0,3.30,Heavy Sour,Mexico,0.920,30,28,35,37
DUBAI,Dubai,31.0,2.00,Medium,UAE,0.871,15,42,35,23
MARS,Mars,29.0,1.90,Medium,Gulf of Mexico USA,0.882,20,38,36,26
MIXED,Mixed,32.0,1.50,Medium,Blend,0.864,10,44,34,22
```

## Dimension: dim_catalyst_cycle

Seed data for catalyst lifecycle stages.

### Records to Load

| catalyst_cycle_id | cycle_stage | stage_description | typical_efficiency_pct | age_days_min | age_days_max | typical_activity_index | requires_attention |
|-------------------|-------------|-------------------|------------------------|--------------|--------------|----------------------|-------------------|
| FRESH | Fresh | Fresh catalyst with maximum activity and selectivity. May require learning period to establish optimal conditions. | 100 | 0 | 90 | 1.00 | 0 |
| EARLY_MID | Early-Mid | Stable high performance period with optimal operating window. Minimal operational challenges. | 97 | 91 | 180 | 0.97 | 0 |
| MID_CYCLE | Mid-Cycle | Gradual performance decline requiring temperature increases to maintain conversion. Monitor deactivation trends. | 92 | 181 | 365 | 0.92 | 0 |
| LATE_MID | Late-Mid | Noticeable performance decline with temperature approaching limits. Plan for regeneration or replacement. | 87 | 366 | 540 | 0.87 | 1 |
| END_OF_RUN | End-of-Run | Severely reduced performance at maximum temperature. Regeneration or replacement required immediately. | 80 | 541 | NULL | 0.80 | 1 |

### Seed File: seeds/dim_catalyst_cycle.csv

```csv
catalyst_cycle_id,cycle_stage,stage_description,typical_efficiency_pct,age_days_min,age_days_max,typical_activity_index,requires_attention
FRESH,Fresh,Fresh catalyst with maximum activity and selectivity. May require learning period to establish optimal conditions.,100,0,90,1.00,0
EARLY_MID,Early-Mid,Stable high performance period with optimal operating window. Minimal operational challenges.,97,91,180,0.97,0
MID_CYCLE,Mid-Cycle,Gradual performance decline requiring temperature increases to maintain conversion. Monitor deactivation trends.,92,181,365,0.92,0
LATE_MID,Late-Mid,Noticeable performance decline with temperature approaching limits. Plan for regeneration or replacement.,87,366,540,0.87,1
END_OF_RUN,End-of-Run,Severely reduced performance at maximum temperature. Regeneration or replacement required immediately.,80,541,,0.80,1
```

## Dimension: dim_unit (Sample Records)

Representative refinery process units for a 150,000 BPD refinery.

### Records to Load (Partial List)

| unit_id | unit_name | unit_type | complex_name | capacity_bpd | design_capacity_bpd | commissioned_date | unit_description | feed_type | product_type |
|---------|-----------|-----------|--------------|--------------|---------------------|-------------------|------------------|-----------|--------------|
| CDU-1 | Crude Unit 1 | CDU | Primary Processing | 100000 | 110000 | 2005-03-15 | Primary atmospheric distillation of crude oil | Crude Oil | Multiple Streams |
| CDU-2 | Crude Unit 2 | CDU | Primary Processing | 50000 | 60000 | 2010-08-20 | Secondary crude distillation unit | Crude Oil | Multiple Streams |
| VDU-1 | Vacuum Unit 1 | VDU | Primary Processing | 55000 | 65000 | 2005-06-10 | Vacuum distillation of atmospheric residue | Atmospheric Residue | Vacuum Gas Oils |
| FCC-1 | FCC Unit 1 | FCC | Conversion Complex | 65000 | 75000 | 2008-04-12 | Fluid catalytic cracking for gasoline production | Vacuum Gas Oil | Gasoline/LCO |
| HCU-1 | Hydrocracker 1 | Hydrocracker | Conversion Complex | 35000 | 40000 | 2012-11-03 | Hydrocracking to diesel and naphtha | Heavy Gas Oil | Diesel/Naphtha |
| REFORM-1 | Reformer 1 | Reformer | Gasoline Complex | 25000 | 30000 | 2006-02-28 | Catalytic reforming for octane upgrade | Heavy Naphtha | Reformate/H2 |
| DHDT-1 | Diesel Hydrotreater 1 | Hydrotreater | Treating Complex | 40000 | 45000 | 2009-07-15 | Desulfurization for ULSD production | Straight Run Diesel | ULSD |
| NHDT-1 | Naphtha Hydrotreater 1 | Hydrotreater | Gasoline Complex | 30000 | 35000 | 2006-05-20 | Naphtha treating for reformer feed | Light/Heavy Naphtha | Treated Naphtha |
| ALKY-1 | Alkylation 1 | Alkylation | Gasoline Complex | 12000 | 15000 | 2007-09-10 | Alkylation for high-octane gasoline | C3-C5 Olefins | Alkylate |
| COKER-1 | Delayed Coker 1 | Coker | Residue Complex | 25000 | 30000 | 2011-01-22 | Thermal cracking of vacuum residue | Vacuum Residue | Coke/Gas Oils |

### Seed File: seeds/dim_unit.csv

```csv
unit_id,unit_name,unit_type,complex_name,capacity_bpd,design_capacity_bpd,commissioned_date,unit_description,feed_type,product_type
CDU-1,Crude Unit 1,CDU,Primary Processing,100000,110000,2005-03-15,Primary atmospheric distillation of crude oil,Crude Oil,Multiple Streams
CDU-2,Crude Unit 2,CDU,Primary Processing,50000,60000,2010-08-20,Secondary crude distillation unit,Crude Oil,Multiple Streams
VDU-1,Vacuum Unit 1,VDU,Primary Processing,55000,65000,2005-06-10,Vacuum distillation of atmospheric residue,Atmospheric Residue,Vacuum Gas Oils
FCC-1,FCC Unit 1,FCC,Conversion Complex,65000,75000,2008-04-12,Fluid catalytic cracking for gasoline production,Vacuum Gas Oil,Gasoline/LCO
HCU-1,Hydrocracker 1,Hydrocracker,Conversion Complex,35000,40000,2012-11-03,Hydrocracking to diesel and naphtha,Heavy Gas Oil,Diesel/Naphtha
REFORM-1,Reformer 1,Reformer,Gasoline Complex,25000,30000,2006-02-28,Catalytic reforming for octane upgrade,Heavy Naphtha,Reformate/H2
DHDT-1,Diesel Hydrotreater 1,Hydrotreater,Treating Complex,40000,45000,2009-07-15,Desulfurization for ULSD production,Straight Run Diesel,ULSD
NHDT-1,Naphtha Hydrotreater 1,Hydrotreater,Gasoline Complex,30000,35000,2006-05-20,Naphtha treating for reformer feed,Light/Heavy Naphtha,Treated Naphtha
ALKY-1,Alkylation 1,Alkylation,Gasoline Complex,12000,15000,2007-09-10,Alkylation for high-octane gasoline,C3-C5 Olefins,Alkylate
COKER-1,Delayed Coker 1,Coker,Residue Complex,25000,30000,2011-01-22,Thermal cracking of vacuum residue,Vacuum Residue,Coke/Gas Oils
```

## Dimension: dim_location (Sample Records)

Representative sources and destinations for crude/products.

### Records to Load (Partial List)

| location_id | location_name | location_type | location_category | region | state_province | country | is_active |
|------------|---------------|---------------|-------------------|--------|----------------|---------|-----------|
| PIPE-WTI-MAIN | WTI Pipeline Main | Pipeline | Source | Southwest | Texas | USA | 1 |
| PIPE-BRENT-INT | Brent Import Pipeline | Pipeline | Source | Gulf Coast | Louisiana | USA | 1 |
| TERM-MARINE-1 | Marine Terminal 1 | Marine Terminal | Both | Gulf Coast | Texas | USA | 1 |
| TERM-TRUCK-EAST | East Truck Terminal | Truck Terminal | Destination | Southeast | Texas | USA | 1 |
| TERM-TRUCK-WEST | West Truck Terminal | Truck Terminal | Destination | Southwest | Texas | USA | 1 |
| PIPE-GAS-MAIN | Gasoline Pipeline Main | Pipeline | Destination | Regional | Texas | USA | 1 |
| PIPE-DIESEL-MAIN | Diesel Pipeline Main | Pipeline | Destination | Regional | Texas | USA | 1 |
| CUST-WHOLESALE-A | Wholesale Customer A | Customer | Destination | Regional | Texas | USA | 1 |
| CUST-WHOLESALE-B | Wholesale Customer B | Customer | Destination | Regional | Louisiana | USA | 1 |
| STOR-TANK-FARM | Tank Farm Storage | Storage | Internal | On-Site | Texas | USA | 1 |

### Seed File: seeds/dim_location.csv

```csv
location_id,location_name,location_type,location_category,region,state_province,country,is_active
PIPE-WTI-MAIN,WTI Pipeline Main,Pipeline,Source,Southwest,Texas,USA,1
PIPE-BRENT-INT,Brent Import Pipeline,Pipeline,Source,Gulf Coast,Louisiana,USA,1
TERM-MARINE-1,Marine Terminal 1,Marine Terminal,Both,Gulf Coast,Texas,USA,1
TERM-TRUCK-EAST,East Truck Terminal,Truck Terminal,Destination,Southeast,Texas,USA,1
TERM-TRUCK-WEST,West Truck Terminal,Truck Terminal,Destination,Southwest,Texas,USA,1
PIPE-GAS-MAIN,Gasoline Pipeline Main,Pipeline,Destination,Regional,Texas,USA,1
PIPE-DIESEL-MAIN,Diesel Pipeline Main,Pipeline,Destination,Regional,Texas,USA,1
CUST-WHOLESALE-A,Wholesale Customer A,Customer,Destination,Regional,Texas,USA,1
CUST-WHOLESALE-B,Wholesale Customer B,Customer,Destination,Regional,Louisiana,USA,1
STOR-TANK-FARM,Tank Farm Storage,Storage,Internal,On-Site,Texas,USA,1
```

## Dimension: dim_stream (Sample Records)

Representative intermediate refinery streams.

### Records to Load (Partial List)

| stream_id | stream_name | stream_type | boiling_range_min_f | boiling_range_max_f | typical_api_gravity | typical_sulfur_pct | primary_use | stream_description |
|-----------|-------------|-------------|---------------------|---------------------|---------------------|-------------------|-------------|-------------------|
| LT-NAPH | Light Naphtha | Light | 90 | 180 | 70 | 0.005 | Gasoline Blending | Light straight-run naphtha for direct blending or isomerization |
| HV-NAPH | Heavy Naphtha | Light | 180 | 380 | 55 | 0.020 | Reformer Feed | Heavy naphtha suitable for catalytic reforming |
| KERO | Kerosene | Medium | 380 | 520 | 45 | 0.100 | Jet Fuel / Diesel | Middle distillate for jet fuel or diesel blending |
| LGO | Light Gas Oil | Medium | 520 | 650 | 35 | 0.300 | Diesel Blending | Straight-run diesel blending component |
| HGO | Heavy Gas Oil | Heavy | 650 | 750 | 25 | 1.000 | FCC Feed | Heavy gas oil for catalytic cracking |
| ATM-RESID | Atmospheric Residue | Residue | 750 | 1100 | 18 | 2.500 | VDU Feed | Bottoms from crude unit sent to vacuum unit |
| LVGO | Light Vacuum Gas Oil | Heavy | 650 | 900 | 28 | 1.200 | FCC Feed | Light cut from vacuum unit for FCC |
| HVGO | Heavy Vacuum Gas Oil | Heavy | 900 | 1100 | 22 | 2.000 | Hydrocracker Feed | Heavy cut from vacuum unit for hydrocracker |
| VAC-RESID | Vacuum Residue | Residue | 1100 | 1200 | 10 | 4.000 | Coker Feed | Vacuum bottoms for delayed coking |
| FCC-GAS | FCC Gasoline | Light | 100 | 420 | 58 | 0.030 | Gasoline Blending | Gasoline-range product from FCC |
| LCO | Light Cycle Oil | Medium | 420 | 650 | 22 | 0.500 | Diesel Blending | Diesel-range product from FCC |
| REFORMATE | Reformate | Light | 180 | 380 | 45 | 0.001 | Gasoline Blending | High-octane aromatics from reformer |

### Seed File: seeds/dim_stream.csv

```csv
stream_id,stream_name,stream_type,boiling_range_min_f,boiling_range_max_f,typical_api_gravity,typical_sulfur_pct,primary_use,stream_description
LT-NAPH,Light Naphtha,Light,90,180,70,0.005,Gasoline Blending,Light straight-run naphtha for direct blending or isomerization
HV-NAPH,Heavy Naphtha,Light,180,380,55,0.020,Reformer Feed,Heavy naphtha suitable for catalytic reforming
KERO,Kerosene,Medium,380,520,45,0.100,Jet Fuel / Diesel,Middle distillate for jet fuel or diesel blending
LGO,Light Gas Oil,Medium,520,650,35,0.300,Diesel Blending,Straight-run diesel blending component
HGO,Heavy Gas Oil,Heavy,650,750,25,1.000,FCC Feed,Heavy gas oil for catalytic cracking
ATM-RESID,Atmospheric Residue,Residue,750,1100,18,2.500,VDU Feed,Bottoms from crude unit sent to vacuum unit
LVGO,Light Vacuum Gas Oil,Heavy,650,900,28,1.200,FCC Feed,Light cut from vacuum unit for FCC
HVGO,Heavy Vacuum Gas Oil,Heavy,900,1100,22,2.000,Hydrocracker Feed,Heavy cut from vacuum unit for hydrocracker
VAC-RESID,Vacuum Residue,Residue,1100,1200,10,4.000,Coker Feed,Vacuum bottoms for delayed coking
FCC-GAS,FCC Gasoline,Light,100,420,58,0.030,Gasoline Blending,Gasoline-range product from FCC
LCO,Light Cycle Oil,Medium,420,650,22,0.500,Diesel Blending,Diesel-range product from FCC
REFORMATE,Reformate,Light,180,380,45,0.001,Gasoline Blending,High-octane aromatics from reformer
```

## Dimension: dim_product (Sample Records)

Representative refined products.

### Records to Load (Partial List)

| product_id | product_name | product_grade | product_type | product_category | api_gravity | sulfur_pct | specific_gravity | pour_point_f | flash_point_f | rvp_psi | cetane_number | octane_number |
|-----------|--------------|---------------|--------------|------------------|-------------|------------|------------------|--------------|---------------|---------|---------------|---------------|
| GAS-REG-87 | Regular Gasoline | 87 Octane | Gasoline | Light Distillates | 60 | 0.001 | 0.739 | -60 | -45 | 9.0 | NULL | 87 |
| GAS-MID-89 | Mid-Grade Gasoline | 89 Octane | Gasoline | Light Distillates | 59 | 0.001 | 0.743 | -60 | -45 | 9.0 | NULL | 89 |
| GAS-PREM-93 | Premium Gasoline | 93 Octane | Gasoline | Light Distillates | 58 | 0.001 | 0.747 | -60 | -45 | 9.0 | NULL | 93 |
| DIESEL-ULSD | Ultra-Low Sulfur Diesel | #2 ULSD | Diesel | Middle Distillates | 36 | 0.0015 | 0.850 | -10 | 130 | NULL | 45 | NULL |
| DIESEL-WINTER | Winter Diesel | #1 Diesel | Diesel | Middle Distillates | 42 | 0.0010 | 0.820 | -40 | 100 | NULL | 48 | NULL |
| JET-A | Jet Fuel | Jet A | Jet Fuel | Middle Distillates | 42 | 0.100 | 0.820 | -40 | 110 | NULL | 47 | NULL |
| PROP-LPG | Propane | LPG Grade | LPG | Light Distillates | 112 | 0.000 | 0.508 | -310 | -156 | NULL | NULL | 112 |
| BUTANE-LPG | Butane | LPG Grade | LPG | Light Distillates | 108 | 0.000 | 0.584 | -217 | -76 | 52 | NULL | 93 |
| FO-RESID6 | Residual Fuel Oil | #6 Fuel Oil | Fuel Oil | Heavy Products | 15 | 2.500 | 0.965 | 50 | 150 | NULL | NULL | NULL |
| ASPH-PG64 | Paving Asphalt | PG 64-22 | Asphalt | Heavy Products | 8 | 0.500 | 1.030 | 140 | 450 | NULL | NULL | NULL |
| PETCOKE | Petroleum Coke | Fuel Grade | Coke | Heavy Products | NULL | 5.000 | NULL | NULL | NULL | NULL | NULL | NULL |

### Seed File: seeds/dim_product.csv

```csv
product_id,product_name,product_grade,product_type,product_category,api_gravity,sulfur_pct,specific_gravity,pour_point_f,flash_point_f,rvp_psi,cetane_number,octane_number
GAS-REG-87,Regular Gasoline,87 Octane,Gasoline,Light Distillates,60,0.001,0.739,-60,-45,9.0,,87
GAS-MID-89,Mid-Grade Gasoline,89 Octane,Gasoline,Light Distillates,59,0.001,0.743,-60,-45,9.0,,89
GAS-PREM-93,Premium Gasoline,93 Octane,Gasoline,Light Distillates,58,0.001,0.747,-60,-45,9.0,,93
DIESEL-ULSD,Ultra-Low Sulfur Diesel,#2 ULSD,Diesel,Middle Distillates,36,0.0015,0.850,-10,130,,45,
DIESEL-WINTER,Winter Diesel,#1 Diesel,Diesel,Middle Distillates,42,0.0010,0.820,-40,100,,48,
JET-A,Jet Fuel,Jet A,Jet Fuel,Middle Distillates,42,0.100,0.820,-40,110,,47,
PROP-LPG,Propane,LPG Grade,LPG,Light Distillates,112,0.000,0.508,-310,-156,,,112
BUTANE-LPG,Butane,LPG Grade,LPG,Light Distillates,108,0.000,0.584,-217,-76,52,,93
FO-RESID6,Residual Fuel Oil,#6 Fuel Oil,Fuel Oil,Heavy Products,15,2.500,0.965,50,150,,,
ASPH-PG64,Paving Asphalt,PG 64-22,Asphalt,Heavy Products,8,0.500,1.030,140,450,,,
PETCOKE,Petroleum Coke,Fuel Grade,Coke,Heavy Products,,5.000,,,,,,
```

## Dimension: dim_date

Date dimension will be generated programmatically covering 2020-2030 with:
- Calendar attributes (year, quarter, month, week, day_of_week, day_name, month_name)
- Fiscal year attributes (fiscal_year, fiscal_quarter)
- Flags (is_weekend, is_holiday)

## Implementation Notes

### Phase 2 Tasks
1. Create CSV seed files in `seeds/` directory
2. Update `schema.yml` with `seed_config_path` references for each dimension
3. Implement seed loading logic in Gorchata (if not already available)
4. Add data validation tests for seed data quality

### Phase 3 Tasks  
1. Create additional seed data for complete product catalog
2. Add more process units for realistic refinery configuration
3. Implement dim_date generator for 10-year date range
4. Add seasonal product specifications (summer vs. winter gasoline)

### Data Quality Checks
- All primary keys are unique and not null
- Foreign key values exist in referenced tables
- Numeric ranges are within specified bounds (API gravity, sulfur content, etc.)
- Required fields are populated
- Descriptive text is meaningful and consistent

## Future Enhancements

### Historical Data
- Track changes to unit capacities over time (slowly changing dimensions)
- Crude grade property variations across time
- Product specification changes due to regulatory updates

### Master Data Management
- Version control for seed data
- Change approval workflow
- Data lineage tracking
- Automated validation before loading
