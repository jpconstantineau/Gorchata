# OSB Machine Event OEE - Seed Data

This folder contains dimension seed data for the OSB manufacturing analytics example.

## Seed Files

### Dimension Tables

#### dim_equipment.csv (16 records)
Equipment inventory for the OSB plant:
- **2 Debarkers** (DEBARK-01, DEBARK-02): 120 logs/hour each
- **2 Stranders** (STRAND-01, STRAND-02): 6 tons/hour each (total 12 tons/hr)
- **1 Rotary Dryer** (DRYER-01): 10 tons/hour ⚠️ **Bottleneck** (83% of upstream capacity)
- **2 Screens** (SCREEN-01, SCREEN-02): 12 tons/hour each
- **2 Blenders** (BLEND-01, BLEND-02): 11 tons/hour each
- **1 Forming Line** (FORM-01): 90 panels/hour
- **1 Continuous Press** (PRESS-01): 90 panels/hour, 8-minute cycle time
- **1 Cooling Conveyor** (COOL-01): 100 panels/hour
- **4 Saws** (SAW-01 to SAW-04): 95 panels/hour each (redundant capacity)

**Criticality Levels:**
- **Critical**: Dryer, Press (single-point failures)
- **Important**: Stranders, Blenders, Former (limited redundancy)
- **Standard**: Debarkers, Screens, Saws (redundant capacity)

#### dim_production_area.csv (8 records)
Production stages in sequence order:
1. **Log_Yard**: Raw log storage
2. **Stranding**: Convert logs to strands → **Green Strand Bins (4-hour buffer)**
3. **Drying**: Remove moisture → **Dry Fiber Silos (8-hour buffer)**
4. **Screening**: Size classification
5. **Blending**: Mix with resin
6. **Forming**: Create mat → **Mat Buffer (30-minute buffer)**
7. **Pressing**: Hot press into panels
8. **Finishing**: Sawing and stacking

**Buffer Strategy:**
- Green Strand Bins (4 hrs): Absorbs stranding variability, protects against dryer downtime
- Dry Fiber Silos (8 hrs): Largest buffer, critical for downstream stability
- Mat Buffer (30 min): Minimal buffer, press is final constraint

#### dim_reason_code.csv (25 records)
Downtime and loss reason codes mapped to OEE categories:

**Availability Losses** (Unplanned Downtime):
- **Mechanical**: Bearing failures (180 min), Hydraulic leaks (240 min), Gear failures (1440 min), Conveyor jams
- **Electrical**: Burner trips (90 min), Motor overheats (120 min)

**Performance Losses**:
- **Process**: Strand bridging (15 min), Resin mix deviations (45 min), Mat folds (20 min), Minor stops (5 min), Speed losses
- **Temperature Control**: Dryer temp control (60 min), Press temp low (45 min)

**Quality Losses**:
- Thickness out-of-spec (90 min), Delamination failures (300 min), Surface defects (120 min), Density out-of-spec (90 min), Edge quality (30 min)

**Planned Downtime** (OEE loss type = None):
- Weekly preventive maintenance (480 min = 8 hours)
- Knife changes (60 min)
- Product changeovers (90 min)
- Shift handovers (30 min)

**MTTR (Mean Time To Repair) Ranges:**
- Quick fixes: 15-60 minutes (strand bridging, minor stops, mat folds)
- Standard repairs: 90-240 minutes (burner trips, bearing failures, hydraulic leaks, quality holds)
- Major repairs: 1440 minutes (24 hours) (gear failures requiring heavy maintenance)

#### dim_shift.csv (3 records)
3×8-hour shift operations:
- **Day Shift**: 06:00-14:00 (12 crew members)
- **Swing Shift**: 14:00-22:00 (12 crew members)
- **Night Shift**: 22:00-06:00 (10 crew members)

Note: 30-minute handover periods are modeled as planned downtime events.

#### dim_product_spec.csv (3 records)
OSB panel specifications:
- **7/16" (0.4375")**: Most common, Rated Sheathing grade, 40 lbs/ft³ density
- **3/8" (0.375")**: Thinner option, Rated Sheathing grade, 40 lbs/ft³ density
- **9/16" (0.5625")**: Thickest option, Structural_1 grade, 40 lbs/ft³ density

**Quality Tolerances:**
- Thickness: ±0.015" (±0.38 mm)
- Density: ±2 lbs/ft³

Product mix assumption: 50% 7/16", 30% 3/8", 20% 9/16"

#### dim_date.csv (90 records)
Date dimension covering 90 days (January 1 - March 31, 2026):
- Full date attributes: year, quarter, month, week, day_of_week
- Enables time-series analysis and trend identification
- Supports weekly maintenance scheduling (Sunday nights)

## Seed Configuration

### seed.yml
Standard seed import configuration:
- **Version**: 1
- **Naming strategy**: filename (table name derived from CSV filename)
- **Import batch size**: 1000 records
- **Import scope**: folder (imports all CSV files in seeds/ directory)

## Usage

Load seed data into database:
```bash
gorchata seed --config examples/osb_machine_event_oee/seeds/seed.yml
```

## Validation

All seed files are validated by comprehensive test suite in `test/osb_seed_test.go`:

✅ Equipment inventory (16 pieces, correct types and capacities)
✅ Production flow sequence (8 areas in correct order)
✅ Dryer bottleneck constraint (83% of upstream capacity)
✅ Buffer capacities (4hr, 8hr, 30min)
✅ Reason code OEE mappings (Planned/Unplanned, Availability/Performance/Quality)
✅ Shift patterns (3 shifts with correct timing)
✅ Maintenance windows (planned downtime defined)
✅ MTBF/MTTR distributions (realistic repair durations)
✅ Product specifications (3 thicknesses with tolerances)

Run validation tests:
```bash
go test ./test -run "TestOSBSeed|TestEquipmentInventory|TestProductionFlowSequence"
```

## Future Enhancements

Phase 3+ will add:
- **Machine event data generation**: 90 days of state transition events
- **Production output data**: Panel counts by shift and product type
- **Quality test results**: Thickness, density, delamination testing
- **Buffer level tracking**: Inventory snapshots over time

These will be generated dynamically or added as additional seed files to support complete end-to-end analytics from raw events to OEE calculations.
