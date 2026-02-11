# Haul Truck Analytics - Seed Data

This directory contains dimension seed data for the Haul Truck Analytics example, representing a realistic open pit mining fleet configuration.

## Fleet Configuration

### Trucks (dim_truck.csv)
**Total: 12 trucks across 3 size classes**

- **100-ton Class (4 trucks)**
  - TRUCK-101: CAT 777F (100 tons, Caterpillar)
  - TRUCK-102: CAT 777G (100 tons, Caterpillar)
  - TRUCK-103: Komatsu HD785-7 (100 tons, Komatsu)
  - TRUCK-104: Hitachi EH1700-3 (100 tons, Hitachi)

- **200-ton Class (6 trucks)**
  - TRUCK-201: CAT 789D (200 tons, Caterpillar)
  - TRUCK-202: CAT 789C (200 tons, Caterpillar)
  - TRUCK-203: Komatsu HD1500-8 (200 tons, Komatsu)
  - TRUCK-204: Komatsu HD1500-7 (200 tons, Komatsu)
  - TRUCK-205: Liebherr T264 (200 tons, Liebherr)
  - TRUCK-206: Hitachi EH3500AC-3 (200 tons, Hitachi)

- **400-ton Class (2 trucks)**
  - TRUCK-401: CAT 797F (400 tons, Caterpillar)
  - TRUCK-402: Liebherr T282C (400 tons, Liebherr)

### Shovels (dim_shovel.csv)
**Total: 3 shovels, matched to truck capacity classes**

- **SHOVEL-A**: CAT 6040 FS (20 m³ bucket) → North Pit
  - Optimized for 100-ton trucks (4-5 passes to fill)
  
- **SHOVEL-B**: Komatsu PC5500 (35 m³ bucket) → East Pit
  - Optimized for 200-ton trucks (5-6 passes to fill)
  
- **SHOVEL-C**: Liebherr R9800 (60 m³ bucket) → South Pit
  - Optimized for 400-ton trucks (6-7 passes to fill)

**Note:** Bucket-to-truck matching follows industry best practice of 3-6 passes per load cycle, assuming 2.5 tons/m³ material density.

### Crusher (dim_crusher.csv)
**Total: 1 crusher (bottleneck scenario)**

- **CRUSHER-01**: Primary Gyratory Crusher (3000 TPH capacity) → Main Processing Area
  - Single crusher creates realistic queue scenarios
  - 3000 TPH capacity reflects typical primary crusher sizing

### Operators (dim_operator.csv)
**Total: 10 operators with varied experience**

- **Senior (3 operators)**
  - OP-001: James Smith (hired 2018-03-15)
  - OP-002: Maria Garcia (hired 2019-01-20)
  - OP-010: Emily Taylor (hired 2017-11-05)

- **Intermediate (4 operators)**
  - OP-003: Robert Johnson (hired 2020-06-10)
  - OP-004: Jennifer Williams (hired 2021-02-28)
  - OP-005: Michael Brown (hired 2021-09-14)
  - OP-009: Thomas Moore (hired 2022-05-20)

- **Junior (3 operators)**
  - OP-006: Sarah Davis (hired 2023-04-01)
  - OP-007: David Miller (hired 2023-08-15)
  - OP-008: Lisa Wilson (hired 2024-01-10)

### Shifts (dim_shift.csv)
**Total: 2 shifts (12-hour rotation)**

- **SHIFT-DAY**: Day shift (07:00 to 19:00) - 12 hours
- **SHIFT-NIGHT**: Night shift (19:00 to 07:00) - 12 hours

### Date Dimension (dim_date.csv)
**Total: 30 days of operation (January 2026)**

- Start Date: 2026-01-01
- End Date: 2026-01-30
- Includes full date dimension attributes: date_key, full_date, year, quarter, month, week, day_of_week

## Data Characteristics

### Design Rationale

1. **Fleet Mix**: The 4:6:2 ratio (100-ton:200-ton:400-ton) represents a typical mining operation where:
   - Medium trucks (200-ton) form the backbone of the fleet
   - Light trucks (100-ton) handle lighter material or tighter areas
   - Ultra-class trucks (400-ton) maximize productivity in main haul routes

2. **Shovel-Truck Matching**: Each shovel's bucket size is optimized for a specific truck class following the industry standard of 3-6 passes per load. This minimizes cycle time while avoiding overloading.

3. **Single Crusher Bottleneck**: The single crusher configuration creates realistic queue scenarios at the dump point, which is typical in many mining operations.

4. **Operator Experience Mix**: The distribution (30% Senior, 40% Intermediate, 30% Junior) reflects typical mining workforce composition with emphasis on developing operators.

5. **12-Hour Shifts**: Standard mining operation pattern allowing continuous 24/7 operations with manageable crew rotations.

## Usage

These seed files are designed to work with Gorchata's seed import functionality. The `seed.yml` configuration file specifies:

```yaml
version: 1
naming:
  strategy: filename
import:
  batch_size: 1000
  scope: folder
```

This configuration uses filename-based table name inference (e.g., `dim_truck.csv` → `dim_truck` table).

## Testing

All seed data is validated by tests in `test/haul_truck_seed_test.go`:
- Fleet composition (12 trucks with correct size distribution)
- Shovel-truck capacity matching (3-6 pass rule)
- Single crusher bottleneck
- Shift timing (12-hour boundaries)
- Operator count and experience distribution
- Date dimension completeness and continuity

Run tests with:
```bash
go test ./test -run "^Test(Fleet|Shovel|Crusher|Shift|Operator|Date)" -v
```

## Next Phase

Phase 3 will add telemetry generation for staging tables (stg_telemetry_events) representing GPS, payload, and operational data from trucks during haul cycles. Phase 2 focused on static dimension data only.
