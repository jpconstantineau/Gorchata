# Haul Truck Analytics Data Warehouse

## Overview

This example demonstrates a comprehensive **data warehouse implementation** for open pit mining haul truck operations. It showcases Gorchata's capabilities in building complex star schema data warehouses with:

- **6 dimension tables** (truck, shovel, crusher, operator, shift, date)
- **2 staging tables** (telemetry events, truck states)
- **1 fact table** (haul cycles)
- **5 metrics aggregations** (truck productivity, shovel utilization, crusher throughput, queue analysis, fleet summary)
- **6 analytical queries** (worst performing trucks, bottleneck analysis, payload compliance, shift performance, fuel efficiency, operator performance)
- **4 data validation checks** (referential integrity, temporal consistency, business rules, state transitions)

The example simulates realistic mining operations for a mixed fleet of 12 haul trucks (4× 100-ton, 6× 200-ton, 2× 400-ton) operating with 3 shovels and 1 crusher, tracking complete haul cycles from shovel loading through crusher dumping and return.

## Business Context

### Open Pit Mining Operations

Open pit mining uses **haul trucks** to transport ore and waste material from loading points (shovels) to processing facilities (crushers) or waste dumps. This example focuses on ore hauling operations where:

- **Shovels** load material into haul trucks
- **Haul trucks** transport material to the crusher
- **Crushers** process the material (primary crushing)
- **Trucks return empty** to repeat the cycle

Operational efficiency depends on optimizing:
- **Cycle time** (loading → hauling → dumping → returning)
- **Payload utilization** (85-105% of rated capacity)
- **Fleet productivity** (tons per hour)
- **Bottleneck management** (queue time at shovels and crusher)

### Haul Cycle Overview

A complete **haul cycle** consists of these distinct operational states:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         HAUL CYCLE STATES                                │
└─────────────────────────────────────────────────────────────────────────┘

  ┌──────────────┐     ┌─────────┐     ┌──────────────┐     ┌─────────┐
  │  Queued at   │────▶│ Loading │────▶│   Hauling    │────▶│ Queued  │
  │    Shovel    │     │ (4-8min)│     │Loaded (12-25m)│     │at Crusher│
  └──────────────┘     └─────────┘     └──────────────┘     └────┬────┘
         ▲                                                         │
         │                                                         ▼
  ┌──────────────┐                                          ┌─────────┐
  │  Returning   │◀────────────────────────────────────────│ Dumping │
  │Empty (8-18m) │                                          │ (1-2min)│
  └──────────────┘                                          └─────────┘

  Optional at any point: Spot Delays (refueling, inspections, minor repairs)
```

**State Descriptions:**
- **Queued at Shovel**: Waiting for loading equipment availability
- **Loading**: Shovel filling truck (3-6 passes, 4-8 minutes)
- **Hauling Loaded**: Traveling to crusher with payload (12-25 minutes @ 20-35 km/h)
- **Queued at Crusher**: Waiting at dumping point (crusher bottleneck)
- **Dumping**: Unloading material at crusher (1-2 minutes)
- **Returning Empty**: Traveling back to shovel (8-18 minutes @ 30-50 km/h)
- **Spot Delay**: Unplanned stops (refueling after 10-12 engine hours, minor issues)

### Key Performance Indicators

**Productivity Metrics:**
- **Tons per Hour**: Material moved per unit time (target: 150-200 TPH for 200-ton trucks)
- **Cycle Time**: Complete cycle duration (target: 30-50 minutes depending on distance)
- **Cycles per Shift**: Number of complete cycles (target: 10-14 cycles per 12-hour shift)

**Utilization Metrics:**
- **Truck Utilization**: Productive time vs available time (target: >80%)
- **Payload Utilization**: Actual payload vs rated capacity (target: 95-105%)
- **Shovel Utilization**: Loading time vs idle time (target: >70%)
- **Crusher Utilization**: Receiving time vs capacity (monitor for bottleneck)

**Queue Metrics:**
- **Queue Time**: Time spent waiting at shovel or crusher (target: <10% of cycle time)
- **Queue Hours Lost**: Total hours lost to queuing (cost impact indicator)
- **Bottleneck Location**: Shovel vs crusher queue patterns

**Efficiency Metrics:**
- **Fuel Efficiency**: Liters per ton hauled (benchmark: 0.8-1.2 L/ton for 200-ton class)
- **Speed Metrics**: Loaded vs empty speed (loaded should be 60-70% of empty speed)
- **Spot Delay Frequency**: Unplanned stops per shift (target: <5% of productive time)

## Architecture

### Star Schema Design

```
                    ┌─────────────┐
                    │  dim_date   │
                    │ (temporal)  │
                    └──────┬──────┘
                           │
    ┌──────────┐    ┌─────┴──────┐    ┌──────────────┐
    │dim_truck ├────┤   FACTS    ├────┤ dim_shovel   │
    │(12 trucks)│    │            │    │  (3 shovels) │
    └──────────┘    │fact_haul_  │    └──────────────┘
                    │  _cycle     │
    ┌──────────┐    │            │    ┌──────────────┐
    │dim_operator├──┤            ├────┤ dim_crusher  │
    │(10 operators)│ │            │    │  (1 crusher) │
    └──────────┘    └─────┬──────┘    └──────────────┘
                           │
                    ┌──────┴──────┐
                    │  dim_shift  │
                    │  (Day/Night)│
                    └─────────────┘
```

### Table Categories

**Dimensions (6 tables)**:
- `dim_truck`: 12 trucks across 3 size classes (100-ton, 200-ton, 400-ton)
- `dim_shovel`: 3 shovels matched to truck capacities
- `dim_crusher`: 1 primary crusher (bottleneck by design)
- `dim_operator`: 10 operators with experience levels (Junior/Intermediate/Senior)
- `dim_shift`: 2 shifts (Day 07:00-19:00, Night 19:00-07:00)
- `dim_date`: Date dimension for time-based analysis

**Staging (2 tables)**:
- `stg_telemetry_events`: Raw GPS, payload, speed, fuel sensor data (5-10 second intervals)
- `stg_truck_states`: Detected operational states using geofence and payload thresholds

**Facts (1 table)**:
- `fact_haul_cycle`: Complete haul cycles with durations, distances, payload, fuel consumption

**Metrics (5 tables)**:
- `truck_daily_productivity`: Tons moved, cycle count, average cycle time per truck per day
- `shovel_utilization`: Loading hours vs idle time by shovel
- `crusher_throughput`: Tons per hour, queue times, truck arrival patterns
- `queue_analysis`: Queue time analysis by location (shovel/crusher) and shift
- `fleet_summary`: Fleet-wide rollup by shift and date

**Analytics (6 tables)**:
- `worst_performing_trucks`: Trucks with lowest productivity
- `bottleneck_analysis`: Shovel vs crusher constraint identification
- `payload_compliance`: Underload and overload patterns
- `shift_performance`: Day vs night shift comparison
- `fuel_efficiency`: Fuel consumption analysis by truck class
- `operator_performance`: Operator efficiency rankings

**Validations (4 tables)**:
- `test_referential_integrity`: Foreign key validation
- `test_temporal_consistency`: Time ordering and overlap checks
- `test_business_rules`: Domain rule compliance (payload limits, speed limits, etc.)
- `test_state_transitions`: Valid operational state sequences

**Total: 24 tables** providing complete operational analytics

### Data Flow

```
Raw Telemetry (GPS, Weight, Speed, Fuel)
    │
    ▼
stg_telemetry_events (5-10 second intervals)
    │
    ▼ State Detection Algorithm
stg_truck_states (operational states with durations)
    │
    ▼ Cycle Aggregation
fact_haul_cycle (complete cycles from loading to loading)
    │
    ▼ Metrics Rollup
Metrics (productivity, utilization, queue analysis)
    │
    ▼ Analytical Queries
Analytics (insights, rankings, bottlenecks, trends)
```

## Quick Start

### Prerequisites

- **Go 1.25 or higher**
- **Gorchata installed**: `go install github.com/jpconstantineau/gorchata/cmd/gorchata@latest`
- **Terminal** (PowerShell on Windows, bash on Linux/Mac)

### Running the Example

**1. Navigate to the example directory:**

```powershell
cd examples/haul_truck_analytics
```

**2. View the seed data (dimensions only - telemetry would be generated separately):**

```powershell
# Dimension tables are pre-populated
# See seeds/*.csv for truck, shovel, crusher, operator, shift, date data
# See seeds/README.md for fleet configuration details
```

**3. Initialize the database and load seed data:**

```powershell
gorchata init --schema schema.yml --seed seeds/seed.yml --db haul_truck.db
```

This creates:
- SQLite database: `haul_truck.db`
- All dimension tables loaded from CSV files
- Schema structure for staging and fact tables

**4. Generate telemetry and run state detection (requires telemetry generator):**

Note: This example includes the schema and SQL transformation logic. A telemetry generator would produce `stg_telemetry_events` data based on realistic haul cycle patterns.

**5. Run transformations:**

```powershell
# Run staging transformation (state detection)
gorchata test --profile profiles.yml --model stg_truck_states

# Run fact aggregation (cycle assembly)
gorchata test --profile profiles.yml --model fact_haul_cycle

# Run metrics aggregations
gorchata test --profile profiles.yml --model truck_daily_productivity
gorchata test --profile profiles.yml --model shovel_utilization
gorchata test --profile profiles.yml --model crusher_throughput
gorchata test --profile profiles.yml --model queue_analysis
gorchata test --profile profiles.yml --model fleet_summary

# Run analytical queries
gorchata test --profile profiles.yml --model worst_performing_trucks
gorchata test --profile profiles.yml --model bottleneck_analysis
gorchata test --profile profiles.yml --model payload_compliance
gorchata test --profile profiles.yml --model shift_performance
gorchata test --profile profiles.yml --model fuel_efficiency
gorchata test --profile profiles.yml --model operator_performance
```

**6. Query results:**

```bash
# View worst performing trucks
sqlite3 haul_truck.db "SELECT * FROM worst_performing_trucks LIMIT 5;"

# Check bottleneck analysis
sqlite3 haul_truck.db "SELECT * FROM bottleneck_analysis;"

# Analyze payload compliance
sqlite3 haul_truck.db "SELECT * FROM payload_compliance;"
```

**7. Run data quality tests:**

```powershell
gorchata test --profile profiles.yml --model test_referential_integrity
gorchata test --profile profiles.yml --model test_temporal_consistency
gorchata test --profile profiles.yml --model test_business_rules
gorchata test --profile profiles.yml --model test_state_transitions
```

### Alternative: Use PowerShell Build Script

```powershell
# Run all tests and transformations
.\scripts\build_test.ps1 -Example haul_truck_analytics
```

## Key Metrics Explained

### Productivity Metrics

**Tons per Hour (TPH)**
- **Definition**: Material moved per unit time
- **Calculation**: `total_payload_tons / (total_productive_hours)`
- **Targets**: 
  - 100-ton trucks: 80-120 TPH
  - 200-ton trucks: 150-200 TPH
  - 400-ton trucks: 280-350 TPH
- **Interpretation**: Higher is better; compare to fleet class benchmarks

**Cycle Time**
- **Definition**: Complete cycle duration (loading → dumping → return)
- **Components**: Loading + Hauling Loaded + Queue Crusher + Dumping + Returning Empty + Queue Shovel
- **Targets**:
  - Short haul (3-5 km): 25-35 minutes
  - Medium haul (5-8 km): 35-50 minutes
  - Long haul (8-12 km): 50-75 minutes
- **Interpretation**: Lower is better; look for trends and outliers

**Cycles per Shift**
- **Definition**: Number of complete cycles in 12-hour shift
- **Targets**: 
  - 10-14 cycles (medium haul distance)
  - 14-18 cycles (short haul distance)
- **Interpretation**: Higher is better; accounts for shift delays and refueling

### Utilization Metrics

**Truck Utilization**
- **Definition**: Productive time as % of available time
- **Calculation**: `(productive_hours / scheduled_shift_hours) * 100`
- **Target**: >80%
- **Interpretation**: Below target indicates excessive delays or idle time

**Payload Utilization**
- **Definition**: Actual payload as % of rated capacity
- **Bands**:
  - Underload: <85% (inefficient, wasted cycles)
  - Suboptimal: 85-95% (acceptable but not ideal)
  - Optimal: 95-105% (target range)
  - Overload: >105% (safety concern, equipment stress)
- **Target**: 95-105%
- **Interpretation**: Consistent overloading indicates shovel-truck mismatch

### Queue Metrics

**Queue Time**
- **Definition**: Time spent waiting at shovel or crusher
- **Measurement**: Minutes per cycle at each queue point
- **Targets**: 
  - Shovel queue: <3 minutes average
  - Crusher queue: <8 minutes average (acceptable if crusher is constraint)
- **Interpretation**: Identifies bottleneck location

**Queue Hours Lost**
- **Definition**: Total hours lost to queuing across fleet
- **Calculation**: `sum(queue_time_hours) across all trucks`
- **Impact**: Direct cost (fuel burned while idle, opportunity cost)
- **Action**: High queue hours indicate need for capacity expansion

### Efficiency Metrics

**Fuel Efficiency**
- **Definition**: Fuel consumed per ton hauled
- **Calculation**: `fuel_consumed_liters / payload_tons`
- **Benchmarks**:
  - 100-ton: 1.0-1.5 L/ton
  - 200-ton: 0.8-1.2 L/ton
  - 400-ton: 0.6-0.9 L/ton
- **Interpretation**: Higher consumption may indicate mechanical issues or poor driving practice

## Analytical Queries

### 1. Worst Performing Trucks

**Business Question**: Which trucks should be prioritized for maintenance or operator training?

**Query Location**: `models/analytics/worst_performing_trucks.sql`

**Key Insights**:
- Trucks with lowest tons per hour
- High cycle time variability
- Frequent spot delays
- Poor payload utilization

**Action Items**: Schedule maintenance, review operator assignment, analyze route efficiency

### 2. Bottleneck Analysis

**Business Question**: Is the shovel or crusher the system constraint?

**Query Location**: `models/analytics/bottleneck_analysis.sql`

**Key Insights**:
- Average queue time by location
- Utilization rates (loading vs receiving)
- Constraint identification (highest queue time indicates bottleneck)

**Action Items**: 
- If crusher is bottleneck: Consider crusher upgrades, optimize dispatch to smooth arrivals
- If shovel is bottleneck: Add loading equipment, improve truck-shovel matching

### 3. Payload Compliance

**Business Question**: Are trucks loaded appropriately? Are there safety concerns?

**Query Location**: `models/analytics/payload_compliance.sql`

**Key Insights**:
- Frequency of underloading (<85%)
- Frequency of overloading (>105%)
- Patterns by truck, shovel, or operator

**Action Items**: 
- Underloading: Retrain operators, verify bucket fill factors
- Overloading: Safety concern - enforce limits, check shovel operators

### 4. Shift Performance

**Business Question**: Are there performance differences between day and night shifts?

**Query Location**: `models/analytics/shift_performance.sql`

**Key Insights**:
- Cycle time comparison (day vs night)
- Productivity differences
- Queue time patterns by shift

**Action Items**: Investigate root causes (lighting, crew experience, equipment availability)

### 5. Fuel Efficiency

**Business Question**: Which trucks or operators consume excessive fuel?

**Query Location**: `models/analytics/fuel_efficiency.sql`

**Key Insights**:
- Liters per ton by truck
- Fuel consumed per cycle
- Operator fuel efficiency patterns

**Action Items**: Maintenance for high consumers, operator coaching, route optimization

### 6. Operator Performance

**Business Question**: Which operators are most efficient? Who needs coaching?

**Query Location**: `models/analytics/operator_performance.sql`

**Key Insights**:
- Cycle time efficiency by operator
- Payload utilization by operator
- Ranking within experience level

**Action Items**: Recognize top performers, provide targeted training for bottom quartile

## Example Insights

### Sample Findings from Analysis

**Finding 1: Crusher is the System Bottleneck**
- Analysis shows average crusher queue time of 12 minutes vs shovel queue time of 2 minutes
- Crusher utilization at 95%, shovel utilization at 68%
- **Recommendation**: Optimize truck dispatch to smooth crusher arrivals; consider crusher capacity upgrade if capital available

**Finding 2: 400-ton Trucks Underutilized**
- Only 2 trucks in 400-ton class, but SHOVEL-C (matched to them) has low utilization (52%)
- High shovel queue times indicate insufficient 400-ton truck supply
- **Recommendation**: Add 1-2 more 400-ton trucks to match SHOVEL-C capacity

**Finding 3: Night Shift 15% Less Productive**
- Night shift averages 40-minute cycles vs 35-minute day shift
- Primary factor: longer crusher queue times at night (crew scheduling issue)
- **Recommendation**: Adjust crusher crew scheduling to ensure consistent capacity across shifts

**Finding 4: 3 Trucks with Excessive Fuel Consumption**
- TRUCK-203, TRUCK-204, TRUCK-401 consuming 25-30% more fuel per ton than fleet average
- **Recommendation**: Schedule maintenance inspections; check for engine, transmission, or tire issues

**Finding 5: Junior Operators 20% Slower**
- Junior operators average 45-minute cycles vs 37-minute for senior operators
- Primarily due to slower loading (poor spotting) and conservative haul speeds
- **Recommendation**: Implement mentorship program pairing junior operators with senior operators

## Troubleshooting

### Issue: Database file not found

**Symptom**: `Error: unable to open database file`

**Solution**: 
```powershell
# Ensure you've run init first
gorchata init --schema schema.yml --seed seeds/seed.yml --db haul_truck.db
```

### Issue: No data in staging tables

**Symptom**: `stg_truck_states` has 0 rows

**Solution**: This example requires telemetry data generation:
- The seed data only includes dimensions (trucks, shovels, etc.)
- Telemetry events (`stg_telemetry_events`) would be generated by a separate telemetry simulator
- State detection (`stg_truck_states`) processes telemetry to identify operational states

### Issue: Referential integrity errors

**Symptom**: Data quality tests show foreign key violations

**Solution**: 
```powershell
# Run referential integrity test to see specific issues
gorchata test --profile profiles.yml --model test_referential_integrity

# Verify all dimension tables are populated
sqlite3 haul_truck.db "SELECT COUNT(*) FROM dim_truck;"
```

### Issue: Cycle times seem unrealistic

**Symptom**: Very long or very short cycle times

**Solution**: Check for:
- Incomplete state detection (missing states in cycle)
- Incorrect geofence zone definitions
- Payload threshold tuning (80% loaded, 20% empty may need adjustment)

### Clean Slate / Reset

To start fresh:

```powershell
# Remove database
Remove-Item haul_truck.db -ErrorAction SilentlyContinue

# Re-initialize
gorchata init --schema schema.yml --seed seeds/seed.yml --db haul_truck.db
```

## Project Structure

```
examples/haul_truck_analytics/
├── README.md                    # This file
├── ARCHITECTURE.md              # Technical architecture details
├── METRICS.md                   # Comprehensive KPI definitions
├── schema.yml                   # Schema definition (dimensions, staging, facts)
│
├── seeds/                       # Dimension seed data
│   ├── seed.yml                 # Seed configuration
│   ├── README.md                # Fleet configuration details
│   ├── dim_truck.csv            # 12 trucks across 3 size classes
│   ├── dim_shovel.csv           # 3 shovels matched to truck capacities
│   ├── dim_crusher.csv          # 1 crusher (bottleneck design)
│   ├── dim_operator.csv         # 10 operators with experience levels
│   ├── dim_shift.csv            # Day/Night shifts
│   └── dim_date.csv             # Date dimension
│
├── models/                      # SQL transformation models
│   ├── staging/
│   │   └── stg_truck_states.sql        # State detection from telemetry
│   ├── facts/
│   │   └── fact_haul_cycle.sql         # Cycle aggregation
│   ├── metrics/
│   │   ├── truck_daily_productivity.sql
│   │   ├── shovel_utilization.sql
│   │   ├── crusher_throughput.sql
│   │   ├── queue_analysis.sql
│   │   └── fleet_summary.sql
│   └── analytics/
│       ├── worst_performing_trucks.sql
│       ├── bottleneck_analysis.sql
│       ├── payload_compliance.sql
│       ├── shift_performance.sql
│       ├── fuel_efficiency.sql
│       └── operator_performance.sql
│
└── tests/                       # Data quality tests
    ├── test_referential_integrity.sql
    ├── test_temporal_consistency.sql
    ├── test_business_rules.sql
    └── test_state_transitions.sql
```

## Next Steps

1. **Explore Schema**: Review `schema.yml` to understand table structures and data tests
2. **Study Transformations**: Read SQL models to understand state detection and cycle aggregation logic
3. **Review Metrics**: See `METRICS.md` for detailed KPI definitions and calculation formulas
4. **Understand Architecture**: Read `ARCHITECTURE.md` for technical design decisions and data flow
5. **Run Data Quality Tests**: Verify data integrity with built-in validation checks
6. **Customize for Your Mine**: Adapt fleet configuration, geofence zones, and thresholds to your operation

## Related Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) - Technical architecture and design decisions
- [METRICS.md](METRICS.md) - Comprehensive KPI catalog and calculation formulas
- [seeds/README.md](seeds/README.md) - Fleet configuration details

## References

### Industry Standards

- **SME Mining Engineering Handbook** - Haulage systems design
- **CRC Mining** - Haul truck productivity benchmarks
- **Caterpillar Performance Handbook** - Equipment specifications and cycle time estimates

### Related Examples

- [Unit Train Analytics](../unit_train_analytics/README.md) - Similar pattern applied to railroad operations
- [DCS Alarm Analytics](../dcs_alarm_example/README.md) - Time-series event processing
- [Bottleneck Analysis](../bottleneck_analysis/README.md) - Theory of Constraints applications

---

**Note**: This example demonstrates the data warehouse schema and SQL transformation logic. Production implementations would include:
- Real-time telemetry ingestion from fleet management systems
- Integration with dispatch systems for real-time optimization
- Dashboards and visualization layers (Power BI, Tableau, etc.)
- Alerting for anomalies (overloading, excessive cycle times, etc.)
- Predictive maintenance models using historical performance data
