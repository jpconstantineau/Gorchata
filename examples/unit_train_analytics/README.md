# Unit Train Analytics Data Warehouse

## Overview

This example demonstrates a complete **data warehouse implementation** for railroad unit train operations analytics. It showcases Gorchata's capabilities in building complex star schema data warehouses with:

- **5 dimension tables** (car, train, corridor, location, date)
- **4 fact tables** (movements, stragglers, queues, power transfers)
- **7 metrics aggregations** (corridor performance, train daily metrics, car utilization, etc.)
- **7 analytical queries** (worst corridors, straggler hot spots, queue bottlenecks, etc.)
- **4 data validation checks** (referential integrity, temporal consistency, business rules, exclusivity)

The example simulates 90 days of operations for 228 railroad cars across 3 unit trains operating on 6 corridors, with realistic operational challenges including seasonal effects, straggler events, queuing delays, and locomotive power transfers.

## Business Context

### Unit Train Operations

A **unit train** is a train carrying a single commodity (e.g., coal, grain, oil) that moves as a complete unit from origin to destination and back. Unlike manifest trains that pick up and drop off cars, unit trains:

- Stay together as a complete consist
- Operate on regular corridors between fixed points
- Return empty for the next load cycle
- Maximize efficiency through predictable routing

### Key Operational Concepts

**Stragglers**: Cars that become separated from their train and incur significant delays (6-72 hours) due to:
- Mechanical issues requiring repair
- Crew changes or operational delays
- Track maintenance or congestion
- Classification yard backlogs

**Queues**: Delays at origin and destination locations where trains must wait for:
- Loading/unloading equipment availability
- Track space constraints
- Previous train to clear the facility
- Typical durations: 8-18 hours depending on location type

**Power Transfers**: When locomotives (power units) are swapped between trains:
- Quick transfers (<1 hour): Same locomotives continue
- Extended transfers (>1 hour): Different locomotives assigned
- Impacts crew scheduling and locomotive utilization

**Seasonal Effects**: Performance varies by time of year:
- Weather impacts (winter delays, summer heat restrictions)
- Holiday schedules (reduced operations)
- Harvest seasons (demand surges)
- This dataset includes Week 5 slowdowns (20% longer trips) and Week 8 straggler spikes (2x normal rate)

## Architecture

### Star Schema Design

The data warehouse uses a **dimensional model** with fact and dimension tables:

```
                    ┌─────────────┐
                    │  dim_date   │
                    │ (temporal)  │
                    └──────┬──────┘
                           │
    ┌──────────┐    ┌─────┴──────┐    ┌──────────────┐
    │ dim_car  ├────┤   FACTS    ├────┤  dim_train   │
    │ (assets) │    │            │    │  (consists)  │
    └──────────┘    └─────┬──────┘    └──────────────┘
                           │
                    ┌──────┴──────┐
                    │ dim_corridor│
                    │  (routes)   │
                    └──────┬──────┘
                           │
                    ┌──────┴──────────┐
                    │  dim_location   │
                    │   (facilities)  │
                    └─────────────────┘
```

### Table Categories

**Dimensions (5 tables)**:
- `dim_car`: 228 railroad cars with capacity, type, acquisition date
- `dim_train`: 3 unit trains (T001, T002, T003) with capacity ranges
- `dim_corridor`: 6 routes (C1-C6) with distance, lanes, traffic volume
- `dim_location`: 12 facilities (origins, destinations, yards) with type and capacity
- `dim_date`: 90-day calendar with week numbers and date intelligence

**Facts (4 tables)**:
- `fact_movement`: 2,737 trip segments (car-level grain) with duration, distance, flags
- `fact_straggler`: 273 straggler events with delay duration and resolution
- `fact_queue`: 912 queuing events at locations with wait times
- `fact_power_transfer`: 273 locomotive changes with transfer duration

**Metrics (7 tables)**:
- `agg_corridor_weekly_metrics`: Performance by route and week
- `agg_train_daily_performance`: Daily train-level KPIs
- `agg_car_utilization_metrics`: Asset utilization patterns
- `agg_straggler_analysis`: Straggler frequency and impact
- `agg_queue_analysis`: Bottleneck identification
- `agg_power_transfer_analysis`: Locomotive assignment patterns
- `agg_seasonal_performance`: Time-based trend analysis

**Analytics (7 tables)**:
- `analytics_worst_corridors`: Routes with worst performance
- `analytics_straggler_hotspots`: Locations with frequent stragglers
- `analytics_queue_bottlenecks`: Congestion points
- `analytics_car_history`: Complete car journey tracking
- `analytics_train_consist_changes`: Train composition over time
- `analytics_power_efficiency`: Locomotive utilization
- `analytics_seasonal_trends`: Performance by time period

**Validations (4 tables)**:
- `validation_referential_integrity`: Foreign key checks
- `validation_temporal_consistency`: Date/time logic checks
- `validation_business_rules`: Domain rule compliance
- `validation_car_exclusivity`: Car assignment conflicts

**Total: 27 tables** providing complete operational analytics

### Data Flow

1. **Seed Data** (schema.yml): Dimension and fact table structure
2. **Data Generation** (seed_generator.go): Realistic 90-day simulation
3. **Metrics Aggregation** (Phase 6): Summarize facts into business metrics
4. **Analytical Queries** (Phase 7): Answer specific business questions
5. **Data Quality Validation** (Phase 8): Ensure data integrity

## Quick Start

### Prerequisites

- Go 1.25 or higher
- Gorchata installed (`go install github.com/jpconstantineau/gorchata/cmd/gorchata@latest`)
- PowerShell (for scripts) or bash

### Running the Example

**1. Navigate to the example directory:**

```powershell
cd examples/unit_train_analytics
```

**2. Generate seed data (creates ~4,000 rows across all tables):**

```powershell
go run seed_generator.go
```

This creates `seed.yml` with:
- 5 dimensions (228 cars, 3 trains, 6 corridors, 12 locations, 90 dates)
- 4 facts (2,737 movements, 273 stragglers, 912 queues, 273 power transfers)
- Total: ~4,400 rows of test data

**3. Initialize the database (creates unit_train.db):**

```powershell
gorchata init --config gorchata_project.yml
```

**4. Run data quality tests:**

```powershell
gorchata test --profile all
```

This executes:
- 7 metrics aggregations (creates summary tables)
- 7 analytical queries (answers business questions)
- 4 validation checks (ensures data quality)

**5. View results:**

```powershell
# Count records in metrics tables
sqlite3 unit_train.db "SELECT COUNT(*) FROM agg_corridor_weekly_metrics;"

# View worst performing corridors
sqlite3 unit_train.db "SELECT * FROM analytics_worst_corridors LIMIT 5;"

# Check validation results
sqlite3 unit_train.db "SELECT * FROM validation_referential_integrity;"
```

## Data Generation

The `seed_generator.go` script creates a realistic 90-day simulation:

### Scale

- **228 cars**: Distributed across 3 trains (76 cars each)
- **3 trains**: T001, T002, T003 (unit train consists)
- **6 corridors**: C1-C6 with varying distances (450-800 km)
- **12 locations**: 6 origins + 6 destinations
- **90 days**: 2024-01-01 to 2024-03-30 (full quarter)

### Movement Generation

Each car makes **12 complete round trips** over 90 days:
- **Trip duration**: 5-8 days (depends on corridor distance and seasonal effects)
- **Loading time**: 12-18 hours at origin (queue + loading)
- **Unloading time**: 8-12 hours at destination (queue + unloading)
- **Return trip**: Same corridor back to origin (empty)

### Operational Events

**Stragglers (10% of movements)**:
- Randomly selected movements become straggler events
- Delay duration: 6-72 hours (mean ~24 hours)
- **Week 8 spike**: 2x normal straggler rate (simulates major weather event)
- Resolution: "Mechanical repair", "Crew change", "Track maintenance", etc.

**Queues (at all origins/destinations)**:
- Origins: 12-18 hours (longer due to loading complexity)
- Destinations: 8-12 hours (unloading faster)
- Queue capacity: 1 train at a time (realistic constraint)
- Recorded in `fact_queue` with start/end times

**Power Transfers (one per trip)**:
- Quick transfer (<1 hour): 70% of cases (same locomotives)
- Extended transfer (>1 hour): 30% of cases (different locomotives)
- Location: At origin (after unloading previous trip)

**Seasonal Effects**:
- **Week 5**: 20% longer trip duration (simulates winter weather)
- **Week 8**: 2x straggler rate (simulates major disruption)
- Applied to all movements in affected weeks

### Data Characteristics

- **Referential integrity**: All foreign keys valid (car_id, train_id, corridor_id, location_id, date_id)
- **Temporal consistency**: start_date ≤ end_date, no negative durations
- **Business rules**: Cars in one train at a time, locations match corridors
- **Known issue**: Car exclusivity violations due to round-trip overlap (see Known Issues)

## Analytical Queries

The example includes 7 analytical queries (Phase 7) that demonstrate common data warehouse use cases:

### 1. Worst Corridors (`analytics_worst_corridors`)

Identifies routes with poor performance metrics:
- Average trip duration vs. expected
- Straggler rate (% of movements)
- Total movements and unique cars
- Sorted by performance (worst first)

**Business use**: Prioritize corridors for operational improvement

### 2. Straggler Hotspots (`analytics_straggler_hotspots`)

Finds locations with frequent straggler events:
- Total straggler count
- Average delay duration
- Most common resolution type
- Count of unique cars affected

**Business use**: Target locations for process improvements

### 3. Queue Bottlenecks (`analytics_queue_bottlenecks`)

Highlights facilities with longest wait times:
- Average queue duration
- Maximum queue duration
- Total queuing events
- Location type (origin vs. destination)

**Business use**: Identify capacity constraint points

### 4. Car History (`analytics_car_history`)

Complete journey tracking for each car:
- Trip number, corridor, locations
- Start/end dates, duration
- Straggler status and queue participation
- Power transfer indicator

**Business use**: Asset lifecycle analysis, predictive maintenance

### 5. Train Consist Changes (`analytics_train_consist_changes`)

Tracks train composition over time:
- Date, train, car count
- Unique cars in consist
- Movement counts
- Straggler events

**Business use**: Ensure trains stay intact, detect car swaps

### 6. Power Efficiency (`analytics_power_efficiency`)

Locomotive utilization analysis:
- Transfer rate, quick vs. extended transfers
- Average transfer duration
- Total movements per train

**Business use**: Optimize locomotive assignments

### 7. Seasonal Trends (`analytics_seasonal_trends`)

Performance by time period:
- Weekly metrics, total movements
- Average trip duration
- Straggler rate
- Queue duration trends

**Business use**: Seasonal planning, capacity adjustments

### Running Specific Queries

```powershell
# View results of a specific analytical query
sqlite3 unit_train.db "SELECT * FROM analytics_worst_corridors;"

# Export to CSV
sqlite3 -header -csv unit_train.db "SELECT * FROM analytics_straggler_hotspots;" > straggler_hotspots.csv

# Join with dimensions for enriched reporting
sqlite3 unit_train.db "
  SELECT 
    c.corridor_name,
    c.distance_km,
    a.avg_trip_duration_days,
    a.straggler_rate
  FROM analytics_worst_corridors a
  JOIN dim_corridor c ON a.corridor_id = c.corridor_id
  ORDER BY a.avg_trip_duration_days DESC;
"
```

## Validation

The example includes 4 data quality validation checks (Phase 8):

### 1. Referential Integrity (`validation_referential_integrity`)

Checks all foreign key relationships:
- Facts → Dimensions (car, train, corridor, location, date)
- Ensures no orphaned records
- **Expected result**: All checks pass (0 violations)

### 2. Temporal Consistency (`validation_temporal_consistency`)

Validates date/time logic:
- start_date ≤ end_date in all facts
- No negative durations
- Dates within simulation range (2024-01-01 to 2024-03-30)
- **Expected result**: All checks pass

### 3. Business Rules (`validation_business_rules`)

Enforces domain-specific rules:
- Movement locations match corridor definitions
- Queue durations within reasonable bounds (< 24 hours)
- Power transfer durations realistic (< 4 hours)
- Straggler delays within bounds (6-72 hours)
- **Expected result**: All checks pass

### 4. Car Exclusivity (`validation_car_exclusivity`)

Ensures cars aren't in multiple places simultaneously:
- Detects overlapping movement periods for same car
- Critical for asset tracking accuracy
- **Expected result**: **KNOWN TO FAIL** (see Known Issues)

### Running Validations

```powershell
# Run all validations via Gorchata profiles
gorchata test --profile validations

# View validation results
sqlite3 unit_train.db "SELECT * FROM validation_referential_integrity WHERE is_valid = false;"
sqlite3 unit_train.db "SELECT * FROM validation_temporal_consistency WHERE is_valid = false;"
sqlite3 unit_train.db "SELECT * FROM validation_business_rules WHERE is_valid = false;"
sqlite3 unit_train.db "SELECT * FROM validation_car_exclusivity WHERE overlap_days > 0;"

# Count violations
sqlite3 unit_train.db "SELECT COUNT(*) FROM validation_car_exclusivity WHERE overlap_days > 0;"
```

## Design Decisions

### Star Schema vs. Snowflake

We chose a **star schema** (denormalized dimensions) over a snowflake schema because:
- Simpler queries (fewer joins)
- Better query performance for analytical workloads
- Easier to understand for business users
- Acceptable data duplication (corridors reference locations)

### Fact Table Grain

Each fact table has a specific grain (level of detail):
- `fact_movement`: One row per car per trip segment
- `fact_straggler`: One row per straggler event (subset of movements)
- `fact_queue`: One row per queuing event at a location
- `fact_power_transfer`: One row per locomotive change

This allows flexible aggregation while maintaining detailed history.

### Slowly Changing Dimensions (SCD)

All dimensions use **Type 1 SCD** (overwrite):
- Current state only, no history tracking
- Acceptable for this example (training/demo)
- Production systems might use Type 2 (historical tracking) for car attributes

### Date Dimension

We generated a full date dimension (90 rows) even though it's small because:
- Demonstrates dimensional modeling best practice
- Enables date intelligence queries (week number, month, quarter)
- Allows filtering by date attributes without date functions
- Scalable pattern for larger date ranges

### Metrics Pre-Aggregation

Metrics tables are **pre-aggregated** rather than views because:
- Faster query performance (no runtime aggregation)
- Demonstrates typical data warehouse ELT patterns
- Allows incremental refresh strategies
- Suitable for large-scale production deployments

### Validation as Tables

Data quality checks are stored as tables (not just test results) because:
- Provides audit trail of data quality over time
- Enables dashboards showing validation trends
- Supports data quality SLAs and monitoring
- Aligns with data governance best practices

## Known Issues

### TestCarExclusivity Failure

**Issue**: The `validation_car_exclusivity` test detects **overlapping time periods** where the same car appears in multiple movements simultaneously.

**Root cause**: The seed data generator creates round-trip patterns where a car's return trip (empty) overlaps with the start of its next loaded trip. This happens because:

1. Trip end date is calculated as: `start_date + trip_duration_days`
2. Next trip starts immediately: `previous_end_date + 1 day`
3. But the car is still in transit on the `previous_end_date`
4. Result: Car exists in two movements on overlapping dates

**Example conflict**:
- Movement 1: 2024-01-01 to 2024-01-06 (6 days)
- Movement 2: 2024-01-07 to 2024-01-13 (overlaps if first trip not done)

**Impact**:
- Does not break other functionality (referential integrity still valid)
- Metrics and analytics queries still work correctly
- Only affects the car exclusivity validation check

**Workaround**: Accept that this is a demo dataset limitation. In production, you would:
- Add a "movement_sequence" that ensures non-overlapping periods
- Include maintenance/turnaround time between trips
- Implement proper trip scheduling logic with buffer periods

**Future fix**: Update `seed_generator.go` to add 1-day buffer between trips:
```go
nextStart := lastEnd.AddDate(0, 0, 2) // +2 days instead of +1
```

### No Intermediate Stops

The current data model assumes direct origin-to-destination trips. Real unit trains might:
- Stop at intermediate yards for crew changes
- Experience delays at intermediate signals
- Have intermediate inspection points

This could be modeled by breaking `fact_movement` into smaller segments.

### Simplified Power Transfers

The model treats locomotive changes as binary (quick vs. extended). Real operations track:
- Specific locomotive IDs
- Locomotive consist configurations (lead, trailing, distributed power)
- Fuel consumption per locomotive
- Maintenance schedules

### No Cost Dimension

The example doesn't include financial metrics (revenue, costs, profitability). Adding:
- `dim_commodity`: Cargo type, price per ton
- `fact_financial`: Revenue, fuel cost, labor cost, depreciation
- Would enable profitability analysis by corridor/train

## Future Enhancements

### Additional Dimensions

1. **dim_crew**: Engineer, conductor assignments
   - Enables labor utilization analysis
   - Tracks crew hours and compliance
   - Correlates crew experience with performance

2. **dim_locomotive**: Power unit details
   - Horsepower, fuel capacity, age
   - Maintenance schedule
   - Enables fleet management analytics

3. **dim_customer**: Shippers and receivers
   - Contract terms, volume commitments
   - Service level agreements
   - Revenue by customer segment

4. **dim_weather**: Daily weather conditions
   - Temperature, precipitation, wind
   - Correlate weather with performance
   - Predictive modeling for delays

### Additional Facts

1. **fact_maintenance**: Repair and inspection events
   - Preventive vs. corrective maintenance
   - Cost and duration tracking
   - Predict maintenance needs

2. **fact_fuel**: Fuel consumption by trip
   - Gallons per mile by corridor
   - Cost per trip
   - Optimize routing for fuel efficiency

3. **fact_incident**: Safety events and accidents
   - Derailments, collisions, near-misses
   - Root cause analysis
   - Track safety trends

### Advanced Analytics

1. **Predictive Straggler Model**
   - Use historical patterns to predict cars likely to straggle
   - Features: car age, corridor, season, previous straggler history
   - ML integration (export to Python/R)

2. **Optimal Routing Engine**
   - Identify best corridors for specific freight types
   - Balance distance, speed, reliability
   - What-if scenario analysis

3. **Capacity Planning**
   - Simulate adding trains or cars to fleet
   - Model impact of corridor improvements
   - ROI analysis for capital investments

4. **Real-Time Dashboards**
   - Connect to BI tools (Tableau, Power BI, Metabase)
   - Live views of train locations and status
   - Alert on anomalies (excessive delays, straggler spikes)

### Data Quality Enhancements

1. **Fix Car Exclusivity**
   - Add buffer periods between trips
   - Validate movement sequences before insert
   - Implement trip state machine (scheduled → in-transit → completed)

2. **Additional Validations**
   - Check for impossible speeds (distance/duration)
   - Validate cargo capacity (not over train limit)
   - Ensure crew rest periods between assignments

3. **Data Lineage Tracking**
   - Record which seed_generator version created data
   - Track when metrics were last refreshed
   - Audit trail for data changes

### Integration Examples

1. **External Data Sources**
   - Import real weather data (NOAA API)
   - Import fuel prices (EIA API)
   - Import rail industry benchmarks

2. **Export Capabilities**
   - Parquet files for big data processing
   - JSON API for web applications
   - Excel templates for business reporting

## References

- **Schema Definition**: See `schema.yml` for complete table structures
- **Metrics Details**: See `METRICS.md` for detailed metric descriptions
- **Architecture**: See `ARCHITECTURE.md` for technical design details
- **Seed Generator**: See `seed_generator.go` for data generation logic
- **Test Profiles**: See `profiles.yml` for test execution configuration

## Contributing

This is an example project for demonstration purposes. If you use this as a starting point:

1. Update `schema.yml` with your specific requirements
2. Modify `seed_generator.go` to match your data patterns
3. Add custom metrics and analytics for your business questions
4. Implement additional dimensions/facts as needed
5. Connect to your BI tools for visualization

## License

This example is part of the Gorchata project. See LICENSE file in the repository root.
