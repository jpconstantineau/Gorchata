# Unit Train Analytics Data Warehouse

This example demonstrates a comprehensive data warehouse design for unit train operations analytics.

## Business Context

This data warehouse models a unit train operation with the following characteristics:

- **Fleet**: 250 rail cars total
- **Operations**: 3 parallel trains, 75 cars each
- **Origins**: 2 loading facilities (single queue, 12-18 hour loading time)
- **Destinations**: 3 unloading facilities (single queue, 8-12 hour unloading time)
- **Corridors**: 6 routes (2 origins × 3 destinations)
- **Transit Time Classes**: 2-day, 3-day, 4-day
- **Stragglers**: Cars that separate from trains (6 hours to 3 days delay before independent travel)
- **Power Inference**: Locomotive changes inferred from time gaps (<1 hour = same power, >1 hour = different power)

### Seasonal Effects

- **Week 5**: General slowdown in operations
- **Week 8**: Spike in straggler occurrences

## Data Model

### Star Schema Design

#### Dimension Tables

1. **dim_train** - Unit train information
   - train_id, train_name, num_cars, formed_at, completed_at

2. **dim_car** - Rail car fleet
   - car_id, car_type, capacity_tons

3. **dim_location** - Origins, destinations, and intermediate stations
   - location_id, location_name, location_type (ORIGIN/DESTINATION/STATION), avg_queue_hours

4. **dim_corridor** - Route combinations
   - corridor_id, origin_location_id, destination_location_id, transit_time_class
   - Combines origin + destination + intermediate stations + transit time classification

5. **dim_date** - Time dimension with week-level granularity
   - date_key, full_date, year, quarter, month, week, day_of_week, is_weekend

#### Fact Tables

1. **fact_car_location_event** - Primary event log from CLM (Car Location Messages)
   - Grain: One row per car location event
   - Event types: train_formed, departed_origin, arrived_station, departed_station, arrived_destination, car_set_out, car_picked_up, train_completed
   - Source: CSV CLM input files

2. **fact_train_trip** - Aggregated train journey metrics
   - Grain: One row per train trip (origin to destination)
   - Metrics: origin_queue_hours, destination_queue_hours, transit_hours, total_trip_hours, num_stragglers

3. **fact_straggler** - Cars separated from trains
   - Grain: One row per straggler occurrence
   - Metrics: set_out_timestamp, picked_up_timestamp, delay_hours, delay_category

4. **fact_inferred_power_transfer** - Locomotive change detection
   - Grain: One row per potential power transfer
   - Inference logic: gap_hours < 1 = same locomotives, gap_hours > 1 = different locomotives
   - Fields: arrival_timestamp, departure_timestamp, transfer_timestamp, gap_hours, inferred_same_power

#### Aggregated Metrics Tables (Performance Optimization)

1. **agg_corridor_weekly_metrics** - Weekly KPIs by corridor
   - Supports seasonal analysis (week 5 slowdown, week 8 straggler spike)
   - Metrics: total_trips, avg_transit_hours, avg_queue_hours, total_stragglers

2. **agg_fleet_utilization_daily** - Daily fleet status
   - Total 250 cars: cars_on_trains, cars_as_stragglers, cars_idle, utilization_pct

## Input Data Format

The primary input is CSV files containing Car Location Messages (CLM) with the following event types:

- `train_formed` - Train assembled at origin
- `departed_origin` - Train leaves loading facility
- `arrived_station` - Train arrives at intermediate station
- `departed_station` - Train leaves intermediate station
- `arrived_destination` - Train arrives at unloading facility
- `car_set_out` - Car separated from train
- `car_picked_up` - Straggler begins independent travel
- `train_completed` - Train journey completed

## Data Quality Tests

The schema includes comprehensive data quality tests:

### Generic Tests (defined in schema.yml)
- `unique` - Primary key validation
- `not_null` - Required field validation
- `not_empty_string` - String content validation
- `accepted_values` - Enumerated value lists (event types, location types, transit time classes)
- `accepted_range` - Numeric bounds (hours, counts, percentages)
- `relationships` - Foreign key integrity
- `unique_combination_of_columns` - Composite key validation

### Key Business Rules Validated
- Event types match CLM specification
- Location types are valid (ORIGIN/DESTINATION/STATION)
- Transit time classes are valid (2-day/3-day/4-day)
- Queue times are reasonable (0-72 hours)
- Transit times are reasonable (24-120 hours)
- Fleet size is constant (250 cars)
- Delay categories classify correctly (short/medium/long/extended)
- Power inference logic is consistent (gap < 1 hour vs > 1 hour)

## Seed Data Configuration

The `seeds/` directory contains configuration for generating realistic CLM data. See [seeds/README.md](seeds/README.md) for details.

Key features of the seed configuration:
- 250-car fleet with 3 parallel 75-car unit trains
- 6 corridors (2 origins × 3 destinations)
- Queue bottlenecks at origins (loading) and destinations (unloading)
- Straggler simulation with 6-hour to 3-day delays
- Seasonal effects (week 5 slowdown, week 8 straggler spike)
- 90 days of operational data

## Running Tests

```bash
# Run all schema validation tests
go test -v ./test -run TestUnitTrain

# Run seed configuration validation tests
go test -v ./test -run "UnitTrainSeed|CarFleet|TrainFormation|OriginDestination|Queue|Transit|Straggler|ParallelTrain|Seasonal|CSV"

# Discover data quality tests from schema
cd examples/unit_train_analytics
gorchata test
```

## Test Coverage

### Schema Design Tests

1. **TestUnitTrainSchemaValidation** - Validates YAML structure
2. **TestUnitTrainSchemaParsing** - Ensures schema parses correctly
3. **TestUnitTrainDimensionTables** - Verifies all required dimensions exist
4. **TestUnitTrainFactTables** - Verifies fact table structure including power inference
5. **TestUnitTrainCorridorLogic** - Validates corridor combination logic
6. **TestUnitTrainTestDiscovery** - Confirms data quality tests can be discovered

### Seed Configuration Tests

7. **TestUnitTrainSeedConfiguration** - Validates seed YAML parses
8. **TestCarFleetAllocation** - Ensures 225+ cars available (3 trains × 75 cars with buffer)
9. **TestTrainFormationLogic** - Validates 75 cars per train constraint
10. **TestOriginDestinationPairs** - Verifies 2 origins × 3 destinations = 6 corridors
11. **TestOriginQueueLogic** - Ensures only 1 train loading at origin at a time (12-18 hours)
12. **TestDestinationQueueLogic** - Ensures only 1 train unloading at destination at a time (8-12 hours)
13. **TestTransitTimeDistribution** - Ensures 2-4 day variation with 5-10 stations
14. **TestStragglerDelayRange** - Validates straggler delay between 6 hours and 3 days
15. **TestStragglerIndependentTravel** - Validates stragglers travel independently then rejoin
16. **TestStragglerGeneration** - Validates 1 car per train per day in transit, doubles during week 8
17. **TestParallelTrainOperations** - Confirms 3 trains can operate simultaneously
18. **TestSeasonalSlowdown** - Validates 1 corridor slower for 1 week
19. **TestCSVFormatOutput** - Ensures CLM messages output as valid CSV

## Key Design Decisions

1. **Corridor Modeling**: Corridors are modeled as a dimension combining origin, destination, intermediate stations, and transit time class. This allows for flexible analysis of route performance.

2. **Power Inference**: Locomotive changes are inferred from time gaps rather than explicit tracking. This provides reasonable accuracy without requiring detailed locomotive data.

3. **Straggler Tracking**: Stragglers are tracked in a separate fact table with detailed delay categorization, enabling analysis of operational disruptions.

4. **Event-Based Design**: Primary fact table captures all CLM events, with higher-level aggregations derived for performance.

5. **Seasonal Effects**: Week-level time dimension supports analysis of seasonal patterns (week 5 slowdown, week 8 straggler spike).

6. **Queue Time Tracking**: Both origin and destination queue times are captured separately to identify bottlenecks.

## Project Status

- ✅ Phase 1 Complete: Schema design with 5 dimensions, 4 facts, 2 aggregations
- ✅ Phase 2 Complete: Seed configuration for CLM message generation
- 🔲 Phase 3: CLM event generation logic
- 🔲 Phase 4: Staging and dimension loading transformations
- 🔲 Phase 5: Fact table transformations
- 🔲 Phase 6: Analytical metrics and aggregations
