# Unit Train Analytics - Seed Configuration

This directory contains seed configuration for generating realistic Car Location Message (CLM) data for the unit train analytics data warehouse.

## Files

### clm_generation_config.yml
Configuration file that defines parameters for generating CLM messages simulating railroad operations:

- **Fleet Configuration**: 250 rail cars (225 operational + 25 buffer)
  - Car type: COAL_HOPPER
  - Commodity: COAL
  - Capacity: 110 tons per car

- **Locations**:
  - 2 Origins: COAL_MINE_A, COAL_MINE_B (loading queue, 12-18 hours)
  - 3 Destinations: POWER_PLANT_1, POWER_PLANT_2, PORT_TERMINAL (unloading queue, 8-12 hours)

- **Corridors**: 6 total (2 origins × 3 destinations)
  - Transit time: 2-4 days per corridor
  - Intermediate stations: 5-10 per corridor
  - Empty return: 30% faster than loaded trips

- **Train Operations**:
  - 3 parallel unit trains
  - 75 cars per train
  - Origin queue: 1 train loading at a time
  - Destination queue: 1 train unloading at a time

- **Straggler Simulation**:
  - Base rate: 1 car per train per day in transit (both directions)
  - Delay period: 6 hours to 3 days
  - Stragglers travel independently to destination after delay
  - Stragglers rejoin next returning train from same destination

- **Seasonal Effects**:
  - Week 5: Corridor CORR_A2 becomes 20% slower
  - Week 8: Straggler rate doubles across all corridors

- **Time Window**: 90 days of operations (January 1, 2024 - March 31, 2024)

- **Output Format**: CSV with CLM event fields
  - event_id, event_timestamp, car_id, train_id, location_id
  - event_type, loaded_flag, commodity, weight_tons

### seed.yml
Standard Gorchata seed configuration for importing CSV files into the database.

## Usage

The configuration in `clm_generation_config.yml` will be used by the seed generation logic (Phase 3) to produce realistic CLM event data that demonstrates:

1. **Queue Bottlenecks**: Only 1 train can load/unload at a time at origins/destinations
2. **Train Formation**: Wait until 75 cars are available at origin
3. **Straggler Delays**: Cars set out during transit with 6-hour to 3-day delays
4. **Independent Travel**: Stragglers continue to destination after delay period
5. **Return Trip Rejoin**: Stragglers join next returning train from destination
6. **Seasonal Variations**: Corridor slowdowns and increased straggler rates
7. **Power Inference**: Timing patterns that allow locomotive change detection

## Validation

Run tests to validate the configuration:

```powershell
go test -v ./test -run "UnitTrainSeed|CarFleet|TrainFormation|OriginDestination|Queue|Transit|Straggler|ParallelTrain|Seasonal|CSV"
```

All 13 validation tests ensure:
- Fleet size supports 3 trains with 75 cars each plus buffer
- 6 corridors properly configured (2×3)
- Queue constraints properly defined (capacity 1)
- Straggler delays within 6-hour to 3-day range
- Seasonal effects properly configured
- CSV output format includes all required CLM fields
