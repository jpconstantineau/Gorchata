# Haul Truck Analytics Architecture

## Overview

This document describes the technical architecture of the Haul Truck Analytics data warehouse, including data flow, state detection algorithms, cycle aggregation logic, schema design rationale, and extensibility points.

## Data Flow

### End-to-End Pipeline

```
┌──────────────────────────┐
│ Fleet Management System  │
│ (Real-time telemetry)   │
└────────────┬─────────────┘
             │ GPS, Payload, Speed, Fuel @ 5-10 sec intervals
             ▼
┌──────────────────────────┐
│ stg_telemetry_events     │ ← Raw sensor data
│                          │
│ Columns:                 │
│ - truck_id              │
│ - timestamp             │
│ - gps_lat/gps_lon       │
│ - speed_kmh             │
│ - payload_tons          │
│ - suspension_pressure   │
│ - engine_rpm            │
│ - fuel_level_liters     │
│ - engine_hours          │
│ - geofence_zone         │
└────────────┬─────────────┘
             │ State Detection Algorithm
             ▼
┌──────────────────────────┐
│ stg_truck_states         │ ← Detected operational states
│                          │
│ Columns:                 │
│ - truck_id              │
│ - state_start/state_end │
│ - operational_state     │
│ - location_zone         │
│ - payload_at_start/end  │
└────────────┬─────────────┘
             │ Cycle Aggregation
             ▼
┌──────────────────────────┐
│ fact_haul_cycle          │ ← Complete cycles
│                          │
│ Grain: One row per       │
│ complete haul cycle      │
│                          │
│ Metrics:                 │
│ - Duration breakdowns   │
│ - Distance (loaded/empty)│
│ - Payload utilization   │
│ - Fuel consumption      │
│ - Speed averages        │
└────────────┬─────────────┘
             │ Metrics Rollup
             ▼
┌──────────────────────────┐
│ Metrics Aggregations (5) │
│                          │
│ - truck_daily_productivity│
│ - shovel_utilization    │
│ - crusher_throughput    │
│ - queue_analysis        │
│ - fleet_summary         │
└────────────┬─────────────┘
             │ Analytical Queries
             ▼
┌──────────────────────────┐
│ Analytics Views (6)      │
│                          │
│ - worst_performing_trucks│
│ - bottleneck_analysis   │
│ - payload_compliance    │
│ - shift_performance     │
│ - fuel_efficiency       │
│ - operator_performance  │
└────────────┬─────────────┘
             │ Data Quality Validation
             ▼
┌──────────────────────────┐
│ Validation Tests (4)     │
│                          │
│ - referential_integrity │
│ - temporal_consistency  │
│ - business_rules        │
│ - state_transitions     │
└──────────────────────────┘
```

### Components

1. **Telemetry Ingestion**:
   - Fleet management system emits events every 5-10 seconds
   - GPS coordinates, payload sensors, engine diagnostics
   - Geofence zones pre-computed (shovel locations, crusher location, roads)
   - Stored in `stg_telemetry_events`

2. **State Detection**:
   - SQL transformation processes telemetry using rules
   - Identifies operational states (loading, hauling, dumping, queued, etc.)
   - Calculates state durations using window functions
   - Outputs to `stg_truck_states`

3. **Cycle Aggregation**:
   - Groups states into complete cycles
   - Calculates distance using Haversine formula (GPS coordinates)
   - Aggregates fuel consumption (delta between cycle start/end)
   - Validates cycle completeness (all required states present)
   - Outputs to `fact_haul_cycle`

4. **Metrics Calculation**:
   - Pre-aggregates common queries for performance
   - Materializes as tables (not views) for fast access
   - Refreshed on schedule (hourly, daily, or on-demand)

5. **Analytical Queries**:
   - Business intelligence views
   - Joins metrics with dimensions
   - Rankings, comparisons, trend analysis
   - Optimized for dashboard consumption

6. **Data Quality Validation**:
   - Automated checks for referential integrity
   - Temporal consistency (no overlaps, correct ordering)
   - Business rule compliance
   - State transition validity

## State Detection Algorithm

### Overview

The state detection algorithm transforms continuous telemetry streams into discrete operational states. It uses **payload thresholds**, **geofence zones**, **speed patterns**, and **temporal rules** to classify truck activity.

## Transformation Logic

### Overview

The haul truck data warehouse implements a **multi-stage transformation pipeline** that converts raw telemetry into business insights:

**Stage 1: Telemetry Ingestion** → Raw sensor data (GPS, payload, speed, fuel)  
**Stage 2: State Detection** → Classify operational states using business rules  
**Stage 3: Cycle Aggregation** → Group states into complete haul cycles  
**Stage 4: Metrics Rollup** → Pre-aggregate common queries for performance  
**Stage 5: Analytics** → Business intelligence views for insights  

Each stage is implemented as **SQL transformations** that can be executed independently or as a complete pipeline.

### Stage 1: Telemetry Ingestion

**Input**: Fleet management system events (GPS coordinates, payload sensors, engine diagnostics)

**Output**: `stg_telemetry_events` table with normalized sensor readings

**Transformation Logic**:
```sql
-- Example telemetry ingestion (from external system)
INSERT INTO stg_telemetry_events (
  truck_id, timestamp, gps_lat, gps_lon, speed_kmh, payload_tons,
  suspension_pressure_psi, engine_rpm, fuel_level_liters, engine_hours, geofence_zone
)
SELECT 
  raw.truck_id,
  raw.event_timestamp,
  raw.latitude,
  raw.longitude,
  raw.speed,
  -- Convert suspension pressure to payload estimate
  (raw.suspension_pressure - truck.empty_pressure) / truck.pressure_per_ton AS payload_tons,
  raw.suspension_pressure,
  raw.engine_rpm,
  raw.fuel_level,
  raw.engine_hours,
  -- Determine geofence zone from GPS coordinates
  determine_geofence(raw.latitude, raw.longitude) AS geofence_zone
FROM raw_fleet_events raw
JOIN dim_truck truck ON raw.truck_id = truck.truck_id;
```

**Key Transformations**:
- GPS coordinate validation and normalization
- Suspension pressure to payload conversion (calibrated per truck)
- Geofence zone determination (point-in-polygon checks)
- Timestamp standardization (UTC)
- Data quality checks (remove outliers, sensor errors)

### Stage 2: State Detection

**Input**: `stg_telemetry_events` (5-10 second interval sensor readings)

**Output**: `stg_truck_states` (discrete operational states with durations)

**Transformation Logic**: See [State Detection Algorithm](#state-detection-algorithm) section for detailed rules

**SQL Reference**: `models/staging/stg_truck_states.sql`

**Key Transformations**:
- Apply payload thresholds to classify loaded/empty states
- Use geofence zones to determine operational context
- Analyze speed patterns to differentiate hauling vs queued
- Detect state transitions using LAG/LEAD window functions
- Calculate state durations
- Identify special cases (refueling, spot delays)

### Stage 3: Cycle Aggregation

**Input**: `stg_truck_states` (operational states)

**Output**: `fact_haul_cycle` (complete cycles with metrics)

**Transformation Logic**: See [Cycle Boundary Logic](#cycle-boundary-logic) section for detailed algorithm

**SQL Reference**: `models/facts/fact_haul_cycle.sql`

**Key Transformations**:
- Identify cycle boundaries (loading → next loading)
- Aggregate state durations into cycle components
- Calculate distances using Haversine formula from GPS coordinates
- Compute average speeds (loaded vs empty)
- Aggregate fuel consumption (excluding refueling events)
- Join dimension tables for truck, shovel, crusher, operator, shift, date
- Validate cycle completeness (all required states present)

### Stage 4: Metrics Rollup

**Input**: `fact_haul_cycle` (grain: one row per cycle)

**Output**: Metrics tables (pre-aggregated summaries)

**SQL References**: 
- `models/metrics/truck_daily_productivity.sql`
- `models/metrics/shovel_utilization.sql`
- `models/metrics/crusher_throughput.sql`
- `models/metrics/queue_analysis.sql`
- `models/metrics/fleet_summary.sql`

**Key Transformations**:
- Aggregate cycles by truck and date → truck_daily_productivity
- Aggregate loading states by shovel → shovel_utilization
- Aggregate dumping/queuing by crusher → crusher_throughput
- Extract and analyze queue states → queue_analysis
- Roll up fleet-wide metrics by shift → fleet_summary
- Calculate derived metrics (tons per hour, utilization %, efficiency scores)
- Apply payload utilization bands (underload, optimal, overload)

### Stage 5: Analytics

**Input**: Metrics tables + fact tables

**Output**: Analytics views (business intelligence queries)

**SQL References**:
- `models/analytics/worst_performing_trucks.sql`
- `models/analytics/bottleneck_analysis.sql`
- `models/analytics/payload_compliance.sql`
- `models/analytics/shift_performance.sql`
- `models/analytics/fuel_efficiency.sql`
- `models/analytics/operator_performance.sql`

**Key Transformations**:
- Rank trucks by productivity for performance identification
- Compare shovel vs crusher queue patterns for bottleneck analysis
- Calculate payload compliance rates by truck/operator/shovel
- Compare day vs night shift metrics for operational insights
- Calculate fuel per ton and fuel per ton-mile for efficiency analysis
- Score and rank operators using composite efficiency metrics

### Incremental Processing

**Challenge**: Process only new data without reprocessing entire history

**Approach**:
```sql
-- Incremental state detection (process only new telemetry)
CREATE TABLE stg_truck_states_new AS
SELECT ...
FROM stg_telemetry_events
WHERE timestamp > (SELECT MAX(state_end) FROM stg_truck_states)
  AND timestamp <= CURRENT_TIMESTAMP;

-- Merge new states
INSERT INTO stg_truck_states SELECT * FROM stg_truck_states_new;

-- Incremental cycle completion (detect new complete cycles)
CREATE TABLE fact_haul_cycle_new AS
SELECT ...
FROM stg_truck_states
WHERE state_start > (SELECT MAX(cycle_end) FROM fact_haul_cycle)
  AND -- has next loading state (cycle is complete)
  ...;

-- Merge new cycles
INSERT INTO fact_haul_cycle SELECT * FROM fact_haul_cycle_new;
```

**Benefits**:
- Reduced processing time (only new data)
- Near real-time analytics (process every 5-15 minutes)
- Lower compute costs

**Challenges**:
- Handle late-arriving data (events arrive out of order)
- Manage incomplete cycles (in-progress, waiting for next loading)
- Coordinate cross-stage dependencies

### Data Quality Validation

Each transformation stage includes **data quality checks**:

**Telemetry Validation**:
- GPS coordinates within valid ranges (lat: -90 to 90, lon: -180 to 180)
- Speed within reasonable limits (0-80 km/h)
- Payload within truck capacity (0-115% of rated capacity)
- Engine parameters within specs

**State Validation**:
- No overlapping states for same truck
- States in temporal order (start < end)
- Valid state transitions (loading → hauling, not loading → dumping)
- State durations reasonable (loading 2-15 minutes, not 120 minutes)

**Cycle Validation**:
- All required states present (loading, hauling, dumping, returning)
- Cycle times within bounds (20-120 minutes depending on distance)
- Foreign keys exist in dimension tables
- No duplicate cycle_ids

**SQL Reference**: `tests/test_*.sql` files

### Payload Thresholds

Payload thresholds determine loaded vs empty states:

```
Threshold Definitions (per truck):
- Loaded Threshold = 80% of rated capacity
- Empty Threshold = 20% of rated capacity

Example for 200-ton truck:
- Loaded Threshold = 200 * 0.80 = 160 tons
- Empty Threshold = 200 * 0.20 = 40 tons

State Classification:
- Payload > 160 tons → Truck is LOADED
- Payload < 40 tons  → Truck is EMPTY
- Payload 40-160     → TRANSITION (loading or dumping in progress)
```

**Rationale**:
- 80% threshold accounts for bucket fill factors (not all truck capacity used every pass)
- 20% threshold allows for residual material after dumping
- Transition zone prevents flickering between states

### Geofence Zones

Geofence zones define operational areas:

```
Zone Definitions:
┌─────────────┬──────────────┬─────────────────────┐
│ Zone ID     │ Type         │ Associated Equipment│
├─────────────┼──────────────┼─────────────────────┤
│ Shovel_A    │ Loading      │ SHOVEL-A (North Pit)│
│ Shovel_B    │ Loading      │ SHOVEL-B (East Pit) │
│ Shovel_C    │ Loading      │ SHOVEL-C (South Pit)│
│ Crusher     │ Dumping      │ CRUSHER-01          │
│ Road        │ Travel       │ Haul roads          │
│ Other       │ Non-productive│ Maintenance, parking│
└─────────────┴──────────────┴─────────────────────┘

Zone Detection:
- GPS coordinates compared to geofence polygons
- Zones typically 50-100m radius circles or polygons
- Road zones: corridors between loading/dumping points
```

### Speed Patterns

Speed indicates operational state:

```
Speed Thresholds:
- Stationary: < 3 km/h (queued, loading, dumping, idle)
- Slow Speed:  3-10 km/h (maneuvering, spotting under shovel)
- Normal Speed: > 10 km/h (hauling or returning)

Speed Context:
- Loaded haul: 20-35 km/h (limited by weight, terrain, road conditions)
- Empty return: 30-50 km/h (faster without payload)
- Queued/Spotting: 0-5 km/h (minor adjustments)
```

### State Detection Rules

State detection uses **decision tree logic** combining zone, payload, and speed:

```sql
-- Simplified State Detection Logic
CASE
  -- LOADING: At shovel, stationary or slow, payload increasing
  WHEN geofence_zone LIKE 'Shovel_%' 
    AND speed_kmh < 10 
    AND payload_tons BETWEEN 0 AND loaded_threshold
  THEN 'loading'
  
  -- HAULING LOADED: On road, normal speed, payload above loaded threshold
  WHEN geofence_zone IN ('Road', 'Crusher')
    AND speed_kmh > 10
    AND payload_tons >= loaded_threshold
  THEN 'hauling_loaded'
  
  -- QUEUED AT CRUSHER: At crusher, stationary, loaded
  WHEN geofence_zone = 'Crusher'
    AND speed_kmh < 3
    AND payload_tons >= loaded_threshold
  THEN 'queued_at_crusher'
  
  -- DUMPING: At crusher, payload dropping rapidly
  WHEN geofence_zone = 'Crusher'
    AND speed_kmh < 5
    AND (payload_current - LAG(payload_tons) OVER (ORDER BY timestamp)) < -10
  THEN 'dumping'
  
  -- RETURNING EMPTY: On road, normal speed, payload below empty threshold
  WHEN geofence_zone IN ('Road', 'Shovel_A', 'Shovel_B', 'Shovel_C')
    AND speed_kmh > 10
    AND payload_tons <= empty_threshold
  THEN 'returning_empty'
  
  -- QUEUED AT SHOVEL: At shovel, stationary, empty
  WHEN geofence_zone LIKE 'Shovel_%'
    AND speed_kmh < 3
    AND payload_tons <= empty_threshold
  THEN 'queued_at_shovel'
  
  -- SPOT DELAY: Stopped outside operational zones for >2 minutes
  WHEN geofence_zone NOT IN ('Shovel_A', 'Shovel_B', 'Shovel_C', 'Crusher')
    AND speed_kmh < 1
    AND duration > 120 -- seconds
  THEN 'spot_delay'
  
  -- IDLE: All other cases
  ELSE 'idle'
END AS operational_state
```

### State Duration Calculation

Durations are calculated using **window functions**:

```sql
-- Identify state boundaries (state changes)
WITH state_changes AS (
  SELECT
    truck_id,
    timestamp,
    operational_state,
    -- Detect when state changes
    LAG(operational_state) OVER (PARTITION BY truck_id ORDER BY timestamp) AS prev_state,
    CASE
      WHEN operational_state != LAG(operational_state) OVER (PARTITION BY truck_id ORDER BY timestamp)
        THEN 1
      ELSE 0
    END AS is_state_change
  FROM telemetry_with_state_rules
),

-- Assign state_id to group consecutive identical states
state_groups AS (
  SELECT
    truck_id,
    timestamp,
    operational_state,
    SUM(is_state_change) OVER (PARTITION BY truck_id ORDER BY timestamp) AS state_group_id
  FROM state_changes
),

-- Aggregate to get state start/end times
truck_states AS (
  SELECT
    truck_id,
    state_group_id,
    operational_state,
    MIN(timestamp) AS state_start,
    MAX(timestamp) AS state_end,
    -- Duration in minutes
    (julianday(MAX(timestamp)) - julianday(MIN(timestamp))) * 24 * 60 AS duration_min,
    -- Payload at start and end
    FIRST_VALUE(payload_tons) OVER (PARTITION BY truck_id, state_group_id ORDER BY timestamp) AS payload_at_start,
    LAST_VALUE(payload_tons) OVER (PARTITION BY truck_id, state_group_id ORDER BY timestamp) AS payload_at_end
  FROM state_groups
  GROUP BY truck_id, state_group_id, operational_state
)
```

### Refueling Detection

Refueling is identified as a special type of spot delay:

```
Refueling Indicators:
1. Engine hours threshold crossed (10-12 hours since last refuel)
2. Spot delay duration: 15-30 minutes
3. Fuel level increases significantly (not just minor fluctuation)

Detection Logic:
- Track cumulative engine_hours per truck
- When engine_hours reaches 10-12, expect refueling
- Look for spot_delay states with:
  - Duration 15-30 minutes
  - Fuel level increase >500 liters
  - Location in designated refueling area (if available)

Handling in Fuel Consumption Calculation:
- Exclude refueling events when calculating fuel_consumed_liters
- Only count fuel decreases (consumption)
- Ignore fuel increases (refueling, tanker fills)
```

## Cycle Boundary Logic

### Cycle Definition

A **complete haul cycle** is defined as:

```
Cycle Start:   Loading begins (first 'loading' state)
Cycle End:     Next loading begins (next 'loading' state)

Complete Cycle States:
1. [Optional] queued_at_shovel
2. loading                          ← CYCLE START
3. hauling_loaded
4. [Optional] queued_at_crusher
5. dumping
6. returning_empty
7. [Optional] spot_delay (can occur at any point)
8. [Repeat] queued_at_shovel
9. loading                          ← CYCLE END (next cycle start)
```

### Cycle Identification Algorithm

Cycles are identified using window functions to find loading state boundaries:

```sql
WITH loading_events AS (
  -- Identify all loading states
  SELECT
    truck_id,
    state_start AS loading_start,
    state_end AS loading_end,
    -- Get next loading start time for this truck
    LEAD(state_start) OVER (PARTITION BY truck_id ORDER BY state_start) AS next_loading_start
  FROM stg_truck_states
  WHERE operational_state = 'loading'
),

cycle_boundaries AS (
  -- Define cycle boundaries
  SELECT
    truck_id,
    loading_start AS cycle_start,
    next_loading_start AS cycle_end,
    -- Generate unique cycle_id
    truck_id || '_' || loading_start AS cycle_id
  FROM loading_events
  WHERE next_loading_start IS NOT NULL  -- Exclude incomplete cycles
)
```

### Cycle Aggregation

States are aggregated into cycle metrics:

```sql
SELECT
  cb.cycle_id,
  cb.truck_id,
  cb.cycle_start,
  cb.cycle_end,
  
  -- Aggregate state durations
  SUM(CASE WHEN s.operational_state = 'loading' THEN s.duration_min ELSE 0 END) AS duration_loading_min,
  SUM(CASE WHEN s.operational_state = 'hauling_loaded' THEN s.duration_min ELSE 0 END) AS duration_hauling_loaded_min,
  SUM(CASE WHEN s.operational_state = 'queued_at_crusher' THEN s.duration_min ELSE 0 END) AS duration_queue_crusher_min,
  SUM(CASE WHEN s.operational_state = 'dumping' THEN s.duration_min ELSE 0 END) AS duration_dumping_min,
  SUM(CASE WHEN s.operational_state = 'returning_empty' THEN s.duration_min ELSE 0 END) AS duration_returning_min,
  SUM(CASE WHEN s.operational_state = 'queued_at_shovel' THEN s.duration_min ELSE 0 END) AS duration_queue_shovel_min,
  SUM(CASE WHEN s.operational_state = 'spot_delay' THEN s.duration_min ELSE 0 END) AS duration_spot_delays_min,
  
  -- Payload (max payload during hauling state)
  MAX(CASE WHEN s.operational_state = 'hauling_loaded' THEN s.payload_at_start ELSE 0 END) AS payload_tons

FROM cycle_boundaries cb
JOIN stg_truck_states s
  ON s.truck_id = cb.truck_id
  AND s.state_start >= cb.cycle_start
  AND s.state_end <= cb.cycle_end
GROUP BY cb.cycle_id, cb.truck_id, cb.cycle_start, cb.cycle_end
```

### Distance Calculation

Distance is calculated using the **Haversine formula** for GPS coordinates:

```sql
-- Haversine formula for distance between two GPS points
CREATE FUNCTION haversine_distance(
  lat1 REAL, lon1 REAL,  -- Point 1
  lat2 REAL, lon2 REAL   -- Point 2
) RETURNS REAL AS $$
DECLARE
  R CONSTANT REAL := 6371;  -- Earth radius in km
  dLat REAL;
  dLon REAL;
  a REAL;
  c REAL;
BEGIN
  dLat := radians(lat2 - lat1);
  dLon := radians(lon2 - lon1);
  
  a := sin(dLat/2) * sin(dLat/2) +
       cos(radians(lat1)) * cos(radians(lat2)) *
       sin(dLon/2) * sin(dLon/2);
  
  c := 2 * atan2(sqrt(a), sqrt(1-a));
  
  RETURN R * c;  -- Distance in kilometers
END;
$$;

-- Calculate distance for loaded segment (shovel to crusher)
SELECT
  cycle_id,
  SUM(
    haversine_distance(
      LAG(gps_lat) OVER (ORDER BY timestamp),
      LAG(gps_lon) OVER (ORDER BY timestamp),
      gps_lat,
      gps_lon
    )
  ) AS distance_loaded_km
FROM stg_telemetry_events
WHERE operational_state = 'hauling_loaded'
  AND cycle_id = :cycle_id
GROUP BY cycle_id;
```

Note: SQLite doesn't support custom functions directly, so distance calculation is approximated using sum of incremental GPS movements.

### Speed Calculation

Average speeds are calculated as distance divided by duration:

```sql
SELECT
  cycle_id,
  distance_loaded_km,
  duration_hauling_loaded_min,
  -- Average loaded speed
  (distance_loaded_km / (duration_hauling_loaded_min / 60.0)) AS speed_avg_loaded_kmh,
  
  distance_empty_km,
  duration_returning_min,
  -- Average empty speed
  (distance_empty_km / (duration_returning_min / 60.0)) AS speed_avg_empty_kmh
FROM fact_haul_cycle;
```

### Fuel Consumption Calculation

Fuel consumption aggregates fuel level decreases (excludes refueling):

```sql
WITH fuel_changes AS (
  SELECT
    truck_id,
    timestamp,
    fuel_level_liters,
    LAG(fuel_level_liters) OVER (PARTITION BY truck_id ORDER BY timestamp) AS prev_fuel_level,
    fuel_level_liters - LAG(fuel_level_liters) OVER (PARTITION BY truck_id ORDER BY timestamp) AS fuel_delta
  FROM stg_telemetry_events
  WHERE cycle_id = :cycle_id
)

SELECT
  cycle_id,
  -- Sum only negative deltas (consumption), exclude positive deltas (refueling)
  ABS(SUM(CASE WHEN fuel_delta < 0 THEN fuel_delta ELSE 0 END)) AS fuel_consumed_liters
FROM fuel_changes
GROUP BY cycle_id;
```

## Queue Detection

### Queue Identification

Queues are identified by:
1. **Location**: Truck is at shovel or crusher zone
2. **Stationary**: Speed < 3 km/h
3. **Duration**: Stopped for >30 seconds (not just momentary pause)
4. **State Context**: Appropriate payload state (empty at shovel, loaded at crusher)

```sql
-- Queue detection logic
CASE
  WHEN geofence_zone LIKE 'Shovel_%'
    AND speed_kmh < 3
    AND payload_tons <= empty_threshold
    AND duration_seconds > 30
  THEN 'queued_at_shovel'
  
  WHEN geofence_zone = 'Crusher'
    AND speed_kmh < 3
    AND payload_tons >= loaded_threshold
    AND duration_seconds > 30
  THEN 'queued_at_crusher'
END AS queue_state
```

### Queue Time Aggregation

Queue times are aggregated separately for bottleneck analysis:

```sql
CREATE TABLE queue_analysis AS
SELECT
  -- By location and shift
  CASE
    WHEN operational_state = 'queued_at_shovel' THEN 'SHOVEL'
    WHEN operational_state = 'queued_at_crusher' THEN 'CRUSHER'
  END AS location_type,
  
  location_zone,
  shift_id,
  date_id,
  
  -- Queue metrics
  COUNT(*) AS queue_events_count,
  AVG(duration_min) AS avg_queue_time_min,
  MAX(duration_min) AS max_queue_time_min,
  SUM(duration_min) / 60.0 AS total_queue_hours,
  
  -- Trucks affected
  COUNT(DISTINCT truck_id) AS trucks_affected

FROM stg_truck_states
WHERE operational_state IN ('queued_at_shovel', 'queued_at_crusher')
GROUP BY location_type, location_zone, shift_id, date_id;
```

## Schema Design Decisions

### Star Schema Rationale

**Why Star Schema?**
1. **Query Simplicity**: 1-2 joins max from fact to dimensions
2. **Performance**: Denormalized dimensions reduce join overhead
3. **User-Friendly**: Business analysts can write queries easily
4. **Aggregate-Friendly**: Pre-aggregation patterns work naturally
5. **Tool Compatibility**: Works with BI tools (Power BI, Tableau, etc.)

**Trade-offs Accepted**:
- ❌ Some data duplication in dimensions (acceptable at this scale)
- ❌ Updates harder in denormalized dims (not an issue for historical data)
- ❌ Slightly more storage (negligible for analysis workload)

**Alternatives Considered**:
- **Normalized (3NF)**: Rejected - too many joins for analytical queries
- **Snowflake**: Rejected - added complexity without significant benefit at this scale
- **Wide Fact Table**: Rejected - would require many NULL columns for optional metrics

### Dimension Design

**Slowly Changing Dimensions (SCD)**:
- Currently Type 1 (overwrite) for all dimensions
- Production would use Type 2 for truck attributes to track:
  - Maintenance history (truck condition changes over time)
  - Operator skill level progression
  - Equipment modifications

**Dimension Hierarchies**:
```
dim_truck:
  - truck_id (PK)
    └── fleet_class (100-ton, 200-ton, 400-ton)
        └── [Future] manufacturer hierarchy

dim_operator:
  - operator_id (PK)
    └── experience_level (Junior, Intermediate, Senior)
        └── [Future] crew/team hierarchy

dim_date:
  - date_key (PK)
    └── week
        └── month
            └── quarter
                └── year
```

### Fact Grain

**fact_haul_cycle Grain**: One row per complete haul cycle per truck

**Why This Grain?**
- Matches natural business process (complete cycle is atomic unit)
- Enables both summary metrics and detailed analysis
- Avoids excessive row counts (vs. event-level grain)
- Supports all required analytical queries

**Alternative Grains Considered**:
- **Event-Level**: Too granular (millions of rows), query performance issues
- **Daily Truck Aggregates**: Too coarse, loses cycle-level detail needed for analysis
- **State-Level**: Considered but rejected - states are staging, cycles are facts

## Performance Considerations

### Indexing Strategy

**Dimension Tables**:
- Primary keys auto-indexed (SQLite)
- No additional indexes needed (small dimensions)

**Staging Tables**:
```sql
-- stg_telemetry_events
CREATE INDEX idx_telemetry_truck_time ON stg_telemetry_events(truck_id, timestamp);
CREATE INDEX idx_telemetry_time ON stg_telemetry_events(timestamp);

-- stg_truck_states
CREATE INDEX idx_states_truck_time ON stg_truck_states(truck_id, state_start);
CREATE INDEX idx_states_operational ON stg_truck_states(operational_state);
```

**Fact Table**:
```sql
-- fact_haul_cycle
CREATE INDEX idx_cycle_truck_date ON fact_haul_cycle(truck_id, date_id);
CREATE INDEX idx_cycle_shovel ON fact_haul_cycle(shovel_id);
CREATE INDEX idx_cycle_crusher ON fact_haul_cycle(crusher_id);
CREATE INDEX idx_cycle_operator ON fact_haul_cycle(operator_id);
CREATE INDEX idx_cycle_shift ON fact_haul_cycle(shift_id);
CREATE INDEX idx_cycle_time ON fact_haul_cycle(cycle_start, cycle_end);
```

**Composite Indexes for Common Queries**:
```sql
-- Truck performance over time
CREATE INDEX idx_cycle_truck_date_payload ON fact_haul_cycle(truck_id, date_id, payload_tons);

-- Bottleneck analysis
CREATE INDEX idx_cycle_location_queue ON fact_haul_cycle(crusher_id, duration_queue_crusher_min);
```

### Materialization Strategy

**Metrics Tables**: Materialized as **tables** (not views)
- Faster query performance (no runtime aggregation)
- Refresh on schedule (hourly/daily batch jobs)
- Trade-off: Slightly stale data for much faster queries

**Analytics Views**: Materialized as **views**
- Benefit from metrics table materialization
- Always current (query latest metrics)
- Acceptable performance (metrics already pre-aggregated)

### Query Optimization Patterns

**Use Metrics Tables for Dashboards**:
```sql
-- GOOD: Query pre-aggregated metrics
SELECT * FROM truck_daily_productivity
WHERE date_id >= '2024-01-01';

-- AVOID: Aggregating facts on-the-fly in dashboards
SELECT truck_id, date_id, SUM(payload_tons), AVG(cycle_time)
FROM fact_haul_cycle
GROUP BY truck_id, date_id;
```

**Partition Large Queries**:
```sql
-- For date range queries on large fact tables
SELECT * FROM fact_haul_cycle
WHERE date_id BETWEEN '2024-01-01' AND '2024-01-31'
  AND truck_id = 'TRUCK-201';
-- Index on (truck_id, date_id) makes this efficient
```

## Extension Points

### Adding New Metrics

To add a new metric (e.g., "ton-miles per shift"):

1. **Create SQL model**:
```sql
-- models/metrics/ton_miles_analysis.sql
{{ config "materialized" "table" }}

SELECT
  shift_id,
  date_id,
  SUM(payload_tons * (distance_loaded_km + distance_empty_km)) AS total_ton_miles,
  SUM(payload_tons * (distance_loaded_km + distance_empty_km)) / 
    (COUNT(DISTINCT truck_id) * 12) AS ton_miles_per_truck_per_hour
FROM {{ ref "fact_haul_cycle" }}
GROUP BY shift_id, date_id;
```

2. **Add to profiles.yml** for testing

3. **Document in METRICS.md**

### Adding New Dimensions

To add a new dimension (e.g., "pit zone" or "material type"):

1. **Update schema.yml**:
```yaml
- name: dim_material_type
  columns:
    - name: material_type_id
      data_tests: [unique, not_null]
    - name: material_name
    - name: density_tons_per_m3
```

2. **Create seed data**: `seeds/dim_material_type.csv`

3. **Add foreign key to facts**:
```yaml
- name: fact_haul_cycle
  columns:
    - name: material_type_id
      data_tests:
        - relationships:
            to: dim_material_type
            field: material_type_id
```

4. **Update cycle aggregation logic** to capture material type

### Adding Real-Time Telemetry Ingestion

For production deployment with live telemetry:

1. **Replace stg_telemetry_events** with streaming ingestion:
   - Kafka consumer for fleet management system events
   - Batch insert events (e.g., every 10 seconds)
   - Handle late-arriving events

2. **Implement incremental state detection**:
   - Process only new telemetry since last run
   - Use watermarks for late data handling
   - Maintain state across runs (streaming aggregation)

3. **Incremental cycle completion**:
   - Detect cycle boundaries in real-time
   - Update fact table as cycles complete
   - Flag incomplete cycles (in-progress)

### Adding Predictive Maintenance

Extend the warehouse with ML features:

1. **Feature Engineering Table**:
```sql
CREATE TABLE ml_truck_features AS
SELECT
  truck_id,
  date_id,
  -- Performance features
  AVG(cycle_duration_min) AS avg_cycle_time,
  STDDEV(cycle_duration_min) AS cycle_time_variance,
  AVG(speed_avg_loaded_kmh) AS avg_loaded_speed,
  AVG(fuel_consumed_liters / payload_tons) AS fuel_per_ton,
  -- Trend features
  AVG(cycle_duration_min) OVER (PARTITION BY truck_id ORDER BY date_id ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS cycle_time_7day_avg,
  -- Flags
  COUNT(CASE WHEN duration_spot_delays_min > 10 THEN 1 END) AS spot_delay_count
FROM fact_haul_cycle
GROUP BY truck_id, date_id;
```

2. **Train ML model** (external to data warehouse):
   - Export features to Python/R
   - Train regression or classification model
   - Predict: days until maintenance needed

3. **Score in database**:
```sql
-- Apply model coefficients in SQL for real-time scoring
SELECT
  truck_id,
  (coef1 * avg_cycle_time + coef2 * fuel_per_ton + intercept) AS maintenance_risk_score
FROM ml_truck_features;
```

## Design Decisions

### Why Separate Staging Tables?

**Decision**: Use `stg_telemetry_events` → `stg_truck_states` → `fact_haul_cycle`

**Rationale**:
- **Separation of Concerns**: State detection logic isolated from cycle aggregation
- **Debuggability**: Can inspect intermediate states for validation
- **Performance**: State detection runs once, multiple downstream queries reuse results
- **Testing**: Can test state detection independently of cycle logic

**Alternative Considered**: Direct telemetry → facts
- Rejected: Mixing concerns, harder to debug, can't reuse state detection

### Why Single Crusher?

**Decision**: Example includes only 1 crusher (bottleneck by design)

**Rationale**:
- **Realistic**: Many operations have single primary crusher
- **Teaches Bottleneck Analysis**: Queue patterns demonstrate constraint management
- **Simplicity**: Reduces dimensions, easier to understand

**Production Adaptation**: Add `crusher_id` to differentiate multiple crushers if needed

### Why No Shift Handover Modeling?

**Decision**: Shift boundaries treated as hard cutoffs (no explicit handover state)

**Rationale**:
- **Simplicity**: Handovers are < 30 minutes, negligible impact on daily aggregates
- **Data Availability**: Handover delays harder to detect in telemetry

**Production Enhancement**: Add explicit handover detection:
- Identify long idle periods at shift boundaries
- Flag incomplete cycles at shift end
- Model shift overlap productivity

### Why PostgreSQL/SQLite?

**Decision**: Example uses SQLite for local development, production would use PostgreSQL

**Rationale**:
- **SQLite**: Zero config, file-based, perfect for examples and testing
- **PostgreSQL**: Production-grade, better concurrency, partitioning, window functions

**Migration Path**:
- All SQL is ANSI-standard (or close)
- PostgreSQL-specific features (partitioning, materialized views with refresh) would improve performance
- No code changes to application logic

## References

### Technical Papers

- "Real-Time Fleet Management in Open Pit Mining" - SME Mining Engineering Handbook
- "Haulage System Simulation and Optimization" - Mining Technology Journal
- "GPS-Based State Detection for Heavy Equipment" - ICCV 2018

### Tools and Systems

- **Fleet Management Systems**: Modular (Hitachi), MineStar (Caterpillar), DISPATCH (Hexagon)
- **Telemetry Standards**: ISO 15143 (AEMP), CAN Bus protocols
- **GIS/Geofencing**: Mapbox, QGIS for zone definition

### Related Patterns

- **Event Sourcing**: Telemetry as immutable event log
- **State Machine Modeling**: Operational states as finite state machine
- **Slowly Changing Dimensions**: Track historical changes in dimensions
- **Type 4 SCD**: History table pattern for auditing

---

**Note**: This architecture is designed for analytical workloads. Operational systems (dispatch, real-time monitoring) would require different optimizations (in-memory stores, stream processing, etc.).
