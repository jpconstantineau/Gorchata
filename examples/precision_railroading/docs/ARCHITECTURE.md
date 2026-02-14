# Precision Scheduled Railroading - Architecture Documentation

## Table of Contents
1. [Overview](#overview)
2. [Data Model](#data-model)
3. [Data Lineage](#data-lineage)
4. [Technical Stack](#technical-stack)
5. [Dimensional Modeling](#dimensional-modeling)
6. [State-Interval Transform](#state-interval-transform)
7. [Minute-Level Precision](#minute-level-precision)
8. [PSR Business Logic](#psr-business-logic)
9. [Testing Strategy](#testing-strategy)
10. [Build Process](#build-process)

---

## Overview

The Precision Scheduled Railroading (PSR) data warehouse is built on a **star schema** design optimized for analytic queries. The architecture follows industry-standard dimensional modeling practices with a clear separation of concerns across four layers:

1. **Staging Layer**: Raw CLM events ingestion
2. **Intermediate Layer**: Business logic transformations and state-interval modeling
3. **Dimensional/Fact Layer**: Star schema with conformed dimensions
4. **Metrics Layer**: Pre-aggregated KPI tables
5. **Analytics Layer**: High-level business insight queries

```
┌─────────────────────────────────────────────────────────────┐
│                     ANALYTICS LAYER                         │
│  worst_corridors | shadow_yards | seasonal_trends |         │
│  psr_shifts | congestion_hotspots | directional_efficiency │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                      METRICS LAYER                          │
│  agg_network_fluidity | agg_shadow_yards | agg_psr_evolution│
│  agg_slot_adherence | agg_directional_asymmetry |          │
│  agg_congestion | agg_corridor_weekly_performance          │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                 DIMENSIONAL/FACT LAYER                      │
│  fact_trip | fact_dwell | fact_corridor_transit |          │
│  dim_location | dim_railcar | dim_train | dim_corridor     │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                  INTERMEDIATE LAYER                         │
│  int_trip_segments | int_dwell_classification |            │
│  int_velocity_vectors | int_nodal_dwell |                  │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                    STAGING LAYER                            │
│  raw_clm_events (seed CSV data)                            │
└─────────────────────────────────────────────────────────────┘
```

---

## Data Model

### Star Schema Design

The dimensional model follows Kimball methodology with a clear separation between dimensions (descriptive attributes) and facts (measurable events).

#### Fact Tables

**1. fact_trip**
- **Grain**: One row per trip segment (origin → destination movement)
- **Measures**:
  - distance_miles (additive)
  - duration_minutes (semi-additive)
  - average_velocity_mph (non-additive)
  - dwell_count (additive)
  - stop_count (additive)
- **Dimensions**: railcar, train, corridor, origin_location, destination_location, trip_start_date
- **Degenerate Dimensions**: trip_segment_id, trip_type (loaded/empty), psr_period

**2. fact_dwell**
- **Grain**: One row per dwell event (stop at a location)
- **Measures**:
  - dwell_duration_minutes (additive)
- **Dimensions**: railcar, location, dwell_start_date
- **Degenerate Dimensions**: dwell_id, facility_type, dwell_classification, shadow_yard_flag, is_loaded, event_type_at_arrival, event_type_at_departure, psr_period

**3. fact_corridor_transit** (not fully implemented in example)
- **Grain**: One row per corridor passage
- **Measures**: transit_time_minutes, velocity_mph
- **Dimensions**: railcar, corridor, transit_date

**4. fact_stop_classification** (not fully implemented in example)
- **Grain**: One row per stop with detailed classification
- **Measures**: stop_duration_minutes, priority_score
- **Dimensions**: railcar, location, stop_date, stop_type

#### Dimension Tables

**1. dim_location**
- **Grain**: One row per physical location (SPLC code)
- **Attributes**: location_name, location_type, region, state, latitude, longitude, elevation, timezone, yard_capacity
- **SCD Type**: Type 1 (overwrites)
- **Row Count**: 30-50 locations

**2. dim_railcar**
- **Grain**: One row per railcar
- **Attributes**: car_number, car_type, capacity_tons, tare_weight, owner, last_inspection_date
- **SCD Type**: Type 1
- **Row Count**: 30 railcars

**3. dim_train**
- **Grain**: One row per train consist
- **Attributes**: train_number, train_type, origin_terminal, destination_terminal, scheduled_departure_time, priority_level
- **SCD Type**: Type 1
- **Row Count**: 10-15 trains

**4. dim_corridor**
- **Grain**: One row per origin-destination corridor pair
- **Attributes**: corridor_code, origin_splc, destination_splc, distance_miles, lane_type, traffic_volume_class, congestion_level
- **Derived**: Generated from CLM event pairs (high-traffic OD pairs)
- **Row Count**: 30-40 corridors

**5. dim_date** (not fully implemented, implicit)
- **Grain**: One row per calendar date
- **Attributes**: date_key, full_date, year, quarter, month, week, day_of_week

---

## Data Lineage

### End-to-End Data Flow

```
┌──────────────────┐
│ raw_clm_events   │  30 railcars × 25 events = 750 rows
│ (Seed CSV)       │  Event types: DEPA, ARRI, LOAD, UNLD, DWEL
└────────┬─────────┘
         │
         ▼
┌───────────────────────────────┐
│ INTERMEDIATE LAYER            │
│                               │
│ int_trip_segments             │  30 trips
│   - State-interval transform  │  - Pair DEPA → ARRI events
│   - PSR period assignment     │  - Calculate duration, assign PSR period
│                               │
│ int_velocity_vectors          │  30 velocity calculations
│   - Distance calculation      │  - Lat/long → distance (Haversine-like)
│   - Velocity = dist/time * 60 │  - mph calculation
│                               │
│ int_dwell_classification      │  30 dwells
│   - Classify dwell types      │  - Shadow yard detection at event level
│   - Shadow yard flagging      │  - Dwell classification logic
│                               │
│ int_nodal_dwell               │  Dwells with DEPA context
│   - Dwells + DEPA join        │  - Prepare for trip-dwell join
└────────┬──────────────────────┘
         │
         ▼
┌───────────────────────────────┐
│ DIMENSIONAL/FACT LAYER        │
│                               │
│ dim_location                  │  30-50 unique locations
│   - From SPLC codes           │  - Enriched with synthetic attributes
│                               │
│ dim_corridor                  │  30-40 corridors
│   - From OD pairs (DEPA→ARRI) │  - High-traffic pairs only (>10 trips)
│   - Distance, lane type       │
│                               │
│ fact_trip                     │  30 trips
│   - Joins: trip_segments +    │  - All dimensions joined
│     velocity + train + corridor│  - Dwell counts aggregated
│                               │
│ fact_dwell                    │  30 dwells
│   - From int_dwell_class.     │  - Enriched with date dimension
└────────┬──────────────────────┘
         │
         ▼
┌───────────────────────────────┐
│ METRICS LAYER                 │
│                               │
│ agg_network_fluidity          │  1-2 rows (network-level aggregation)
│   - AVG(velocity), AVG(dist)  │  - May group by corridor_id (mostly NULL)
│                               │
│ agg_shadow_yards              │  5 locations
│   - Shadow yard % by location │  - Locations with shadow yard patterns
│   - Dwell statistics          │
│                               │
│ agg_psr_evolution             │  3 rows (pre-PSR, transition, mature)
│   - Metrics by PSR period     │  - Pre-PSR and transition may be empty
│   - Velocity, dwell, duration │  - All periods guaranteed (left join)
│                               │
│ agg_directional_asymmetry     │  0 rows (no corridors in dataset)
│   - Loaded vs empty velocity  │  - All corridor_id NULL in fact_trip
│                               │
│ agg_corridor_weekly_perf.     │  Variable (temporal grouping)
│   - Weekly aggregations       │  - By corridor + week
└────────┬──────────────────────┘
         │
         ▼
┌───────────────────────────────┐
│ ANALYTICS LAYER               │
│                               │
│ worst_performing_corridors    │  0 rows (corridor_id filter)
│   - Ranked by fluidity        │  - Requires corridor_id NOT NULL
│                               │
│ shadow_yard_identification    │  5 locations
│   - Composite scoring         │  - Sophisticated detection logic
│   - Top 10 suspicious locs    │
│                               │
│ seasonal_performance_trends   │  1 row (single quarter)
│   - QoQ and YoY changes       │  - Temporal aggregation
│   - Winter/summer patterns    │
│                               │
│ psr_strategy_shifts           │  1 row (mature period only)
│   - Period-over-period deltas │  - Compares vs baseline
│   - Asset utilization         │  - First period used as baseline
│                               │
│ network_congestion_hotspots   │  5 locations
│   - Congestion score ranking  │  - Composite metric
│   - Severity classification   │
│                               │
│ directional_efficiency        │  0 rows (requires corridor_id)
│   - Asymmetry ratio           │  - Loaded vs empty comparison
└───────────────────────────────┘
```

### Row Count Expectations (30-trip dataset)

| Table | Expected Rows | Actual | Notes |
|-------|--------------|--------|-------|
| raw_clm_events | ~750 | 750 | 30 cars × 25 events |
| fact_trip | 30 | 30 | One trip per car |
| fact_dwell | 30 | 30 | One dwell per car |
| dim_location | 30-50 | 42 | Unique SPLC codes |
| dim_corridor | 30-40 | 0 | All corridor_id NULL |
| agg_shadow_yards | 3-10 | 5 | Locations with patterns |
| agg_psr_evolution | 3 | 3 | All periods (some empty) |
| psr_strategy_shifts | 1-3 | 1 | Periods with data only |

**Note**: Many corridor-based analytics return 0 rows because corridor assignments are NULL in the test dataset (Phase 6 limitation). Full production data would populate corridors.

---

## Technical Stack

### Core Technologies

**1. Gorchata (dbt-like Framework)**
- **Purpose**: SQL transformation orchestration
- **Features**:
  - Jinja2-like templating (`{{ ref "table_name" }}`, `{{ config "materialized" "table" }}`)
  - Dependency resolution and DAG execution
  - Incremental model support (not used in this example)
  - Seed data loading from CSV

**2. SQLite (Pure Go Driver)**
- **Driver**: `modernc.org/sqlite` v1.44.3
- **Constraint**: `CGO_ENABLED=0` (no C dependencies)
- **Functions**:
  - `julianday()`: Convert timestamps to Julian day numbers
  - `strftime()`: Format and extract timestamp components
  - `CAST()`, `ROUND()`, `COALESCE()`: Standard SQL functions
  - Manual STDDEV calculation (SQLite lacks built-in)

**3. Go 1.25+**
- **Usage**: 
  - Data generation (generate_clm_data.go)
  - Build tooling (build_phaseX.go)
  - Test execution (test_phaseX.go)
- **Constraint**: No CGO, pure Go only

**4. PowerShell**
- **Scripts**: `build_phaseX.ps1`, `test_phaseX.ps1`
- **Purpose**: Orchestrate Go tool builds and executions
- **Environment**: Sets `CGO_ENABLED=0` before `go build`

---

## Dimensional Modeling

### Grain Definitions

Each fact table has a clearly defined grain (level of detail):

**fact_trip Grain**: One row per trip segment
- **Atomic Event**: A railcar moving from origin to destination in one continuous movement
- **Date/Time**: trip_start_timestamp to trip_end_timestamp
- **Identifiers**: trip_segment_id (surrogate key), railcar_id, train_id, corridor_id

**fact_dwell Grain**: One row per dwell event
- **Atomic Event**: A railcar stopping at a location
- **Date/Time**: dwell_start_timestamp to dwell_end_timestamp
- **Identifiers**: dwell_id (surrogate key), railcar_id, location_id

### Foreign Key Relationships

```
fact_trip
  |
  ├── FK: railcar_id → dim_railcar.railcar_id
  ├── FK: train_id → dim_train.train_id
  ├── FK: corridor_id → dim_corridor.corridor_id
  ├── FK: origin_location_id → dim_location.location_id
  ├── FK: destination_location_id → dim_location.location_id
  └── FK: trip_start_date_id → dim_date.date_key (implicit)

fact_dwell
  |
  ├── FK: railcar_id → dim_railcar.railcar_id
  ├── FK: location_id → dim_location.location_id
  └── FK: dwell_start_date_id → dim_date.date_key (implicit)
```

### Slowly Changing Dimensions (SCD)

**Current Implementation**: Type 1 (Overwrite)
- All dimensions use Type 1 SCD
- Historical changes are not tracked
- Simplifies queries and reduces storage

**Future Enhancement**: Type 2 (Historical Tracking)
- Track effective_start_date, effective_end_date, is_current
- Maintain full history for regulatory compliance
- Would apply to dim_railcar (ownership changes) and dim_location (capacity changes)

---

## State-Interval Transform

### State-Event Model

CLM raw data comes as **state events** (momentary observations):
```
| car_number | timestamp           | event_type | splc_code |
|------------|---------------------|------------|-----------|
| CAR001     | 2023-01-15 08:00:00 | DEPA       | LOC123    |
| CAR001     | 2023-01-15 12:30:00 | ARRI       | LOC456    |
```

### Interval Transform

Analytics requires **intervals** (durations with start and end):
```
| trip_segment_id | car | start_time | end_time   | origin | dest   | duration |
|-----------------|-----|------------|------------|--------|--------|----------|
| TRIP001         | CAR001 | 08:00:00 | 12:30:00 | LOC123 | LOC456 | 270 min  |
```

### Transformation Logic

**Algorithm** (in int_trip_segments.sql):
```sql
WITH departure_events AS (
  SELECT * FROM raw_clm_events WHERE event_type = 'DEPA'
),
arrival_events AS (
  SELECT * FROM raw_clm_events WHERE event_type = 'ARRI'
)
SELECT
  d.timestamp AS trip_start_timestamp,
  a.timestamp AS trip_end_timestamp,
  (julianday(a.timestamp) - julianday(d.timestamp)) * 24 * 60 AS duration_minutes
FROM departure_events d
JOIN arrival_events a 
  ON d.car_number = a.car_number
  AND a.timestamp > d.timestamp
  AND a.timestamp = (
    SELECT MIN(timestamp) 
    FROM arrival_events a2 
    WHERE a2.car_number = d.car_number 
      AND a2.timestamp > d.timestamp
  )
```

**Key Techniques**:
1. **Self-join with inequality**: `a.timestamp > d.timestamp`
2. **Nearest neighbor**: `MIN(timestamp)` finds next ARRI event
3. **Duration calculation**: `(julianday(end) - julianday(start)) * 24 * 60` for minutes

---

## Minute-Level Precision

### Timestamp Handling

All temporal calculations preserve **minute-level granularity**:

```sql
-- Duration in minutes (preserves fractional minutes)
duration_minutes = (julianday(end_time) - julianday(start_time)) * 24 * 60

-- Velocity in mph (distance per minute × 60)
velocity_mph = (distance_miles / duration_minutes) * 60

-- Example:
-- 150 miles traveled in 3 hours 27 minutes (207 minutes)
-- velocity_mph = (150 / 207) * 60 = 43.48 mph
```

### Why Minute Precision Matters

1. **Operational Accuracy**: Railroad schedules operate on minute-level precision
2. **Velocity Calculations**: Sub-hour precision prevents rounding errors (18.5 mph vs 18 mph vs 19 mph)
3. **Dwell Classification**: Short dwells (< 30 min) require fine granularity
4. **Schedule Adherence**: Slot adherence measured in minutes, not hours

### SQLite Date Functions

```sql
-- Convert to Julian day (days since 4714 BC)
julianday('2023-01-15 08:30:00')  -- Result: 2459956.854166667

-- Calculate difference in days
julianday(end) - julianday(start)  -- Result: 0.1458333 (3.5 hours)

-- Convert to minutes
(julianday(end) - julianday(start)) * 24 * 60  -- Result: 210 minutes

-- Extract components
strftime('%Y', timestamp)  -- Year
strftime('%m', timestamp)  -- Month
strftime('%H', timestamp)  -- Hour
```

---

## PSR Business Logic

### Gradual Adoption Modeling (2016-2025)

PSR adoption is not binary; railroads transition gradually. The model reflects this:

**Period Assignment Logic** (in int_trip_segments.sql):
```sql
CASE
  WHEN CAST(strftime('%Y', departure_time) AS INTEGER) <= 2017 THEN 'pre-PSR'
  WHEN CAST(strftime('%Y', departure_time) AS INTEGER) BETWEEN 2018 AND 2020 THEN 'transition'
  WHEN CAST(strftime('%Y', departure_time) AS INTEGER) >= 2021 THEN 'mature'
END AS psr_period
```

**Operational Characteristics by Period**:

| Aspect | Pre-PSR | Transition | Mature |
|--------|---------|------------|--------|
| Network Design | Hub-and-spoke | Mixed | Point-to-point |
| Velocity Target | 18-22 mph | 25-30 mph | 32-40 mph |
| Dwell Optimization | Limited | Active | Fully optimized |
| Shadow Yards | Many (uncontrolled) | Reducing | Minimal |
| Schedule Precision | Low (ad-hoc) | Improving | High (slot adherence) |

### Shadow Yard Detection

**Business Context**: Under PSR, railroads aim to eliminate unofficial holding locations ("shadow yards") where railcars accumulate outside of official yards. These indicate:
- Hidden capacity constraints
- Operational inefficiencies
- Network congestion points

**Detection Algorithm** (in shadow_yard_identification.sql):

1. **Primary Indicator**: Shadow yard percentage > 30%
   ```sql
   shadow_yard_percentage = (total_dwell_time_at_location / total_time_in_network) * 100
   ```

2. **Secondary Indicators**:
   - High dwell variance (unpredictable usage)
   - Time-of-day clustering (systematic patterns)

3. **Composite Score**:
   ```sql
   composite_score = (shadow_yard_% * 0.5) + (variance_score * 0.3) + (time_clustering * 0.2)
   ```

4. **Threshold**: Locations with `composite_score > 60` flagged as shadow yards

---

## Testing Strategy

### TDD Approach

**Mandatory Workflow**:
1. Write test SQL first (test_*.sql)
2. Run test and confirm failure (analytics don't exist yet)
3. Implement analytics SQL
4. Run test until passing
5. Refactor while keeping tests green

### Test Coverage (Phase 8)

**Execution Tests** (6 tests): Verify queries execute without error
- `test_worst_corridors_executes`
- `test_shadow_yard_executes`
- `test_seasonal_trends_executes`
- `test_psr_shifts_executes`
- `test_congestion_hotspots_executes`
- `test_directional_efficiency_executes`

**Logic Tests** (24 tests): Validate business rules
- Range checks (velocity 0-80 mph, scores 0-100)
- Positive counts (trip_count > 0)
- Sequential rankings (fluidity_rank = 1, 2, 3...)
- Threshold logic (shadow_yard_flag when score > 60)
- Period coverage (at least 1 PSR period)

### Test Patterns

**Data Quality Tests** (common pattern):
```sql
WITH tests AS (
  SELECT 
    'test_name' AS test_name,
    COUNT(*) AS violation_count,
    'Description' AS description,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status
  FROM analytics_table
  WHERE invalid_condition
  
  UNION ALL
  
  -- More tests...
)
SELECT * FROM tests ORDER BY test_name
```

**Test Execution** (Go tool pattern):
```go
for _, testFile := range testFiles {
    sqlContent := processTemplate(readFile(testFile))
    rows := db.Query(sqlContent)
    for rows.Next() {
        var result TestResult
        rows.Scan(&result.TestName, &result.ViolationCount, ...)
        if result.Status == "PASS" {
            passedTests++
        } else {
            failedTests++
        }
    }
}
```

---

## Build Process

### Build Script Architecture

**PowerShell Wrapper** (`build_phase8.ps1`):
```powershell
$env:CGO_ENABLED = "0"  # Force pure Go
go build -o build_phase8_tool.exe build_phase8.go
.\build_phase8_tool.exe
Remove-Item build_phase8_tool.exe
```

**Go Build Tool** (`build_phase8.go`):
```go
models := []Model{
    {Name: "worst_performing_corridors", Path: "models/analytics/..."},
    {Name: "shadow_yard_identification", Path: "models/analytics/..."},
    // ...
}

for _, model := range models {
    sqlContent := readFile(model.Path)
    sqlContent = processTemplate(sqlContent)  // {{ ref }} processing
    createSQL := fmt.Sprintf("CREATE TABLE %s AS\n%s", model.Name, sqlContent)
    db.Exec(createSQL)
}
```

### Template Processing

**Jinja2-like Syntax** (Gorchata templates):
```sql
{{ config "materialized" "table" }}

SELECT *
FROM {{ ref "fact_trip" }}
JOIN {{ ref "dim_corridor" }} c ON ...
```

**Go Processing** (regex-based):
```go
func processTemplate(sql string) string {
    // Remove config directives
    re := regexp.MustCompile(`\{\{\s*config\s*"materialized".*\}\}`)
    sql = re.ReplaceAllString(sql, "")
    
    // Replace ref() calls
    refPatterns := map[string]string{
        `\{\{\s*ref\s*"fact_trip"\s*\}\}`: "fact_trip",
        `\{\{\s*ref\s*"dim_corridor"\s*\}\}`: "dim_corridor",
        // ...
    }
    for pattern, replacement := range refPatterns {
        re := regexp.MustCompile(pattern)
        sql = re.ReplaceAllString(sql, replacement)
    }
    return sql
}
```

### Dependency Ordering

Analytics depend on metrics, which depend on facts, which depend on intermediates:

```
Analytics Layer  → build_phase8.ps1  (depends on metrics)
Metrics Layer    → build_phase7.ps1  (depends on facts)
Facts Layer      → build_phase6.ps1  (depends on intermediate)
Intermediate     → build_phase5.ps1  (depends on dimensions)
Dimensions       → build_phase4.ps1  (depends on staging)
Staging          → Seed CSV load
```

**Build Order**:
```powershell
.\build_phase4.ps1  # Dimensions
.\build_phase5.ps1  # Intermediate
.\build_phase6.ps1  # Facts
.\build_phase7.ps1  # Metrics
.\build_phase8.ps1  # Analytics
```

---

## Conclusion

This architecture demonstrates a production-grade analytical data warehouse with:

- **Clear separation of concerns** across 5 layers
- **Dimensional modeling** following Kimball methodology
- **State-to-interval transformation** for event-based source data
- **Minute-level temporal precision** for operational accuracy
- **Pure Go + SQLite stack** with no CGO dependencies
- **TDD-driven development** with comprehensive test coverage
- **Automated build pipeline** using PowerShell + Go tooling

The design balances analytical query performance, data quality, and maintainability while modeling complex real-world PSR business logic.

For metric definitions and business interpretation, see [METRICS.md](./METRICS.md).
