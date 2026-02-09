# Manufacturing Bottleneck Analysis - Schema Diagram

## Entity Relationship Diagram (ERD)

```
┌─────────────────────────┐
│   raw_resources         │
│  (Seed: CSV Import)     │
├─────────────────────────┤
│ PK resource_id          │
│    resource_name        │
│    resource_type        │
│    capacity_per_hour    │
└───────────┬─────────────┘
            │
            │ 1:N
            ▼
┌─────────────────────────┐         ┌─────────────────────────┐
│   dim_resource          │         │   dim_work_order        │
│  (Dimension Table)      │         │  (Dimension Table)      │
├─────────────────────────┤         ├─────────────────────────┤
│ PK resource_key         │         │ PK work_order_key       │
│    resource_id          │         │    work_order_id        │
│    resource_name        │         │    part_number          │
│    resource_type        │         │    quantity_ordered     │
│    capacity_per_hour    │         │    work_order_date      │
│    shifts_per_day       │         └───────────┬─────────────┘
└───────────┬─────────────┘                     │
            │                                   │
            │                                   │
            │                    ┌──────────────┘
            │                    │
            │ N                  │ N
            │                    │
            └────────┬───────────┘
                     │
                     ▼
            ┌─────────────────────────┐
            │   fact_operation        │
            │   (Fact Table)          │
            ├─────────────────────────┤
            │ PK operation_key        │
            │ FK resource_key         │
            │ FK work_order_key       │
            │ FK date_key             │
            │    operation_seq        │
            │    start_timestamp      │
            │    end_timestamp        │
            │    duration_hours       │
            │    parts_produced       │
            └───────────┬─────────────┘
                        │
                        │
                        ▼
            ┌─────────────────────────┐
            │ int_resource_daily_     │
            │     utilization         │
            │  (Intermediate Table)   │
            ├─────────────────────────┤
            │ PK resource_key         │
            │ PK operation_date       │
            │    total_operation_hrs  │
            │    available_hours      │
            │    utilization_pct      │
            │    downtime_hours       │
            │    adjusted_util_pct    │
            │    parts_produced       │
            │    avg_queue_time_hrs   │
            └───────────┬─────────────┘
                        │
                        │
                        ▼
            ┌─────────────────────────┐
            │ anl_bottleneck_ranking  │
            │   (Analytics Rollup)    │
            ├─────────────────────────┤
            │ PK resource_key         │
            │    resource_name        │
            │    avg_utilization_pct  │
            │    avg_queue_time_hrs   │
            │    avg_wip_accumulation │
            │    downtime_frequency   │
            │    bottleneck_score     │◄── Composite score
            │    bottleneck_rank      │
            └─────────────────────────┘


┌─────────────────────────┐         ┌─────────────────────────┐
│   raw_downtime          │         │   dim_date              │
│  (Seed: CSV Import)     │         │  (Dimension Table)      │
├─────────────────────────┤         ├─────────────────────────┤
│    downtime_id          │         │ PK date_key             │
│    resource_id          │         │    date                 │
│    event_timestamp      │         │    year                 │
│    duration_hours       │         │    quarter              │
│    downtime_type        │         │    month                │
│    reason               │         │    day_of_week          │
└───────────┬─────────────┘         │    is_weekend           │
            │                       └─────────────────────────┘
            │ N:1
            ▼
┌─────────────────────────┐
│   fact_downtime         │
│   (Fact Table)          │
├─────────────────────────┤
│ PK downtime_key         │
│ FK resource_key         │
│ FK date_key             │
│    event_timestamp      │
│    duration_hours       │
│    downtime_type        │
│    reason_category      │
└─────────────────────────┘
```

## Data Flow Diagram

```
PHASE 1: DATA IMPORT
┌──────────────────┐
│  CSV Files       │
│  (seeds/)        │
│                  │
│ • raw_resources  │
│ • raw_work_orders│
│ • raw_operations │
│ • raw_downtime   │
└────────┬─────────┘
         │ gorchata seed
         ▼
┌──────────────────┐
│  SQLite Database │
│  Seed Tables     │
└────────┬─────────┘
         │
         │

------------------------------------------------------------------

PHASE 2: DIMENSION BUILD
         │
         │ gorchata run --models dimensions
         ▼
┌────────────────────────────────────────┐
│  Dimension Tables (SCD Type 1)         │
│                                        │
│  • dim_resource                        │
│    - Natural key: resource_id          │
│    - Attributes: name, type, capacity  │
│                                        │
│  • dim_work_order                      │
│    - Natural key: work_order_id        │
│    - Attributes: part_number, quantity │
│                                        │
│  • dim_part                            │
│    - Natural key: part_number          │
│    - Attributes: name, category        │
│                                        │
│  • dim_date                            │
│    - Natural key: date                 │
│    - Attributes: year, quarter, month  │
└────────┬───────────────────────────────┘
         │
         │

------------------------------------------------------------------

PHASE 3: FACT BUILD
         │
         │ gorchata run --models facts
         ▼
┌────────────────────────────────────────┐
│  Fact Tables (Transaction Grain)       │
│                                        │
│  • fact_operation                      │
│    - Grain: One row per operation      │
│    - Measures: duration, parts_produced│
│    - Dimensions: resource, work_order  │
│                                        │
│  • fact_downtime                       │
│    - Grain: One row per downtime event │
│    - Measures: duration_hours          │
│    - Dimensions: resource, date        │
└────────┬───────────────────────────────┘
         │
         │

------------------------------------------------------------------

PHASE 4: INTERMEDIATE CALCULATIONS
         │
         │ gorchata run --models int_resource_daily_utilization
         ▼
┌────────────────────────────────────────┐
│  int_resource_daily_utilization        │
│  (Grain: Resource per Day)             │
│                                        │
│  Calculations:                         │
│  • utilization_pct =                   │
│      (operation_hours / available_hrs) │
│      * 100                             │
│                                        │
│  • adjusted_utilization_pct =          │
│      (operation_hours /                │
│       (available_hrs - downtime_hrs))  │
│      * 100                             │
│                                        │
│  • avg_queue_time_hrs =                │
│      AVG(LAG window function on        │
│          operation timestamps)         │
└────────┬───────────────────────────────┘
         │
         │

------------------------------------------------------------------

PHASE 5: ANALYTICS ROLLUP
         │
         │ gorchata run --models anl_bottleneck_ranking
         ▼
┌────────────────────────────────────────┐
│  anl_bottleneck_ranking                │
│  (Grain: One row per resource)         │
│                                        │
│  Composite Bottleneck Score:           │
│  ─────────────────────────────────     │
│  score = (avg_utilization_pct * 0.40)  │
│        + (avg_queue_time_hrs * 100     │
│           * 0.30)                      │
│        + (avg_wip_accumulation * 0.20) │
│        + (downtime_frequency * 0.10)   │
│                                        │
│  Rank resources by score (DESC)        │
│  → NCX-10 and Heat Treat at top        │
└────────────────────────────────────────┘
```

## Table Specifications

### Dimension Tables

#### dim_resource
- **Grain**: One row per manufacturing resource
- **Primary Key**: `resource_key` (surrogate)
- **Natural Key**: `resource_id`
- **Attributes**:
  - `resource_name` (e.g., "NCX-10", "Heat Treat")
  - `resource_type` (e.g., "Machine", "Department")
  - `theoretical_capacity_per_hour`
  - `available_hours_per_shift`
  - `shifts_per_day`
- **SCD Type**: Type 1 (overwrite)

#### dim_work_order
- **Grain**: One row per work order
- **Primary Key**: `work_order_key` (surrogate)
- **Natural Key**: `work_order_id`
- **Attributes**:
  - `part_number`
  - `quantity_ordered`
  - `work_order_date`
  - `priority_level`
- **SCD Type**: Type 1 (overwrite)

#### dim_date
- **Grain**: One row per calendar date
- **Primary Key**: `date_key` (surrogate)
- **Natural Key**: `date`
- **Attributes**:
  - `year`, `quarter`, `month`
  - `day_of_week`, `day_of_month`
  - `is_weekend`, `is_holiday`

### Fact Tables

#### fact_operation
- **Grain**: One row per manufacturing operation
- **Primary Key**: `operation_key` (surrogate)
- **Foreign Keys**:
  - `resource_key` → dim_resource
  - `work_order_key` → dim_work_order
  - `date_key` → dim_date
- **Measures**:
  - `duration_hours` (numeric, additive)
  - `parts_produced` (integer, additive)
  - `setup_time_hours` (numeric, additive)
- **Degenerate Dimensions**:
  - `operation_seq` (sequence within work order)
- **Timestamps**:
  - `start_timestamp`, `end_timestamp`

#### fact_downtime
- **Grain**: One row per downtime event
- **Primary Key**: `downtime_key` (surrogate)
- **Foreign Keys**:
  - `resource_key` → dim_resource
  - `date_key` → dim_date
- **Measures**:
  - `duration_hours` (numeric, additive)
- **Attributes**:
  - `downtime_type` (planned, unplanned)
  - `reason_category` (maintenance, breakdown, setup)

### Intermediate Tables

#### int_resource_daily_utilization
- **Grain**: One row per resource per day
- **Primary Key**: `resource_key`, `operation_date` (composite)
- **Foreign Keys**:
  - `resource_key` → dim_resource
- **Measures**:
  - `total_operation_hours` (sum of operation durations)
  - `available_hours` (theoretical capacity for the day)
  - `downtime_hours` (sum of downtime durations)
  - `parts_produced` (sum of parts produced)
- **Calculated Metrics**:
  - `utilization_pct` = (operation_hours / available_hours) × 100
  - `adjusted_utilization_pct` = operation_hours / (available_hours - downtime_hours) × 100
  - `avg_queue_time_hrs` = average queue time from LAG window function

### Analytics Rollups

#### anl_bottleneck_ranking
- **Grain**: One row per resource (summary across all dates)
- **Primary Key**: `resource_key`
- **Foreign Keys**:
  - `resource_key` → dim_resource
- **Aggregated Metrics**:
  - `avg_utilization_pct` (average across all days)
  - `avg_queue_time_hrs` (average queue time)
  - `avg_wip_accumulation` (average work-in-process)
  - `downtime_frequency` (count of downtime events)
- **Composite Score**:
  - `bottleneck_score` (weighted combination)
  - `bottleneck_rank` (ordinal ranking, 1 = highest bottleneck)

## Calculation Formulas

### Queue Time Calculation
```sql
-- Use LAG window function to calculate time between operations
LAG(end_timestamp) OVER (
    PARTITION BY resource_id 
    ORDER BY start_timestamp
) AS previous_operation_end,

-- Queue time = current start - previous end
(JULIANDAY(start_timestamp) - JULIANDAY(previous_operation_end)) * 24 
    AS queue_time_hours
```

### Utilization Calculation
```sql
-- Basic utilization (ignoring downtime)
utilization_pct = (SUM(operation_duration) / available_hours) * 100

-- Adjusted utilization (accounting for downtime)
adjusted_utilization_pct = 
    (SUM(operation_duration) / (available_hours - downtime_hours)) * 100
```

### Composite Bottleneck Score
```sql
bottleneck_score = 
    (avg_utilization_pct * 0.40)        -- 40% weight on utilization
  + (avg_queue_time_hrs * 100 * 0.30)   -- 30% weight on queue time
  + (avg_wip_accumulation * 0.20)       -- 20% weight on WIP
  + (downtime_frequency * 0.10)         -- 10% weight on downtime
```

## Bottleneck Identification Logic

The `anl_bottleneck_ranking` rollup uses a multi-factor approach:

1. **High Utilization** (40% weight): Resources operating near capacity (>85%)
2. **Queue Time** (30% weight): Time parts wait before processing
3. **WIP Accumulation** (20% weight): Work-in-process building up
4. **Downtime Impact** (10% weight): Frequency of disruptions

**Expected Results:**
- **NCX-10**: High score due to 85-95% utilization, 2-4 hour queues
- **Heat Treat**: Secondary bottleneck with 75-85% utilization
- **Milling/Assembly**: Lower scores, not constraints

## Relationships Summary

- **1:N**: One resource → many operations
- **1:N**: One work order → many operations
- **N:1**: Many operations → one date
- **N:1**: Many downtime events → one resource
- **1:1**: One resource → one daily utilization record per date
- **1:1**: One resource → one bottleneck ranking (summary)

## Data Quality Considerations

- **Referential Integrity**: All foreign keys validated
- **Temporal Consistency**: Start timestamp < end timestamp
- **Logical Bounds**: Utilization ≤ 100%, queue time ≥ 0
- **Completeness**: No NULL values in fact measures
- **Uniqueness**: No duplicate operations or downtime events

