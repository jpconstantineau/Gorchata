# DCS Alarm Analytics - Schema Documentation

This document provides comprehensive schema documentation for the DCS Alarm Analytics dimensional model, including entity relationships, column specifications, and data flow diagrams.

## Schema Overview

The dimensional model follows a **star schema** pattern optimized for ISA 18.2 alarm analytics:

- **2 Source Tables**: Raw alarm events and configuration data
- **7 Dimension Tables**: Tag, equipment, process area, priority, operator, dates, time buckets
- **2 Fact Tables**: Alarm occurrence lifecycle and state transitions
- **5 Rollup Tables**: Pre-aggregated analytics for operator loading, standing alarms, chattering, bad actors, and system health

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    SOURCE LAYER (Raw Data)                  │
│─────────────────────────────────────────────────────────────│
│  raw_alarm_events (54 events)  raw_alarm_config (15 tags)  │
└──────────────────┬──────────────────────┬───────────────────┘
                   │                      │
                   ▼                      ▼
┌─────────────────────────────────────────────────────────────┐
│                  DIMENSION LAYER (Reference Data)           │
│─────────────────────────────────────────────────────────────│
│  dim_alarm_tag (15 tags)          dim_equipment (12)       │
│  dim_process_area (2)             dim_priority (4)         │
│  dim_operator (3)                 dim_dates (2 days)       │
│  dim_time (144 buckets)                                     │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│                    FACT LAYER (Transactions)                │
│─────────────────────────────────────────────────────────────│
│  fct_alarm_occurrence (18 alarm lifecycles)                 │
│  fct_alarm_state_change (54 state transitions)             │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│               ROLLUP LAYER (Pre-Aggregated Marts)           │
│─────────────────────────────────────────────────────────────│
│  rollup_operator_loading_hourly (10-min buckets)           │
│  rollup_standing_alarms (tags with >10min ack time)        │
│  rollup_chattering_alarms (rapid state cycling detection)  │
│  rollup_bad_actor_tags (Pareto analysis of worst tags)     │
│  rollup_alarm_system_health (ISA 18.2 compliance scorecard)│
└─────────────────────────────────────────────────────────────┘
```

## Entity Relationship Diagram

```
┌─────────────────────────┐          ┌──────────────────────────┐
│   dim_alarm_tag (SCD2)  │          │     dim_equipment        │
│─────────────────────────│          │──────────────────────────│
│ tag_key (PK) *          │          │ equipment_id (PK)        │
│ tag_id (NK)             │◄─────────┤ equipment_type           │
│ tag_description         │          │ equipment_description    │
│ equipment_id (FK) ──────┼─────────►└──────────────────────────┘
│ priority_code           │
│ high_limit              │          ┌──────────────────────────┐
│ low_limit               │          │   dim_process_area       │
│ valid_from *            │          │──────────────────────────│
│ valid_to *              │◄─────────┤ area_key (PK)            │
│ is_current *            │          │ area_code (NK)           │
└──────────┬──────────────┘          │ area_name                │
           │                         └──────────────────────────┘
           │
           │                         ┌──────────────────────────┐
           │                         │     dim_priority         │
           │                         │──────────────────────────│
           │                         │ priority_key (PK)        │
           │                         │ priority_code (NK)       │
           │                         │ priority_description     │
           │                         │ priority_rank            │
           │                         └────────┬─────────────────┘
           │                                  │
           │                                  │
           │          ┌──────────────────────────┐
           │          │     dim_operator         │
           │          │──────────────────────────│
           │          │ operator_key (PK)        │
           │          │ operator_id (NK)         │
           │          │ operator_name            │
           │          └────────┬─────────────────┘
           │                   │
           ▼                   ▼                   ┌──────────────────────────┐
┌────────────────────────────────────────────┐     │      dim_dates           │
│     fct_alarm_occurrence (Fact)            │     │──────────────────────────│
│────────────────────────────────────────────│     │ date_key (PK)            │
│ occurrence_key (PK)                        │◄────┤ full_date                │
│ tag_key (FK) ─────────────────────────────►│     │ year                     │
│ equipment_key (FK)                         │     │ quarter                  │
│ area_key (FK) ─────────────────────────────►│     │ month                    │
│ priority_key (FK) ─────────────────────────►│     │ day, day_of_week         │
│ operator_key_ack (FK) ─────────────────────►│     │ is_weekend               │
│ activation_date_key (FK) ──────────────────►│     └──────────────────────────┘
│ activation_timestamp                       │
│ acknowledged_timestamp                     │     ┌──────────────────────────┐
│ inactive_timestamp                         │     │      dim_time            │
│ duration_to_ack_sec                        │     │──────────────────────────│
│ duration_to_resolve_sec                    │     │ time_key (PK)            │
│ alarm_value, setpoint_value                │     │ time_bucket_key          │
│ is_standing_10min (ISA 18.2) *             │     │ hour, minute             │
│ is_standing_24hr *                         │     │ time_bucket_start        │
│ is_fleeting *                              │     │ time_bucket_end          │
│ is_acknowledged                            │     └──────────────────────────┘
│ is_resolved                                │
└────────────┬───────────────────────────────┘
             │
             │ occurrence_key linkage
             │
             ▼
┌────────────────────────────────────────────┐
│   fct_alarm_state_change (Fact)            │
│────────────────────────────────────────────│
│ state_change_key (PK)                      │
│ occurrence_key (FK - nullable) ────────────┤
│ tag_key (FK)                               │
│ change_timestamp                           │
│ from_state, to_state                       │
│ sequence_number                            │
│ time_since_last_change_sec                 │
└────────────┬───────────────────────────────┘
             │
             │ Aggregated to rollup tables
             │
             ▼
┌──────────────────────────────────────────────────────────┐
│              ROLLUP TABLES (Analytics Marts)             │
│──────────────────────────────────────────────────────────│
│  rollup_operator_loading_hourly                          │
│  rollup_standing_alarms                                  │
│  rollup_chattering_alarms                                │
│  rollup_bad_actor_tags                                   │
│  rollup_alarm_system_health                              │
└──────────────────────────────────────────────────────────┘

Legend:
  * = SCD Type 2 columns or ISA 18.2 derived metrics
  PK = Primary Key
  FK = Foreign Key
  NK = Natural/Business Key
```

## Table Specifications

### Source Layer

#### raw_alarm_events
**Purpose**: Raw alarm event stream from DCS historian

**Row Count**: 54 events (Feb 6-7, 2026)

**Columns**:
| Column | Type | Description |
|--------|------|-------------|
| event_id | INTEGER | Unique event identifier |
| tag_id | TEXT | Alarm tag identifier (e.g., 'TIC-105') |
| event_timestamp | TEXT | ISO 8601 timestamp (YYYY-MM-DD HH:MM:SS) |
| event_type | TEXT | State: ACTIVE, ACKNOWLEDGED, INACTIVE |
| priority_code | TEXT | CRITICAL, HIGH, MEDIUM, LOW |
| alarm_value | REAL | Process value at alarm time |
| setpoint_value | REAL | Alarm setpoint/limit |
| operator_id | TEXT | Operator who acknowledged (nullable) |
| area_code | TEXT | Process area code (C-100, D-200) |

**Key Events**:
- Feb 7, 08:00-08:08: D-200 alarm storm (11 alarms in 10 min)
- TIC-105: Chattering alarm with 5 activations
- 16 standing alarms (>10 min to acknowledge)

---

#### raw_alarm_config
**Purpose**: Alarm tag configuration and metadata

**Row Count**: 15 alarm tags

**Columns**:
| Column | Type | Description |
|--------|------|-------------|
| tag_id | TEXT | Unique tag identifier (primary key) |
| tag_description | TEXT | Human-readable description |
| equipment_id | TEXT | Equipment association |
| area_code | TEXT | Process area code |
| priority_code | TEXT | Configured priority level |
| high_limit | REAL | High alarm limit (nullable) |
| low_limit | REAL | Low alarm limit (nullable) |
| config_effective_date | TEXT | When configuration became active |

---

### Dimension Layer

#### dim_alarm_tag (SCD Type 2)
**Purpose**: Alarm tag dimension with historical tracking

**Materialization**: Table (SCD Type 2)

**Row Count**: 15 rows (one version per tag in test data)

**Special Features**:
- ⭐ **SCD Type 2 Implementation**: Tracks tag configuration changes over time
- Each change creates new version with updated `tag_key`
- Enables point-in-time joins for historical accuracy

**Columns**:
| Column | Type | Description |
|--------|------|-------------|
| tag_key | INTEGER | Surrogate key (PK) - unique per version |
| tag_id | TEXT | Natural key - business identifier |
| tag_description | TEXT | Tag description |
| equipment_id | TEXT | Equipment association (FK to dim_equipment) |
| area_code | TEXT | Process area code |
| priority_code | TEXT | Alarm priority |
| high_limit | REAL | High alarm setpoint |
| low_limit | REAL | Low alarm setpoint |
| valid_from | TEXT | Start date of this version |
| valid_to | TEXT | End date ('9999-12-31' for current) |
| is_current | INTEGER | 1=current version, 0=historical |

**SCD Type 2 Example**: If TIC-105's high_limit changes from 150.0 to 160.0:
- Old row: `tag_key=1001`, `valid_to='2026-02-15'`, `is_current=0`
- New row: `tag_key=1002`, `valid_to='9999-12-31'`, `is_current=1`

---

#### dim_equipment
**Purpose**: Equipment hierarchy and classification

**Row Count**: 12 equipment items

**Columns**:
| Column | Type | Description |
|--------|------|-------------|
| equipment_id | TEXT | Primary key (e.g., 'COMP-001') |
| equipment_type | TEXT | Asset class (Compressor, Pump, Vessel, etc.) |
| equipment_description | TEXT | Human-readable description |

**Equipment Types**: Compressor, Pump, Vessel, Heat Exchanger, Control Valve

---

#### dim_process_area
**Purpose**: Process area organization hierarchy

**Row Count**: 2 areas

**Columns**:
| Column | Type | Description |
|--------|------|-------------|
| area_key | INTEGER | Surrogate key (PK) |
| area_code | TEXT | Business key (C-100, D-200) |
| area_name | TEXT | Full area name |

**Test Data Areas**:
- **C-100**: Compression Unit (~60% of alarms)
- **D-200**: Distillation Unit (~40% of alarms, contains 08:00 storm)

---

#### dim_priority
**Purpose**: Alarm priority classification

**Row Count**: 4 priority levels

**Columns**:
| Column | Type | Description |
|--------|------|-------------|
| priority_key | INTEGER | Surrogate key (PK) |
| priority_code | TEXT | Business key (CRITICAL, HIGH, MEDIUM, LOW) |
| priority_description | TEXT | Human description |
| priority_rank | INTEGER | Sort order (1=highest) |

**ISA 18.2 Guidance**:
- CRITICAL: <5% of total alarms
- HIGH: ~15% of total
- MEDIUM/LOW: ~80% of total

---

#### dim_operator
**Purpose**: Operator/user dimension for acknowledgment tracking

**Row Count**: 3 operators

**Columns**:
| Column | Type | Description |
|--------|------|-------------|
| operator_key | INTEGER | Surrogate key (PK) |
| operator_id | TEXT | Business key (OP001, OP002, OP003) |
| operator_name | TEXT | Operator name |

---

#### dim_dates
**Purpose**: Date dimension for temporal analysis

**Row Count**: 2 days (Feb 6-7, 2026)

**Columns**:
| Column | Type | Description |
|--------|------|-------------|
| date_key | INTEGER | Primary key (YYYYMMDD format) |
| full_date | TEXT | ISO 8601 date (YYYY-MM-DD) |
| year | INTEGER | Year |
| quarter | INTEGER | Quarter (1-4) |
| month | INTEGER | Month (1-12) |
| day | INTEGER | Day of month |
| day_of_week | INTEGER | 0=Sunday, 6=Saturday |
| is_weekend | INTEGER | 1=weekend, 0=weekday |

---

#### dim_time
**Purpose**: Time buckets for ISA 18.2 rate calculations

**Row Count**: 144 buckets (24 hours × 6 buckets/hour)

**Columns**:
| Column | Type | Description |
|--------|------|-------------|
| time_key | INTEGER | Primary key (0-143) |
| time_bucket_key | INTEGER | Same as time_key (0-143) |
| hour | INTEGER | Hour of day (0-23) |
| minute | INTEGER | Starting minute (0, 10, 20, 30, 40, 50) |
| time_bucket_start | TEXT | HH:MM start time |
| time_bucket_end | TEXT | HH:MM end time |

**ISA 18.2 Usage**: 10-minute buckets enable calculation of alarms-per-10-minutes operator loading metric.

---

### Fact Layer

#### fct_alarm_occurrence
**Purpose**: Alarm lifecycle fact table with ISA 18.2 metrics

**Grain**: One row per alarm activation/lifecycle

**Row Count**: 18 alarm lifecycles

**Special Features**:
- ⭐ **Point-in-Time Joins**: Uses SCD Type 2 temporal join to dim_alarm_tag
- ⭐ **ISA 18.2 Derived Flags**: Standing, fleeting, acknowledgment indicators

**Columns**:
| Column | Type | Description |
|--------|------|-------------|
| occurrence_key | INTEGER | Primary key (same as activation event_id) |
| alarm_id | TEXT | Business key (tag_id \|\| activation_timestamp) |
| tag_key | INTEGER | FK to dim_alarm_tag (correct version) |
| equipment_key | TEXT | FK to dim_equipment |
| area_key | INTEGER | FK to dim_process_area |
| priority_key | INTEGER | FK to dim_priority |
| operator_key_ack | INTEGER | FK to dim_operator (who acknowledged) |
| activation_date_key | INTEGER | FK to dim_dates |
| activation_timestamp | TEXT | When alarm became ACTIVE |
| acknowledged_timestamp | TEXT | When operator acknowledged (nullable) |
| inactive_timestamp | TEXT | When alarm cleared (nullable) |
| duration_to_ack_sec | INTEGER | Seconds to acknowledgment |
| duration_to_resolve_sec | INTEGER | Seconds to resolution |
| alarm_value | REAL | Process value at activation |
| setpoint_value | REAL | Alarm limit/setpoint |
| **is_standing_10min** | INTEGER | **1 if ack time >10 min (ISA 18.2)** |
| **is_standing_24hr** | INTEGER | **1 if ack time >24 hours** |
| **is_fleeting** | INTEGER | **1 if duration <2 seconds** |
| is_acknowledged | INTEGER | 1 if operator acknowledged |
| is_resolved | INTEGER | 1 if alarm cleared |

**ISA 18.2 Standing Alarm Definition**: 
- `is_standing_10min = 1` when `duration_to_ack_sec > 600`
- Indicates alarm remained unacknowledged >10 minutes
- Suggests nuisance alarm or incorrect priority

---

#### fct_alarm_state_change
**Purpose**: Detailed state transition history for chattering detection

**Grain**: One row per state transition event

**Row Count**: 54 state changes

**Columns**:
| Column | Type | Description |
|--------|------|-------------|
| state_change_key | INTEGER | Primary key (event_id) |
| occurrence_key | INTEGER | FK to fct_alarm_occurrence (nullable) |
| tag_key | INTEGER | FK to dim_alarm_tag |
| change_timestamp | TEXT | When state changed |
| from_state | TEXT | Previous state (ACTIVE, ACKNOWLEDGED, INACTIVE) |
| to_state | TEXT | New state |
| sequence_number | INTEGER | Order within tag (1, 2, 3...) |
| time_since_last_change_sec | INTEGER | Seconds since previous state change |

**Chattering Detection**: Window functions identify ≥5 state changes within 600 seconds (10 minutes).

---

### Rollup Layer

#### rollup_operator_loading_hourly
**Purpose**: ISA 18.2 operator loading by 10-minute time buckets

**Grain**: One row per 10-minute time bucket (may aggregate areas)

**Row Count**: Varies by date range (144 buckets/day maximum)

**ISA 18.2 Loading Categories**:
| Category | Alarms per 10 min | Interpretation |
|----------|-------------------|----------------|
| ACCEPTABLE | 1-2 | Operator can respond effectively |
| MANAGEABLE | 3-10 | Increased workload but manageable |
| UNACCEPTABLE | >10 | Alarm flood - operator overload |

**Columns**:
| Column | Type | Description |
|--------|------|-------------|
| loading_key | INTEGER | Surrogate key (PK) |
| date_key | INTEGER | FK to dim_dates |
| time_bucket_key | INTEGER | 10-min bucket (0-143) |
| time_bucket_start | TEXT | HH:MM bucket start |
| area_key | INTEGER | FK to dim_process_area (nullable for aggregate) |
| alarm_count | INTEGER | Total activations in bucket |
| alarm_count_critical | INTEGER | CRITICAL priority count |
| alarm_count_high | INTEGER | HIGH priority count |
| alarm_count_medium | INTEGER | MEDIUM priority count |
| alarm_count_low | INTEGER | LOW priority count |
| avg_time_to_ack_sec | REAL | Average acknowledgment time |
| max_time_to_ack_sec | INTEGER | Slowest acknowledgment |
| standing_alarm_count | INTEGER | Count of >10min alarms |
| **loading_category** | TEXT | **ACCEPTABLE/MANAGEABLE/UNACCEPTABLE** |
| **is_alarm_flood** | INTEGER | **1 if >10 alarms (alarm flood)**  |

**Example**: Storm at Feb 7, 08:00-08:10 (bucket 48): 11 alarms → UNACCEPTABLE, is_alarm_flood=1

---

#### rollup_standing_alarms
**Purpose**: Tags with extended unacknowledged durations

**Grain**: One row per tag with standing alarm occurrences

**Columns**:
| Column | Type | Description |
|--------|------|-------------|
| standing_key | INTEGER | Surrogate key (PK) |
| tag_key | INTEGER | FK to dim_alarm_tag |
| area_key | INTEGER | FK to dim_process_area |
| standing_alarm_count | INTEGER | Count of >10min acknowledgments |
| total_standing_duration_sec | INTEGER | Sum of all standing durations |
| avg_standing_duration_sec | REAL | Average standing duration |
| max_standing_duration_sec | INTEGER | Longest standing duration |
| avg_standing_duration_min | REAL | Average in minutes |
| max_standing_duration_hrs | REAL | Maximum in hours |
| total_standing_duration_hrs | REAL | Total in hours |

**Purpose**: Identifies worst offender tags requiring priority review or suppression.

---

#### rollup_chattering_alarms
**Purpose**: Rapid state cycling detection (chattering behavior)

**Grain**: One row per tag with chattering episodes

**ISA 18.2 Chattering Definition**: ≥5 state transitions within 10 minutes (600 seconds)

**Columns**:
| Column | Type | Description |
|--------|------|-------------|
| chattering_key | INTEGER | Surrogate key (PK) |
| tag_key | INTEGER | FK to dim_alarm_tag |
| area_key | INTEGER | FK to dim_process_area |
| **chattering_episode_count** | INTEGER | **Count of chattering episodes** |
| total_state_changes | INTEGER | Total state transitions |
| max_activations_per_hour | REAL | Peak hourly activation rate |
| min_cycle_time_sec | INTEGER | Fastest cycle time |
| avg_cycle_time_sec | INTEGER | Average cycle time |

**Example**: TIC-105 has 1 chattering episode with 6 rapid state changes.

---

#### rollup_bad_actor_tags
**Purpose**: Pareto analysis of high-frequency alarm tags

**Grain**: One row per tag ranked by alarm contribution

**Bad Actor Scoring**:
- **Alarm Frequency** (40%): Activation count contribution
- **Standing Alarms** (30%): Extent of >10min acknowledgments
- **Chattering** (30%): Presence of rapid cycling

**Composite Score** = frequency_score + standing_score + chattering_score (0-100 scale)

**Categories**:
| Score Range | Category | Action Required |
|-------------|----------|-----------------|
| 70-100 | CRITICAL | Immediate rationalization |
| 50-69 | SIGNIFICANT | Priority review within 30 days |
| 30-49 | MODERATE | Monitor and evaluate |
| 0-29 | NORMAL | No action required |

**Columns**:
| Column | Type | Description |
|--------|------|-------------|
| bad_actor_key | INTEGER | Surrogate key (PK) |
| tag_key | INTEGER | FK to dim_alarm_tag |
| area_key | INTEGER | FK to dim_process_area |
| total_activations | INTEGER | Total alarm count |
| avg_activations_per_day | REAL | Daily average |
| **alarm_rank** | INTEGER | **Rank by activation count (1 = worst)** |
| **contribution_pct** | REAL | **% of total alarms** |
| **cumulative_pct** | REAL | **Cumulative % (Pareto)** |
| is_top_10_pct | INTEGER | 1 if in top 10% of tags |
| **bad_actor_score** | REAL | **Composite score (0-100)** |
| **bad_actor_category** | TEXT | **CRITICAL/SIGNIFICANT/MODERATE/NORMAL** |

**Pareto Principle**: Typically 20% of tags cause 80% of alarms.

---

#### rollup_alarm_system_health
**Purpose**: Overall DCS alarm system health scorecard

**Grain**: One row for overall summary, optional daily rows

**Columns**:
| Column | Type | Description |
|--------|------|-------------|
| health_key | INTEGER | Surrogate key (PK) |
| analysis_date | TEXT | Date for daily rollup (NULL for overall) |
| date_key | INTEGER | FK to dim_dates (NULL for overall) |
| total_alarm_count | INTEGER | Total activations |
| unique_tag_count | INTEGER | Distinct tags |
| avg_alarms_per_hour | REAL | Hourly alarm rate |
| peak_alarms_per_10min | INTEGER | Worst 10-min bucket |
| **pct_time_acceptable** | REAL | **% 10-min buckets with 1-2 alarms** |
| **pct_time_manageable** | REAL | **% buckets with 3-10 alarms** |
| **pct_time_unacceptable** | REAL | **% buckets with >10 alarms** |
| alarm_flood_count | INTEGER | Count of >10 alarm buckets |
| total_standing_alarms | INTEGER | Sum of >10min alarms |
| avg_standing_duration_min | REAL | Average standing duration |
| chattering_tag_count | INTEGER | Count of chattering tags |
| top10_contribution_pct | REAL | % of alarms from worst 10% tags |
| bad_actor_count | INTEGER | Count of SIGNIFICANT+ tags (score ≥50) |
| **isa_compliance_score** | REAL | **Overall ISA 18.2 score (0-100)** |

**ISA Compliance Score Calculation**:
```
score = (pct_acceptable × 1.0) + (pct_manageable × 0.5) + (pct_unacceptable × 0.0)
```
- Score ≥80: Excellent compliance
- Score 60-79: Acceptable with improvements needed
- Score <60: Poor compliance, urgent action required

---

## Key Design Patterns

### 1. SCD Type 2 (dim_alarm_tag)
**Preserves historical accuracy** for alarm configurations:
- Each configuration change creates new row
- `valid_from` / `valid_to` mark temporal validity
- `is_current` flag identifies latest version
- Enables "as-was" reporting for any historical point

### 2. Point-in-Time Joins
**Accurate dimensional lookups** for transactional facts:
```sql
FROM fct_alarm_occurrence f
JOIN dim_alarm_tag d 
  ON f.tag_key = d.tag_key 
  AND f.activation_timestamp >= d.valid_from 
  AND f.activation_timestamp < d.valid_to
```

### 3. ISA 18.2 Derived Metrics
**Pre-calculated compliance indicators**:
- `is_standing_10min`: >600 seconds to acknowledge
- `loading_category`: ACCEPTABLE/MANAGEABLE/UNACCEPTABLE
- `bad_actor_score`: Composite alarm health score
- `isa_compliance_score`: System-wide health metric

### 4. Pre-Aggregation Strategy
**Rollup tables optimize query performance**:
- Common analyses pre-calculated
- Eliminates need for complex joins/aggregations at query time
- Supports fast dashboard and reporting queries

---

## Data Quality Rules

### Referential Integrity
- All foreign keys must have corresponding dimension rows
- No orphan tag_key, area_key, priority_key references

### Temporal Consistency
- `acknowledged_timestamp` ≥ `activation_timestamp`
- `inactive_timestamp` ≥ `acknowledged_timestamp`
- `duration_to_resolve_sec` ≥ `duration_to_ack_sec`

### Business Rules
- `date_key` format: YYYYMMDD (e.g., 20260207)
- `time_bucket_key` range: 0-143 (144 buckets per day)
- `loading_category` domain: ACCEPTABLE, MANAGEABLE, UNACCEPTABLE
- `bad_actor_category` domain: CRITICAL, SIGNIFICANT, MODERATE, NORMAL

---

## References

- **ISA-18.2-2016**: Management of Alarm Systems for the Process Industries
- **EEMUA 191**: Alarm Systems - A Guide to Design, Management and Procurement (Edition 3)
- **Kimball & Ross**: The Data Warehouse Toolkit (3rd Edition) - Star Schema design patterns
