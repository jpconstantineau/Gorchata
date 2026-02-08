# DCS Alarm Analytics Example

## Overview

This example demonstrates how to use Gorchata to analyze Distributed Control System (DCS) alarm data according to ISA 18.2 standards for alarm management rationalization.

## ISA 18.2 Context

ISA 18.2 is the ANSI/ISA standard for "Management of Alarm Systems for the Process Industries." It provides guidance on designing, managing, and rationalizing alarm systems to improve safety, operational efficiency, and reduce alarm flooding.

Key metrics from ISA 18.2:
- **Acceptable alarm rate**: Average of 1-2 alarms per 10 minutes per operator
- **Manageable alarm rate**: Up to 6 alarms per 10 minutes per operator  
- **Unacceptable alarm rate**: More than 10 alarms per 10 minutes per operator

This example uses Gorchata's Go text/template capabilities to build dimensional models that help identify alarm management issues and support rationalization efforts.

## Template Engine

This example uses Go's built-in `text/template` library (NOT Jinja). Template syntax:
- Variables: `{{ .Vars.variable_name }}`
- Config directives: `{{ config(materialized='table') }}`
- Control flow: `{{ if }}`, `{{ range }}`, etc.

## Project Structure

```
dcs_alarm_example/
├── gorchata_project.yml          # Project configuration
├── profiles.yml                   # Database connection profiles
├── README.md                      # This file
└── models/
    ├── sources/                   # Raw alarm data models
    ├── dimensions/                # Dimension tables (operators, alarm types, etc.)
    ├── facts/                     # Fact tables (alarm events)
    └── rollups/                   # Aggregations and analytics models
```

## Configuration

### Project Variables

Defined in `gorchata_project.yml`:
- `start_date`: Analysis period start date
- `end_date`: Analysis period end date
- `alarm_rate_threshold_acceptable`: ISA 18.2 acceptable rate (alarms per 10 min)
- `alarm_rate_threshold_unacceptable`: ISA 18.2 unacceptable rate (alarms per 10 min)

### Database Configuration

The example uses SQLite by default. Database path can be customized via environment variable:

```bash
# Use default path (./examples/dcs_alarm_example/dcs_alarms.db)
gorchata run

# Use custom database path
$env:DCS_ALARM_DB="C:\data\my_alarms.db"
gorchata run
```

## How to Run

*Coming in Phase 2: Source data models*

```bash
# From repository root
cd examples/dcs_alarm_example
gorchata run
```

## Schema Overview

The example implements a star schema with:

### Dimension Tables (Phase 3)
- **dim_alarm_tag**: Alarm tag configurations with SCD Type 2 versioning
- **dim_equipment**: Equipment hierarchy (pumps, compressors, etc.)
- **dim_process_area**: Process area organization
- **dim_priority**: Alarm priority levels (CRITICAL, HIGH, MEDIUM, LOW)
- **dim_operator**: Console operators for alarm response tracking
- **dim_dates**: Date dimension for temporal analysis
- **dim_time**: Time buckets (10-minute intervals for ISA 18.2 rate calculations)

### Fact Tables (Phase 4)

#### fct_alarm_occurrence
Primary fact table capturing alarm lifecycle events.

**Grain**: One row per alarm activation/lifecycle

**Key Columns**:
- `occurrence_key`: Primary key (event_id of ACTIVE event)
- `alarm_id`: Business key (tag_id || activation_timestamp)
- Foreign keys to dimension tables (tag_key, equipment_key, area_key, priority_key, operator_key_ack)

**Timestamps & Metrics**:
- `activation_timestamp`: When alarm became active
- `acknowledged_timestamp`: When operator acknowledged (nullable)
- `inactive_timestamp`: When alarm cleared (nullable)
- `duration_to_ack_sec`: Time to acknowledgment in seconds
- `duration_to_resolve_sec`: Time to resolution in seconds
- `alarm_value`, `setpoint_value`: Process measurements

**ISA 18.2 Derived Flags**:
- `is_standing_10min`: 1 if alarm took >10 minutes to acknowledge
- `is_standing_24hr`: 1 if alarm took >24 hours to acknowledge
- `is_fleeting`: 1 if alarm duration <2 seconds
- `is_acknowledged`: 1 if operator acknowledged
- `is_resolved`: 1 if alarm cleared

#### fct_alarm_state_change
Secondary fact table for chattering detection.

**Grain**: One row per state transition

**Key Columns**:
- `state_change_key`: Primary key (event_id)
- `occurrence_key`: Link to parent alarm occurrence (nullable)
- `tag_key`: Foreign key to dim_alarm_tag
- `from_state`, `to_state`: State transition (ACTIVE, ACKNOWLEDGED, INACTIVE)
- `sequence_number`: Order within tag (1, 2, 3, ...)
- `time_since_last_change_sec`: Time since previous state change

**Purpose**: Enables identification of:
- Chattering alarms (rapid state cycling)
- Alarm storms (many alarms in short time)
- Equipment behavior patterns

### Rollup Tables (Phase 5)
*Coming soon*: ISA 18.2 compliance metrics, alarm rate analysis, flood detection

## Testing

Run the test suite to verify project setup:

```bash
cd examples/dcs_alarm_example
go test -v
```

Tests verify:
- Project configuration loads correctly
- Database connection configuration is valid
- Directory structure exists
- Environment variable expansion works

## Development

This example follows TDD principles. Each phase includes:
1. Tests written first
2. Implementation to pass tests
3. Refactoring as needed

Refer to phase completion documents in `/plans/` for detailed implementation history.

## References

- ISA 18.2-2016: Management of Alarm Systems for the Process Industries
- Go text/template documentation: https://pkg.go.dev/text/template
- Gorchata documentation: [Repository README](../../README.md)
