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

*Coming in Phase 3-5: Full dimensional model structure*

The example implements a star schema with:
- **Dimension tables**: Time, operators, alarm tags, alarm priorities
- **Fact table**: Individual alarm events (activate/clear/acknowledge)
- **Rollup tables**: ISA 18.2 compliance metrics, alarm rate analysis, flood detection

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
