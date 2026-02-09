# Manufacturing Bottleneck Analysis

## Overview

This example demonstrates how to use Gorchata to analyze manufacturing bottlenecks using principles from **The Goal** by Eliyahu M. Goldratt and the **Theory of Constraints (TOC)**.

### The Goal and Theory of Constraints

*The Goal* is a business novel that introduces the Theory of Constraints, a management philosophy focused on identifying and managing the constraint (bottleneck) that limits a system's ability to achieve its goal. Key concepts include:

- **Throughput**: The rate at which the system generates money through sales
- **Inventory**: All the money invested in purchasing things the system intends to sell
- **Operating Expense**: All the money spent to turn inventory into throughput
- **Bottleneck**: Any resource whose capacity is equal to or less than the demand placed on it
- **Non-Bottleneck**: Any resource whose capacity is greater than the demand placed on it

The five focusing steps of TOC:
1. **Identify** the system's constraint(s)
2. **Exploit** the constraint(s)
3. **Subordinate** everything else to the above decision
4. **Elevate** the system's constraint(s)
5. **Repeat** - If a constraint is broken, go back to step 1

### UniCo Manufacturing Plant Context

This example uses data from the fictional UniCo manufacturing plant, specifically analyzing:

- **NCX-10 Machine**: A critical machining center often identified as a bottleneck
- **Heat Treat Department**: A downstream process that can become a bottleneck
- **Assembly Operations**: Final assembly and quality control processes
- **Material Flow**: Raw materials through work-in-process to finished goods

### What This Example Demonstrates

This Gorchata project shows how to:

1. Model manufacturing operations data (work orders, machine states, production events)
2. Calculate key bottleneck metrics (utilization, cycle time, throughput)
3. Identify constraints using dimensional modeling
4. Track performance improvements over time
5. Generate actionable insights for plant managers

The dimensional model includes:

- **Sources**: Raw production events, machine logs, work orders
- **Dimensions**: Machines, shifts, products, operators
- **Facts**: Production events, downtime events, quality events
- **Rollups**: Daily/weekly utilization, throughput analysis, bottleneck identification

## Prerequisites

- Go 1.25 or higher
- Gorchata installed and available in PATH
- SQLite (via modernc.org/sqlite - no CGO required)

## Project Structure

```
bottleneck_analysis/
├── gorchata_project.yml      # Main project configuration
├── profiles.yml              # Database connection profiles
├── README.md                 # This file
├── seeds/                    # Seed data files
├── models/                   # SQL model definitions
│   ├── sources/              # Raw data source models
│   ├── dimensions/           # Dimension tables
│   ├── facts/                # Fact tables
│   └── rollups/              # Aggregated analytics
├── tests/                    # Data quality tests
│   └── generic/              # Generic test configurations
└── docs/                     # Additional documentation
```

## How to Run

### Initial Setup

1. Review and customize configuration in `gorchata_project.yml`
2. Set database location if needed:
   ```powershell
   $env:BOTTLENECK_ANALYSIS_DB="path\to\db.db"
   ```
3. Load seed data:
   ```powershell
   gorchata seed
   ```

This will create the database and load the raw manufacturing data from CSV files in the `seeds/` directory.

### Build Models

```powershell
# Build all models (sources → dimensions → facts → rollups)
gorchata run

# Build specific model
gorchata run --models bottleneck_hourly_utilization

# Build with full lineage
gorchata run --models +bottleneck_ranking
```

The build process follows this order:
1. **Sources**: Import seed data into staging tables
2. **Dimensions**: Build dimension tables (resources, work orders, parts, dates)
3. **Facts**: Create fact tables for operations and downtime
4. **Intermediate**: Calculate daily utilization metrics
5. **Rollups**: Generate bottleneck rankings and analytics

### Run Tests

```powershell
# Run data quality tests
gorchata test

# Run Go integration tests
go test ./examples/bottleneck_analysis/...
```

### Verify Results

Use the verification SQL script to manually inspect results:

```powershell
# Run all verification queries
sqlite3 bottleneck_analysis.db < verify_bottleneck_analysis.sql

# Or run interactively
sqlite3 bottleneck_analysis.db
.read verify_bottleneck_analysis.sql
```

## Configuration

### Variables (gorchata_project.yml)

Key configuration variables:

- `analysis_start_date`, `analysis_end_date`: Time period for analysis
- `shift_hours`: Length of work shifts (default: 8 hours)
- `utilization_threshold_high`: Threshold for bottleneck identification (default: 90%)
- `target_throughput_units_per_hour`: Expected production rate (default: 25 units/hour)
- `ncx10_capacity_parts_per_hour`: NCX-10 machine capacity (default: 25)
- `heat_treat_capacity_parts_per_hour`: Heat treat capacity (default: 15)

### Database

The default database is `bottleneck_analysis.db` in the project directory. Override with:

```powershell
$env:BOTTLENECK_ANALYSIS_DB="path\to\custom.db"
```

## Expected Outputs

### Bottleneck Identification

The `anl_bottleneck_ranking` rollup produces a ranked list of resources by bottleneck severity. You should see:

**Top Bottlenecks (Highest Scores):**
1. **NCX-10** - Primary constraint with high utilization (>85%) and significant queue time
2. **Heat Treat** - Secondary bottleneck with capacity constraints and WIP accumulation

**Non-Bottlenecks (Lower Scores):**
- Milling, Assembly, Grinding - Lower utilization and minimal queuing

### Key Metrics

- **Utilization Rate**: NCX-10 typically shows 85-95% utilization, Heat Treat 75-85%
- **Queue Time**: NCX-10 averages 2-4 hours of queue time per operation
- **WIP Accumulation**: Visible buildup before NCX-10 and Heat Treat
- **Downtime Impact**: Unplanned downtime on NCX-10 creates cascading delays

### Daily Utilization Trends

The `int_resource_daily_utilization` intermediate table shows:
- Day-over-day utilization patterns
- Downtime-adjusted utilization rates
- Shift performance variations
- Capacity constraint identification

### Actionable Insights

According to Theory of Constraints, focus improvements on:
1. **Exploit the constraint**: Maximize NCX-10 uptime, eliminate idle time
2. **Subordinate operations**: Ensure upstream processes feed NCX-10 continuously
3. **Elevate capacity**: Consider additional NCX-10 capacity if exploitation maxed out
4. **Monitor Heat Treat**: Watch for constraint shifting downstream

## Development

This example follows strict TDD practices:

1. Tests written first
2. Minimal implementation to pass tests
3. Refactor while keeping tests green
4. No CGO dependencies

Run tests:
```powershell
go test -v ./examples/bottleneck_analysis/...
```

## References

- **The Goal** by Eliyahu M. Goldratt
- Theory of Constraints Institute: https://www.tocinstitute.org/
- Gorchata documentation: [Project README](../../README.md)

## License

Same as parent Gorchata project.
