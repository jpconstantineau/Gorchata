# Precision Scheduled Railroading (PSR) Analytics Example

## Overview

This example demonstrates advanced analytics for Precision Scheduled Railroading (PSR) operations using real-world-scale railcar location data. PSR is a freight rail operating model that emphasizes running trains on fixed schedules with minimal handling, reducing transit times and improving asset utilization.

The dataset captures the gradual adoption of PSR principles across a North American railroad network from 2016 to 2025, including the subtle operational patterns that emerge during transformation, such as "shadow yards" where dwell time is artificially shifted to game KPI metrics.

This analytics suite enables railroad executives, operations analysts, and data scientists to quantify PSR implementation effectiveness, identify bottlenecks, detect shadow yards, and benchmark performance across pre-PSR, transition, and mature PSR periods.

## Dataset Specifications

- **Time Period**: 10 years (2016-01-01 to 2025-12-31)
- **Fleet Size**: 12,000 railcars across 7 major North American railroads
- **Locations**: 200 locations including terminals, interchanges, yards, customer sites, and sidings
- **Events**: ~110 million Car Location Messages (CLM) at minute-level precision
- **PSR Periods**:
  - Pre-PSR Baseline: 2016-2017
  - PSR Transition: 2018-2020
  - Mature PSR: 2021-2025
- **Shadow Yards**: 6 subtle cases requiring analytical detection
- **Seasonal Effects**: 25% performance variation between winter and summer

## Quick Start

### Prerequisites

- Go 1.25 or higher
- Gorchata CLI tool

### Setup and Run

1. **Generate seed data** (if not already present):
   ```powershell
   go run examples/precision_railroading/generate_clm_data.go
   ```

2. **Run tests** to validate data generator:
   ```powershell
   go test ./examples/precision_railroading -v
   ```

3. **Initialize Gorchata project** (future phases):
   ```bash
   cd examples/precision_railroading
   gorchata seed
   gorchata run
   ```

## Project Structure

```
precision_railroading/
├── models/              # dbt-style SQL models (to be added in later phases)
│   ├── dimensions/      # Dimension tables
│   ├── staging/         # Staging layer
│   ├── intermediate/    # Intermediate transformations
│   ├── facts/           # Fact tables
│   ├── metrics/         # KPI metrics
│   └── analytics/       # Advanced analytics
├── seeds/               # Seed data
│   ├── clm_generation_config.yml  # Data generator configuration
│   ├── seed.yml                   # Gorchata seed configuration
│   └── raw_clm_events.csv         # Generated CLM events (~8GB)
├── tests/               # Data quality tests (to be added)
├── docs/                # Additional documentation (to be added)
├── generate_clm_data.go       # CLM data generator
├── generate_clm_data_test.go  # Generator tests
├── gorchata_project.yml       # Project configuration
├── profiles.yml               # Database profiles
└── README.md                  # This file
```

## Event Types

The dataset includes four primary CLM event types:

- **PLAC** (Placement): Car is loaded at origin
- **DEPA** (Departure): Loaded car departs origin
- **ARRI** (Arrival): Car arrives at destination
- **PULL** (Pulled): Car is unloaded at destination

## Key Analytics Capabilities (To Be Implemented)

- **Velocity Metrics**: Track average train velocity across PSR periods
- **Dwell Time Analysis**: Measure terminal dwell improvements
- **Shadow Yard Detection**: Identify locations gaming metrics
- **Seasonal Performance**: Quantify weather impacts
- **Network Bottlenecks**: Pinpoint congestion points
- **Asset Utilization**: Calculate car cycle times and turns per year

## Configuration Files

- `gorchata_project.yml`: Main project configuration
- `profiles.yml`: Database connection profiles
- `seeds/seed.yml`: Seed data import configuration
- `seeds/clm_generation_config.yml`: Data generator parameters

## Data Generator Tests

The generator includes comprehensive TDD tests:

1. **TestCLMDataGenerator**: Validates basic event structure
2. **TestEventTypeDistribution**: Ensures proper event type ratios
3. **TestTemporalConsistency**: Verifies chronological ordering and minute precision
4. **TestSeasonalVariation**: Validates 25% seasonal effects
5. **TestPSRGradualAdoption**: Confirms three-period evolution (pre-PSR → transition → mature)

All tests follow strict TDD principles and must pass before data generation.

## Notes

### Phase 3: Staging Layer (COMPLETE ✓)

**Build and Test:**
```powershell
# Load seed data (one-time)
..\..\bin\gorchata.exe seed

# Build staging models
.\build_phase3.ps1

# Run tests (25 tests)
.\test_phase3.ps1

# Data quality report
go run verify_phase3.go
```

**Deliverables:**
- `models/staging/stg_clm_events.sql` - Basic staging
- `models/staging/stg_clm_enriched.sql` - Enriched staging
- `models/staging/schema.yml` - Documentation
- Test coverage: 100% (25/25 tests passing)

See [PHASE3_COMPLETE.md](PHASE3_COMPLETE.md) for implementation details.

### Phase 2: Dimension Tables (COMPLETE)

- `dim_location`, `dim_railcar`, `dim_train`, `dim_corridor`, `dim_date`
- See [PHASE2_COMPLETE.md](PHASE2_COMPLETE.md)

---

## Notes (Original)

- This is Phase 1 of the implementation focusing on project setup and seed data generation
- Full model development, metrics, and analytics will be added in subsequent phases
- The seed data file (`raw_clm_events.csv`) is approximately 8GB and not suitable for version control
- Shadow yards exhibit lower terminal dwell BUT downstream locations show higher dwell (requires analysis to detect)

## Future Development

Planned phases include:

- Phase 2-4: Staging, dimensional, and fact models
- Phase 5-7: Metrics and KPI calculations
- Phase 8-9: Advanced analytics (shadow yard detection, bottleneck analysis)
- Phase 10: Comprehensive documentation and analysis playbooks

---

**Documentation Status**: Phase 1 Complete - Basic setup and seed generation
Full documentation will be expanded in Phase 10.
