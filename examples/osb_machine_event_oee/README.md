# OSB Machine Event Data to OEE Analytics

A comprehensive data engineering example demonstrating how to transform raw machine event logs from an Oriented Strand Board (OSB) manufacturing facility into an integrated **Overall Equipment Effectiveness (OEE)** and operational analytics platform.

## Overview

This example models a complete OSB production facility that converts raw logs into finished structural panels through a multi-stage continuous process. The analytics framework calculates standard OEE metrics (Availability × Performance × Quality) while also providing advanced operational intelligence including:

- **Downtime Analysis**: MTBF, MTTR, bad actor identification, Pareto analysis
- **Buffer & Constraint Analysis**: Material flow tracking, bottleneck identification, capacity utilization
- **Quality Root Cause Analysis**: Defect correlation with process parameters
- **Maintenance Strategy Optimization**: PM effectiveness, predictive maintenance triggers

## OSB Manufacturing Process

```
Log Pond → Debarking → Stranding → [Green Strand Bins: 4hr buffer]
                                            ↓
                                        Drying (Bottleneck)
                                            ↓
                                   [Dry Fiber Silos: 8hr buffer]
                                            ↓
                            Screening → Blending (with resin)
                                            ↓
                                   Forming → [Mat Buffer: 30min]
                                            ↓
                                    Continuous Hot Press
                                            ↓
                                Cooling → Sawing → Stacking
                                            ↓
                                        Warehouse
```

### Critical Equipment

- **Stranders (2×)**: Convert debarked logs into strands (6 tons/hr each)
- **Rotary Dryer (1×)**: Single-point bottleneck (10 tons/hr, 83% of upstream capacity)
- **Continuous Press (1×)**: Hot press with 8-minute cycle time, longest process step
- **Saws (4×)**: Redundant finishing capacity

### Buffer Dynamics

The example demonstrates **downtime propagation** through the production system:

1. Dryer outage → Green strand bins fill → Stranders blocked
2. Continued dryer outage → Dry fiber silos deplete → Former starved
3. Buffer sizing analysis quantifies economic impact of capacity improvements

## OEE Methodology

### The Six Big Losses

**Availability Loss:**
- Equipment Failures (breakdowns)
- Setup and Adjustments

**Performance Loss:**
- Minor Stops (<5 minutes)
- Reduced Speed

**Quality Loss:**
- Startup Rejects
- Production Rejects

### OEE Calculation

```
Planned Production Time = Calendar Time - Planned Downtime (maintenance, shift handovers)

Availability = Operating Time / Planned Production Time
  where Operating Time = Planned Production Time - Unplanned Downtime

Performance = Actual Output / Ideal Output
  where Ideal Output = (Operating Time / Ideal Cycle Time)

Quality = Good Output / Total Output

OEE = Availability × Performance × Quality
```

### World-Class OEE Targets

- **World Class**: OEE ≥ 85%
- **Good**: OEE 70-85%
- **Needs Improvement**: OEE < 70%

## Project Structure

```
osb_machine_event_oee/
├── README.md                      # This file
├── schema.yml                     # Complete star schema definition
├── models/
│   ├── staging/                   # State duration calculations
│   ├── facts/                     # OEE calculations
│   ├── metrics/                   # Downtime, buffer, and constraint analysis
│   └── analytics/                 # Advanced analytics (bad actors, shift comparison)
├── seeds/                         # Seed data configuration
└── tests/                         # Data quality tests
```

## Schema Overview

### Dimension Tables

1. **dim_equipment**: 15+ pieces of OSB manufacturing equipment
   - Stranders, dryers, blenders, press, saws
   - Rated capacities and ideal cycle times
   - Criticality levels (Critical, Important, Standard)

2. **dim_production_area**: 7 production stages
   - Defines process flow sequence
   - Buffer capacities between stages
   - Upstream/downstream relationships

3. **dim_reason_code**: Downtime reason codes mapped to OEE model
   - Mechanical, Electrical, Process, Quality, Material failures
   - Classified as Planned vs Unplanned
   - Mapped to Availability, Performance, or Quality loss

4. **dim_shift**: 3×8-hour shift operations
   - Day (06:00-14:00), Swing (14:00-22:00), Night (22:00-06:00)
   - 30-minute handover periods

5. **dim_product_spec**: OSB panel specifications
   - Thickness: 3/8", 7/16", 9/16"
   - Density: 38-42 lbs/ft³
   - Quality tolerances (±0.015" thickness, ±2 lbs/ft³ density)

### Staging Tables

1. **stg_machine_events**: Raw discrete state change events
2. **stg_equipment_state_history**: State durations (calculated via LEAD window function)
3. **stg_production_output**: Production count events
4. **stg_quality_tests**: Quality test results
5. **stg_buffer_levels**: Buffer inventory tracking

### Fact Tables

1. **fact_equipment_state**: Equipment state history for OEE analysis
2. **fact_production_output**: Production metrics with cycle time and performance
3. **fact_quality_results**: Aggregated quality results by batch

## Key Metrics

### OEE Metrics
- Availability, Performance, Quality percentages
- Overall OEE (product of A × P × Q)
- Breakdown by equipment, shift, day

### Reliability Metrics
- **MTBF** (Mean Time Between Failures): Operating time / failure count
- **MTTR** (Mean Time To Repair): Average downtime per failure
- Failure frequency (failures per week/month)
- Bad actor scoring (impact = downtime × frequency × criticality)

### Buffer & Constraint Metrics
- Buffer utilization (% capacity)
- Hours of supply remaining
- Starved time (downstream equipment waiting on material)
- Blocked time (upstream equipment waiting for downstream capacity)
- Constraint identification (equipment with highest utilization causing most downstream starvation)

### Quality Metrics
- First pass yield
- Scrap rate, downgrade rate
- Thickness and density distributions
- Defect rate trending

## Failure Modes (OSB-Specific)

### Stranders
- Bearing failures (MTBF: 250 hrs, MTTR: 2-4 hrs)
- Knife wear (gradual performance degradation)

### Dryer
- Burner trips (MTBF: 300 hrs, MTTR: 1-2 hrs)
- Gear failures (MTBF: 2000 hrs, MTTR: 24 hrs)
- Temperature control issues

### Press
- Hydraulic leaks (MTBF: 400 hrs, MTTR: 3-6 hrs)
- Heating element failures
- Mat fold/wrinkle (quality issue)

### Process Upsets
- Strand bridging in bins (15 min to clear)
- Resin mix ratio deviation (30-60 min to correct)
- Thickness out-of-spec (1-2 hr investigation + adjustment)

## Quick Start

### 1. Initialize Project
```bash
gorchata init --example osb_machine_event_oee
```

### 2. Generate Seed Data
```bash
gorchata seed --config examples/osb_machine_event_oee/seed.yml
```

This generates 90 days of realistic machine event data including:
- 15+ equipment state transitions
- Realistic failure patterns (MTBF/MTTR distributions)
- Buffer dynamics (filling/depleting based on production rates)
- Quality test results correlated with process conditions

### 3. Run Transformations
```bash
gorchata build --project examples/osb_machine_event_oee
```

This executes the transformation pipeline:
1. Stage: Calculate state durations from discrete events
2. Facts: Compute OEE metrics, production output, quality results
3. Metrics: Aggregate downtime analysis, buffer utilization, constraint identification
4. Analytics: Generate bad actor prioritization, shift comparisons, improvement opportunities

### 4. Query Results

See [EXAMPLE_QUERIES.md](EXAMPLE_QUERIES.md) for common analytics queries.

## Key Insights Demonstrated

### 1. Downtime Propagation
The dryer, operating at 83% of upstream capacity, creates a system bottleneck:
- When the dryer fails, green strand bins fill to capacity
- Stranders become blocked (cannot discharge strands)
- Downstream dry fiber silos deplete
- Former becomes starved (no material to process)

This demonstrates the **Theory of Constraints** principle: improving non-constraint resources doesn't improve system throughput.

### 2. Buffer Sizing Economics
Analysis quantifies the trade-off:
- Larger buffers → more capital cost, more WIP inventory
- Larger buffers → less blocking/starving, higher effective capacity
- Optimal buffer size depends on upstream/downstream reliability and capacity mismatch

### 3. Bad Actor Prioritization
Not all equipment failures have equal impact:
- **Impact Score** = Downtime Hours × Failure Frequency × Criticality Weight
- Focus reliability improvements on highest-impact failures
- ROI calculation: (Downtime reduction × $/hour) / improvement investment

### 4. OEE Loss Waterfall
Visualize losses from theoretical capacity to actual output:
```
Theoretical Capacity (24×7)
  - Planned Downtime (maintenance, breaks)
  = Planned Production Time
  - Unplanned Downtime (breakdowns)
  = Operating Time  ← Availability Loss
  - Speed Losses, Minor Stops
  = Net Operating Time  ← Performance Loss
  - Quality Defects (scrap, rework)
  = Valuable Operating Time  ← Quality Loss
```

## Educational Value

This example teaches:

1. **OEE Fundamentals**: Standard methodology, loss categories, calculation approach
2. **Window Functions**: Using LEAD() to calculate state durations from discrete events
3. **Time-Series Analysis**: Aggregating irregular event streams into regular reporting periods
4. **Dimensional Modeling**: Star schema design for manufacturing analytics
5. **Reliability Engineering**: MTBF/MTTR calculations, Pareto analysis, bad actor identification
6. **Constraint Theory**: Identifying system bottlenecks, quantifying improvement opportunities
7. **Data Quality**: Handling missing state transitions, outlier detection, referential integrity

## Industry Applicability

While modeled on OSB manufacturing, the patterns apply to:

- **Continuous Process**: Chemical, refining, pulp/paper, steel
- **Batch Process**: Pharmaceuticals, food & beverage, specialty chemicals
- **Assembly Lines**: Automotive, electronics, consumer goods
- **Packaging**: Bottling, canning, packaging lines

The key concepts (OEE, MTBF/MTTR, buffer analysis, constraint identification) are universal to manufacturing operations management.

## References

### OEE & Manufacturing Analytics
- [OEE Foundation](https://www.oee.com) - Comprehensive OEE resource
- Nakajima, Seiichi. *Introduction to TPM: Total Productive Maintenance*. Productivity Press, 1988.
- Goldratt, Eliyahu M. *The Goal: A Process of Ongoing Improvement*. North River Press, 1984.

### OSB Manufacturing
- APA – The Engineered Wood Association: OSB Product Guide
- Structural Board Association: OSB Manufacturing Process

### Standards
- **ISA-95**: Enterprise-Control System Integration
- **SEMI E10**: Specification for Definition and Measurement of Equipment Reliability, Availability, and Maintainability (RAM)

## Status

- ✅ **Phase 1 Complete**: Schema Design and DDL Generation
- 🚧 **Phase 2 In Progress**: Seed Configuration for Event Generation
- ⏳ Phase 3: State Duration Calculation Logic
- ⏳ Phase 4: OEE Calculation
- ⏳ Phase 5: Downtime Analysis and Reliability Metrics
- ⏳ Phase 6: Buffer and Constraint Analysis
- ⏳ Phase 7: Advanced Analytics and Improvement Opportunities
- ⏳ Phase 8: Documentation and Visualization Guidance

## Contributing

This example is part of the Gorchata project. For questions or contributions:
- Review [../../plans/osb-machine-event-oee-plan.md](../../plans/osb-machine-event-oee-plan.md) for detailed implementation plan
- Follow TDD approach: write tests first, implement to pass tests
- Ensure all metrics calculations verified against manual calculations

## License

Part of the Gorchata project. See [LICENSE](../../LICENSE) for details.
