# OSB Machine Event to OEE Analytics

Comprehensive data engineering project transforming raw machine event logs from an Oriented Strand Board (OSB) manufacturing facility into an integrated OEE (Overall Equipment Effectiveness) platform with advanced operational analytics.

## Business Context

**Target Users:** Plant Managers | Reliability Engineers | Process Engineers | Maintenance Planners | Shift Supervisors

**Business Value:**
- Reduce unplanned downtime by 20-40% through targeted reliability improvements
- Increase plant throughput 5-15% by eliminating constraints
- Shift from reactive to preventive maintenance strategy
- Quantify economic impact ($1000/hr production loss) to justify investments
- Enable data-driven continuous improvement with ROI calculations

## OSB Manufacturing Process

```
Raw Logs  Debarking  Stranding  Drying (BOTTLENECK 10t/hr)  Screening  Blending  Forming  Pressing  Cooling  Sawing  Packaging
                                                                                                               
          Waste       Green Bins   Dry Silos    Screens       Resin Mix  Mat Buffer  Press Out  Cooldown  Off-Cuts  Finished
                      (4hr buffer) (8hr buffer)                          (30min)
```

**Critical Equipment:**
- **Stranders (2)**: 6 tons/hr each = 12 tons/hr total
- **Primary Dryer (1)**: **10 tons/hr - BOTTLENECK** (83% of upstream, limits plant throughput)
- **Continuous Press (1)**: 18 tons/hr capacity (underutilized due to dryer constraint)
- **Panel Saws (4)**: 20+ tons/hr (redundant capacity)

**Buffer Dynamics:** Upstream failures  buffer fill  blocking. Downstream failures  buffer deplete  starvation.

## OEE Methodology

**OEE = Availability  Performance  Quality**

### Time Model
```
Calendar Time (24 hrs/day)
  - Planned Downtime (PM, breaks) = Planned Production Time
    - Unplanned Downtime (breakdowns) = Operating Time [Availability Loss]
      - Speed Losses (minor stops) = Net Operating Time [Performance Loss]
        - Quality Defects (scrap) = Fully Productive Time [Quality Loss]
```

### Components
- **Availability** = Operating Time / Planned Production Time (Target >90%)
- **Performance** = Actual Output / Ideal Output (Target >95%)
- **Quality** = Good Output / Total Output (Target >99%)

### Six Big Losses
1. Equipment Failure (Availability) - Bearing failures, trips, leaks
2. Setup & Adjustment (Availability) - Changeovers, calibrations
3. Small Stops (Performance) - Strand bridging, resin corrections
4. Reduced Speed (Performance) - Running 80% of rated capacity
5. Startup Rejects (Quality) - Off-spec during warmup
6. Production Rejects (Quality) - Thickness/density deviations

## Quick Start

### 1. Generate Schema
```bash
gorchata generate schema --input schema.yml --output generated/
gorchata execute --input generated/schema.sql --connection your_db
```

### 2. Load Seed Data
```bash
gorchata load seeds --config seeds/seed.yml --connection your_db
# Verify: 16 equipment, 25 reason codes, 3 shifts, 90 dates
```

### 3. Generate Events (90 days of realistic machine events)
- DRYER-01: MTBF 48h, MTTR 2.4h (critical bottleneck)
- PRESS-01: MTBF 140h, MTTR 3h (critical)
- Follow realistic state transitions: Running  Breakdown  Running

### 4. Execute Transformations
```bash
# Staging: State duration calculation
gorchata run model --model models/staging/stg_equipment_state_history.sql

# Facts: Daily OEE
gorchata run model --model models/facts/fact_equipment_daily_oee.sql

# Metrics: Reliability analysis
gorchata run model --model models/metrics/equipment_reliability_metrics.sql
gorchata run model --model models/metrics/equipment_downtime_analysis.sql
gorchata run model --model models/metrics/failure_mode_pareto.sql

# Metrics: Buffer & constraint analysis

gorchata run model --model models/metrics/buffer_utilization_analysis.sql
gorchata run model --model models/metrics/starvation_blocking_analysis.sql
gorchata run model --model models/metrics/constraint_analysis.sql

# Analytics: Advanced insights
gorchata run model --model models/analytics/bad_actor_prioritization.sql
gorchata run model --model models/analytics/shift_performance_comparison.sql
```

### 5. Query Results
```sql
-- Plant OEE Summary
SELECT AVG(oee_pct), AVG(availability_pct), AVG(performance_pct), AVG(quality_pct)
FROM fact_equipment_daily_oee;

-- Top 5 Bad Actors
SELECT equipment_name, criticality_level, total_failures, impact_score
FROM bad_actor_prioritization
ORDER BY priority_rank LIMIT 5;

-- Current Constraint
SELECT equipment_name, current_utilization_pct, constraint_score
FROM constraint_analysis
WHERE analysis_type = 'Constraint Identification'
ORDER BY constraint_score DESC LIMIT 1;
```

## Key Metrics

| Metric | Formula | Target | Use |
|--------|---------|--------|-----|
| **OEE** | A  P  Q | 85% | Benchmark performance, track trends |
| **MTBF** | Operating Time / Failures | Varies | Prioritize reliability improvements |
| **MTTR** | Downtime / Failures | Varies | Improve response time, stock spares |
| **Impact Score** | Downtime  Frequency  Criticality | - | Prioritize maintenance budget |
| **Availability** | Operating / Planned Time | >90% | Track downtime impact |
| **Performance** | Actual / Ideal Output | >95% | Identify speed losses |
| **Quality** | Good / Total Output | >99% | Track process stability |
| **Buffer Util** | Level / Capacity | 40-60% | Optimize sizing, predict propagation |
| **Constraint Score** | Util  (1 + Starvation/100) | - | Identify bottleneck (TOC) |
| **PM Ratio** | PM / (PM + Breakdown) | >50% | Shift to preventive strategy |

## Analytics Use Cases

### 1. Daily Operations (Plant Manager)
**Q:** How did we perform yesterday?  
**A:** Plant OEE 78%, production 240 tons (89% of target), top losses: Dryer bearing (6h), Press hydraulic leak (2h)  
**Action:** Schedule bearing replacement, expedite hydraulic seals

### 2. Maintenance Prioritization (Reliability Engineer)
**Q:** Which equipment needs investment?  
**A:** Bad Actor Score: DRYER-01=180 (5 failures12h3 criticality), PRESS-01=36, STRAND-01=18  
**Action:** Dryer bearing monitoring, increase PM frequency, ROI 56% (7.7mo payback) for MTBF doubling

### 3. Constraint Identification (Process Engineer)
**Q:** What limits throughput? Where to invest capacity?  
**A:** Constraint: DRYER-01 (10 t/hr @ 100% util), throughput 240 t/day, gap vs demand 54 t (12%)  
**Economic Impact:** $13,500/day revenue loss  
**Action:** Add 2nd dryer ($500K)  80% capacity increase  20-day payback

### 4. Shift Performance (Shift Supervisor)
**Q:** Why is Swing shift underperforming?  
**A:** Night 93.8%, Day 87.5%, Swing 75.0% availability. Root cause: Less experienced operators  
**Action:** Cross-train Swing with Night crew, target 88% availability (13% capacity gain)

### 5. Quality Root Cause (Process Engineer)
**Q:** Why more thickness defects?  
**A:** 15/18 defects (83%) when press temp <150C (normal 165-175C)  
**Action:** Replace temp controller ($5K), update SOPs, reduce defects 5%2% ($8K/day value)

### 6. Downtime Propagation (Process Engineer)
**Q:** Why did FORMER-01 stop when STRAND-01 failed?  
**A:** STRAND failure  green bins depleted in 2h  DRYER starved  dry silos depleted in 4.8h  FORMER starved  
**Action:** Increase buffer 4h6h, implement predictive alerts at 20% buffer level

### 7. Maintenance Strategy (Maintenance Planner)
**Q:** Is our maintenance effective?  
**A:** DRYER PM ratio 28.6% (target >50%), breakdown costs $15K vs PM $2K (7.5 ratio)  
**Action:** Increase PM frequency quarterlymonthly, reduce breakdowns 60%, lower costs 40%

## Project Structure

```
examples/osb_machine_event_oee/
 README.md                              # This file
 DATA_DICTIONARY.md                     # Table/column documentation
 EXAMPLE_QUERIES.md                     # Common analytics queries
 VISUALIZATION_GUIDE.md                 # Dashboard guidance
 schema.yml                             # Star schema (15 tables)

 seeds/                                 # 147 dimension records
    README.md
    seed.yml
    dim_equipment.csv (16 records)
    dim_production_area.csv (8 records)
    dim_reason_code.csv (25 records)
    dim_shift.csv (3 records)
    dim_product_spec.csv (3 records)
    dim_date.csv (90 records)

 models/
    staging/
       stg_equipment_state_history.sql    # State duration calculation
    facts/
       fact_equipment_daily_oee.sql       # OEE = APQ
    metrics/
       equipment_reliability_metrics.sql  # MTBF/MTTR
       equipment_downtime_analysis.sql    # By reason code
       failure_mode_pareto.sql            # Pareto ranking
       buffer_utilization_analysis.sql    # Buffer simulation
       starvation_blocking_analysis.sql   # Root cause propagation
       constraint_analysis.sql            # TOC bottleneck ID
    analytics/
        bad_actor_prioritization.sql       # Impact scoring
        shift_performance_comparison.sql   # Shift OEE comparison

 test/ (in project root)
     osb_oee_schema_test.go (7 tests)
     osb_seed_test.go (10 tests)
     osb_state_duration_test.go (8 tests)
     osb_oee_calculation_test.go (8 tests)
     osb_downtime_analysis_test.go (8 tests)
     osb_buffer_constraint_test.go (9 tests)
     osb_advanced_analytics_test.go (6 tests)
```

## Data Model

**Star Schema:** 6 dimensions, 5 staging tables, 1 fact table, 6 metric tables, 2 analytics tables

**Dimensions:**
- dim_equipment (16 records) - Equipment catalog with capacities, criticality
- dim_production_area (8 records) - Process stages with buffer capacities
- dim_reason_code (25 records) - Downtime reasons mapped to OEE model
- dim_shift (3 records) - Day/Swing/Night shifts
- dim_product_spec (3 records) - OSB panel specifications
- dim_date (90 records) - Date dimension

**Staging:**
- stg_equipment_state_history - State durations calculated via LEAD() window function


**Facts:**
- fact_equipment_daily_oee - Daily OEE metrics by equipment

**Metrics:**
- equipment_reliability_metrics - MTBF, MTTR, failure frequency
- equipment_downtime_analysis - Downtime aggregated by reason code
- failure_mode_pareto - Pareto ranking of failure modes
- buffer_utilization_analysis - Buffer inventory time-series simulation
- starvation_blocking_analysis - Root cause propagation analysis
- constraint_analysis - TOC constraint identification

**Analytics:**
- bad_actor_prioritization - Equipment impact scoring for maintenance prioritization
- shift_performance_comparison - OEE comparison across shifts

See [DATA_DICTIONARY.md](DATA_DICTIONARY.md) for complete documentation.

## References

### OEE & Manufacturing
- [OEE Foundation](https://www.oee.com/) - Industry standard OEE methodology
- SEMI E79 - OEE for continuous processes
- ISO 22400 - Manufacturing KPIs

### Theory of Constraints
- Eliyahu Goldratt, "The Goal" - TOC and bottleneck management
- [TOC International](https://www.tocico.org/) - TOC resources
- Buffer Management in TOC

### Standards
- ISA-95 (ANSI/ISA-95) - Enterprise-Control System Integration
- ISA-88 - Batch Control for batch/continuous processes

### OSB Manufacturing  
- Composite Panel Association (CPA) - OSB standards
- APA  The Engineered Wood Association

### Reliability
- RCM (Reliability-Centered Maintenance)
- API 584 - Integrity Operating Windows (IOW)

## Status

-  **Phase 1**: Schema Design and DDL Generation
-  **Phase 2**: Seed Configuration for Event Generation
-  **Phase 3**: State Duration Calculation Logic
-  **Phase 4**: OEE Calculation (Availability, Performance, Quality)
-  **Phase 5**: Downtime Analysis and Reliability Metrics (MTBF, MTTR)
-  **Phase 6**: Buffer and Constraint Analysis (TOC)
-  **Phase 7**: Advanced Analytics and Improvement Opportunities
-  **Phase 8**: Documentation, Example Queries, and Visualization Guidance (In Progress)

**Test Coverage:** 56 test functions, 100% passing  
**SQL Models:** ~1,500 lines across 12 models  
**Documentation:** README , Data Dictionary (pending), Example Queries (pending), Visualization Guide (pending)

## License

Part of the Gorchata project. See [LICENSE](../../LICENSE) for details.
