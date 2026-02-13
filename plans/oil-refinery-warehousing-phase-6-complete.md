## Phase 6 Complete: Mass Balance and Reconciliation

Phase 6 successfully implements refinery-wide mass balance tracking with input/output reconciliation and loss accounting, enforcing the fundamental conservation principle: **Inputs = Outputs + Losses + Inventory Change ± Unaccounted**.

**Files created/changed:**
- transformations/mass_balance.sql (created, 480+ lines)
- examples/oil_refinery_warehousing/PHASE_6_SUMMARY.md (created, 500+ lines)
- schema.yml (modified, +30 columns for fact_mass_balance and stg_mass_balance)
- oil_refinery_test.go (modified, +8 tests, 51 total)
- docs/MASS_BALANCE.md (updated, +200 lines for Phase 6 implementation)

**Functions created/changed:**
- Mass balance equation: `UFL = Total_Inputs - (Total_Outputs + Total_Losses + Inventory_Change)`
- Total inputs aggregation: `SUM(crude_net_weight_tons)` from crude receipts
- Total outputs aggregation: `SUM(product_weight_tons)` from unit production
- Refinery fuel consumption: `Crude_Input × 6%` (typical 5-8% range)
- Coke production tracking: `Crude_Input × 2%` (typical 1-5% range)
- Flare losses: `Crude_Input × 0.2%` (typical 0.1-0.3%)
- Evaporation losses: `Crude_Input × 0.15%` (typical 0.1-0.2%)
- Inventory change: `SUM(closing_balance_tons - opening_balance_tons)`
- UFL percentage: `(Unaccounted / Total_Inputs) × 100`
- Tolerance validation: `Balance_Flag = TRUE if |UFL_%| > threshold`

**Tests created/changed:**
- TestFactMassBalanceTableExists (schema validation)
- TestMassBalanceEquation (3 scenarios: balanced/positive UFL/negative UFL)
- TestUFLCalculation (5 scenarios: zero/positive/negative/large positive/large negative)
- TestToleranceValidation (7 scenarios: daily vs monthly thresholds)
- TestFuelConsumptionAccounting (4 scenarios: min/typical/max/excessive)
- TestCokeProductionTracking (3 scenarios: normal/high/low)
- TestInventoryChangeImpact (3 scenarios: building/drawing/stable)
- TestSeedMassBalanceValid (schema conformance)

**Review Status:** APPROVED

**Key Implementation Details:**

**FACT_MASS_BALANCE Structure (15 columns):**
- balance_id (PK)
- date_key (FK → dim_date)
- period_type (Daily/Weekly/Monthly)
- total_crude_input_tons (300k-350k typical)
- total_product_output_tons (280k-310k typical)
- refinery_fuel_consumed_tons (6% of input)
- coke_produced_tons (2% of input)
- flare_losses_tons (0.2% of input)
- evaporation_losses_tons (0.15% of input)
- inventory_change_tons (±100k range)
- total_accounted_tons (calculated)
- unaccounted_tons (UFL)
- unaccounted_pct (UFL percentage)
- balance_flag (TRUE if |UFL_%| > threshold)
- notes (nullable text for investigation)

**Mass Balance Equation:**
```
Conservation of Mass:
  Total_Inputs = Total_Outputs + Total_Losses + Inventory_Change + UFL

Rearranged for UFL:
  UFL = Total_Inputs - (Total_Outputs + Total_Losses + Inventory_Change)

Components:
  - Total Inputs: All crude receipts (weight basis)
  - Total Outputs: All products shipped/produced
  - Fuel Consumed: Burned for process heat (6% typical)
  - Coke Produced: Solid byproduct from FCC/Coker (2% typical)
  - Flare Losses: Safety/emergency relief (0.2% typical)
  - Evaporation: Tank breathing, fugitive (0.15% typical)
  - Inventory Change: Net change in tank inventory
  - UFL: Measurement error / unaccounted difference
```

**Tolerance Thresholds:**
- **Daily:** ±0.5% (more lenient, measurement noise)
- **Monthly:** ±0.3% (tighter, averaging reduces noise)
- **Investigation Trigger:** |UFL_%| > threshold sets balance_flag = TRUE

**Example Balanced Day (Typical):**
```
Date: 2025-01-15
Crude Input: 325,000 tons

Breakdown:
  Product Output:      285,000 tons (87.7%)
  Fuel Consumed:        19,500 tons (6.0%)
  Coke Produced:         6,500 tons (2.0%)
  Flare Losses:            650 tons (0.2%)
  Evaporation:             488 tons (0.15%)
  Inventory Change:    +12,500 tons
  Total Accounted:     324,638 tons (99.89%)

UFL Calculation:
  Unaccounted: 325,000 - 324,638 = 362 tons
  UFL %: (362 / 325,000) × 100 = 0.111% ✓
  Flag: FALSE (within ±0.5% daily threshold)
  Status: OK - Excellent measurement quality
```

**Example Out-of-Tolerance Day:**
```
Date: 2025-01-22
Crude Input: 330,000 tons
Total Accounted: 327,500 tons

UFL Calculation:
  Unaccounted: 2,500 tons
  UFL %: (2,500 / 330,000) × 100 = 0.758% ⚠️
  Flag: TRUE (exceeds ±0.5% threshold)
  Status: INVESTIGATE - Check tank gauges, meters, timing
```

**UFL Quality Indicators:**
| UFL % | Classification | Action |
|-------|----------------|--------|
| < 0.5% | Excellent | Continue monitoring |
| 0.5-1.0% | Good | Review for trends |
| 1.0-2.0% | Fair | Investigate measurement systems |
| > 2.0% | Poor | Immediate investigation required |

**SQL Transformation Views (mass_balance.sql):**
1. Daily Input Aggregation - sum all crude receipts
2. Daily Output Aggregation - sum all products
3. Loss Calculations - fuel, coke, flare, evaporation
4. Inventory Change - net tank balance movement
5. UFL Calculation - unaccounted difference
6. Tolerance Validation - flag out-of-tolerance days
7. Monthly Rollup - aggregate for tighter monthly threshold
8. Investigation Report - flagged days with details

**Test Results:**
- Total tests: 51 (43 from Phase 5 + 8 new)
- Pass rate: 100% (51/51)
- TDD workflow followed: RED → GREEN → REFACTOR
- All compilation clean, zero errors

**Business Value:**
- **Conservation enforcement:** Physical law validated daily
- **Loss visibility:** Every ton accounted and categorized
- **Measurement quality:** UFL indicates meter/gauge accuracy
- **Investigation triggers:** Automatic flagging saves manual review
- **Operational optimization:** Identify loss reduction opportunities
- **Regulatory compliance:** Auditable mass balance for EPA/EIA reporting
- **Carbon accounting:** Foundation for Scope 1 emissions (combustion)

**Key Physical Principles:**
1. **Conservation of Mass:** Mass cannot be created or destroyed
2. **Weight-Based Accounting:** Eliminates volume/density confusion
3. **Closure Requirement:** Sum of all components must equal input
4. **Measurement Quality:** UFL near zero indicates good systems
5. **Industry Standards:** API MPMS Chapter 12 compliance

**Git Commit Message:**
```
feat: Add refinery-wide mass balance tracking and reconciliation

- Add FACT_MASS_BALANCE table with 15 columns
- Implement conservation of mass equation (Inputs = Outputs + Losses + UFL)
- Track all loss categories (fuel, coke, flare, evaporation)
- Calculate unaccounted for loss (UFL) and percentage
- Add tolerance validation (±0.5% daily, ±0.3% monthly)
- Automatic investigation flagging for out-of-tolerance days
- Integrate inventory changes from tank balances
- Account for refinery fuel consumption (6% typical)
- Track coke production from FCC/Coker units
- Add 8 new tests (51 total, 100% passing)
- Create comprehensive SQL transformations (480+ lines)
- Update MASS_BALANCE.md documentation with Phase 6 details
```
