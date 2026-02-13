## Plan: Oil Refinery Data Transformation and Warehousing

Build a comprehensive data transformation and warehousing system for an integrated oil refinery that tracks mass balance, product yields, and unit performance. Transform raw crude receipts, processing unit operations, and product shipments into an analytical framework enabling engineers to monitor efficiency, identify deviations, and optimize operations.

**Phases: 8**

1. **Phase 1: Project Structure and Core Schema**
    - **Objective:** Establish project structure, dimension tables, and foundational schema for refinery data
    - **Files/Functions to Modify/Create:**
        - [examples/oil_refinery_warehousing/README.md](examples/oil_refinery_warehousing/README.md)
        - [examples/oil_refinery_warehousing/schema.yml](examples/oil_refinery_warehousing/schema.yml)
        - [examples/oil_refinery_warehousing/docs/](examples/oil_refinery_warehousing/docs/)
    - **Tests to Write:**
        - Test schema validation (all required fields present)
        - Test dimension table structures (date, unit, product, crude_grade)
        - Test referential integrity constraints
    - **Steps:**
        1. Create directory structure following Gorchata project layout
        2. Write README with refinery background, process unit descriptions, and mass balance concepts
        3. Define DIM_DATE, DIM_UNIT, DIM_PRODUCT, DIM_CRUDE_GRADE, DIM_STREAM dimension tables in schema.yml
        4. Add seed data configuration for dimensions
        5. Document measurement standards (API gravity, volume/weight conversions, temperature corrections)
        6. Write tests to validate schema parses correctly and dimensions have required keys

2. **Phase 2: Crude Oil Receipt Tracking**
    - **Objective:** Implement crude oil receipt fact table with volume/weight conversions and API gravity handling
    - **Files/Functions to Modify/Create:**
        - [examples/oil_refinery_warehousing/schema.yml](examples/oil_refinery_warehousing/schema.yml) (add FACT_CRUDE_RECEIPTS)
        - [examples/oil_refinery_warehousing/seed_crude_receipts.yml](examples/oil_refinery_warehousing/seed_crude_receipts.yml)
        - [examples/oil_refinery_warehousing/transformations/](examples/oil_refinery_warehousing/transformations/)
    - **Tests to Write:**
        - Test crude receipt record creation
        - Test API gravity to specific gravity conversion: SG = 141.5 / (API + 131.5)
        - Test volume-to-weight conversion: Weight(tons) = Volume(bbl) × 0.1756 × SG
        - Test BS&W (Basic Sediment & Water) deduction calculation
        - Test temperature correction to standard 60°F
    - **Steps:**
        1. Define FACT_CRUDE_RECEIPTS table with measures (gross_volume_bbl, net_volume_bbl, weight_tons, api_gravity, sulfur_pct)
        2. Add foreign keys to DIM_DATE, DIM_CRUDE_GRADE, DIM_LOCATION
        3. Create seed data with realistic crude receipts (WTI, Brent, Maya varieties)
        4. Implement volume-to-weight conversion function
        5. Implement temperature correction logic (ASTM D1250 simplified)
        6. Write transformation SQL to populate fact table from seed data
        7. Write tests for conversion accuracy (±0.1% tolerance)

3. **Phase 3: Process Unit Operations and Feed Tracking**
    - **Objective:** Track daily feed to process units (CDU, FCC, Hydrocracker, Reformer) with operational metrics
    - **Files/Functions to Modify/Create:**
        - [examples/oil_refinery_warehousing/schema.yml](examples/oil_refinery_warehousing/schema.yml) (add FACT_UNIT_FEED, FACT_UNIT_OPERATIONS)
        - [examples/oil_refinery_warehousing/seed_unit_operations.yml](examples/oil_refinery_warehousing/seed_unit_operations.yml)
        - [examples/oil_refinery_warehousing/transformations/unit_operations.sql](examples/oil_refinery_warehousing/transformations/unit_operations.sql)
    - **Tests to Write:**
        - Test unit feed record creation for CDU, VDU, FCC, Hydrocracker
        - Test feed volume sum ≤ crude receipts + intermediate transfers
        - Test capacity utilization calculation: actual / nameplate capacity
        - Test operating hours validation (0-24 per day)
        - Test unit hierarchy rollup (complex level aggregation)
    - **Steps:**
        1. Define FACT_UNIT_FEED with measures (feed_volume_bbl, feed_weight_tons, feed_api_gravity, feed_sulfur_pct)
        2. Define FACT_UNIT_OPERATIONS with operational KPIs (throughput, conversion_pct, energy_consumed_mmbtu, operating_hours)
        3. Create seed data for typical daily operations of 6-8 major units
        4. Implement unit hierarchy (Crude Complex, Conversion Complex, Clean Fuels Complex)
        5. Add capacity utilization and efficiency calculations
        6. Write tests ensuring feed totals balance with crude inputs

4. **Phase 4: Product Yield Calculations and Production Facts**
    - **Objective:** Calculate volume and weight yields for each process unit, handling volumetric expansion for light products
    - **Files/Functions to Modify/Create:**
        - [examples/oil_refinery_warehousing/schema.yml](examples/oil_refinery_warehousing/schema.yml) (add FACT_UNIT_PRODUCTION)
        - [examples/oil_refinery_warehousing/seed_unit_production.yml](examples/oil_refinery_warehousing/seed_unit_production.yml)
        - [examples/oil_refinery_warehousing/transformations/yield_calculations.sql](examples/oil_refinery_warehousing/transformations/yield_calculations.sql)
    - **Tests to Write:**
        - Test volume yield calculation: (Product Volume / Feed Volume) × 100
        - Test weight yield calculation: (Product Weight / Feed Weight) × 100
        - Test FCC volumetric expansion (yields can be 105-110% volume basis)
        - Test yield sum validation (95-110% volume, 95-99% weight)
        - Test conversion percentage for FCC and Hydrocracker units
    - **Steps:**
        1. Define FACT_UNIT_PRODUCTION with measures (product_volume_bbl, product_weight_tons, yield_pct_volume, yield_pct_weight)
        2. Create realistic seed data for FCC (51% gasoline, 18% LPG, 17% LCO, 4% coke on weight basis)
        3. Create seed data for Hydrocracker, Reformer, and other units
        4. Implement volume yield calculation handling density differences
        5. Implement weight yield calculation for mass balance
        6. Add conversion percentage logic for upgrading units
        7. Write tests validating yield calculations against known benchmarks

5. **Phase 5: Product Shipments and Inventory Management**
    - **Objective:** Track product shipments and tank inventory with daily reconciliation
    - **Files/Functions to Modify/Create:**
        - [examples/oil_refinery_warehousing/schema.yml](examples/oil_refinery_warehousing/schema.yml) (add FACT_PRODUCT_SHIPMENTS, FACT_TANK_INVENTORY)
        - [examples/oil_refinery_warehousing/seed_shipments_inventory.yml](examples/oil_refinery_warehousing/seed_shipments_inventory.yml)
        - [examples/oil_refinery_warehousing/transformations/inventory_reconciliation.sql](examples/oil_refinery_warehousing/transformations/inventory_reconciliation.sql)
    - **Tests to Write:**
        - Test product shipment record creation
        - Test tank inventory calculation: Closing = Opening + Receipts - Withdrawals
        - Test inventory variance detection (flag if > 0.3%)
        - Test product availability check: shipments ≤ inventory available
        - Test inventory balance by product type
    - **Steps:**
        1. Define FACT_PRODUCT_SHIPMENTS with pipeline, truck, and marine shipment modes
        2. Define FACT_TANK_INVENTORY with opening/closing balances
        3. Create DIM_TANK dimension for storage Tank hierarchy
        4. Create seed data for gasoline, diesel, jet fuel shipments
        5. Implement inventory reconciliation logic
        6. Add variance calculation and flagging (>0.3% triggers investigation)
        7. Write tests for inventory balance equations

6. **Phase 6: Mass Balance and Reconciliation**
    - **Objective:** Implement daily refinery-wide mass balance tracking input/output reconciliation with loss accounting
    - **Files/Functions to Modify/Create:**
        - [examples/oil_refinery_warehousing/schema.yml](examples/oil_refinery_warehousing/schema.yml) (add FACT_MASS_BALANCE)
        - [examples/oil_refinery_warehousing/transformations/mass_balance.sql](examples/oil_refinery_warehousing/transformations/mass_balance.sql)
        - [examples/oil_refinery_warehousing/docs/mass_balance_methodology.md](examples/oil_refinery_warehousing/docs/mass_balance_methodology.md)
    - **Tests to Write:**
        - Test mass balance equation: Inputs = Outputs + Losses + Inventory Change ± Unaccounted
        - Test UFL (Unaccounted for Loss) calculation and percentage
        - Test tolerance threshold validation (±0.5% daily, ±0.3% monthly)
        - Test fuel gas consumption accounting (5-8% of crude input)
        - Test coke production tracking from FCC and coker units
    - **Steps:**
        1. Define FACT_MASS_BALANCE with measures for total inputs, outputs, fuel consumed, losses, unaccounted
        2. Document mass balance methodology with formulas
        3. Implement daily balance calculation aggregating all crude receipts
        4. Sum all product outputs including shipments and inventory changes
        5. Account for refinery fuel consumption (5-8% typical)
        6. Calculate unaccounted difference and percentage
        7. Write tests ensuring balance within industry tolerances (±0.5%)

7. **Phase 7: Data Quality Validation and Anomaly Detection**
    - **Objective:** Implement comprehensive data quality checks including range validations, statistical outliers, and cross-attribute consistency
    - **Files/Functions to Modify/Create:**
        - [examples/oil_refinery_warehousing/schema.yml](examples/oil_refinery_warehousing/schema.yml) (add quality check tables)
        - [examples/oil_refinery_warehousing/transformations/data_quality_checks.sql](examples/oil_refinery_warehousing/transformations/data_quality_checks.sql)
        - [examples/oil_refinery_warehousing/docs/data_quality_rules.md](examples/oil_refinery_warehousing/docs/data_quality_rules.md)
    - **Tests to Write:**
        - Test range validations (API gravity 5-50°, sulfur 0.01-7%)
        - Test Z-score outlier detection (flag if |Z-score| > 3)
        - Test API gravity vs density consistency: SG = 141.5/(API+131.5) within 0.5%
        - Test volume-weight consistency within 1%
        - Test yield sum reasonableness (95-110% volume basis)
    - **Steps:**
        1. Define data quality check tables to log validation results
        2. Implement range validation rules for crude properties, product specs, operating parameters
        3. Implement statistical outlier detection using Z-score method (3-sigma rule)
        4. Add moving average deviation checks (flag if >15% from 7-day average)
        5. Implement cross-attribute validations (API vs density, volume vs weight)
        6. Create data quality dashboard queries
        7. Write tests for each validation rule with edge cases

8. **Phase 8: KPI Dashboards and Performance Analytics**
    - **Objective:** Build analytical queries and aggregation tables for yield optimization, energy efficiency, and operational KPIs
    - **Files/Functions to Modify/Create:**
        - [examples/oil_refinery_warehousing/schema.yml](examples/oil_refinery_warehousing/schema.yml) (add aggregate tables)
        - [examples/oil_refinery_warehousing/transformations/kpi_aggregations.sql](examples/oil_refinery_warehousing/transformations/kpi_aggregations.sql)
        - [examples/oil_refinery_warehousing/queries/operational_dashboards.sql](examples/oil_refinery_warehousing/queries/operational_dashboards.sql)
        - [examples/oil_refinery_warehousing/docs/kpi_definitions.md](examples/oil_refinery_warehousing/docs/kpi_definitions.md)
    - **Tests to Write:**
        - Test Energy Intensity Index (EII) calculation: MMBtu/bbl throughput
        - Test gasoline yield percentage from crude input
        - Test unit conversion efficiency vs target (FCC 72-78%)
        - Test capacity utilization rollup by complex
        - Test yield gap: (Theoretical - Actual) / Theoretical × 100
    - **Steps:**
        1. Create aggregate tables for daily, monthly KPI summaries
        2. Implement Energy Intensity Index (EII) calculation
        3. Calculate product slate percentages (gasoline yield, distillate yield, high-value ratio)
        4. Implement yield gap analysis comparing actual vs theoretical from assay data
        5. Create capacity utilization queries by unit and complex
        6. Build trending queries for 7-day, 30-day moving averages
        7. Document each KPI definition, formula, and target benchmark
        8. Write tests comparing calculated KPIs to expected ranges

**Design Decisions (Approved)**

1. **Downtime Tracking**: Model planned vs unplanned downtime separately in FACT_UNIT_OPERATIONS
2. **Crude Slate Complexity**: Blend of 4-5 crude grades (WTI, Brent, Maya, Dubai, Mars) with varying API gravity and sulfur content
3. **Catalyst Lifecycle**: Track catalyst cycle stage (fresh/mid-cycle/end-of-run) as dimension, correlate with conversion efficiency degradation
4. **Operating Modes**: Balanced product slate baseline, with crude mix composition shifting product balance by ±10% (lighter crude → more gasoline, heavier crude → more diesel/residual)
5. **Seasonal Specifications**: Include seasonal gasoline RVP specs (summer ≤7.8 psi June-Sept, winter ≤13.5 psi Oct-May) as validation rules
