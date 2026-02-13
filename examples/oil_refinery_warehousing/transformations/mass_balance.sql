-- ==============================================================================
-- Mass Balance Calculations - Phase 6
-- ==============================================================================
--
-- This transformation file implements refinery-wide mass balance tracking to
-- ensure conservation of mass and identify losses. It demonstrates:
--
-- 1. Conservation of Mass Equation
-- 2. Unaccounted for Loss (UFL) Calculation
-- 3. Tolerance Validation (Daily ±0.5%, Monthly ±0.3%)
-- 4. Fuel Consumption Accounting (5-8% typical)
-- 5. Loss Categorization (Coke, Flare, Evaporation)
-- 6. Inventory Change Integration
--
-- FUNDAMENTAL EQUATION:
-- Total Inputs = Total Outputs + Total Losses + Inventory Change + UFL
--
-- References:
-- - docs/MASS_BALANCE.md
-- - API MPMS Chapter 12: Calculation of Petroleum Quantities
-- ==============================================================================

-- ==============================================================================
-- SECTION 1: Daily Mass Balance Calculation
-- ==============================================================================
--
-- This section aggregates all inputs, outputs, and losses for each day to
-- calculate the daily mass balance and identify unaccounted variances.
--
-- CALCULATION METHODOLOGY:
--
-- INPUTS (Weight Basis - Short Tons):
--   Total_Crude_Input = SUM(weight_short_tons) FROM fact_crude_receipts
--
-- OUTPUTS (Weight Basis - Short Tons):
--   Total_Product_Output = SUM(product_weight_tons) FROM fact_unit_production
--
-- FUEL CONSUMPTION (5-8% of Crude Input):
--   Refinery burns fuel gas for process heat
--   Typical: 6.0% of crude input
--   Range: 5.0-8.0% depending on configuration and efficiency
--
-- COKE PRODUCTION (1-3% of Crude Input):
--   Petroleum coke from FCC and Coker units
--   This is a solid product, not shipped as liquid
--   Burned in regenerator or sold as solid fuel
--
-- FLARE LOSSES (0.1-0.3% of Crude Input):
--   Emergency relief, safety flaring
--   Typical: 0.2% of crude input
--
-- EVAPORATION LOSSES (0.1-0.2% of Crude Input):
--   Tank breathing, loading losses
--   Typical: 0.15% of crude input
--
-- INVENTORY CHANGE:
--   Net change in tank inventory (closing - opening)
--   Positive = Building inventory (accumulation)
--   Negative = Drawing from inventory (depletion)
--
-- UNACCOUNTED FOR LOSS (UFL):
--   UFL = Total_Input - Total_Accounted
--   UFL_Pct = (UFL / Total_Input) × 100
--
-- TOLERANCE THRESHOLDS:
--   Daily: ±0.5% (exceeding triggers investigation)
--   Monthly: ±0.3% (tighter due to averaging)
-- ==============================================================================

INSERT INTO stg_mass_balance (
    balance_id,
    date_key,
    period_type,
    total_crude_input_tons,
    total_product_output_tons,
    refinery_fuel_consumed_tons,
    coke_produced_tons,
    flare_losses_tons,
    evaporation_losses_tons,
    inventory_change_tons,
    total_accounted_tons,
    unaccounted_tons,
    unaccounted_pct,
    balance_flag,
    notes
)
SELECT
    -- Generate unique balance_id from date and period type
    date_key || '_' || period_type AS balance_id,
    
    date_key,
    'Daily' AS period_type,
    
    -- ==================================================================
    -- COMPONENT 1: Total Crude Input
    -- ==================================================================
    -- Sum all crude receipts for the day
    -- Weight basis provides accurate mass accounting
    -- ==================================================================
    COALESCE(crude_input.total_input, 0) AS total_crude_input_tons,
    
    -- ==================================================================
    -- COMPONENT 2: Total Product Output
    -- ==================================================================
    -- Sum all products shipped (from unit production)
    -- Includes all liquid products (gasoline, diesel, jet, etc.)
    -- Excludes coke (tracked separately as solid)
    -- ==================================================================
    COALESCE(product_output.total_output, 0) AS total_product_output_tons,
    
    -- ==================================================================
    -- COMPONENT 3: Refinery Fuel Consumption
    -- ==================================================================
    -- Fuel gas burned in process heaters
    -- Calculated as 6.0% of crude input (typical)
    -- Range: 5.0-8.0% depending on configuration
    -- 
    -- Factors affecting fuel consumption:
    -- - Unit complexity (more units = more fuel)
    -- - Crude quality (heavy crude requires more heat)
    -- - Energy efficiency (newer refineries use less)
    -- - Weather (cold climates use more fuel)
    -- ==================================================================
    COALESCE(crude_input.total_input, 0) * 0.06 AS refinery_fuel_consumed_tons,
    
    -- ==================================================================
    -- COMPONENT 4: Coke Production
    -- ==================================================================
    -- Petroleum coke from FCC and Coker units
    -- Calculated as 2.0% of crude input (typical)
    -- Range: 1.0-5.0% depending on crude quality
    -- 
    -- Coke Formation:
    -- - Heavy crude → more coke
    -- - Light crude → less coke
    -- - FCC: 4-5% on feed basis
    -- - Coker: produces marketable coke product
    -- ==================================================================
    COALESCE(crude_input.total_input, 0) * 0.02 AS coke_produced_tons,
    
    -- ==================================================================
    -- COMPONENT 5: Flare Losses
    -- ==================================================================
    -- Flaring for safety relief and venting
    -- Calculated as 0.2% of crude input (typical)
    -- Range: 0.1-0.3%
    -- 
    -- Flaring Scenarios:
    -- - Emergency pressure relief
    -- - Startup/shutdown purging
    -- - Off-spec product disposal
    -- - Process upsets
    -- ==================================================================
    COALESCE(crude_input.total_input, 0) * 0.002 AS flare_losses_tons,
    
    -- ==================================================================
    -- COMPONENT 6: Evaporation Losses
    -- ==================================================================
    -- Tank breathing and loading evaporation
    -- Calculated as 0.15% of crude input (typical)
    -- Range: 0.1-0.2%
    -- 
    -- Evaporation Sources:
    -- - Tank breathing (thermal expansion/contraction)
    -- - Loading operations (displacement vapor)
    -- - Fugitive emissions (valves, seals)
    -- ==================================================================
    COALESCE(crude_input.total_input, 0) * 0.0015 AS evaporation_losses_tons,
    
    -- ==================================================================
    -- COMPONENT 7: Inventory Change
    -- ==================================================================
    -- Net change in tank inventory
    -- Positive = Building inventory (more in tanks at end of day)
    -- Negative = Drawing inventory (less in tanks at end of day)
    -- 
    -- Calculation:
    -- Inventory_Change = SUM(closing_balance - opening_balance)
    -- 
    -- Convert barrels to tons using average SG of 0.85
    -- Weight (tons) = Volume (bbl) × 0.1364 × SG
    -- ==================================================================
    COALESCE(inventory_change.net_change_bbl, 0) * 0.1364 * 0.85 AS inventory_change_tons,
    
    -- ==================================================================
    -- COMPONENT 8: Total Accounted
    -- ==================================================================
    -- Sum of all known outputs and losses
    -- Formula:
    -- Total_Accounted = Product_Output + Fuel_Consumed + Coke_Produced +
    --                   Flare_Losses + Evaporation_Losses + Inventory_Change
    -- ==================================================================
    (
        COALESCE(product_output.total_output, 0) +
        COALESCE(crude_input.total_input, 0) * 0.06 +      -- Fuel (6%)
        COALESCE(crude_input.total_input, 0) * 0.02 +      -- Coke (2%)
        COALESCE(crude_input.total_input, 0) * 0.002 +     -- Flare (0.2%)
        COALESCE(crude_input.total_input, 0) * 0.0015 +    -- Evaporation (0.15%)
        COALESCE(inventory_change.net_change_bbl, 0) * 0.1364 * 0.85  -- Inventory
    ) AS total_accounted_tons,
    
    -- ==================================================================
    -- COMPONENT 9: Unaccounted For Loss (UFL)
    -- ==================================================================
    -- Difference between input and all accounted outputs
    -- Formula:
    -- UFL = Total_Input - Total_Accounted
    -- 
    -- Interpretation:
    -- - Positive UFL: More input than accounted output (likely measurement error or actual loss)
    -- - Negative UFL: More output than input (likely measurement error or inventory error)
    -- - Near Zero: Good measurement systems and accounting
    -- ==================================================================
    (
        COALESCE(crude_input.total_input, 0) -
        (
            COALESCE(product_output.total_output, 0) +
            COALESCE(crude_input.total_input, 0) * 0.06 +
            COALESCE(crude_input.total_input, 0) * 0.02 +
            COALESCE(crude_input.total_input, 0) * 0.002 +
            COALESCE(crude_input.total_input, 0) * 0.0015 +
            COALESCE(inventory_change.net_change_bbl, 0) * 0.1364 * 0.85
        )
    ) AS unaccounted_tons,
    
    -- ==================================================================
    -- COMPONENT 10: Unaccounted Percentage
    -- ==================================================================
    -- UFL expressed as percentage of total input
    -- Formula:
    -- UFL_Pct = (UFL / Total_Input) × 100
    -- 
    -- Industry Standards:
    -- - Excellent: < 0.5%
    -- - Good: 0.5-1.0%
    -- - Fair: 1.0-2.0%
    -- - Poor: > 2.0% (requires investigation)
    -- ==================================================================
    CASE
        WHEN COALESCE(crude_input.total_input, 0) > 0 THEN
            (
                (
                    COALESCE(crude_input.total_input, 0) -
                    (
                        COALESCE(product_output.total_output, 0) +
                        COALESCE(crude_input.total_input, 0) * 0.06 +
                        COALESCE(crude_input.total_input, 0) * 0.02 +
                        COALESCE(crude_input.total_input, 0) * 0.002 +
                        COALESCE(crude_input.total_input, 0) * 0.0015 +
                        COALESCE(inventory_change.net_change_bbl, 0) * 0.1364 * 0.85
                    )
                ) / COALESCE(crude_input.total_input, 0)
            ) * 100.0
        ELSE 0
    END AS unaccounted_pct,
    
    -- ==================================================================
    -- COMPONENT 11: Balance Flag (Tolerance Validation)
    -- ==================================================================
    -- TRUE if |unaccounted_pct| exceeds tolerance threshold
    -- Daily Threshold: ±0.5%
    -- Monthly Threshold: ±0.3%
    -- 
    -- Flag Triggers Investigation:
    -- - Check meter calibrations
    -- - Review tank gauging accuracy
    -- - Verify temperature corrections
    -- - Inspect for leaks or theft
    -- - Validate calculation parameters
    -- ==================================================================
    CASE
        WHEN COALESCE(crude_input.total_input, 0) > 0 THEN
            CASE
                WHEN ABS(
                    (
                        (
                            COALESCE(crude_input.total_input, 0) -
                            (
                                COALESCE(product_output.total_output, 0) +
                                COALESCE(crude_input.total_input, 0) * 0.06 +
                                COALESCE(crude_input.total_input, 0) * 0.02 +
                                COALESCE(crude_input.total_input, 0) * 0.002 +
                                COALESCE(crude_input.total_input, 0) * 0.0015 +
                                COALESCE(inventory_change.net_change_bbl, 0) * 0.1364 * 0.85
                            )
                        ) / COALESCE(crude_input.total_input, 0)
                    ) * 100.0
                ) > 0.5 THEN TRUE  -- Daily threshold
                ELSE FALSE
            END
        ELSE FALSE
    END AS balance_flag,
    
    -- ==================================================================
    -- COMPONENT 12: Notes
    -- ==================================================================
    -- Automatic notes for flagged balances
    -- ==================================================================
    CASE
        WHEN COALESCE(crude_input.total_input, 0) = 0 THEN
            'No crude input recorded for this day'
        WHEN ABS(
            (
                (
                    COALESCE(crude_input.total_input, 0) -
                    (
                        COALESCE(product_output.total_output, 0) +
                        COALESCE(crude_input.total_input, 0) * 0.06 +
                        COALESCE(crude_input.total_input, 0) * 0.02 +
                        COALESCE(crude_input.total_input, 0) * 0.002 +
                        COALESCE(crude_input.total_input, 0) * 0.0015 +
                        COALESCE(inventory_change.net_change_bbl, 0) * 0.1364 * 0.85
                    )
                ) / COALESCE(crude_input.total_input, 0)
            ) * 100.0
        ) > 0.5 THEN
            'FLAGGED: Unaccounted exceeds daily tolerance (±0.5%). Investigation required.'
        ELSE
            'Within tolerance'
    END AS notes

FROM
    dim_date d

-- ==================================================================
-- JOIN: Crude Input Aggregation
-- ==================================================================
LEFT JOIN (
    SELECT
        date_key,
        SUM(weight_short_tons) AS total_input
    FROM fact_crude_receipts
    GROUP BY date_key
) crude_input ON d.date_key = crude_input.date_key

-- ==================================================================
-- JOIN: Product Output Aggregation
-- ==================================================================
LEFT JOIN (
    SELECT
        date_key,
        SUM(product_weight_tons) AS total_output
    FROM fact_unit_production
    WHERE product_name NOT LIKE '%Coke%'  -- Exclude coke (tracked separately)
    GROUP BY date_key
) product_output ON d.date_key = product_output.date_key

-- ==================================================================
-- JOIN: Inventory Change Aggregation
-- ==================================================================
LEFT JOIN (
    SELECT
        date_key,
        SUM(closing_balance_bbl - opening_balance_bbl) AS net_change_bbl
    FROM fact_tank_inventory
    GROUP BY date_key
) inventory_change ON d.date_key = inventory_change.date_key

WHERE
    -- Only calculate balances for days with crude input
    COALESCE(crude_input.total_input, 0) > 0

ORDER BY date_key;

-- ==============================================================================
-- SECTION 2: Final Fact Table Load
-- ==============================================================================
--
-- Load validated staging data into final fact table
-- ==============================================================================

INSERT INTO fact_mass_balance
SELECT * FROM stg_mass_balance;

-- ==============================================================================
-- SECTION 3: Validation Queries
-- ==============================================================================

/*
-- Query 1: Check Balance Status Summary
-----------------------------------------
SELECT
    COUNT(*) AS total_days,
    SUM(CASE WHEN balance_flag = TRUE THEN 1 ELSE 0 END) AS flagged_days,
    ROUND(AVG(unaccounted_pct), 3) AS avg_ufl_pct,
    ROUND(MIN(unaccounted_pct), 3) AS min_ufl_pct,
    ROUND(MAX(unaccounted_pct), 3) AS max_ufl_pct,
    ROUND(STDDEV(unaccounted_pct), 3) AS stddev_ufl_pct
FROM fact_mass_balance
WHERE period_type = 'Daily';

-- Query 2: Identify Out-of-Tolerance Days
-------------------------------------------
SELECT
    date_key,
    total_crude_input_tons,
    total_product_output_tons,
    total_accounted_tons,
    unaccounted_tons,
    unaccounted_pct,
    notes
FROM fact_mass_balance
WHERE balance_flag = TRUE
ORDER BY ABS(unaccounted_pct) DESC;

-- Query 3: Mass Balance Component Breakdown
---------------------------------------------
SELECT
    date_key,
    total_crude_input_tons,
    total_product_output_tons,
    refinery_fuel_consumed_tons,
    coke_produced_tons,
    flare_losses_tons,
    evaporation_losses_tons,
    inventory_change_tons,
    unaccounted_tons,
    unaccounted_pct
FROM fact_mass_balance
WHERE period_type = 'Daily'
ORDER BY date_key;

-- Query 4: Verify Conservation of Mass
----------------------------------------
-- This query should show minimal deviation from zero
SELECT
    date_key,
    total_crude_input_tons AS input,
    (
        total_product_output_tons +
        refinery_fuel_consumed_tons +
        coke_produced_tons +
        flare_losses_tons +
        evaporation_losses_tons +
        inventory_change_tons
    ) AS total_output_and_losses,
    unaccounted_tons,
    -- Verify: Input = Output + Losses + Unaccounted
    (
        total_crude_input_tons -
        (
            total_product_output_tons +
            refinery_fuel_consumed_tons +
            coke_produced_tons +
            flare_losses_tons +
            evaporation_losses_tons +
            inventory_change_tons +
            unaccounted_tons
        )
    ) AS mass_balance_check  -- Should be ~0
FROM fact_mass_balance
ORDER BY date_key;
*/

-- ==============================================================================
-- END OF TRANSFORMATION
-- ==============================================================================
