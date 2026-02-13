-- ===================================================================
-- OIL REFINERY DATA WAREHOUSING - PHASE 5 TRANSFORMATION SQL
-- Inventory Reconciliation and Variance Detection
-- ===================================================================
-- Purpose: Transform tank inventory data with reconciliation logic
--
-- Key Business Rules:
-- 1. Inventory Equation: Closing = Opening + Receipts - Withdrawals
-- 2. Variance Detection: Flag if |variance_pct| > 0.3%
-- 3. Product Availability: Verify inventory before shipments
-- 4. Balance Aggregation: Sum by product type
-- ===================================================================

-- ===================================================================
-- DAILY INVENTORY RECONCILIATION
-- ===================================================================
-- This transformation calculates expected closing balances and
-- identifies variances that require investigation.
--
-- Expected_Closing = Opening_Balance + Receipts - Withdrawals
-- Variance = Actual_Closing - Expected_Closing
-- Variance_Pct = (Variance / Expected_Closing) × 100
-- Variance_Flag = TRUE if |Variance_Pct| > 0.3%
-- ===================================================================

CREATE OR REPLACE VIEW vw_inventory_reconciliation AS
SELECT 
    inv.inventory_id,
    inv.date_key,
    dd.full_date,
    dt.tank_code,
    dt.tank_name,
    dt.product_type,
    dp.product_name,
    dt.location,
    
    -- Inventory Components
    inv.opening_balance_bbl,
    inv.receipts_bbl,
    inv.withdrawals_bbl,
    inv.closing_balance_bbl,
    
    -- Calculated Expected Closing
    inv.expected_closing_bbl,
    -- Formula: expected_closing_bbl = opening_balance_bbl + receipts_bbl - withdrawals_bbl
    
    -- Variance Calculation
    inv.variance_bbl,
    -- Formula: variance_bbl = closing_balance_bbl - expected_closing_bbl
    
    inv.variance_pct,
    -- Formula: variance_pct = (variance_bbl / expected_closing_bbl) * 100
    
    inv.variance_flag,
    -- Formula: variance_flag = CASE WHEN ABS(variance_pct) > 0.3 THEN TRUE ELSE FALSE END
    
    -- Additional Metrics
    inv.temperature_f,
    dt.capacity_bbl,
    (inv.closing_balance_bbl / dt.capacity_bbl) * 100 AS fill_percentage,
    dt.operational_status
    
FROM fact_tank_inventory inv
JOIN dim_tank dt ON inv.tank_id = dt.tank_id
JOIN dim_product dp ON inv.product_id = dp.product_id
JOIN dim_date dd ON inv.date_key = dd.date_key
ORDER BY 
    inv.date_key DESC,
    dt.product_type,
    dt.tank_code;

-- ===================================================================
-- VARIANCE INVESTIGATION REPORT
-- ===================================================================
-- Lists all tanks with variance exceeding threshold (>0.3%)
-- Ordered by absolute variance percentage (highest first)
-- ===================================================================

CREATE OR REPLACE VIEW vw_variance_investigation AS
SELECT 
    dd.full_date AS investigation_date,
    dt.tank_code,
    dt.tank_name,
    dt.product_type,
    dp.product_name,
    dt.location,
    
    -- Key Variance Metrics
    inv.expected_closing_bbl,
    inv.closing_balance_bbl,
    inv.variance_bbl,
    inv.variance_pct,
    
    -- Context for Investigation
    inv.opening_balance_bbl,
    inv.receipts_bbl,
    inv.withdrawals_bbl,
    
    -- Classification
    CASE 
        WHEN inv.variance_pct > 0.5 THEN 'Critical - Immediate Investigation'
        WHEN inv.variance_pct > 0.3 THEN 'High - Investigation Required'
        WHEN inv.variance_pct < -0.5 THEN 'Critical - Loss Investigation'
        WHEN inv.variance_pct < -0.3 THEN 'High - Loss Investigation'
        ELSE 'Normal'
    END AS investigation_priority,
    
    -- Potential Causes
    CASE 
        WHEN ABS(inv.variance_bbl) > 5000 THEN 'Large Volume - Check Meter Calibration'
        WHEN inv.variance_pct > 0 THEN 'Gain - Check for Temperature Effects or Receipts'
        WHEN inv.variance_pct < 0 THEN 'Loss - Check for Leaks or Unrecorded Withdrawals'
        ELSE 'Within Tolerance'
    END AS suggested_cause
    
FROM fact_tank_inventory inv
JOIN dim_tank dt ON inv.tank_id = dt.tank_id
JOIN dim_product dp ON inv.product_id = dp.product_id
JOIN dim_date dd ON inv.date_key = dd.date_key
WHERE inv.variance_flag = TRUE
ORDER BY 
    ABS(inv.variance_pct) DESC,
    dd.full_date DESC;

-- ===================================================================
-- PRODUCT AVAILABILITY CHECK
-- ===================================================================
-- Verifies sufficient inventory before allowing shipments
-- Formula: Available = Opening_Balance + Expected_Receipts
-- Validates: Available_Inventory >= Requested_Shipment_Volume
-- ===================================================================

CREATE OR REPLACE VIEW vw_product_availability AS
SELECT 
    dt.tank_code,
    dt.tank_name,
    dt.product_type,
    dp.product_name,
    dt.location,
    
    -- Current Inventory Position
    inv.closing_balance_bbl AS current_inventory_bbl,
    dt.capacity_bbl,
    (inv.closing_balance_bbl / dt.capacity_bbl) * 100 AS current_fill_pct,
    
    -- Available for Shipment
    inv.closing_balance_bbl - (dt.capacity_bbl * 0.05) AS available_for_shipment_bbl,
    -- Note: Keep 5% heel/safety stock
    
    -- Today's Activity
    inv.receipts_bbl AS todays_receipts_bbl,
    inv.withdrawals_bbl AS todays_withdrawals_bbl,
    
    -- Shipment Capacity Assessment
    CASE 
        WHEN (inv.closing_balance_bbl - (dt.capacity_bbl * 0.05)) > 50000 
            THEN 'High - Marine/Large Pipeline'
        WHEN (inv.closing_balance_bbl - (dt.capacity_bbl * 0.05)) > 10000 
            THEN 'Medium - Rail/Pipeline'
        WHEN (inv.closing_balance_bbl - (dt.capacity_bbl * 0.05)) > 1000 
            THEN 'Low - Truck Only'
        ELSE 'Critical - No Shipments'
    END AS shipment_capability,
    
    -- Operational Status
    dt.operational_status,
    
    -- Latest Date
    dd.full_date AS as_of_date
    
FROM fact_tank_inventory inv
JOIN dim_tank dt ON inv.tank_id = dt.tank_id
JOIN dim_product dp ON inv.product_id = dp.product_id
JOIN dim_date dd ON inv.date_key = dd.date_key
WHERE dd.date_key = (SELECT MAX(date_key) FROM fact_tank_inventory)
  AND dt.operational_status = 'Active'
ORDER BY 
    dt.product_type,
    available_for_shipment_bbl DESC;

-- ===================================================================
-- INVENTORY BALANCE BY PRODUCT
-- ===================================================================
-- Aggregates inventory across all tanks for each product type
-- Provides enterprise-wide product availability view
-- ===================================================================

CREATE OR REPLACE VIEW vw_inventory_balance_by_product AS
SELECT 
    dd.full_date AS report_date,
    dt.product_type,
    dp.product_category,
    dt.location,
    
    -- Tank Count
    COUNT(DISTINCT dt.tank_id) AS number_of_tanks,
    
    -- Aggregate Inventory
    SUM(inv.opening_balance_bbl) AS total_opening_balance_bbl,
    SUM(inv.receipts_bbl) AS total_receipts_bbl,
    SUM(inv.withdrawals_bbl) AS total_withdrawals_bbl,
    SUM(inv.closing_balance_bbl) AS total_closing_balance_bbl,
    
    -- Capacity Utilization
    SUM(dt.capacity_bbl) AS total_capacity_bbl,
    (SUM(inv.closing_balance_bbl) / SUM(dt.capacity_bbl)) * 100 AS avg_fill_percentage,
    
    -- Days of Supply (assuming constant withdrawal rate)
    CASE 
        WHEN SUM(inv.withdrawals_bbl) > 0 
            THEN SUM(inv.closing_balance_bbl) / SUM(inv.withdrawals_bbl)
        ELSE NULL
    END AS days_of_supply,
    
    -- Variance Summary
    SUM(inv.variance_bbl) AS total_variance_bbl,
    AVG(inv.variance_pct) AS avg_variance_pct,
    SUM(CASE WHEN inv.variance_flag = TRUE THEN 1 ELSE 0 END) AS tanks_with_variance_flags
    
FROM fact_tank_inventory inv
JOIN dim_tank dt ON inv.tank_id = dt.tank_id
JOIN dim_product dp ON inv.product_id = dp.product_id
JOIN dim_date dd ON inv.date_key = dd.date_key
GROUP BY 
    dd.full_date,
    dt.product_type,
    dp.product_category,
    dt.location
ORDER BY 
    dd.full_date DESC,
    dt.product_type,
    dt.location;

-- ===================================================================
-- SHIPMENT VOLUME VALIDATION
-- ===================================================================
-- Validates that shipment withdrawals match inventory withdrawals
-- Ensures data consistency between shipments and inventory
-- ===================================================================

CREATE OR REPLACE VIEW vw_shipment_inventory_validation AS
SELECT 
    dd.full_date,
    dt.tank_code,
    dt.product_type,
    
    -- Inventory Withdrawals
    inv.withdrawals_bbl AS inventory_withdrawals_bbl,
    
    -- Shipment Volumes (summed for the day)
    COALESCE(ship.total_shipments_bbl, 0) AS shipment_volumes_bbl,
    
    -- Variance
    inv.withdrawals_bbl - COALESCE(ship.total_shipments_bbl, 0) AS withdrawal_variance_bbl,
    
    -- Match Status
    CASE 
        WHEN ABS(inv.withdrawals_bbl - COALESCE(ship.total_shipments_bbl, 0)) < 10 
            THEN 'Match'
        WHEN inv.withdrawals_bbl > COALESCE(ship.total_shipments_bbl, 0) 
            THEN 'Unrecorded Shipments'
        ELSE 'Shipment Overstatement'
    END AS reconciliation_status,
    
    -- Shipment Details
    ship.shipment_count,
    ship.shipment_modes
    
FROM fact_tank_inventory inv
JOIN dim_tank dt ON inv.tank_id = dt.tank_id
JOIN dim_date dd ON inv.date_key = dd.date_key
LEFT JOIN (
    SELECT 
        date_key,
        tank_id,
        SUM(shipment_volume_bbl) AS total_shipments_bbl,
        COUNT(*) AS shipment_count,
        STRING_AGG(DISTINCT shipment_mode, ', ') AS shipment_modes
    FROM fact_product_shipments
    GROUP BY date_key, tank_id
) ship ON inv.date_key = ship.date_key AND inv.tank_id = ship.tank_id
WHERE dd.date_key >= DATEADD(day, -7, CURRENT_DATE)
ORDER BY 
    dd.full_date DESC,
    dt.tank_code;

-- ===================================================================
-- TANK UTILIZATION TREND
-- ===================================================================
-- Tracks tank fill levels over time to identify trends
-- Useful for capacity planning and operational optimization
-- ===================================================================

CREATE OR REPLACE VIEW vw_tank_utilization_trend AS
SELECT 
    dd.full_date,
    dd.week,
    dd.month,
    dt.tank_code,
    dt.product_type,
    dt.location,
    
    -- Daily Position
    inv.closing_balance_bbl,
    dt.capacity_bbl,
    (inv.closing_balance_bbl / dt.capacity_bbl) * 100 AS fill_percentage,
    
    -- Activity Metrics
    inv.receipts_bbl,
    inv.withdrawals_bbl,
    inv.receipts_bbl - inv.withdrawals_bbl AS net_change_bbl,
    
    -- 7-Day Moving Average
    AVG(inv.closing_balance_bbl) OVER (
        PARTITION BY dt.tank_id 
        ORDER BY dd.date_key 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS ma_7day_balance_bbl,
    
    -- Utilization Status
    CASE 
        WHEN (inv.closing_balance_bbl / dt.capacity_bbl) > 0.90 THEN 'Very High - Consider Shipments'
        WHEN (inv.closing_balance_bbl / dt.capacity_bbl) > 0.75 THEN 'High - Normal'
        WHEN (inv.closing_balance_bbl / dt.capacity_bbl) > 0.50 THEN 'Medium - Normal'
        WHEN (inv.closing_balance_bbl / dt.capacity_bbl) > 0.25 THEN 'Low - Monitor'
        ELSE 'Critical - Refill Required'
    END AS utilization_status
    
FROM fact_tank_inventory inv
JOIN dim_tank dt ON inv.tank_id = dt.tank_id
JOIN dim_date dd ON inv.date_key = dd.date_key
ORDER BY 
    dd.date_key DESC,
    dt.tank_code;

-- ===================================================================
-- VALIDATION QUERIES
-- ===================================================================

-- Verify inventory equation holds for all records
-- Expected_Closing = Opening + Receipts - Withdrawals
-- This should return 0 rows (all match)
SELECT 
    inventory_id,
    opening_balance_bbl,
    receipts_bbl,
    withdrawals_bbl,
    expected_closing_bbl,
    opening_balance_bbl + receipts_bbl - withdrawals_bbl AS calculated_expected,
    expected_closing_bbl - (opening_balance_bbl + receipts_bbl - withdrawals_bbl) AS difference
FROM fact_tank_inventory
WHERE ABS(expected_closing_bbl - (opening_balance_bbl + receipts_bbl - withdrawals_bbl)) > 0.01;

-- Verify variance calculation
-- Variance = Actual_Closing - Expected_Closing
-- This should return 0 rows (all match)
SELECT 
    inventory_id,
    closing_balance_bbl,
    expected_closing_bbl,
    variance_bbl,
    closing_balance_bbl - expected_closing_bbl AS calculated_variance,
    variance_bbl - (closing_balance_bbl - expected_closing_bbl) AS difference
FROM fact_tank_inventory
WHERE ABS(variance_bbl - (closing_balance_bbl - expected_closing_bbl)) > 0.01;

-- Verify variance percentage calculation
-- Variance_Pct = (Variance / Expected_Closing) × 100
-- This should return 0 rows (all match)
SELECT 
    inventory_id,
    variance_bbl,
    expected_closing_bbl,
    variance_pct,
    (variance_bbl / expected_closing_bbl) * 100 AS calculated_variance_pct,
    variance_pct - ((variance_bbl / expected_closing_bbl) * 100) AS difference
FROM fact_tank_inventory
WHERE expected_closing_bbl > 0
  AND ABS(variance_pct - ((variance_bbl / expected_closing_bbl) * 100)) > 0.01;

-- Verify variance flag logic
-- Variance_Flag = TRUE if |Variance_Pct| > 0.3%
SELECT 
    inventory_id,
    variance_pct,
    variance_flag,
    CASE WHEN ABS(variance_pct) > 0.3 THEN TRUE ELSE FALSE END AS expected_flag
FROM fact_tank_inventory
WHERE variance_flag != (CASE WHEN ABS(variance_pct) > 0.3 THEN TRUE ELSE FALSE END);
