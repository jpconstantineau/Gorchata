-- ==============================================================================
-- Yield Calculations - Phase 4
-- ==============================================================================
--
-- This transformation file implements yield calculations for refinery unit
-- production with volume and weight accounting. It demonstrates:
--
-- 1. Volume Yield Calculation (can exceed 100% for cracking units)
-- 2. Weight Yield Calculation (always < 100% due to losses)
-- 3. Conversion Percentage for upgrading units (FCC, Hydrocracker)
-- 4. Yield Sum Validation (physical constraint checks)
-- 5. Material Balance Verification
--
-- ==============================================================================

-- ==============================================================================
-- SECTION 1: Basic Yield Calculations
-- ==============================================================================

/*
Volume Yield Percentage
-----------------------
Formula: Yield % Volume = (Product Volume / Feed Volume) × 100

Physical Principles:
- For separation units (CDU, VDU): ~100% (conservation of volume)
- For cracking units (FCC): 105-110% (volumetric expansion due to lighter products)
- For reforming: 90-93% (volume loss due to H2 production)
- For hydrotreating: 98-99% (minimal volume change)

Example Calculation (FCC):
  Feed Volume: 45,000 bbl
  FCC Gasoline Product: 22,500 bbl
  Volume Yield % = (22,500 / 45,000) × 100 = 50.0%
*/

SELECT 
    production_id,
    date_key,
    unit_id,
    product_id,
    feed_volume_bbl,
    product_volume_bbl,
    -- Volume Yield Calculation
    ROUND((product_volume_bbl / NULLIF(feed_volume_bbl, 0)) * 100, 2) as calculated_yield_pct_volume,
    yield_pct_volume as stored_yield_pct_volume,
    -- Validation: Check calculation matches stored value
    CASE 
        WHEN ABS(ROUND((product_volume_bbl / NULLIF(feed_volume_bbl, 0)) * 100, 2) - yield_pct_volume) > 0.1
        THEN 'MISMATCH'
        ELSE 'OK'
    END as volume_yield_validation
FROM fact_unit_production
WHERE feed_volume_bbl > 0
ORDER BY date_key, unit_id, product_id;

/*
Weight Yield Percentage
-----------------------
Formula: Yield % Weight = (Product Weight / Feed Weight) × 100

Physical Principles:
- Weight is always conserved (Law of Conservation of Mass)
- Total weight yield sum must be < 100% due to losses
- Losses include: coke formation, gas losses, light ends
- Typical ranges:
  * CDU/VDU: 98-99% (minimal losses)
  * FCC: 96-97% (coke formation 4-5%)
  * Hydrocracker: 98-99% (minimal losses)
  * Reformer: 88-90% (H2 production)

Example Calculation (FCC):
  Feed Weight: 5,212 tons
  FCC Gasoline Product: 2,085 tons
  Weight Yield % = (2,085 / 5,212) × 100 = 40.0%
*/

SELECT 
    production_id,
    date_key,
    unit_id,
    product_id,
    feed_weight_tons,
    product_weight_tons,
    -- Weight Yield Calculation
    ROUND((product_weight_tons / NULLIF(feed_weight_tons, 0)) * 100, 2) as calculated_yield_pct_weight,
    yield_pct_weight as stored_yield_pct_weight,
    -- Validation: Check calculation matches stored value
    CASE 
        WHEN ABS(ROUND((product_weight_tons / NULLIF(feed_weight_tons, 0)) * 100, 2) - yield_pct_weight) > 0.1
        THEN 'MISMATCH'
        ELSE 'OK'
    END as weight_yield_validation
FROM fact_unit_production
WHERE feed_weight_tons > 0
ORDER BY date_key, unit_id, product_id;

-- ==============================================================================
-- SECTION 2: Yield Sum Validation by Unit
-- ==============================================================================

/*
Total Yield Validation
----------------------
Physical Constraints:
- Volume Yield Sum: 95-110% (can exceed 100% for FCC due to volumetric expansion)
- Weight Yield Sum: 95-99% (always < 100% due to physical losses)

This query aggregates all product yields for each unit operation to verify
physical constraints are met.
*/

SELECT 
    date_key,
    unit_id,
    COUNT(*) as product_count,
    -- Volume Yield Sum
    ROUND(SUM(yield_pct_volume), 2) as total_volume_yield_pct,
    CASE 
        WHEN SUM(yield_pct_volume) < 95.0 THEN 'LOW - Check for missing products'
        WHEN SUM(yield_pct_volume) > 110.0 THEN 'HIGH - Exceeds physical limits'
        ELSE 'OK'
    END as volume_yield_validation,
    -- Weight Yield Sum
    ROUND(SUM(yield_pct_weight), 2) as total_weight_yield_pct,
    CASE 
        WHEN SUM(yield_pct_weight) < 95.0 THEN 'LOW - Excessive losses'
        WHEN SUM(yield_pct_weight) > 100.0 THEN 'INVALID - Violates conservation of mass'
        ELSE 'OK'
    END as weight_yield_validation,
    -- Material Loss
    ROUND(100.0 - SUM(yield_pct_weight), 2) as total_loss_pct
FROM fact_unit_production
GROUP BY date_key, unit_id
ORDER BY date_key, unit_id;

-- ==============================================================================
-- SECTION 3: Unit-Specific Yield Pattern Validation
-- ==============================================================================

/*
Unit Type Expected Yield Ranges
--------------------------------
Different unit types have characteristic yield patterns based on their chemistry.
*/

-- CDU (Crude Distillation Unit) Validation
-- Expected: 7 product streams, volume ~100%, weight 98-99%
SELECT 
    'CDU' as unit_type,
    date_key,
    unit_id,
    COUNT(*) as product_streams,
    ROUND(SUM(yield_pct_volume), 2) as total_volume_yield,
    ROUND(SUM(yield_pct_weight), 2) as total_weight_yield,
    CASE 
        WHEN COUNT(*) < 7 THEN 'MISSING PRODUCTS'
        WHEN SUM(yield_pct_volume) NOT BETWEEN 98.0 AND 102.0 THEN 'VOLUME OUT OF RANGE'
        WHEN SUM(yield_pct_weight) NOT BETWEEN 98.0 AND 99.5 THEN 'WEIGHT OUT OF RANGE'
        ELSE 'OK'
    END as validation_status
FROM fact_unit_production
WHERE unit_id LIKE 'CDU%'
GROUP BY date_key, unit_id
ORDER BY date_key;

-- FCC (Fluid Catalytic Cracker) Validation
-- Expected: 5-6 product streams, volume 95-98% (excluding coke), weight 96-97%
-- NOTE: Volume can appear > 100% when considering density changes
SELECT 
    'FCC' as unit_type,
    date_key,
    unit_id,
    COUNT(*) as product_streams,
    ROUND(SUM(yield_pct_volume), 2) as total_volume_yield,
    ROUND(SUM(yield_pct_weight), 2) as total_weight_yield,
    CASE 
        WHEN COUNT(*) < 5 THEN 'MISSING PRODUCTS'
        WHEN SUM(yield_pct_volume) NOT BETWEEN 90.0 AND 110.0 THEN 'VOLUME OUT OF RANGE'
        WHEN SUM(yield_pct_weight) NOT BETWEEN 95.0 AND 98.0 THEN 'WEIGHT OUT OF RANGE'
        ELSE 'OK'
    END as validation_status
FROM fact_unit_production
WHERE unit_id LIKE 'FCC%'
GROUP BY date_key, unit_id
ORDER BY date_key;

-- Hydrocracker Validation
-- Expected: 5 product streams, volume 100-103%, weight 98-99%
SELECT 
    'Hydrocracker' as unit_type,
    date_key,
    unit_id,
    COUNT(*) as product_streams,
    ROUND(SUM(yield_pct_volume), 2) as total_volume_yield,
    ROUND(SUM(yield_pct_weight), 2) as total_weight_yield,
    CASE 
        WHEN COUNT(*) < 5 THEN 'MISSING PRODUCTS'
        WHEN SUM(yield_pct_volume) NOT BETWEEN 99.0 AND 103.0 THEN 'VOLUME OUT OF RANGE'
        WHEN SUM(yield_pct_weight) NOT BETWEEN 97.0 AND 99.5 THEN 'WEIGHT OUT OF RANGE'
        ELSE 'OK'
    END as validation_status
FROM fact_unit_production
WHERE unit_id LIKE 'HCU%'
GROUP BY date_key, unit_id
ORDER BY date_key;

-- Reformer Validation
-- Expected: 2-3 product streams, volume 90-93%, weight 88-92% (H2 loss)
SELECT 
    'Reformer' as unit_type,
    date_key,
    unit_id,
    COUNT(*) as product_streams,
    ROUND(SUM(yield_pct_volume), 2) as total_volume_yield,
    ROUND(SUM(yield_pct_weight), 2) as total_weight_yield,
    CASE 
        WHEN COUNT(*) < 2 THEN 'MISSING PRODUCTS'
        WHEN SUM(yield_pct_volume) NOT BETWEEN 88.0 AND 94.0 THEN 'VOLUME OUT OF RANGE'
        WHEN SUM(yield_pct_weight) NOT BETWEEN 87.0 AND 93.0 THEN 'WEIGHT OUT OF RANGE'
        ELSE 'OK'
    END as validation_status
FROM fact_unit_production
WHERE unit_id LIKE 'REF%'
GROUP BY date_key, unit_id
ORDER BY date_key;

-- ==============================================================================
-- SECTION 4: Conversion Percentage Calculation
-- ==============================================================================

/*
Conversion Percentage
---------------------
For upgrading units (FCC, Hydrocracker), conversion measures the fraction of
heavy feed converted to lighter products.

Formula: Conversion % = (Light Products < Cutpoint / Total Feed) × 100

Typical Cutpoints:
- FCC: 430°F (products lighter than LCO)
- Hydrocracker: 650°F (products lighter than unconverted oil)

Example (FCC with 45,000 bbl feed):
  Dry Gas:      1,800 bbl (4.0%)
  LPG:          7,650 bbl (17.0%)
  Gasoline:    22,500 bbl (50.0%)
  Total Light: 31,950 bbl
  Conversion = (31,950 / 45,000) × 100 = 71.0%
*/

-- FCC Conversion Calculation
-- Light products: Dry Gas + LPG + Gasoline (< 430°F)
WITH fcc_light_products AS (
    SELECT 
        date_key,
        unit_id,
        feed_volume_bbl,
        SUM(CASE 
            WHEN product_id IN ('PROD-FGAS', 'PROD-LPG', 'PROD-FCCGAS') 
            THEN product_volume_bbl 
            ELSE 0 
        END) as light_product_volume
    FROM fact_unit_production
    WHERE unit_id LIKE 'FCC%'
    GROUP BY date_key, unit_id, feed_volume_bbl
)
SELECT 
    date_key,
    unit_id,
    feed_volume_bbl,
    light_product_volume,
    ROUND((light_product_volume / NULLIF(feed_volume_bbl, 0)) * 100, 2) as conversion_pct,
    CASE 
        WHEN (light_product_volume / NULLIF(feed_volume_bbl, 0)) * 100 < 65.0 THEN 'LOW CONVERSION'
        WHEN (light_product_volume / NULLIF(feed_volume_bbl, 0)) * 100 > 80.0 THEN 'HIGH CONVERSION'
        ELSE 'NORMAL'
    END as conversion_category
FROM fcc_light_products
ORDER BY date_key;

-- Hydrocracker Conversion Calculation
-- Light products: Gas + LPG + Naphtha + Diesel (excluding unconverted oil)
WITH hcu_light_products AS (
    SELECT 
        date_key,
        unit_id,
        feed_volume_bbl,
        SUM(CASE 
            WHEN product_id NOT IN ('PROD-UCO', 'PROD-HVGO') 
            THEN product_volume_bbl 
            ELSE 0 
        END) as light_product_volume
    FROM fact_unit_production
    WHERE unit_id LIKE 'HCU%'
    GROUP BY date_key, unit_id, feed_volume_bbl
)
SELECT 
    date_key,
    unit_id,
    feed_volume_bbl,
    light_product_volume,
    ROUND((light_product_volume / NULLIF(feed_volume_bbl, 0)) * 100, 2) as conversion_pct,
    CASE 
        WHEN (light_product_volume / NULLIF(feed_volume_bbl, 0)) * 100 < 85.0 THEN 'LOW CONVERSION'
        WHEN (light_product_volume / NULLIF(feed_volume_bbl, 0)) * 100 > 95.0 THEN 'HIGH CONVERSION'
        ELSE 'NORMAL'
    END as conversion_category
FROM hcu_light_products
ORDER BY date_key;

-- ==============================================================================
-- SECTION 5: Product Quality Tracking
-- ==============================================================================

/*
Product Quality Metrics
-----------------------
Track product quality properties across units to ensure specifications are met.
*/

-- Gasoline Blending Pool - Octane and Sulfur
SELECT 
    date_key,
    unit_id,
    product_id,
    product_volume_bbl,
    product_octane_ron,
    product_sulfur_ppm,
    CASE 
        WHEN product_octane_ron < 87.0 THEN 'BELOW SPEC'
        WHEN product_octane_ron >= 91.0 THEN 'PREMIUM'
        ELSE 'REGULAR'
    END as gasoline_grade,
    CASE 
        WHEN product_sulfur_ppm > 30 THEN 'HIGH SULFUR - Needs Treatment'
        ELSE 'ON SPEC'
    END as sulfur_status
FROM fact_unit_production
WHERE product_octane_ron IS NOT NULL
ORDER BY date_key, product_octane_ron DESC;

-- Diesel Pool - Cetane and Sulfur
SELECT 
    date_key,
    unit_id,
    product_id,
    product_volume_bbl,
    product_cetane,
    product_sulfur_ppm,
    CASE 
        WHEN product_cetane < 40 THEN 'BELOW SPEC'
        WHEN product_cetane >= 50 THEN 'PREMIUM DIESEL'
        ELSE 'REGULAR DIESEL'
    END as diesel_grade,
    CASE 
        WHEN product_sulfur_ppm > 15 THEN 'HIGH SULFUR - Needs Hydrotreating'
        ELSE 'ULSD'
    END as sulfur_category
FROM fact_unit_production
WHERE product_cetane IS NOT NULL
ORDER BY date_key, product_cetane DESC;

-- ==============================================================================
-- SECTION 6: Volumetric Expansion Analysis (FCC Specific)
-- ==============================================================================

/*
FCC Volumetric Expansion
------------------------
FCC units can show apparent volume yields > 100% due to production of lighter,
less dense products from heavier feed. This is physically correct and reflects
the density difference between feed and products.

Example:
  Feed: 1,000 bbl @ 0.93 SG = 930 tons
  Products: 1,050 bbl @ 0.75 avg SG = 788 tons
  Volume Yield: 105% (apparent expansion)
  Weight Yield: 84.7% (actual loss due to coke/gas)
*/

SELECT 
    date_key,
    unit_id,
    -- Calculate weighted average product density
    SUM(product_volume_bbl) as total_product_volume,
    feed_volume_bbl as total_feed_volume,
    ROUND(SUM(product_volume_bbl) / NULLIF(feed_volume_bbl, 0) * 100, 2) as apparent_volume_yield,
    SUM(product_weight_tons) as total_product_weight,
    feed_weight_tons as total_feed_weight,
    ROUND(SUM(product_weight_tons) / NULLIF(feed_weight_tons, 0) * 100, 2) as actual_weight_yield,
    -- Calculate density change
    ROUND((feed_weight_tons / NULLIF(feed_volume_bbl, 0)) / 0.1364, 3) as feed_sg,
    ROUND((SUM(product_weight_tons) / NULLIF(SUM(product_volume_bbl), 0)) / 0.1364, 3) as product_avg_sg,
    CASE 
        WHEN SUM(product_volume_bbl) / NULLIF(feed_volume_bbl, 0) > 1.0 
        THEN 'VOLUMETRIC EXPANSION - Normal for FCC'
        ELSE 'VOLUMETRIC CONTRACTION'
    END as volume_behavior
FROM fact_unit_production
WHERE unit_id LIKE 'FCC%'
GROUP BY date_key, unit_id, feed_volume_bbl, feed_weight_tons
ORDER BY date_key;

-- ==============================================================================
-- SECTION 7: Material Balance Summary Report
-- ==============================================================================

/*
Overall Material Balance
------------------------
Comprehensive summary showing total input vs output for the refinery complex.
*/

SELECT 
    date_key,
    'TOTAL REFINERY' as scope,
    -- Volume Balance
    SUM(DISTINCT feed_volume_bbl) as total_feed_volume_bbl,
    SUM(product_volume_bbl) as total_product_volume_bbl,
    ROUND(SUM(product_volume_bbl) / NULLIF(SUM(DISTINCT feed_volume_bbl), 0) * 100, 2) as overall_volume_yield_pct,
    -- Weight Balance
    SUM(DISTINCT feed_weight_tons) as total_feed_weight_tons,
    SUM(product_weight_tons) as total_product_weight_tons,
    ROUND(SUM(product_weight_tons) / NULLIF(SUM(DISTINCT feed_weight_tons), 0) * 100, 2) as overall_weight_yield_pct,
    -- Loss Accounting
    ROUND(SUM(DISTINCT feed_weight_tons) - SUM(product_weight_tons), 2) as total_loss_tons,
    ROUND((SUM(DISTINCT feed_weight_tons) - SUM(product_weight_tons)) / NULLIF(SUM(DISTINCT feed_weight_tons), 0) * 100, 2) as loss_pct
FROM fact_unit_production
GROUP BY date_key
ORDER BY date_key;
