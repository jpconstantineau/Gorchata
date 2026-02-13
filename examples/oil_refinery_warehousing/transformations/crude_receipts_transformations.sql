-- ==============================================================================
-- Crude Receipts Transformations
-- ==============================================================================
--
-- This file contains SQL transformations for crude oil receipt data, including:
-- - API gravity to specific gravity conversions
-- - Volume to weight calculations
-- - BS&W (Basic Sediment & Water) deductions
-- - Temperature corrections to standard 60°F
--
-- References:
-- - API MPMS Chapter 11.1: Volume Correction Factors
-- - docs/MEASUREMENT_STANDARDS.md
-- ==============================================================================

-- ==============================================================================
-- TRANSFORMATION: stg_crude_receipts → fact_crude_receipts
-- ==============================================================================
-- 
-- This transformation applies petroleum measurement calculations to convert
-- raw crude receipt data into standardized fact records with calculated metrics.
-- ==============================================================================

INSERT INTO fact_crude_receipts (
    receipt_id,
    date_key,
    crude_grade_id,
    source_location_id,
    receipt_mode,
    gross_volume_bbl,
    observed_temperature_f,
    observed_api_gravity,
    bsw_pct,
    api_gravity_60f,
    net_volume_bbl,
    specific_gravity_60f,
    weight_short_tons,
    sulfur_wt_pct
)
SELECT
    receipt_id,
    date_key,
    crude_grade_id,
    source_location_id,
    receipt_mode,
    gross_volume_bbl,
    observed_temperature_f,
    observed_api_gravity,
    bsw_pct,
    
    -- ==================================================================
    -- API Gravity Temperature Correction
    -- ==================================================================
    -- For simplicity, using observed API as corrected API in this example.
    -- In production, apply ASTM D1250 temperature correction tables.
    -- Typical correction: ±0.0001 API per °F from 60°F baseline
    -- ==================================================================
    CASE 
        WHEN api_gravity_60f IS NOT NULL THEN api_gravity_60f
        ELSE observed_api_gravity
    END AS api_gravity_60f,
    
    -- ==================================================================
    -- Net Volume Calculation (BS&W Deduction)
    -- ==================================================================
    -- Formula: Net Volume = Gross Volume × (1 - BSW% / 100)
    -- 
    -- Basic Sediment and Water (BS&W) is impurities measured as percentage
    -- of total volume. Must be deducted to determine saleable crude volume.
    -- 
    -- Typical ranges:
    -- - Pipeline crude: 0.05% - 0.2%
    -- - Marine crude: 0.2% - 0.5%
    -- - High BS&W: > 0.5% (may require additional treatment)
    -- ==================================================================
    CASE
        WHEN net_volume_bbl IS NOT NULL THEN net_volume_bbl
        ELSE gross_volume_bbl * (1.0 - bsw_pct / 100.0)
    END AS net_volume_bbl,
    
    -- ==================================================================
    -- Specific Gravity Calculation
    -- ==================================================================
    -- Formula: SG = 141.5 / (API + 131.5)
    -- 
    -- Converts API gravity to specific gravity at 60°F/60°F
    -- (relative to water at 60°F)
    -- 
    -- API Gravity is the petroleum industry standard for density:
    -- - Higher API = Lighter crude (lower density)
    -- - Lower API = Heavier crude (higher density)
    -- 
    -- Typical ranges:
    -- - Light crude (WTI, Brent): API 38-40° → SG 0.83-0.84
    -- - Medium crude (Mars, Dubai): API 29-31° → SG 0.87-0.88
    -- - Heavy crude (Maya): API 22° → SG 0.92
    -- ==================================================================
    CASE
        WHEN specific_gravity_60f IS NOT NULL THEN specific_gravity_60f
        WHEN api_gravity_60f IS NOT NULL THEN 141.5 / (api_gravity_60f + 131.5)
        ELSE 141.5 / (observed_api_gravity + 131.5)
    END AS specific_gravity_60f,
    
    -- ==================================================================
    -- Weight Calculation (Volume to Mass Conversion)
    -- ==================================================================
    -- Formula: Weight (short tons) = Volume (bbl) × 0.1364 × SG
    -- 
    -- Derivation:
    -- - 1 barrel = 42 US gallons
    -- - Water at 60°F = 8.337 lb/gal
    -- - 1 short ton = 2000 pounds
    -- - Factor = (42 × 8.337) / 2000 = 0.175077
    -- - Simplified to 0.1364 for standard calculations
    -- 
    -- Example: 10,000 bbl of WTI (SG 0.827)
    -- Weight = 10,000 × 0.1364 × 0.827 = 1,128 short tons
    -- 
    -- Note: Heavier crude (higher SG) weighs more per barrel
    -- - 10k bbl WTI (SG 0.827) = 1,128 tons
    -- - 10k bbl Maya (SG 0.920) = 1,255 tons (11% heavier)
    -- ==================================================================
    CASE
        WHEN weight_short_tons IS NOT NULL THEN weight_short_tons
        WHEN net_volume_bbl IS NOT NULL AND specific_gravity_60f IS NOT NULL 
            THEN net_volume_bbl * 0.1364 * specific_gravity_60f
        WHEN net_volume_bbl IS NOT NULL AND api_gravity_60f IS NOT NULL
            THEN net_volume_bbl * 0.1364 * (141.5 / (api_gravity_60f + 131.5))
        ELSE (gross_volume_bbl * (1.0 - bsw_pct / 100.0)) * 0.1364 * 
             (141.5 / (observed_api_gravity + 131.5))
    END AS weight_short_tons,
    
    sulfur_wt_pct

FROM stg_crude_receipts;

-- ==============================================================================
-- NOTES ON TEMPERATURE CORRECTION
-- ==============================================================================
-- 
-- In this simplified transformation, we assume temperature corrections have been
-- pre-applied or API gravity at 60°F is provided directly. 
--
-- In production systems, implement Volume Correction Factor (VCF) lookup tables
-- based on API MPMS Chapter 11.1 (formerly ASTM D1250):
-- 
-- Volume at 60°F = Volume observed × VCF(T, API)
-- 
-- Simplified approximation (NOT for custody transfer):
-- VCF ≈ 1 + α × (60 - T)
-- where α ≈ 0.0004 to 0.0007 per °F depending on API gravity
-- 
-- Effect of temperature on volume:
-- - Hot crude (85°F): Volume expands ~1% above standard
-- - Cold crude (40°F): Volume contracts ~0.8% below standard
-- - Standard (60°F): No correction needed
-- 
-- ==============================================================================

-- ==============================================================================
-- QUALITY CHECKS (Can be implemented as separate tests)
-- ==============================================================================
-- 
-- 1. Volume Balance Check:
--    Net Volume should be slightly less than Gross Volume (BS&W deduction)
--    Expected: Net = Gross × (1 - BSW% / 100)
-- 
-- 2. Density Reasonableness:
--    - API gravity: 10-50° (typical refinery range)
--    - Specific gravity: 0.70-1.00 (petroleum products)
--    - Light crude: SG < 0.87 (API > 31°)
--    - Heavy crude: SG > 0.92 (API < 22°)
-- 
-- 3. Weight Calculation Verification:
--    For 10,000 bbl at SG 0.85:
--    Weight = 10,000 × 0.1364 × 0.85 = 1,159 tons
--    Should be in reasonable range (not 10× or 0.1× expected value)
-- 
-- 4. BS&W Reasonableness:
--    - Typical range: 0.05% - 0.5%
--    - Flag for review: > 1.0%
--    - Rejection criteria: > 2.0%
-- 
-- ==============================================================================
