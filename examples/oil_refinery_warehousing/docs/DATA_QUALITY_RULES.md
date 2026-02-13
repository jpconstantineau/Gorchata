# Data Quality Rules and Anomaly Detection

## Overview

This document defines the comprehensive data quality validation framework for the Oil Refinery Data Warehousing project. The framework implements multiple categories of validation rules designed to detect measurement errors, equipment malfunction, data entry issues, and operational anomalies.

**Purpose:**
- Ensure data accuracy and reliability for decision-making
- Detect anomalies early to enable corrective action
- Maintain compliance with industry standards and specifications
- Build trust in analytical results and KPIs

**Coverage:**
- 6 rule categories
- 10+ specific validation rules
- Multi-layer detection (range, statistical, consistency, seasonal, trend)
- Automated daily execution

---

## Rule Categories

### 1. Range Validations
Validates that measurements fall within industry-standard acceptable ranges.

### 2. Statistical Outlier Detection
Uses Z-score methodology to identify statistically significant deviations.

### 3. Cross-Attribute Consistency
Validates consistency between related measurements using physical relationships.

### 4. Seasonal Compliance
Ensures products meet season-specific regulatory specifications.

### 5. Trend Analysis
Detects deviations from recent historical patterns using moving averages.

### 6. Business Logic Validation
Validates domain-specific business rules and constraints.

---

## Detailed Rule Definitions

### CATEGORY 1: RANGE VALIDATIONS

These checks validate that measured values fall within industry-standard acceptable ranges. Violations typically indicate measurement errors, equipment malfunction, or data entry issues.

#### RULE 1.1: API Gravity Range Check

**Rule Code:** `RANGE_API_GRAVITY`  
**Target:** `fact_crude_receipts.api_gravity_60f`  
**Threshold:** 5.0° - 50.0° API  
**Severity:** Critical

**Range Interpretation:**
- **Light Crude:** 35-45° API
- **Medium Crude:** 25-35° API
- **Heavy Crude:** 10-25° API
- **Extra Heavy:** 5-10° API

**Validation Logic:**
```sql
CASE 
    WHEN api_gravity_60f BETWEEN 5.0 AND 50.0 THEN 'Pass'
    ELSE 'Fail'
END
```

**Investigation Steps:**
1. Verify laboratory analysis methods (ASTM D287)
2. Check hydrometer calibration and temperature correction
3. Confirm crude grade assignment is correct
4. Review sample collection procedures

**Common Causes of Failure:**
- Uncalibrated hydrometer
- Incorrect temperature correction
- Sample contamination
- Data entry error (decimal point)

---

#### RULE 1.2: Sulfur Content Range Check

**Rule Code:** `RANGE_SULFUR_CONTENT`  
**Target:** `fact_crude_receipts.sulfur_wt_pct`  
**Threshold:** 0.01% - 7.0% by weight  
**Severity:** Critical

**Range Interpretation:**
- **Sweet Crude:** < 0.5% sulfur
- **Intermediate:** 0.5% - 1.0% sulfur
- **Sour Crude:** 1.0% - 2.0% sulfur
- **High Sour:** > 2.0% sulfur

**Validation Logic:**
```sql
CASE 
    WHEN sulfur_wt_pct BETWEEN 0.01 AND 7.0 THEN 'Pass'
    ELSE 'Fail'
END
```

**Investigation Steps:**
1. Verify sulfur analysis method (ASTM D4294 or D5453)
2. Check analyzer calibration with reference standards
3. Confirm crude source and typical sulfur content
4. Review sample preparation

**Common Causes of Failure:**
- Analyzer malfunction or drift
- Sample mix-up
- Detection limit issue (very low sulfur)
- Incorrect units conversion

---

#### RULE 1.3: Temperature Range Check

**Rule Code:** `RANGE_TEMPERATURE`  
**Target:** `fact_crude_receipts.observed_temperature_f`  
**Threshold:** 30°F - 150°F  
**Severity:** Warning

**Range Rationale:**
- **Below 30°F:** Unusual for crude storage/transfer (viscosity issues, potential freezing of water)
- **Above 150°F:** Exceeds typical operating temperature (safety, vapor loss concerns)

**Validation Logic:**
```sql
CASE 
    WHEN observed_temperature_f BETWEEN 30.0 AND 150.0 THEN 'Pass'
    ELSE 'Fail'
END
```

**Investigation Steps:**
1. Verify temperature sensor calibration
2. Check for environmental factors (winter/summer extremes)
3. Confirm heating system status if applicable
4. Review operator logs for unusual conditions

---

#### RULE 1.4: Capacity Utilization Range Check

**Rule Code:** `RANGE_CAPACITY_UTILIZATION`  
**Target:** `fact_unit_operations.capacity_utilization_pct`  
**Threshold:** 0% - 105%  
**Severity:** Warning

**Range Rationale:**
- **0-100%:** Normal operating range
- **100-105%:** Short-term excursion acceptable (debottlenecking, favorable conditions)
- **> 105%:** Likely calculation error or throughput measurement issue

**Validation Logic:**
```sql
CASE 
    WHEN capacity_utilization_pct BETWEEN 0.0 AND 105.0 THEN 'Pass'
    ELSE 'Fail'
END
```

**Investigation Steps:**
1. Verify throughput measurement accuracy
2. Check nameplate capacity vs. current capacity rating
3. Review recent unit modifications or debottlenecks
4. Confirm calculation formula: (Throughput / Capacity) × 100

---

#### RULE 1.5: BS&W Range Check

**Rule Code:** `RANGE_BSW`  
**Target:** `fact_crude_receipts.bsw_pct`  
**Threshold:** 0% - 2.0%  
**Severity:** Critical

**Range Rationale:**
- **0-0.5%:** Typical for pipeline crude
- **0.5-1.0%:** Acceptable for some marine receipts
- **1.0-2.0%:** Maximum acceptable (requires treatment)
- **> 2.0%:** Quality specification violation

**Validation Logic:**
```sql
CASE 
    WHEN bsw_pct BETWEEN 0.0 AND 2.0 THEN 'Pass'
    ELSE 'Fail'
END
```

**Investigation Steps:**
1. Verify BS&W test procedure (ASTM D4007)
2. Check crude desalting system performance
3. Review supplier quality certifications
4. Evaluate need for rejection or price adjustment

---

### CATEGORY 2: STATISTICAL OUTLIER DETECTION (Z-SCORE METHOD)

Uses the 3-sigma rule to identify statistically significant outliers in time-series data.

#### Statistical Foundation: Z-Score

**Formula:**
```
Z-score = (Observed Value - Mean) / Standard Deviation
```

**Interpretation:**
- **|Z| < 2.0:** Normal variation (within 95% confidence interval)
- **2.0 ≤ |Z| ≤ 3.0:** Warning zone (95-99.7% confidence interval)
- **|Z| > 3.0:** Statistical outlier (outside 99.7% confidence interval)

**Requirements:**
- Minimum 10 observations in rolling 30-day window
- Standard deviation > 0 (non-constant values)
- Exclude known planned shutdowns/turnarounds

---

#### RULE 2.1: Crude Receipt Volume Outlier Detection

**Rule Code:** `OUTLIER_CRUDE_VOLUME`  
**Target:** `fact_crude_receipts.net_volume_bbl`  
**Threshold:** |Z-score| > 3.0  
**Severity:** Warning

**Analysis Window:** Rolling 30 days by crude grade

**Validation Logic:**
```sql
WITH stats AS (
    SELECT crude_grade_id,
           AVG(net_volume_bbl) AS mean_volume,
           STDDEV(net_volume_bbl) AS stddev_volume
    FROM fact_crude_receipts
    WHERE date_key >= [Last 30 days]
    GROUP BY crude_grade_id
)
SELECT 
    (net_volume_bbl - mean_volume) / stddev_volume AS z_score,
    CASE 
        WHEN ABS(z_score) > 3.0 THEN 'Fail'
        WHEN ABS(z_score) > 2.0 THEN 'Warning'
        ELSE 'Pass'
    END AS pass_fail
```

**Investigation Steps:**
1. Verify receipt documentation (bill of lading, meter tickets)
2. Check for one-time large or small shipments
3. Review supplier shipping patterns
4. Confirm measurement system accuracy

**Common Causes:**
- Actual exceptional shipment (planned or unplanned)
- Meter malfunction or calibration drift
- Data entry error (extra/missing digit)
- Wrong crude grade assignment

---

#### RULE 2.2: Unit Throughput Outlier Detection

**Rule Code:** `OUTLIER_UNIT_THROUGHPUT`  
**Target:** `fact_unit_operations.throughput_bbl`  
**Threshold:** |Z-score| > 3.0  
**Severity:** Warning

**Analysis Window:** Rolling 30 days by unit

**Validation Logic:**
```sql
WITH stats AS (
    SELECT unit_id,
           AVG(throughput_bbl) AS mean_throughput,
           STDDEV(throughput_bbl) AS stddev_throughput
    FROM fact_unit_operations
    WHERE date_key >= [Last 30 days]
    GROUP BY unit_id
)
SELECT 
    (throughput_bbl - mean_throughput) / stddev_throughput AS z_score,
    CASE 
        WHEN ABS(z_score) > 3.0 THEN 'Fail'
        WHEN ABS(z_score) > 2.0 THEN 'Warning'
        ELSE 'Pass'
    END AS pass_fail
```

**Investigation Steps:**
1. Review daily operations log for unit status
2. Check for partial day operations (startup/shutdown)
3. Verify flow meter readings
4. Confirm feed availability

**Common Causes:**
- Planned unit rate change
- Unplanned trip or process upset
- Maintenance day (partial operation)
- Feed interruption

---

### CATEGORY 3: CROSS-ATTRIBUTE CONSISTENCY CHECKS

Validates consistency between related measurements using known physical or chemical relationships.

#### RULE 3.1: API Gravity vs. Specific Gravity Consistency

**Rule Code:** `CONSISTENCY_API_SG`  
**Target:** `fact_crude_receipts.api_gravity_60f` vs. `specific_gravity_60f`  
**Threshold:** Deviation ≤ 0.5%  
**Severity:** Critical

**Physical Relationship:**
```
Specific Gravity = 141.5 / (API Gravity + 131.5)
```

**Validation Logic:**
```sql
WITH calculated AS (
    SELECT 
        specific_gravity_60f AS measured_sg,
        141.5 / (api_gravity_60f + 131.5) AS calculated_sg
    FROM fact_crude_receipts
)
SELECT 
    ABS(measured_sg - calculated_sg) / calculated_sg * 100 AS deviation_pct,
    CASE 
        WHEN deviation_pct <= 0.5 THEN 'Pass'
        ELSE 'Fail'
    END AS pass_fail
```

**Tolerance Rationale:**
- ±0.5% allows for measurement precision limits
- API gravity: ±0.1° typical precision
- Specific gravity: ±0.0005 typical precision

**Investigation Steps:**
1. Verify both measurements are at 60°F reference temperature
2. Check hydrometer and pycnometer calibration
3. Confirm same sample used for both tests
4. Review temperature correction calculations

**Common Causes of Failure:**
- Temperature correction error
- Different samples tested
- Transcription error
- Instrument out of calibration

---

#### RULE 3.2: Volume-Weight Consistency

**Rule Code:** `CONSISTENCY_VOLUME_WEIGHT`  
**Target:** `fact_crude_receipts.net_volume_bbl` vs. `weight_short_tons`  
**Threshold:** Deviation ≤ 1.0%  
**Severity:** Critical

**Physical Relationship:**
```
Weight (tons) = Volume (bbl) × 0.1756 × Specific Gravity
```

Where:
- 0.1756 = conversion factor (tons per barrel at SG=1.0)
- 1 barrel = 42 US gallons = 0.1589873 cubic meters

**Validation Logic:**
```sql
WITH calculated AS (
    SELECT 
        weight_short_tons AS measured_weight,
        net_volume_bbl * 0.1756 * specific_gravity_60f AS calculated_weight
    FROM fact_crude_receipts
)
SELECT 
    ABS(measured_weight - calculated_weight) / calculated_weight * 100 AS deviation_pct,
    CASE 
        WHEN deviation_pct <= 1.0 THEN 'Pass'
        ELSE 'Fail'
    END AS pass_fail
```

**Tolerance Rationale:**
- ±1.0% accommodates tank measurement uncertainties
- Volume: ±0.25% typical (tank strapping accuracy)
- Weight: ±0.5% typical (scale accuracy)
- Specific gravity: ±0.5% typical
- Combined uncertainty: ~0.75-1.0%

**Investigation Steps:**
1. Verify volume measurement method (tank gauging, meter)
2. Check weight measurement method (scale, calculated)
3. Confirm specific gravity used in calculation
4. Review measurement timestamps (ensure same batch)

**Common Causes of Failure:**
- Tank measurement error (temperature, water, tilt)
- Scale calibration issue
- Wrong specific gravity applied
- Measurements from different times/batches

---

### CATEGORY 4: SEASONAL COMPLIANCE CHECKS

Ensures products meet season-specific regulatory specifications.

#### RULE 4.1: Gasoline RVP Seasonal Compliance

**Rule Code:** `SEASONAL_RVP_COMPLIANCE`  
**Target:** `fact_product_shipments.rvp_psi` (Gasoline only)  
**Threshold:** Summer ≤ 7.8 psi, Winter ≤ 13.5 psi  
**Severity:** Critical

**Seasonal Specifications:**

| Season | Period | Max RVP | Rationale |
|--------|--------|---------|-----------|
| Summer | June 1 - September 15 | 7.8 psi | Reduce evaporative emissions in hot weather |
| Winter | September 16 - May 31 | 13.5 psi | Ensure cold-start performance |

**Regulatory Basis:**
- EPA 40 CFR Part 80 Subpart C
- State-specific variations (California, TX, etc.)
- Ozone season controls

**Validation Logic:**
```sql
WITH seasonal_spec AS (
    SELECT 
        CASE 
            WHEN month BETWEEN 6 AND 9 AND (month < 9 OR day <= 15)
            THEN 7.8   -- Summer
            ELSE 13.5  -- Winter
        END AS max_rvp
    FROM dim_date
)
SELECT 
    rvp_psi,
    max_rvp,
    CASE 
        WHEN rvp_psi <= max_rvp THEN 'Pass'
        ELSE 'Fail'
    END AS pass_fail
```

**Investigation Steps:**
1. Verify product is **actually gasoline** (not blendstock)
2. Check RVP test method (ASTM D5191 or D323)
3. Review blending calculations and component RVPs
4. Confirm butane blending rate appropriate for season
5. Check for transition period buffer (blend for season, not current date)

**Common Causes of Failure:**
- Excessive butane in summer blend
- Insufficient light ends in winter blend
- Blending calculation error
- Test method issue
- Wrong product specification applied

**Transition Period Best Practice:**
- Start producing summer blend by **May 1** (30-day buffer)
- Start producing winter blend by **August 15** (30-day buffer)

---

### CATEGORY 5: TREND ANALYSIS (MOVING AVERAGE DEVIATION)

Detects deviations from recent historical patterns to identify process changes, equipment degradation, or data issues.

#### RULE 5.1: Unit Throughput Moving Average Deviation

**Rule Code:** `TREND_MOVING_AVERAGE`  
**Target:** `fact_unit_operations.throughput_bbl`  
**Threshold:** Deviation > ±15% from 7-day moving average  
**Severity:** Warning

**Analysis Window:** 7-day rolling average by unit

**Validation Logic:**
```sql
WITH moving_avg AS (
    SELECT 
        throughput_bbl,
        AVG(throughput_bbl) OVER (
            PARTITION BY unit_id 
            ORDER BY date_key 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS avg_7day
    FROM fact_unit_operations
)
SELECT 
    (throughput_bbl - avg_7day) / avg_7day * 100 AS deviation_pct,
    CASE 
        WHEN ABS(deviation_pct) > 15.0 THEN 'Warning'
        ELSE 'Pass'
    END AS pass_fail
```

**Threshold Rationale:**
- ±15% allows for normal operational variability
- Captures significant changes (planned or unplanned)
- Reduces false positives from day-to-day fluctuations

**Investigation Steps:**
1. Review operations log for planned changes
2. Check for equipment issues or maintenance
3. Verify feed quality and availability
4. Confirm measurement system accuracy

**Common Causes:**
- Planned rate change
- Feed quality variation
- Equipment constraint
- Catalyst deactivation (gradual)
- Process upset

---

### CATEGORY 6: BUSINESS LOGIC VALIDATION

Validates domain-specific business rules and constraints.

#### RULE 6.1: Yield Sum Reasonableness

**Rule Code:** `CONSISTENCY_YIELD_SUM`  
**Target:** `fact_unit_production` (sum of yields by date and unit)  
**Threshold:** Volume: 95-110%, Weight: 95-99%  
**Severity:** Critical

**Volume Yield Rationale:**
- **95-100%:** Normal for simple processing (CDU, VDU)
- **100-110%:** Acceptable for cracking units (FCC, coker)
- **> 110%:** Calculation error or data issue

**Weight Yield Rationale:**
- **95-99%:** Normal accounting for refinery fuel, coke, losses
- **< 95%:** Excessive losses (investigate)
- **> 99%:** Unlikely (violates mass conservation)

**Validation Logic:**
```sql
WITH yield_totals AS (
    SELECT 
        date_key,
        unit_id,
        SUM(yield_pct_volume) AS total_volume_yield,
        SUM(yield_pct_weight) AS total_weight_yield
    FROM fact_unit_production
    GROUP BY date_key, unit_id
)
SELECT 
    CASE 
        WHEN total_volume_yield BETWEEN 95.0 AND 110.0 
             AND total_weight_yield BETWEEN 95.0 AND 99.0 
        THEN 'Pass'
        ELSE 'Fail'
    END AS pass_fail
```

**Investigation Steps:**
1. Verify all product streams are included
2. Check yield calculation methodology
3. Review unit type (cracking vs. simple processing)
4. Confirm feed and product measurements

**Common Causes of Failure:**
- Missing product stream in calculation
- Incorrect basis (should be feed = 100%)
- Feed measurement error
- Product measurement error

---

## Investigation Workflow

### Step 1: Detection
- Automated daily execution of data quality checks
- Results loaded to `fact_data_quality_checks`
- Failures generate alerts by severity

### Step 2: Triage
1. **Critical Failures:** Immediate investigation required
2. **Warnings:** Review within 24 hours
3. **Info:** Review weekly

### Step 3: Root Cause Analysis
Follow rule-specific investigation steps:
1. Verify raw data and measurements
2. Check equipment calibration and status
3. Review calculation logic
4. Investigate operational context

### Step 4: Resolution
- **Data Error:** Correct source data and reprocess
- **Equipment Issue:** Repair/calibrate and verify
- **Process Anomaly:** Document and communicate
- **False Positive:** Adjust rule threshold if justified

### Step 5: Documentation
- Log investigation results in `notes` field
- Track resolution in operations system
- Update procedures if systemic issue

---

## Threshold Tuning Guidelines

### When to Adjust Thresholds

**Consider adjustment if:**
- High false positive rate (> 10%)
- Missing true issues (false negatives)
- Process/equipment changes alter normal range
- Seasonal patterns not captured

**DO NOT adjust for:**
- Single incidents
- Convenience (avoiding investigation)
- Pressure to "look better"

### Tuning Process

1. **Analyze Historical Data:** Review 90+ days of check results
2. **Calculate Statistics:** Mean, standard deviation, percentiles
3. **Propose New Threshold:** Document rationale
4. **Test:** Apply to historical data, evaluate performance
5. **Approve:** Management sign-off required
6. **Implement:** Update rule definition
7. **Monitor:** Track performance for 30 days

---

## Performance Metrics

### Data Quality KPIs

1. **Overall Pass Rate:** Target ≥ 95%
2. **Critical Failure Rate:** Target < 1%
3. **Mean Time to Resolution:** Target < 24 hours
4. **False Positive Rate:** Target < 10%

### Monthly Dashboard

```sql
-- Overall Pass Rate by Category
SELECT 
    qr.rule_category,
    COUNT(*) AS total_checks,
    SUM(CASE WHEN dq.pass_fail = 'Pass' THEN 1 ELSE 0 END) AS passed,
    ROUND(100.0 * SUM(CASE WHEN dq.pass_fail = 'Pass' THEN 1 ELSE 0 END) / COUNT(*), 2) AS pass_rate_pct
FROM fact_data_quality_checks dq
INNER JOIN dim_quality_rule qr ON dq.rule_id = qr.rule_id
WHERE dq.date_key >= [Last 30 days]
GROUP BY qr.rule_category;

-- Critical Failures This Month
SELECT 
    COUNT(*) AS critical_failures,
    COUNT(DISTINCT dq.date_key) AS days_affected,
    COUNT(DISTINCT dq.entity_id) AS entities_affected
FROM fact_data_quality_checks dq
INNER JOIN dim_quality_rule qr ON dq.rule_id = qr.rule_id
WHERE qr.severity = 'Critical'
  AND dq.pass_fail = 'Fail'
  AND dq.date_key >= [Last 30 days];
```

---

## References

### Industry Standards
- **ASTM D287:** API Gravity of Crude Petroleum
- **ASTM D4294/D5453:** Sulfur in Petroleum
- **ASTM D5191/D323:** Reid Vapor Pressure
- **API MPMS Chapter 11.1:** Volume Correction Factors

### Statistical Methods
- **Statistical Process Control (SPC)**
- **Six Sigma Methodology**
- **3-Sigma Rule (Empirical Rule)**

### Regulatory
- **EPA 40 CFR Part 80:** Regulation of Fuels and Fuel Additives
- **State Air Quality Regulations:** Seasonal RVP requirements

---

## Appendix: Rule Summary Table

| Rule Code | Category | Target | Threshold | Severity | Auto-Execute |
|-----------|----------|--------|-----------|----------|--------------|
| RANGE_API_GRAVITY | Range | api_gravity_60f | 5-50° | Critical | Yes |
| RANGE_SULFUR_CONTENT | Range | sulfur_wt_pct | 0.01-7.0% | Critical | Yes |
| RANGE_TEMPERATURE | Range | observed_temperature_f | 30-150°F | Warning | Yes |
| RANGE_CAPACITY_UTILIZATION | Range | capacity_utilization_pct | 0-105% | Warning | Yes |
| RANGE_BSW | Range | bsw_pct | 0-2.0% | Critical | Yes |
| OUTLIER_CRUDE_VOLUME | Outlier | net_volume_bbl | \|Z\| > 3.0 | Warning | Yes |
| OUTLIER_UNIT_THROUGHPUT | Outlier | throughput_bbl | \|Z\| > 3.0 | Warning | Yes |
| CONSISTENCY_API_SG | Consistency | api_gravity + SG | ≤0.5% deviation | Critical | Yes |
| CONSISTENCY_VOLUME_WEIGHT | Consistency | volume + weight | ≤1.0% deviation | Critical | Yes |
| SEASONAL_RVP_COMPLIANCE | Seasonal | rvp_psi | 7.8/13.5 psi | Critical | Yes |
| TREND_MOVING_AVERAGE | Trend | throughput_bbl | ±15% from 7-day avg | Warning | Yes |
| CONSISTENCY_YIELD_SUM | Business Logic | yield totals | Vol:95-110%, Wt:95-99% | Critical | Yes |

---

**Document Version:** 1.0  
**Last Updated:** Phase 7 Implementation  
**Owner:** Data Quality Team  
**Review Frequency:** Quarterly
