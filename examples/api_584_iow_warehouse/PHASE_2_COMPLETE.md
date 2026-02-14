# Phase 2 Complete: Staging Layer (Raw Sensor Telemetry)

**Project:** API 584 Integrity Operating Window (IOW) Data Warehouse  
**Phase:** 2 - Staging Layer (Raw Sensor Telemetry)  
**Status:** ✅ COMPLETE  
**Completion Date:** 2026-02-13  
**Test-Driven Development:** Strict TDD followed

---

## Objective

Ingest 5-minute interval sensor measurements with timestamps spanning 1 month for testing, establishing the staging layer for downstream IOW excursion detection.

---

## Implementation Summary

### Files Created

1. **`transformations/generate_sensor_data.go`** (346 lines)
   - Synthetic sensor telemetry generator
   - Implements realistic operational patterns
   - Supports 4 parameter types (Pressure, Temperature, pH, Flow)
   - Generates 5-minute interval readings
   - Includes data quality variations

2. **`seeds/raw_sensor_readings.csv`** (67MB, 1,296,000 records)
   - 1 month of sensor data (January 2025)
   - 150 sensors across 100 assets
   - 5-minute intervals (12 readings/hour × 24 hours × 30 days)
   - Average 8,640 readings per sensor

3. **`models/staging/stg_sensor_readings.sql`** (48 lines)
   - Staging model with data enrichment
   - Joins to dim_asset and dim_parameter_type for referential integrity
   - Filters out 'Bad' quality readings
   - Adds calculated columns: reading_date_key, hour_of_day, is_excursion_candidate
   - Data validation for physical limits

4. **`schema.yml`** (Updated with 9 stg_sensor_readings columns)
   - Column definitions with descriptions
   - Data tests: unique, not_null, relationships, accepted_values, accepted_range
   - FK relationships to dim_asset, dim_parameter_type, dim_date

5. **`api_584_iow_test.go`** (Updated with 7 new Phase 2 tests)
   - TestStagingSensorReadingsSeedExists ✅
   - TestStagingSensorReadingsSchema ✅
   - TestSensorTimestampSequence ✅
   - TestSensorValueRanges ✅
   - TestAssetTagJoin ✅
   - TestDataQualityFlags ✅
   - TestOneMonthCoverage ✅

---

## Data Generation Specifications

### Sensor Assignment by Equipment Type

| Equipment Type | Sensors Assigned |
|----------------|------------------|
| Pumps | Pressure, Flow |
| Columns, Drums, Towers, Vessels, Reactors | Temperature, Pressure |
| Heat Exchangers, Coolers | Temperature |
| Furnaces | Temperature |
| Compressors, Blowers | Pressure |
| Amine/Sour Water/Wash Systems | pH (additional) |

**Total Sensors:** 150 sensors across 100 assets (average 1.5 sensors/asset)

### Operational Patterns

- **75% Normal Operation**: Values within standard IOW limits with small random noise
- **15% Minor Drift**: Gradual trend toward limits over hours/days
- **8% IOW Excursions**: Brief periods outside limits (for Phase 3 detection testing)
- **2% Sensor Errors**: Out-of-range or 'Bad' quality flag

### Data Quality Distribution

| Quality Flag | Count | Percentage |
|--------------|-------|-----------|
| Good | 1,211,791 | 93.5% |
| Questionable | 58,262 | 4.5% |
| Bad | 13,013 | 1.0% |
| Substituted | 12,934 | 1.0% |

**Note:** 'Bad' readings are filtered out in stg_sensor_readings model (only Good/Questionable/Substituted pass through).

### Physical Value Ranges

| Parameter | Min Limit | Max Limit | Normal Range Min | Normal Range Max |
|-----------|-----------|-----------|------------------|------------------|
| Pressure | 0 psig | 3,000 psig | 50 psig | 750 psig |
| Temperature | 32 °F | 1,400 °F | 300 °F | 950 °F |
| pH | 0 pH | 14 pH | 5.0 pH | 9.0 pH |
| Flow | 0 bbl/day | 50,000 bbl/day | 5,000 bbl/day | 85,000 bbl/day |

---

## Staging Model Features

### stg_sensor_readings Columns

1. **reading_id** (BIGINT) - Unique surrogate key
2. **timestamp** (TIMESTAMP) - 5-minute interval timestamps
3. **tag_id** (VARCHAR) - FK to dim_asset.tag_id
4. **parameter_type** (VARCHAR) - FK to dim_parameter_type.parameter_type
5. **measured_value** (FLOAT) - Actual sensor reading
6. **data_quality_flag** (VARCHAR) - Good/Questionable/Substituted only
7. **reading_date_key** (INTEGER) - YYYYMMDD format for FK to dim_date
8. **hour_of_day** (INTEGER) - 0-23 extracted from timestamp
9. **is_excursion_candidate** (BOOLEAN) - Flag for values outside normal IOW limits

### Data Quality Rules

- Filters out data_quality_flag = 'Bad'
- Validates all tag_id exist in dim_asset
- Validates all parameter_type exist in dim_parameter_type
- Enforces physical limit ranges per parameter type
- Ensures NOT NULL on critical columns

---

## Test Results

**All Tests Passing:** 16/16 ✅

### Phase 1 Tests (9 tests)
- Schema validation
- Dimension table schemas (5 dimensions)
- Seed file existence

### Phase 2 Tests (7 tests)
- Seed file existence
- Schema definition with required columns
- 5-minute timestamp sequence validation
- Physical value range validation (4 parameter types)
- Referential integrity (tag_id → dim_asset)
- Data quality flag validation (4 valid values)
- 1-month coverage per sensor (~8,640 readings)

---

## TDD Workflow Verification

✅ **Step 1:** Wrote all tests first (7 new tests in api_584_iow_test.go)  
✅ **Step 2:** Ran tests - confirmed FAIL (missing files, schema)  
✅ **Step 3:** Implemented generator (generate_sensor_data.go)  
✅ **Step 4:** Generated seed data (raw_sensor_readings.csv)  
✅ **Step 5:** Created SQL model (stg_sensor_readings.sql)  
✅ **Step 6:** Updated schema.yml with model definition  
✅ **Step 7:** Ran tests - confirmed PASS (all 16 tests passing)  
✅ **Step 8:** Fixed duplicate sensor issue (refactored determineSensorTypes)  
✅ **Step 9:** Regenerated data and re-tested  

---

## Data Volume Statistics

- **Seed File Size:** 67 MB
- **Total Readings:** 1,296,000
- **Time Period:** January 2025 (30 days)
- **Interval:** 5 minutes
- **Assets:** 100
- **Sensors:** 150
- **Average Readings/Sensor:** 8,640 per month
- **Projected Annual Volume:** ~15.5M readings (for future scaling)

---

## Key Achievements

1. ✅ Created realistic synthetic sensor telemetry generator
2. ✅ Generated 1 month of representative sensor data
3. ✅ Established staging model with data quality filters
4. ✅ Implemented 7 comprehensive data validation tests
5. ✅ Verified referential integrity with dimension tables
6. ✅ Modeled operational variety (normal, drift, excursions, errors)
7. ✅ Set up foundation for Phase 3 (IOW excursion detection)

---

## Sensor Data Characteristics

### Operational Events Modeled

- **Daily cycles:** 12-hour sinusoidal patterns for refinery operations
- **Normal steady-state:** Most common pattern (75%)
- **Drift patterns:** Gradual trends toward limits
- **Excursions:** Brief periods outside normal IOW limits (8%)
- **Sensor maintenance:** Data gaps with 'Substituted' quality flag
- **Sensor failures:** 'Bad' quality readings (filtered out in staging)

### Deterministic Generation

- Uses timestamp + tag_id as seed for reproducibility
- Consistent results across regenerations
- Supports testing and debugging

---

## Next Phase Preview: Phase 3

**Phase 3 will implement:**
- IOW excursion detection logic
- Three-tier limit violations (Critical/Standard/Informational)
- Fact table: fct_iow_excursions
- Aggregate analytics: asset risk scoring
- Time-series analysis: excursion duration, frequency, severity

**Phase 2 provides the foundation with:**
- Clean, validated sensor data in staging
- is_excursion_candidate flag for preliminary filtering
- Referential integrity for join operations
- 1-month dataset for testing detection algorithms

---

## Maintenance Notes

### Regenerating Sensor Data

```powershell
cd examples\api_584_iow_warehouse\transformations
go run generate_sensor_data.go
```

**Configuration:** Edit `generate_sensor_data.go` to adjust:
- `numDays` (default: 30)
- `startDate` (default: 2025-01-01)
- Operational pattern percentages
- Data quality distribution

### Testing

```powershell
cd examples\api_584_iow_warehouse
go test -v ./...
```

---

## Files Modified/Created Summary

| File | Type | Lines | Status |
|------|------|-------|--------|
| transformations/generate_sensor_data.go | Created | 346 | ✅ |
| seeds/raw_sensor_readings.csv | Created | 1.3M | ✅ |
| models/staging/stg_sensor_readings.sql | Created | 48 | ✅ |
| schema.yml | Updated | +60 | ✅ |
| api_584_iow_test.go | Updated | +250 | ✅ |

**Total Tests:** 16 (9 Phase 1 + 7 Phase 2)  
**Test Status:** All passing ✅  

---

**Phase 2 Implementation Complete - Ready for Phase 3: IOW Excursion Detection** 🎉
