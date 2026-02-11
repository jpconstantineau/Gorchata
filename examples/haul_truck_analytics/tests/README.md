# Haul Truck Analytics - Data Quality Tests

This directory contains SQL-based data quality validation tests for the haul truck analytics example.

## Test Files

### 1. test_referential_integrity.sql
**Purpose:** Validates all foreign key relationships exist in dimension tables

**Checks:**
- `fact_haul_cycle.truck_id` → `dim_truck.truck_id`
- `fact_haul_cycle.shovel_id` → `dim_shovel.shovel_id`
- `fact_haul_cycle.crusher_id` → `dim_crusher.crusher_id`
- `fact_haul_cycle.operator_id` → `dim_operator.operator_id`
- `fact_haul_cycle.shift_id` → `dim_shift.shift_id`
- `fact_haul_cycle.date_id` → `dim_date.date_key`

**Expected:** 0 violations (all foreign keys must resolve)

### 2. test_temporal_consistency.sql
**Purpose:** Validates time-based rules and temporal ordering

**Checks:**
- `cycle_end > cycle_start` (no negative durations)
- No overlapping cycles for the same truck
- `state_end > state_start` in `stg_truck_states`
- No excessive gaps between states (>2 hours indicates missing data)

**Expected:** 0 violations (strict temporal ordering)

### 3. test_business_rules.sql
**Purpose:** Validates business logic rules

**Checks:**
- **Payload Range:** 0 ≤ payload ≤ 115% of truck capacity
- **Cycle Time Range:** 10 ≤ total_cycle_time ≤ 180 minutes
- **Loading Duration:** 2 ≤ duration_loading ≤ 15 minutes
- **Dumping Duration:** 0.5 ≤ duration_dumping ≤ 5 minutes
- **Distance Range:** 0 ≤ distance ≤ 50 km
- **Speed Range:** 0 < speed < 80 km/h
- **Speed Logic:** loaded_speed < empty_speed (physics constraint)
- **Fuel Consumption:** 0 ≤ fuel_consumed ≤ 1000 liters per cycle

**Expected:** 0 violations (or <1% for acceptable edge cases)

### 4. test_state_transitions.sql
**Purpose:** Validates operational state sequence validity

**Valid Haul Cycle Sequence:**
```
queued_at_shovel → loading → hauling_loaded → 
queued_at_crusher → dumping → returning_empty → (repeat)
```

**Checks:**
- Only valid state transitions occur
- No invalid sequences (e.g., loading → dumping without hauling)
- Each complete cycle includes required states: loading, hauling_loaded, dumping, returning_empty
- Queue states and spot_delays are optional but must be in correct position

**Expected:** 0 invalid transitions (or acceptable variations for partial cycles)

## Running the Tests

### Via Go Test Suite
```powershell
# Run all data quality tests
go test ./test -run "Referential|Temporal|Business|State|Speed|Queue|Fuel|Refueling|Payload|CycleTime|DataQuality" -v

# Run specific test
go test ./test -run "TestReferentialIntegrity" -v
```

### Via SQL Directly (SQLite)
```bash
sqlite3 warehouse.db < tests/test_referential_integrity.sql
sqlite3 warehouse.db < tests/test_temporal_consistency.sql
sqlite3 warehouse.db < tests/test_business_rules.sql
sqlite3 warehouse.db < tests/test_state_transitions.sql
```

## Test Results Format

All SQL tests return results in a consistent format:

### Single-Row Tests (Referential Integrity, Temporal Consistency)
```
violation_count | test_name | test_description | test_result
```

### Multi-Row Tests (Business Rules, State Transitions)
```
rule_name | violation_count | test_result
```

## Data Quality Scoring

The `TestDataQualitySummary` test generates an overall data quality report:
- Total cycles processed
- Distinct trucks/operators
- Date range coverage
- Average cycle time and payload
- Overall data completeness

## Acceptable Thresholds

For synthetic/generated data, some edge cases are acceptable:
- **Business Rules:** <1% violation rate (e.g., occasional overload cycles)
- **State Transitions:** Incomplete cycles at shift boundaries are acceptable
- **Speed Logic:** Occasional violations due to terrain variations

For production data, stricter thresholds apply (typically 0 violations for referential integrity and temporal consistency).

## Integration with CI/CD

These tests can be integrated into automated data quality pipelines:

```powershell
# Example: Run tests and capture results
go test ./test -run "DataQuality" -v > data_quality_report.txt

# Parse results for threshold violations
# Fail pipeline if critical tests (referential integrity, temporal consistency) have violations > 0
```

## Troubleshooting

### High Violation Counts
- Check seed data generation parameters
- Verify state detection logic is correctly identifying transitions
- Review cycle aggregation logic for timestamp handling

### Missing Data
- Ensure all dimension CSVs are loaded before running tests
- Verify staging transformations completed successfully
- Check that fact table aggregations executed

### False Positives
- Review acceptable threshold definitions
- Consider operational realities (e.g., occasional overloading is normal)
- Adjust test SQL logic for edge cases specific to your domain

## Related Files

- **Go Tests:** `test/haul_truck_data_quality_test.go`
- **Schema Definition:** `examples/haul_truck_analytics/schema.yml`
- **Staging Models:** `examples/haul_truck_analytics/models/staging/`
- **Fact Models:** `examples/haul_truck_analytics/models/facts/`
- **Analytics Queries:** `examples/haul_truck_analytics/models/analytics/`
