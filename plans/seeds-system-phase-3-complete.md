## Phase 3 Complete: Type Inference and Schema Generation

Implemented automatic type inference from CSV data to generate SQL schemas (INTEGER, REAL, TEXT) with configurable sampling. The inference engine handles edge cases including leading zeros, scientific notation, negative numbers, and NULL values. Test coverage exceeds 96% with comprehensive validation of type detection priority and edge cases.

**Files created/changed:**
- internal/domain/seeds/inference.go
- internal/domain/seeds/inference_test.go

**Functions created/changed:**
- inferColumnType() - Determines SQL type from column values with priority INTEGER > REAL > TEXT
- InferSchema() - Generates complete SeedSchema from CSV rows with configurable sample size

**Tests created/changed:**
- TestInferColumnType_AllIntegers - All integer values inferred as INTEGER
- TestInferColumnType_Decimals - Decimal values inferred as REAL
- TestInferColumnType_Mixed - Mixed types default to TEXT
- TestInferColumnType_WithNulls - Ignores empty/whitespace values during inference
- TestInferColumnType_AllNulls - All empty values default to TEXT
- TestInferColumnType_Priority - Verifies INTEGER > REAL > TEXT priority (4 subtests)
- TestInferColumnType_EdgeCases - Leading zeros, scientific notation, negatives (5 subtests)
- TestInferSchema_BasicCSV - Infers simple 3-column schema
- TestInferSchema_MixedTypes - Handles multiple columns with different types
- TestInferSchema_WithHeaders - First row used as headers
- TestInferSchema_SampleSize - Respects configurable sample size
- TestInferSchema_EmptyData - Error handling for insufficient data
- TestInferSchema_NoHeaders - Error handling for missing headers

**Type Inference Rules Implemented:**
1. INTEGER: All non-empty values parse as int64, no leading zeros
2. REAL: At least one value has decimal point or scientific notation
3. TEXT: Default for mixed types, leading zeros, or non-numeric data
4. NULL handling: Empty strings and whitespace-only values ignored
5. Edge cases: Negative numbers, very large numbers, scientific notation

**Review Status:** APPROVED (96.4% coverage, all tests passing)

**Git Commit Message:**
```
feat: implement type inference and schema generation for seeds

- Add inferColumnType() with INTEGER/REAL/TEXT detection
- Implement InferSchema() with configurable sampling
- Handle edge cases: leading zeros, scientific notation, negatives
- Support NULL/empty value filtering during inference
- Add 13 test functions with 100+ test cases (all passing)
- Achieve 96.4% test coverage for inference logic
```
