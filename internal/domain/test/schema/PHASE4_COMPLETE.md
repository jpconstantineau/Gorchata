# Phase 4: YAML Schema Configuration - Implementation Complete

## Overview
Phase 4 implements DBT-compatible schema.yml parsing to configure generic tests on models and columns. This allows declarative test configuration similar to dbt's testing framework.

## Implementation Summary

### Files Created (6 implementation files + 3 test files)

#### Core Implementation
1. **schema/schema.go** - Data structures for schema representation
   - `SchemaFile` - Root schema.yml structure
   - `ModelSchema` - Model configuration with columns and tests
   - `ColumnSchema` - Column configuration with tests

2. **schema/parser.go** - YAML file parsing
   - `ParseSchemaFile()` - Parse single schema.yml file
   - `LoadSchemaFiles()` - Recursively find and parse schema files
   - Supports `*schema.yml` and `*_schema.yml` naming patterns

3. **schema/builder.go** - Test instance creation
   - `BuildTestsFromSchema()` - Convert schema definitions to Test instances
   - `buildColumnTests()` - Build column-level tests
   - `buildTableTests()` - Build table-level tests with field extraction
   - `parseTestDefinition()` - Handle string and map test formats
   - `separateArgsAndConfig()` - Extract test args vs configuration
   - `applyTestConfig()` - Apply severity, where, store_failures, etc.
   - `generateTestID()` - Create unique test identifiers

#### Test Files
4. **schema/schema_test.go** - 6 tests for struct marshaling
5. **schema/parser_test.go** - 8 tests for file parsing
6. **schema/builder_test.go** - 10 tests for test building

#### Test Fixtures
7. **testdata/simple_schema.yml** - Basic column tests
8. **testdata/complex_schema.yml** - Table-level tests, relationships
9. **testdata/invalid_schema.yml** - Error handling
10. **testdata/multiple/*.yml** - Multi-file loading

## Test Results
```
✓ All 24 tests passing
✓ 81.7% code coverage
✓ No regressions in other packages
✓ Build successful via PowerShell script
```

## Supported Features

### Test Definition Formats
1. **Simple string**: `- not_null`
2. **Map with no args**: `- unique: {}`
3. **Map with args**: `- accepted_values: {values: [...], severity: warn}`

### Test Configuration Options
- `severity`: 'error' or 'warn'
- `store_failures`: true/false
- `where`: SQL filter condition
- `name`: Custom test name override

### Test Types
- **Column-level tests**: Applied to specific columns
- **Table-level tests**: Applied to entire model
  - Supports `field` argument for tests like recency
  - Automatically extracts field and uses as column parameter

### File Discovery
- Recursively scans directories
- Matches `*schema.yml` or `*_schema.yml` patterns
- Supports nested directory structures

## Usage Example

```go
import (
    "github.com/jpconstantineau/gorchata/internal/domain/test/schema"
    "github.com/jpconstantineau/gorchata/internal/domain/test/generic"
)

// Load schema files
schemas, err := schema.LoadSchemaFiles("./models")

// Get test registry
registry := generic.NewDefaultRegistry()

// Build tests
tests, err := schema.BuildTestsFromSchema(schemas, registry)

// tests now contains Test instances ready for execution
for _, test := range tests {
    fmt.Printf("Test: %s on %s.%s\n", 
        test.Name, test.ModelName, test.ColumnName)
}
```

## DBT Compatibility

### Supported DBT Patterns
- `version: 2` schema file format
- `data_tests` array for test definitions
- `ref('model')` syntax in relationships
- Severity levels (error/warn)
- WHERE clause filtering
- Custom test names

### Differences from DBT
- Uses `data_tests` (not `tests`) for clarity
- Field extraction for table-level tests
- Simplified configuration (subset of DBT options)

## Integration Points

### Phase 1 (Test Domain)
- Uses `test.Test` struct
- Uses `test.TestConfig` for configuration
- Generates tests of type `GenericTest`

### Phase 2 (Generic Tests)
- Queries `generic.Registry` for test implementations
- Calls `GenericTest.Validate()` and `GenerateSQL()`
- Supports all registered generic tests

### Phase 3 (Singular Tests)
- No direct integration (different test type)
- Schema YAML only configures generic tests

## Key Design Decisions

1. **interface{} for TestDefinition**
   - Flexible parsing of string or map formats
   - Runtime type checking for validation

2. **Separate args from config**
   - Reserved keys: severity, store_failures, where, name
   - Everything else passed to test implementation

3. **Field extraction for table tests**
   - Table-level tests can specify `field` argument
   - Builder extracts and uses as column parameter
   - Maintains empty ColumnName in Test struct

4. **Lenient error handling**
   - Unknown tests are skipped (not errors)
   - Continue processing on single test failures
   - Collect errors and report at end

## Testing Strategy

### TDD Process Followed
1. ✓ Wrote all tests first
2. ✓ Ran tests to confirm failures
3. ✓ Implemented minimal code
4. ✓ Incremental test runs
5. ✓ Refactored with passing tests
6. ✓ Build script verification

### Test Coverage
- **Schema structs**: YAML unmarshaling, nested structures
- **Parser**: File reading, directory scanning, error cases
- **Builder**: Simple tests, complex args, severity, custom names
- **Integration**: Multiple files, unknown tests, edge cases

## Next Steps (Phase 5)

Phase 4 completes the test configuration layer. Next:
- Execute Test instances against actual databases
- Collect and store test results
- Handle test failures and warnings
- Generate test reports

## Files Modified/Created

```
internal/domain/test/schema/
  ├── schema.go              (NEW - 23 lines)
  ├── schema_test.go         (NEW - 219 lines)
  ├── parser.go              (NEW - 99 lines)
  ├── parser_test.go         (NEW - 218 lines)
  ├── builder.go             (NEW - 247 lines)
  ├── builder_test.go        (NEW - 313 lines)
  └── testdata/
      ├── simple_schema.yml
      ├── complex_schema.yml
      ├── invalid_schema.yml
      └── multiple/
          ├── a_schema.yml
          └── b_schema.yml

examples/
  ├── schema_parsing_example.go  (NEW)
  └── schema_example.yml         (NEW)
```

## Metrics
- **Implementation Lines**: ~370 lines of production code
- **Test Lines**: ~750 lines of test code
- **Test/Code Ratio**: 2:1
- **Test Count**: 24 tests
- **Coverage**: 81.7%
- **Build Time**: <1 second
- **Test Time**: <0.5 seconds
