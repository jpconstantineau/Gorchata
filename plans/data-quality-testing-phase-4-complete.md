## Phase 4 Complete: YAML Schema Configuration

Successfully implemented DBT-compatible schema.yml parsing to configure generic tests on models and columns following strict TDD principles.

**Date:** February 8, 2026
**Status:** ✅ Complete

---

## Files Created/Changed

### Implementation Files (6 files)
- **internal/domain/test/schema/schema.go** - SchemaFile, ModelSchema, ColumnSchema, TestDefinition structs
- **internal/domain/test/schema/parser.go** - ParseSchemaFile(), LoadSchemaFiles()
- **internal/domain/test/schema/builder.go** - BuildTestsFromSchema()
- **internal/domain/test/schema/schema_test.go** (6 test functions)
- **internal/domain/test/schema/parser_test.go** (8 test functions)
- **internal/domain/test/schema/builder_test.go** (10 test functions)

### Test Fixtures (5 files)
- **internal/domain/test/schema/testdata/simple_schema.yml**
- **internal/domain/test/schema/testdata/complex_schema.yml**
- **internal/domain/test/schema/testdata/invalid_schema.yml**
- **internal/domain/test/schema/testdata/multi_model_schema.yml**
- **internal/domain/test/schema/testdata/subdir/nested_schema.yml**

### Examples (2 files)
- **examples/schema_parsing_example.go** - Usage demonstration
- **examples/schema_example.yml** - Comprehensive schema examples

**Total:** 13 files created

---

## Functionality Implemented

### DBT-Compatible schema.yml Format

**Supported Structure:**
```yaml
version: 2

models:
  - name: users
    description: "User table"
    columns:
      - name: user_id
        description: "Primary key"
        data_tests:
          - unique
          - not_null
      
      - name: email
        data_tests:
          - not_null
          - not_empty_string
      
      - name: status
        data_tests:
          - accepted_values:
              values: ['active', 'inactive', 'pending']
              severity: warn
              store_failures: true
      
      - name: created_at
        data_tests:
          - not_null
    
    data_tests:
      - recency:
          datepart: day
          field: created_at
          interval: 7
          severity: warn
```

### Schema Structs

**SchemaFile:**
```go
type SchemaFile struct {
    Version int           `yaml:"version"`
    Models  []ModelSchema `yaml:"models"`
}
```

**ModelSchema:**
```go
type ModelSchema struct {
    Name        string            `yaml:"name"`
    Description string            `yaml:"description,omitempty"`
    Columns     []ColumnSchema    `yaml:"columns,omitempty"`
    DataTests   []TestDefinition  `yaml:"data_tests,omitempty"`  // Table-level
}
```

**ColumnSchema:**
```go
type ColumnSchema struct {
    Name        string           `yaml:"name"`
    Description string           `yaml:"description,omitempty"`
    DataTests   []TestDefinition `yaml:"data_tests,omitempty"`  // Column-level
}
```

**TestDefinition:**
```go
// Can be:
// - string: "not_null"
// - map: {"accepted_values": {"values": [...], "severity": "warn"}}
type TestDefinition interface{}
```

### Parser Capabilities

**ParseSchemaFile(filePath string) (*SchemaFile, error)**
- Reads YAML file at filePath
- Parses into SchemaFile struct using gopkg.in/yaml.v3
- Validates structure (version, models list)
- Returns descriptive errors for malformed YAML
- Handles missing optional fields

**LoadSchemaFiles(directory string) ([]*SchemaFile, error)**
- Recursively scans directory for `*schema.yml` or `*_schema.yml` files
- Parses each file
- Collects all SchemaFile structs
- Continues on partial failures (non-fatal errors)
- Returns all successfully parsed files + error list

**Example:**
```go
// Load all schema files from models/ directory
schemas, err := schema.LoadSchemaFiles("./models")
if err != nil {
    log.Printf("Warning: some files failed to parse: %v", err)
}
// schemas contains all successfully parsed schema files
```

### Builder Capabilities

**BuildTestsFromSchema(schemaFiles []*SchemaFile, registry *generic.Registry) ([]*test.Test, error)**

Converts schema definitions into executable test instances:

1. **Column-Level Tests:**
   - Iterates through each model's columns
   - For each data_test in column:
     - Extracts test name (string or map key)
     - Retrieves test from registry
     - Extracts test arguments
     - Separates test config (severity, where, etc.)
     - Creates test.Test instance

2. **Table-Level Tests:**
   - Iterates through model's data_tests
   - Same process but column name is empty
   - Handles special case: extracts `field` parameter for tests like recency

3. **Test Generation:**
   - Generates unique test ID: `<test_name>_<model>_<column>` (or custom name)
   - Uses GenericTest.GenerateSQL() to create SQL template
   - Applies test configuration (severity, where, store_failures)
   - Creates fully configured test.Test instance

**Example:**
```go
registry := generic.NewDefaultRegistry()
schemas, _ := schema.LoadSchemaFiles("./models")

tests, err := schema.BuildTestsFromSchema(schemas, registry)
// tests contains Test instances ready for execution

for _, t := range tests {
    fmt.Printf("Test: %s on %s.%s\n", t.Name, t.ModelName, t.ColumnName)
    fmt.Printf("SQL: %s\n", t.SQLTemplate)
}
```

### Test Configuration Extraction

**Reserved Configuration Keys:**
- `severity`: 'error' or 'warn'
- `store_failures`: true/false
- `where`: SQL WHERE clause
- `name`: Custom test name override
- `config`: Nested config object (DBT pattern)

**Test-Specific Arguments:**
Everything else is passed to the test as arguments.

**Example:**
```yaml
- relationships:
    to: ref('users')          # Test argument
    field: user_id            # Test argument
    severity: error           # Config
    store_failures: true      # Config
    where: "status = 'active'" # Config
```

Parsed as:
- Test args: `{to: "ref('users')", field: "user_id"}`
- Test config: `{Severity: Error, StoreFailures: true, Where: "status = 'active'"}`

### Handling Test Definition Formats

**1. Simple String Test:**
```yaml
data_tests:
  - not_null
```

**2. Map with No Args:**
```yaml
data_tests:
  - unique: {}
```

**3. Map with Args:**
```yaml
data_tests:
  - accepted_values:
      values: ['active', 'inactive']
      severity: warn
```

**4. Multiple Tests:**
```yaml
data_tests:
  - not_null
  - unique
  - not_empty_string
```

All formats are correctly parsed and converted to test instances.

---

## Test Coverage

**Total Tests:** 24 test functions  
**Coverage:** 81.7% (schema package)  
**Execution Time:** 0.386s  
**Status:** ✅ All passing

### Test Functions Implemented

**schema_test.go (6 tests):**
- `TestSchemaFile_Unmarshal` - Parse basic YAML
- `TestModelSchema_WithColumns` - Parse model with columns
- `TestColumnSchema_WithTests` - Parse column with tests
- `TestTestDefinition_SimpleString` - Simple test format
- `TestTestDefinition_MapWithArgs` - Complex test format
- `TestSchemaFile_InvalidYAML` - Error handling

**parser_test.go (8 tests):**
- `TestParseSchemaFile_ValidYAML` - Parse valid schema
- `TestParseSchemaFile_InvalidYAML` - Handle malformed YAML
- `TestParseSchemaFile_MissingFile` - Handle file not found
- `TestLoadSchemaFiles_MultipleFiles` - Load multiple schemas
- `TestLoadSchemaFiles_EmptyDirectory` - Handle no schemas
- `TestLoadSchemaFiles_RecursiveSearch` - Find nested schemas
- `TestLoadSchemaFiles_PatternMatching` - Match *schema.yml pattern
- `TestLoadSchemaFiles_PartialFailure` - Continue on errors

**builder_test.go (10 tests):**
- `TestBuildTestsFromSchema_SimpleColumnTest` - Build not_null
- `TestBuildTestsFromSchema_TestWithArguments` - Build accepted_values
- `TestBuildTestsFromSchema_RelationshipsTest` - Build relationships
- `TestBuildTestsFromSchema_TableLevelTest` - Build recency
- `TestBuildTestsFromSchema_WithSeverity` - Extract severity config
- `TestBuildTestsFromSchema_WithWhereClause` - Extract where clause
- `TestBuildTestsFromSchema_MultipleModels` - Multiple models
- `TestBuildTestsFromSchema_TestNotInRegistry` - Handle unknown test
- `TestBuildTestsFromSchema_CustomTestName` - Custom naming
- `TestBuildTestsFromSchema_ComplexSchema` - Full integration

---

## Architecture & Design

### Three-Layer Architecture

**1. Schema Layer (schema.go):**
- Defines YAML structure
- Implements yaml.Unmarshaler for flexible parsing
- Handles both simple and complex test definitions

**2. Parser Layer (parser.go):**
- File I/O and YAML parsing
- Recursive directory scanning
- Error collection and reporting

**3. Builder Layer (builder.go):**
- Test instantiation from schema
- Configuration extraction and separation
- Integration with test registry

### Integration with Previous Phases

**Phase 1 (Test Domain):**
- Uses `test.Test` struct for created tests
- Uses `test.TestConfig` for test configuration
- Uses `test.TestType` enum (GenericTest)

**Phase 2 (Generic Tests):**
- Queries `generic.Registry` to retrieve test implementations
- Calls `GenericTest.GenerateSQL()` to create SQL templates
- Validates test existence before building

**Phase 3 (Singular/Custom Tests):**
- Independent (different test loading mechanism)
- Custom generic tests registered in same registry work seamlessly

### Error Handling Strategy

**Lenient Parsing:**
- Missing optional fields use defaults
- Malformed tests skip with warnings
- Partial success: continue processing other tests

**Descriptive Errors:**
- Include file paths
- Include model/column names
- Include test names
- Clear validation messages

**Graceful Degradation:**
- One bad test doesn't fail entire schema
- One bad file doesn't fail entire directory
- Collect errors and return partial results

---

## Usage Examples

### Basic Usage

```go
import (
    "github.com/jpconstantineau/gorchata/internal/domain/test/schema"
    "github.com/jpconstantineau/gorchata/internal/domain/test/generic"
)

// 1. Load schema files
schemas, err := schema.LoadSchemaFiles("./models")
if err != nil {
    log.Printf("Warning: %v", err)
}

// 2. Get test registry
registry := generic.NewDefaultRegistry()

// 3. Build tests
tests, err := schema.BuildTestsFromSchema(schemas, registry)
if err != nil {
    log.Printf("Warning: %v", err)
}

// 4. Execute tests (Phase 5)
for _, t := range tests {
    fmt.Printf("Test: %s\n", t.ID)
    fmt.Printf("Model: %s\n", t.ModelName)
    fmt.Printf("Column: %s\n", t.ColumnName)
    fmt.Printf("SQL: %s\n", t.SQLTemplate)
}
```

### Creating schema.yml

```yaml
version: 2

models:
  - name: raw_alarm_events
    description: "DCS alarm event stream"
    
    columns:
      - name: event_id
        description: "Unique event identifier"
        data_tests:
          - unique
          - not_null
      
      - name: tag_id
        description: "Alarm tag identifier"
        data_tests:
          - not_null
          - not_empty_string
          - relationships:
              to: ref('raw_alarm_config')
              field: tag_id
      
      - name: event_type
        data_tests:
          - accepted_values:
              values: ['ACTIVE', 'ACKNOWLEDGED', 'INACTIVE']
              severity: error
      
      - name: priority_code
        data_tests:
          - accepted_values:
              values: ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']
              severity: warn
      
      - name: alarm_value
        data_tests:
          - not_null
          - accepted_range:
              min_value: -1000
              max_value: 10000
      
      - name: event_timestamp
        data_tests:
          - not_null
    
    data_tests:
      - recency:
          datepart: day
          field: event_timestamp
          interval: 7
          severity: warn
```

### Advanced: Custom Test Names

```yaml
columns:
  - name: email
    data_tests:
      - unique:
          name: email_should_be_unique
          severity: error
          store_failures: true
```

### Advanced: WHERE Clause Filtering

```yaml
columns:
  - name: status
    data_tests:
      - accepted_values:
          values: ['active', 'inactive']
          where: "deleted_at IS NULL"
```

---

## Design Patterns Applied

### Builder Pattern
- BuildTestsFromSchema() constructs complex test objects
- Separates construction logic from representation

### Strategy Pattern
- Different test types (from registry) used interchangeably
- Polymorphic test execution via GenericTest interface

### Factory Pattern
- Registry provides test instances
- Builder creates Test instances based on schema

### Adapter Pattern
- YAML structure adapted to internal Test representation
- Configuration extraction adapts to TestConfig

---

## Quality Characteristics

✅ **DBT Compatibility:** Follows DBT schema.yml structure closely  
✅ **Flexibility:** Handles simple and complex test definitions  
✅ **Robustness:** Lenient parsing with graceful degradation  
✅ **Error Handling:** Descriptive errors with context  
✅ **Integration:** Seamless with Phases 1-3  
✅ **Test Coverage:** 81.7% with 24 comprehensive tests  
✅ **Performance:** Efficient YAML parsing and test building  
✅ **Extensibility:** Easy to add new configuration options  

---

## TDD Process Verification

1. ✅ **Tests written first** - All 24 test functions before implementation
2. ✅ **Red phase** - Confirmed compilation failures
3. ✅ **Green phase** - Implemented minimal code
4. ✅ **Refactor phase** - Cleaned up while keeping tests green
5. ✅ **Coverage** - 81.7% code coverage achieved
6. ✅ **Integration** - Full project builds successfully

---

## Next Phase

Phase 5 will implement:
- Test execution engine with database adapter integration
- Adaptive sampling (>1M rows)
- Test selection logic (by name, tag, model)
- Parallel test execution
- Result aggregation and reporting
- CLI commands: `gorchata test`, `gorchata run --test`, `gorchata build`
- Console and JSON result writers

---

## Git Commit Message

```
feat: implement YAML schema configuration for tests (Phase 4)

Schema Structure:
- Add SchemaFile, ModelSchema, ColumnSchema structs
- Support DBT-compatible schema.yml version 2 format
- Handle column-level and table-level test definitions
- Parse simple string tests and complex map tests

Parser:
- Add ParseSchemaFile() for YAML parsing
- Add LoadSchemaFiles() for recursive directory scanning
- Pattern matching: *schema.yml, *_schema.yml
- Graceful error handling with partial success

Builder:
- Add BuildTestsFromSchema() to instantiate tests
- Extract test arguments vs test configuration
- Support reserved config keys: severity, where, store_failures, name
- Integrate with Phase 2 generic test registry
- Generate unique test IDs

Configuration Extraction:
- Severity levels (error/warn)
- WHERE clause filtering
- Custom test names
- Store failures option
- Field extraction for table-level tests

Tests:
- 24 test functions across 3 test files
- 81.7% code coverage
- All tests passing in 0.386s
```
