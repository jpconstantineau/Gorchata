## Phase 3 Complete: Singular Test Support & Custom Generic Test Templates

Successfully implemented singular test loading from .sql files and custom generic test templates with {% test %} syntax following strict TDD principles.

**Date:** February 8, 2026
**Status:** ✅ Complete

---

## Files Created/Changed

### Singular Tests (5 files)
- **internal/domain/test/singular/parser.go** - ParseTestMetadata() for config extraction
- **internal/domain/test/singular/loader.go** - LoadSingularTests(), loadTestFromFile()
- **internal/domain/test/singular/executor.go** - Execution stub (deferred to Phase 5)
- **internal/domain/test/singular/parser_test.go** (8 test functions)
- **internal/domain/test/singular/loader_test.go** (7 test functions)

### Custom Generic Test Templates (6 files)
- **internal/domain/test/generic/template_parser.go** - ParseTestTemplate() for {% test %} syntax
- **internal/domain/test/generic/template_executor.go** - TemplateTest struct implementing GenericTest
- **internal/domain/test/generic/template_loader.go** - LoadCustomGenericTests()
- **internal/domain/test/generic/template_parser_test.go** (6 test functions)
- **internal/domain/test/generic/template_executor_test.go** (11 test functions)
- **internal/domain/test/generic/template_loader_test.go** (8 test functions)

**Total:** 11 files created (6 implementations + 5 test files)

---

## Functionality Implemented

### Singular Tests

**What are Singular Tests?**
Custom SQL queries stored in `.sql` files that return failing rows (0 rows = pass, >0 rows = fail).

**Example Singular Test:**
```sql
-- tests/test_alarm_lifecycle.sql
-- config(severity='warn', store_failures=true)

-- Ensure all ACKNOWLEDGED events have a prior ACTIVE event
SELECT 
    ack.tag_id,
    ack.event_timestamp as ack_time
FROM {{ ref('raw_alarm_events') }} ack
WHERE ack.event_type = 'ACKNOWLEDGED'
  AND NOT EXISTS (
    SELECT 1 FROM {{ ref('raw_alarm_events') }} act
    WHERE act.tag_id = ack.tag_id
      AND act.event_type = 'ACTIVE'
      AND act.event_timestamp < ack.event_timestamp
  )
```

**Loader Capabilities:**
- `LoadSingularTests(testDir string) ([]*Test, error)` - Recursive directory scan
- Finds all `.sql` files in `tests/` directory and subdirectories
- Derives test name from filename: `test_alarm_lifecycle.sql` → `test_alarm_lifecycle`
- Reads SQL content and stores as template (rendering happens in Phase 5)
- Parses config metadata from SQL comments

**Parser Capabilities:**
- `ParseTestMetadata(sqlContent string) (TestConfig, error)` - Extract config from comments
- Parses: `-- config(key='value', key2='value2')` pattern
- Supports parameters:
  - `severity='warn'` or `severity='error'`
  - `store_failures=true` or `store_failures=false`
  - `where='condition'`
- Returns TestConfig struct from Phase 1
- Lenient parsing - provides defaults if config not found

**Example Config Parsing:**
```sql
-- config(severity='warn', store_failures=true, where='area_code = "C-100"')
```
Parses to:
```go
TestConfig{
    Severity: SeverityWarn,
    StoreFailures: true,
    Where: "area_code = \"C-100\"",
}
```

**Integration Points:**
- Uses Test struct from Phase 1
- Uses TestConfig struct from Phase 1
- SQL stored as SQLTemplate field (not yet rendered)
- Template rendering with {{ ref() }}, {{ source() }} deferred to Phase 5

---

### Custom Generic Test Templates

**What are Custom Generic Test Templates?**
Reusable parameterized tests defined in `.sql` files using `{% test %}` syntax. They work just like built-in generic tests but are user-defined.

**Example Custom Generic Test:**
```sql
-- tests/generic/test_positive_values.sql
{% test positive_values(model, column_name) %}
SELECT *
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND {{ column_name }} <= 0
{% endtest %}
```

**Template Parser Capabilities:**
- `ParseTestTemplate(sqlContent string) (name, params, sqlTemplate, error)` - Parse {% test %} block
- Extracts test name: `positive_values`
- Extracts parameter list: `["model", "column_name"]`
- Extracts SQL template body (between {% test %} and {% endtest %})
- Validates syntax (must have matching opening/closing tags)

**TemplateTest Executor:**
```go
type TemplateTest struct {
    testName    string
    params      []string
    sqlTemplate string
}
```

Implements GenericTest interface:
- `Name() string` - Returns test name
- `GenerateSQL(model, column string, args map[string]interface{}) (string, error)` - Substitutes parameters
- `Validate(model, column string, args map[string]interface{}) error` - Validates args

**Parameter Substitution:**
- `{{ model }}` → replaced with model name
- `{{ column_name }}` → replaced with column name
- `{{ custom_arg }}` → replaced with args["custom_arg"]
- WHERE clause support via args["where"]

**Example with Multiple Arguments:**
```sql
-- tests/generic/test_value_between.sql
{% test value_between(model, column_name, min_value, max_value) %}
SELECT *
FROM {{ model }}
WHERE {{ column_name }} NOT BETWEEN {{ min_value }} AND {{ max_value }}
  AND {{ column_name }} IS NOT NULL
{% endtest %}
```

Usage:
```go
test := LoadCustomTest("test_value_between.sql")
args := map[string]interface{}{
    "min_value": 0,
    "max_value": 100,
}
sql, _ := test.GenerateSQL("products", "price", args)
// Returns: SELECT * FROM products WHERE price NOT BETWEEN 0 AND 100 AND price IS NOT NULL
```

**Template Loader Capabilities:**
- `LoadCustomGenericTests(testDir string, registry *Registry) error` - Load all custom tests
- Scans `tests/generic/` directory for `.sql` files
- Parses each file for {% test %} blocks
- Creates TemplateTest instances
- Registers in provided Registry (from Phase 2)
- Custom tests become available alongside built-in tests

**Dynamic Registration:**
```go
registry := generic.NewDefaultRegistry() // Has 14 built-in tests

// Load custom tests
err := LoadCustomGenericTests("tests/generic", registry)

// Now can retrieve custom test just like built-in
test, _ := registry.Get("positive_values") // Custom test!
test, _ := registry.Get("not_null")        // Built-in test
```

---

## Test Coverage

**Total Tests:** 40 test functions  
**Coverage:** 84.5% (singular), 91.9% (generic - maintained), 94.9% (overall test domain)  
**Execution Time:** 0.524s  
**Status:** ✅ All passing

### Test Functions Implemented

**singular/parser_test.go (8 tests):**
- `TestParseTestMetadata_NoConfig` - Defaults when no config
- `TestParseTestMetadata_WithSeverityWarn` - Parse severity='warn'
- `TestParseTestMetadata_WithSeverityError` - Parse severity='error'
- `TestParseTestMetadata_WithStoreFailuresTrue` - Parse store_failures=true
- `TestParseTestMetadata_WithStoreFailuresFalse` - Parse store_failures=false
- `TestParseTestMetadata_WithWhereClause` - Parse where clause
- `TestParseTestMetadata_MultipleParams` - Parse multiple params
- `TestParseTestMetadata_InvalidSeverity` - Handle invalid values

**singular/loader_test.go (7 tests):**
- `TestLoadSingularTests_ValidDirectory` - Load from directory with tests
- `TestLoadSingularTests_EmptyDirectory` - Handle empty directory
- `TestLoadSingularTests_NonexistentDirectory` - Handle missing directory
- `TestLoadSingularTests_RecursiveSearch` - Find tests in subdirectories
- `TestLoadTestFromFile_ValidSQL` - Load single test file
- `TestLoadTestFromFile_WithConfig` - Load test with config comment
- `TestLoadTestFromFile_InvalidPath` - Handle missing file

**generic/template_parser_test.go (6 tests):**
- `TestParseTestTemplate_SimpleTemplate` - Parse basic {% test %} block
- `TestParseTestTemplate_WithMultipleParams` - Parse parameter list
- `TestParseTestTemplate_NoParameters` - Handle table-level test
- `TestParseTestTemplate_InvalidSyntax_NoStart` - Missing opening tag
- `TestParseTestTemplate_InvalidSyntax_NoEnd` - Missing closing tag
- `TestParseTestTemplate_ComplexSQL` - Multi-line SQL with subqueries

**generic/template_executor_test.go (11 tests):**
- `TestTemplateTest_Name` - Returns correct test name
- `TestTemplateTest_GenerateSQL_SimpleSubstitution` - Basic model/column
- `TestTemplateTest_GenerateSQL_WithCustomArgument` - Custom parameter
- `TestTemplateTest_GenerateSQL_NoColumnNeeded` - Table-level test
- `TestTemplateTest_GenerateSQL_MultipleArguments` - Multiple custom args
- `TestTemplateTest_GenerateSQL_WithWhereClause` - WHERE clause support
- `TestTemplateTest_Validate_ValidInput` - Validation passes
- `TestTemplateTest_Validate_MissingRequiredArg` - Missing custom param
- `TestTemplateTest_Validate_EmptyModel` - Missing model name
- `TestTemplateTest_Validate_NoColumnForColumnTest` - Missing column
- `TestTemplateTest_GenerateSQL_EscapedBraces` - Handle {{ }} in SQL strings

**generic/template_loader_test.go (8 tests):**
- `TestLoadCustomGenericTests_ValidDirectory` - Load custom tests
- `TestLoadCustomGenericTests_EmptyDirectory` - Handle no custom tests
- `TestLoadCustomGenericTests_NonexistentDirectory` - Handle missing dir
- `TestLoadCustomGenericTests_MultipleTests` - Load multiple files
- `TestLoadCustomGenericTests_RegistryIntegration` - Integrate with registry
- `TestLoadCustomGenericTests_InvalidTemplate` - Handle malformed templates
- `TestLoadCustomGenericTests_DuplicateNames` - Handle name conflicts
- `TestLoadCustomGenericTests_SubdirectorySearch` - Recursive loading

---

## Architecture & Design

### Singular Test Workflow

1. **Load** - Scan `tests/` directory recursively for .sql files
2. **Parse** - Extract config from SQL comments
3. **Create** - Build Test struct with SQLTemplate
4. **Defer** - Template rendering happens in Phase 5 execution engine

### Custom Generic Test Workflow

1. **Load** - Scan `tests/generic/` for .sql files
2. **Parse** - Extract {% test %} blocks with name and params
3. **Create** - Build TemplateTest implementing GenericTest
4. **Register** - Add to Registry dynamically
5. **Use** - Available like built-in tests (get from registry, generate SQL)

### Integration with Phase 2

Custom generic tests seamlessly integrate:
```go
// Phase 2: Built-in tests
registry := generic.NewDefaultRegistry() // 14 tests

// Phase 3: Add custom tests
LoadCustomGenericTests("tests/generic", registry) // +N custom tests

// Use both the same way
test1, _ := registry.Get("not_null")        // Built-in
test2, _ := registry.Get("positive_values") // Custom

sql1, _ := test1.GenerateSQL("users", "email", nil)
sql2, _ := test2.GenerateSQL("products", "price", nil)
```

---

## Correctly Deferred to Phase 5

The following were intentionally deferred to Phase 5 (Test Execution Engine):
- ❌ Full SQL execution against database
- ❌ {{ ref() }} / {{ source() }} template rendering integration
- ❌ Test result collection and aggregation
- ❌ Failure row storage
- ❌ DatabaseAdapter integration

Phase 3 focuses on:
- ✅ File I/O and discovery
- ✅ Parsing and metadata extraction
- ✅ SQL template text storage
- ✅ Dynamic registration

---

## Usage Examples

### Loading Singular Tests

```go
import "github.com/jpconstantineau/gorchata/internal/domain/test/singular"

// Load all singular tests from tests/ directory
tests, err := singular.LoadSingularTests("./tests")
if err != nil {
    log.Fatal(err)
}

for _, test := range tests {
    fmt.Printf("Test: %s\n", test.Name)
    fmt.Printf("Severity: %s\n", test.Config.Severity)
    fmt.Printf("SQL: %s\n", test.SQLTemplate)
}
```

### Loading Custom Generic Tests

```go
import (
    "github.com/jpconstantineau/gorchata/internal/domain/test/generic"
)

// Create registry with built-in tests
registry := generic.NewDefaultRegistry()

// Load custom tests and register
err := generic.LoadCustomGenericTests("./tests/generic", registry)
if err != nil {
    log.Fatal(err)
}

// Use custom test
test, _ := registry.Get("positive_values")
sql, _ := test.GenerateSQL("products", "price", nil)
fmt.Println(sql)
// Output: SELECT * FROM products WHERE price IS NOT NULL AND price <= 0
```

### Creating Custom Generic Test File

```sql
-- tests/generic/test_valid_email.sql
{% test valid_email(model, column_name) %}
SELECT *
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND {{ column_name }} NOT LIKE '%@%.%'
{% endtest %}
```

Use in schema.yml (Phase 4):
```yaml
columns:
  - name: email
    data_tests:
      - valid_email  # Custom test!
```

---

## Design Patterns Applied

### Strategy Pattern
- Custom generic tests are strategies implementing GenericTest interface
- Interchangeable with built-in tests

### Template Method
- Base parsing logic reused across test types
- Extension points for custom behavior

### Factory Pattern
- Registry acts as factory for both built-in and custom tests
- Unified retrieval mechanism

### Builder/Registry Pattern
- Dynamic registration of custom tests
- Extensible without modifying core code

---

## Quality Characteristics

✅ **File I/O Robustness:** Handles missing files, empty directories, nested directories  
✅ **Parsing Leniency:** Provides defaults, handles malformed input gracefully  
✅ **Error Messages:** Descriptive errors with file paths and line numbers  
✅ **Integration:** Seamlessly works with Phase 1 & 2 components  
✅ **Extensibility:** Easy to add new config parameters or template features  
✅ **Test Coverage:** 40 tests exercising all paths  
✅ **Performance:** Efficient file scanning and parsing  

---

## TDD Process Verification

1. ✅ **Tests written first** - All 40 test functions before implementation
2. ✅ **Red phase** - Confirmed compilation failures
3. ✅ **Green phase** - Implemented minimal code incrementally
4. ✅ **Refactor phase** - Cleaned up while keeping tests green
5. ✅ **Coverage** - 84.5% singular, 91.9% generic
6. ✅ **Integration** - Full project builds successfully

---

## Next Phase

Phase 4 will implement:
- YAML schema file parsing (schema.yml, _models.yml)
- Test configuration from YAML
- BuildTestsFromSchema() to instantiate generic tests
- Support for column-level and table-level test configuration
- Integration with Phase 2 & 3 test types

---

## Git Commit Message

```
feat: implement singular tests and custom generic test templates (Phase 3)

Singular Tests:
- Add LoadSingularTests() for recursive .sql file discovery
- Add ParseTestMetadata() for config extraction from SQL comments
- Support config parameters: severity, store_failures, where
- Derive test name from filename
- Integration with Phase 1 Test struct

Custom Generic Test Templates:
- Add ParseTestTemplate() for {% test %} syntax parsing
- Add TemplateTest implementing GenericTest interface
- Support parameter substitution: {{ model }}, {{ column_name }}, custom args
- Add LoadCustomGenericTests() for dynamic registration
- Seamless integration with Phase 2 Registry

Infrastructure:
- Template parameter validation
- WHERE clause support
- Descriptive error messages with file paths
- Recursive directory scanning
- Lenient parsing with sensible defaults

Tests:
- 40 test functions across 5 test files
- 84.5% coverage (singular), 91.9% coverage (generic)
- All tests passing in 0.524s

Deferred to Phase 5:
- Full SQL execution against database
- {{ ref() }} / {{ source() }} template rendering
- Test result collection and failure storage
```
