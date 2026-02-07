## Plan: Complete Incremental Materialization Implementation

Implement the missing template functions `is_incremental()` and `{{ this }}` to enable efficient incremental filtering in SQL models, completing the incremental materialization feature documented in the README.

**Phases: 4**

### 1. **Phase 1: Add Template Context for Incremental State**
- **Objective:** Extend the template Context to track incremental execution state and current model name
- **Files/Functions to Modify/Create:**
  - [internal/template/context.go](internal/template/context.go) - Add `IsIncremental` bool and `CurrentModelTable` string fields
  - [internal/template/context_test.go](internal/template/context_test.go) - Test context with incremental flags
  - [internal/domain/executor/engine.go](internal/domain/executor/engine.go) - Pass incremental state to template context
- **Tests to Write:**
  - `TestContextWithIncrementalState` - Verify context stores incremental flag
  - `TestContextWithCurrentModelTable` - Verify context stores current model table name
- **Steps:**
  1. Write test `TestContextWithIncrementalState` checking Context.IsIncremental field exists and defaults to false
  2. Run test to see it fail
  3. Add `IsIncremental bool` field to Context struct
  4. Run test to see it pass
  5. Write test `TestContextWithCurrentModelTable` checking Context.CurrentModelTable field
  6. Run test to see it fail
  7. Add `CurrentModelTable string` field to Context struct
  8. Run test to see it pass
  9. Refactor Context constructor to accept optional incremental parameters

### 2. **Phase 2: Implement `is_incremental()` Template Function**
- **Objective:** Add template function that returns whether current execution is incremental run
- **Files/Functions to Modify/Create:**
  - [internal/template/functions.go](internal/template/functions.go) - Add `makeIsIncrementalFunc()`
  - [internal/template/funcmap.go](internal/template/funcmap.go) - Register `is_incremental` in FuncMap
  - [internal/template/functions_test.go](internal/template/functions_test.go) - Test is_incremental function
- **Tests to Write:**
  - `TestIsIncrementalFunc` - Verify function returns correct boolean
  - `TestIsIncrementalInTemplate` - Test actual template rendering with conditional logic
- **Steps:**
  1. Write test `TestIsIncrementalFunc` with Context.IsIncremental=false, expect function returns false
  2. Run test to see it fail
  3. Implement `makeIsIncrementalFunc(ctx *Context) func() bool` returning ctx.IsIncremental
  4. Run test to see it pass
  5. Write test with Context.IsIncremental=true, expect function returns true
  6. Run test to see it pass
  7. Write test `TestIsIncrementalInTemplate` rendering "{{ if is_incremental }}INCREMENTAL{{ end }}" (Go template syntax)
  8. Run test to see it fail (function not registered)
  9. Add `"is_incremental": makeIsIncrementalFunc(ctx)` to BuildFuncMap
  10. Run test to see it pass

### 3. **Phase 3: Implement `{{ this }}` Template Reference**
- **Objective:** Add template reference that resolves to current model's table name with schema qualification support
- **Files/Functions to Modify/Create:**
  - [internal/template/functions.go](internal/template/functions.go) - Add `makeThisFunc()` with schema qualification
  - [internal/template/funcmap.go](internal/template/funcmap.go) - Register `this` in FuncMap
  - [internal/template/functions_test.go](internal/template/functions_test.go) - Test this function
  - [internal/template/integration_test.go](internal/template/integration_test.go) - Integration test with incremental pattern
- **Tests to Write:**
  - `TestThisFunc` - Verify function returns current model table name
  - `TestThisFuncWithSchema` - Verify schema-qualified reference when Context.Schema is set
  - `TestThisInTemplate` - Test template rendering with {{ this }}
  - `TestIncrementalFilterPattern` - Integration test for "SELECT ... WHERE date > (SELECT MAX(date) FROM {{ this }})"
- **Steps:**
  1. Write test `TestThisFunc` with Context.CurrentModelTable="my_table", Context.Schema="", expect "my_table"
  2. Run test to see it fail
  3. Implement `makeThisFunc(ctx *Context) func() string` returning qualified name (similar to ref logic)
  4. Run test to see it pass
  5. Write test `TestThisFuncWithSchema` with Context.CurrentModelTable="my_table", Context.Schema="public", expect "public.my_table"
  6. Run test to see it pass
  7. Write test `TestThisInTemplate` rendering "SELECT * FROM {{ this }}"
  8. Run test to see it fail (function not registered)
  9. Add `"this": makeThisFunc(ctx)` to BuildFuncMap
  10. Run test to see it pass
  11. Write test `TestIncrementalFilterPattern` with full incremental query pattern
  12. Run test to see it pass
  13. Add error handling for empty CurrentModelTable (return error instead of empty string)

### 4. **Phase 4: Integration, --full-refresh Flag, and Documentation Update**
- **Objective:** Wire incremental context through execution engine, add --full-refresh CLI flag, and update documentation
- **Files/Functions to Modify/Create:**
  - [internal/domain/executor/engine.go](internal/domain/executor/engine.go) - Set context fields based on materialization config
  - [internal/domain/executor/engine_test.go](internal/domain/executor/engine_test.go) - Test incremental execution flow
  - [internal/cli/flags.go](internal/cli/flags.go) - Add --full-refresh flag
  - [internal/cli/run.go](internal/cli/run.go) - Wire --full-refresh flag to executor
  - [internal/cli/run_test.go](internal/cli/run_test.go) - Test --full-refresh flag parsing
  - [README.md](README.md) - Remove "(Coming Soon)" from incremental section and document --full-refresh
  - [examples/star_schema_example/models/facts/fct_sales.sql](examples/star_schema_example/models/facts/fct_sales.sql) - Add example using is_incremental
  - [test/integration_test.go](test/integration_test.go) - Add end-to-end incremental test
- **Tests to Write:**
  - `TestEnginePassesIncrementalContext` - Verify engine sets IsIncremental and CurrentModelTable
  - `TestIncrementalModelExecution` - Full execution test with incremental model
  - `TestFullRefreshFlag` - Test --full-refresh flag sets FullRefresh in config
  - `TestFullRefreshOverride` - Test incremental model uses full refresh when flag is set
- **Steps:**
  1. Write test `TestEnginePassesIncrementalContext` checking template context receives correct values
  2. Run test to see it fail
  3. Modify engine Execute() to set Context.IsIncremental based on model config
  4. Modify engine Execute() to set Context.CurrentModelTable to model name
  5. Run test to see it pass
  6. Write test `TestIncrementalModelExecution` with model using both is_incremental() and {{ this }}
  7. Run test to see it fail
  8. Ensure proper context flow from executor through template rendering
  9. Run test to see it pass
  10. Write test `TestFullRefreshFlag` checking flag parsing in CLI
  11. Run test to see it fail
  12. Add --full-refresh flag to flags.go and wire through run.go
  13. Run test to see it pass
  14. Write test `TestFullRefreshOverride` verifying incremental model uses DROP+CREATE when flag set
  15. Run test to see it fail
  16. Wire --full-refresh to MaterializationConfig.FullRefresh in executor
  17. Run test to see it pass
  18. Update README.md to change "Incremental (Coming Soon)" to "Incremental" with Go template examples and --full-refresh documentation
  19. Add incremental filtering example to fct_sales.sql as comment/alternative implementation
  20. Run `go test ./...` to verify all tests pass
  21. Run `scripts/build.ps1 -Task build` to verify compilation
  22. Run `scripts/build.ps1 -Task run` with star schema example to verify end-to-end

**Architectural Decisions (Resolved):**
1. ✅ **Template Syntax:** Use Go template syntax and libraries (not Jinja-style)
2. ✅ **{{ this }} Schema Qualification:** Support schema-qualified references consistent with `ref()` behavior
3. ✅ **{{ this }} Scope:** Works in all model types (view, table, incremental) - useful for self-referential logic
4. ✅ **First Run Handling:** Keep current `CREATE TABLE IF NOT EXISTS` approach - works correctly
5. ✅ **Full Refresh Flag:** Add `--full-refresh` CLI flag and wire through executor in Phase 4
