## Phase 2 Complete: Implement is_incremental Template Function

Phase 2 successfully implemented the `is_incremental` template function that allows SQL models to conditionally render different logic based on whether the current execution is an incremental run.

**Files created/changed:**
- internal/template/functions.go
- internal/template/funcmap.go
- internal/template/functions_test.go

**Functions created/changed:**
- makeIsIncrementalFunc() - New function factory returning closure that reads ctx.IsIncremental
- BuildFuncMap() - Registered "is_incremental" function in template FuncMap

**Tests created/changed:**
- TestIsIncrementalFunc - 2 sub-tests verifying function returns correct boolean values
- TestIsIncrementalInTemplate - 3 sub-tests verifying template rendering with Go template conditionals

**Review Status:** APPROVED

**Git Commit Message:**
```
feat: Implement is_incremental template function

- Add makeIsIncrementalFunc() to create is_incremental template function
- Register is_incremental in BuildFuncMap for template access
- Support Go template syntax: {{ if is_incremental }}...{{ end }}
- Add comprehensive tests with 5 test cases covering true/false and conditionals
- Enable SQL models to conditionally render logic for incremental vs full refresh
```
