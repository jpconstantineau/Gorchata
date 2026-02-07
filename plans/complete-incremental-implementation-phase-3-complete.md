## Phase 3 Complete: Implement {{ this }} Template Reference

Phase 3 successfully implemented the `{{ this }}` template reference that resolves to the current model's table name with schema qualification support, enabling self-referential queries in incremental models.

**Files created/changed:**
- internal/template/functions.go
- internal/template/funcmap.go
- internal/template/functions_test.go

**Functions created/changed:**
- makeThisFunc() - New function factory returning closure that reads ctx.CurrentModelTable with schema qualification
- BuildFuncMap() - Registered "this" function in template FuncMap

**Tests created/changed:**
- TestThisFunc - 3 sub-tests verifying function returns table name with/without schema and errors on empty
- TestThisInTemplate - 3 sub-tests verifying template rendering with schema qualification and error handling
- TestIncrementalFilterPattern - 3 sub-tests verifying full incremental pattern integration with ref, is_incremental, and this

**Review Status:** APPROVED

**Git Commit Message:**
```
feat: Implement {{ this }} template reference with schema qualification

- Add makeThisFunc() to create this template function
- Support schema qualification consistent with ref() behavior
- Return schema.table_name when schema is set, otherwise just table_name
- Add error handling for empty CurrentModelTable to prevent misuse
- Register this in BuildFuncMap for template access
- Add comprehensive tests with 9 test cases covering unit, integration, and error cases
- Enable self-referential queries: WHERE date > (SELECT MAX(date) FROM {{ this }})
```
