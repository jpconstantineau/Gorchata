## Phase 1 Complete: Template Context Incremental State Tracking

Phase 1 successfully extended the template Context to track incremental execution state and current model table names, providing the foundation for implementing `is_incremental()` and `{{ this }}` template functions.

**Files created/changed:**
- internal/template/context.go
- internal/template/context_test.go

**Functions created/changed:**
- Context struct - Added IsIncremental and CurrentModelTable fields
- WithIsIncremental() - New ContextOption function
- WithCurrentModelTable() - New ContextOption function

**Tests created/changed:**
- TestContextWithIncrementalState - 3 sub-tests for IsIncremental field
- TestContextWithCurrentModelTable - 4 sub-tests for CurrentModelTable field

**Review Status:** APPROVED

**Git Commit Message:**
```
feat: Add incremental state tracking to template Context

- Add IsIncremental bool field to track incremental execution mode
- Add CurrentModelTable string field to store current model's table name
- Add WithIsIncremental() ContextOption for setting incremental flag
- Add WithCurrentModelTable() ContextOption for setting table name
- Add comprehensive tests with 7 sub-tests covering defaults and explicit values
- Maintain backward compatibility with existing Context API
```
