## Phase 7 Complete: Add --empty flag support

Implemented --empty flag support allowing users to initialize project structure without sample SQL models. Models directory is still created but remains empty.

**Files created/changed:**
- internal/cli/init.go (modified)
- internal/cli/init_test.go (modified)

**Functions created/changed:**
- Updated `InitCommand` to conditionally call generateModels based on --empty flag
- Modified success message to indicate "0 SQL files (empty project)" when --empty is used

**Tests created/changed:**
- `TestInitCommand_EmptyFlag` - verifies --empty creates structure and configs without models
- `TestInitCommand_EmptyFlag_ModelsDir` - verifies models/ directory exists but is empty

**Review Status:** APPROVED

All tests passing (20 total init tests), --empty flag properly skips model generation, all directories still created, config files generated correctly, success message reflects empty state, backward compatibility maintained (default behavior unchanged).
