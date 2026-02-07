## Phase 5 Complete: Generate sample SQL model files

Created three sample SQL models demonstrating staging → fact pattern with proper materialization strategies and {{ ref }} syntax for dependencies.

**Files created/changed:**
- internal/cli/init.go (modified)
- internal/cli/init_test.go (modified)

**Functions created/changed:**
- `generateModels(projectPath string) error` - generates three sample SQL model files
- Updated `InitCommand` to call generateModels after generateProfiles
- Added three template constants:
  - `stgUsersTemplate` - staging view for clean user data
  - `stgOrdersTemplate` - staging view for clean order data
  - `fctOrderSummaryTemplate` - fact table with joins and aggregations

**Tests created/changed:**
- `TestGenerateModels_AllFiles` - verifies all three model files created in models/ directory
- `TestGenerateModels_ContentCorrect` - verifies SQL content and config blocks present
- `TestGenerateModels_RefSyntax` - verifies {{ ref "stg_users" }} and {{ ref "stg_orders" }} syntax

**Review Status:** APPROVED

All tests passing, SQL templates match sample_project fixture exactly, proper staging views with view materialization, fact table with table materialization, correct {{ ref }} syntax demonstrating dependencies, proper error handling and file permissions (0644).
