## Phase 4 Complete: Generate profiles.yml config file

Created profiles.yml with dev/prod environment configurations using {project_name}.db naming pattern. Comprehensive test coverage validates both environments and database naming.

**Files created/changed:**
- internal/cli/init.go (modified)
- internal/cli/init_test.go (modified)

**Functions created/changed:**
- `generateProfiles(projectPath, projectName string) error` - generates profiles config file
- Updated `InitCommand` to call generateProfiles after generateProjectConfig
- Added `profilesTemplate` constant with YAML template

**Tests created/changed:**
- `TestGenerateProfiles_DatabasePath` - verifies database naming matches project_name.db pattern (4 subtests)
- `TestGenerateProfiles_MultipleEnvs` - verifies dev and prod environments configured correctly
- `TestGenerateProfiles_FileCreation` - verifies file written to correct location

**Review Status:** APPROVED

All tests passing, YAML template well-formed with proper dev/prod structure, database naming follows {project_name}.db and {project_name}_prod.db pattern, proper error handling and file permissions (0644).
