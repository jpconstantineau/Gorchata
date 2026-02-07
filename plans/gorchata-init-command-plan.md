## Plan: Add "gorchata init" Command

Create a `gorchata init` command that initializes a new Gorchata project with folder structure and sample SQL model files, following the example shown in test/fixtures/sample_project.

**Phases: 8**

### Phase 1: Create init command skeleton with tests
- **Objective:** Set up the basic `init` command infrastructure with flag parsing and validation tests
- **Files/Functions to Modify/Create:**
  - Create internal/cli/init.go
  - Create internal/cli/init_test.go
  - Modify internal/cli/cli.go - add `init` case to command router
- **Tests to Write:**
  - `TestInitCommand_RequiresProjectName` - verify error when no project name provided
  - `TestInitCommand_ValidatesProjectName` - verify project name validation (alphanumeric, underscores, hyphens only)
  - `TestInitCommand_HelpFlag` - verify --help output
- **Steps:**
  1. Write tests for command registration and basic validation (tests fail)
  2. Create `InitCommand` function with flag parsing and project name validation
  3. Add "init" case to CLI router in cli.go
  4. Run tests until passing
  5. Build and confirm `gorchata init` is recognized

### Phase 2: Implement directory structure creation
- **Objective:** Create project root directory and subdirectories (models/, seeds/, tests/, macros/) with error handling and --force flag support
- **Files/Functions to Modify/Create:**
  - Modify internal/cli/init.go - add `createProjectDirectories` function
  - Modify internal/cli/init_test.go
- **Tests to Write:**
  - `TestCreateProjectDirectories_Success` - verify folders created correctly
  - `TestCreateProjectDirectories_AlreadyExists` - verify error when directory exists without --force
  - `TestCreateProjectDirectories_ForceOverwrite` - verify --force removes and recreates directory
- **Steps:**
  1. Write tests for directory creation (tests fail)
  2. Implement `createProjectDirectories(projectPath string, force bool) error` function
  3. Add check for existing directory, remove if --force is set
  4. Create all subdirectories: models/, seeds/, tests/, macros/
  5. Run tests until passing
  6. Build and manually test directory creation with `gorchata init test_project` and `gorchata init test_project --force`

### Phase 3: Generate gorchata_project.yml config file
- **Objective:** Create templated gorchata_project.yml with project name parameterization
- **Files/Functions to Modify/Create:**
  - Modify internal/cli/init.go - add project config template and generation function
  - Modify internal/cli/init_test.go
- **Tests to Write:**
  - `TestGenerateProjectConfig_CorrectName` - verify project name is inserted correctly
  - `TestGenerateProjectConfig_DateVar` - verify start_date uses current year
  - `TestGenerateProjectConfig_FileCreation` - verify file is written to correct location
- **Steps:**
  1. Write tests for project config generation (tests fail)
  2. Create `projectConfigTemplate` constant with template content
  3. Implement `generateProjectConfig(projectPath, projectName string) error` function
  4. Run tests until passing
  5. Build and verify gorchata_project.yml is created with correct content

### Phase 4: Generate profiles.yml config file
- **Objective:** Create profiles.yml with dev/prod environment configurations using {project_name}.db naming
- **Files/Functions to Modify/Create:**
  - Modify internal/cli/init.go - add profiles template and generation function
  - Modify internal/cli/init_test.go
- **Tests to Write:**
  - `TestGenerateProfiles_DatabasePath` - verify database path matches project name
  - `TestGenerateProfiles_MultipleEnvs` - verify dev and prod environments are configured
  - `TestGenerateProfiles_FileCreation` - verify file is written correctly
- **Steps:**
  1. Write tests for profiles generation (tests fail)
  2. Create `profilesConfigTemplate` constant
  3. Implement `generateProfiles(projectPath, projectName string) error` function using `{project_name}.db` format
  4. Run tests until passing
  5. Build and verify profiles.yml is created correctly

### Phase 5: Generate sample SQL model files
- **Objective:** Create three sample SQL models (stg_users.sql, stg_orders.sql, fct_order_summary.sql) demonstrating staging → fact pattern
- **Files/Functions to Modify/Create:**
  - Modify internal/cli/init.go - add model templates and generation functions
  - Modify internal/cli/init_test.go
- **Tests to Write:**
  - `TestGenerateModels_AllFiles` - verify all three model files are created
  - `TestGenerateModels_ContentCorrect` - verify SQL content and config blocks
  - `TestGenerateModels_RefSyntax` - verify {{ ref }} syntax is correct
- **Steps:**
  1. Write tests for model file generation (tests fail)
  2. Create template constants for each SQL model
  3. Implement `generateModels(projectPath string) error` function
  4. Run tests until passing
  5. Build and verify all SQL files are created in models/ directory

### Phase 6: Add integration test for complete initialization
- **Objective:** End-to-end test that verifies complete project initialization with all files
- **Files/Functions to Modify/Create:**
  - Modify internal/cli/init_test.go
- **Tests to Write:**
  - `TestInitCommand_Integration_CompleteProject` - verify entire init process creates valid project structure
  - `TestInitCommand_Integration_AllFolders` - verify all folders created (models, seeds, tests, macros)
- **Steps:**
  1. Write integration test that calls InitCommand and validates complete output (test fails)
  2. Ensure InitCommand orchestrates all generation functions
  3. Add success message with next steps to InitCommand
  4. Run tests until passing
  5. Build and run full initialization workflow manually

### Phase 7: Add --empty flag support
- **Objective:** Allow users to initialize empty project without sample models
- **Files/Functions to Modify/Create:**
  - Modify internal/cli/init.go - add --empty flag handling
  - Modify internal/cli/init_test.go
- **Tests to Write:**
  - `TestInitCommand_EmptyFlag` - verify --empty creates only structure and configs, no models
  - `TestInitCommand_EmptyFlag_ModelsDir` - verify models/ directory still created but empty
- **Steps:**
  1. Write tests for --empty flag behavior (tests fail)
  2. Add --empty flag to InitCommand flag set
  3. Conditionally skip model generation when flag is set
  4. Run tests until passing
  5. Build and test `gorchata init myproj --empty`

### Phase 8: Update documentation and usage text
- **Objective:** Update README.md and CLI help with init command documentation
- **Files/Functions to Modify/Create:**
  - Modify README.md - update "Initialize a New Project" section
  - Modify internal/cli/init.go - add help/usage text
  - Modify internal/cli/cli.go - add init to usage output
- **Tests to Write:**
  - No new tests (documentation phase)
- **Steps:**
  1. Remove "(Coming Soon)" from README init section
  2. Document init command with examples and flags
  3. Add init command to printUsage() in cli.go
  4. Create printInitUsage() function with detailed help
  5. Build and verify help text displays correctly

**Decisions Made:**
- `--force` flag will allow overwriting existing directories
- Database naming will use `{project_name}.db` format in profiles.yml
- Will create empty folders for seeds/, tests/, macros/ in addition to models/
