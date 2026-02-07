## Plan: Initialize Gorchata - SQL Transformation CLI Tool

A Go-based command-line tool similar to DBT that transforms data in warehouses using SQL templates powered by Go's text/template engine. Initial implementation uses SQLite for local testing with lightweight, idiomatic Go dependencies.

**Phases: 8**

### **Phase 1: Project Scaffolding & Build Infrastructure**
- **Objective:** Initialize Go module, create standard project structure, and set up PowerShell build/test automation with output to bin/
- **Files/Functions to Create:**
  - `go.mod` (with Go 1.25+)
  - `README.md` (comprehensive documentation)
  - `scripts/build.ps1` (PowerShell build automation, outputs to bin/)
  - Basic directory structure: `cmd/gorchata/`, `internal/{app,cli,domain,config,template,platform}/`, `test/fixtures/`, `bin/`
  - `cmd/gorchata/main.go` (thin entrypoint)
  - `internal/app/app.go` (application wiring)
  - `.gitignore` (ignore bin/, test databases, etc.)
- **Tests to Write:**
  - `scripts/build_test.ps1` (validate build script functionality)
  - `internal/app/app_test.go` (test application initialization)
- **Steps:**
  1. Write test in `scripts/build_test.ps1` that verifies build script can execute with `-Task test`, `-Task build`, `-Task run`, `-Task clean`
  2. Run test to see it fail (script doesn't exist)
  3. Create `build.ps1` with all four tasks, setting `CGO_ENABLED=0`, outputting binary to `bin/gorchata.exe`
  4. Run test to see it pass
  5. Write test in `internal/app/app_test.go` for `New()` function that returns initialized app
  6. Run test to see it fail
  7. Initialize `go.mod` with `go 1.25`, create directory structure including `bin/`
  8. Implement minimal `app.go` and `main.go`
  9. Run test to see it pass
  10. Create `.gitignore` with bin/, *.db, test artifacts
  11. Write comprehensive `README.md` documenting setup, commands, configuration
  12. Run `scripts/build.ps1 -Task build` to verify binary created in bin/

### **Phase 2: Configuration Management (Lightweight YAML Parsing)**
- **Objective:** Implement gorchata_project.yml and profiles.yml parsing using standard library encoding/yaml
- **Files/Functions to Create:**
  - `internal/config/project.go` with `ProjectConfig` struct and `LoadProject()` function
  - `internal/config/profiles.go` with `ProfilesConfig` struct and `LoadProfiles()` function
  - `internal/config/loader.go` with unified `Load()` function and env var interpolation
  - `internal/config/defaults.go` with default configuration values
  - `test/fixtures/configs/gorchata_project.yml` (example project config)
  - `test/fixtures/configs/profiles.yml` (example profiles with SQLite)
  - `configs/profiles.example.yml` (documentation example)
- **Tests to Write:**
  - `internal/config/project_test.go`: `TestLoadProject()`, `TestProjectDefaults()`, `TestProjectValidation()`
  - `internal/config/profiles_test.go`: `TestLoadProfiles()`, `TestEnvVarInterpolation()`, `TestTargetSelection()`
  - `internal/config/loader_test.go`: `TestLoadConfig()`, `TestConfigFileDiscovery()`
- **Steps:**
  1. Write test `TestLoadProject()` that loads sample `gorchata_project.yml` and verifies parsed structure
  2. Run test to see it fail (no implementation)
  3. Implement `LoadProject()` using `encoding/yaml` (standard library)
  4. Run test to see it pass
  5. Write test `TestLoadProfiles()` with SQLite connection config
  6. Run test to see it fail
  7. Implement `LoadProfiles()` with YAML unmarshaling
  8. Run test to see it pass
  9. Write test `TestEnvVarInterpolation()` for ${VAR} expansion in profiles
  10. Run test to see it fail
  11. Implement env var interpolation using `os.ExpandEnv()` or custom logic
  12. Run test to see it pass
  13. Write validation tests for missing required fields
  14. Implement validation logic
  15. Run all config tests to verify they pass

### **Phase 3: Template Engine with Custom Functions**
- **Objective:** Build text/template wrapper with custom FuncMap supporting ref(), var(), config(), source() functions using Go internal functions
- **Files/Functions to Create:**
  - `internal/template/engine.go` with `Engine` struct and `New()` constructor
  - `internal/template/functions.go` with `ref()`, `var()`, `config()`, `source()`, `env_var()` as Go functions
  - `internal/template/renderer.go` with `Render()` function
  - `internal/template/context.go` with `TemplateContext` struct
  - `internal/template/funcmap.go` with `BuildFuncMap()` helper
- **Tests to Write:**
  - `internal/template/functions_test.go`: `TestRefFunction()`, `TestVarFunction()`, `TestConfigFunction()`, `TestSourceFunction()`
  - `internal/template/renderer_test.go`: `TestRenderSimpleTemplate()`, `TestRenderWithDependencies()`, `TestRenderErrors()`
  - `internal/template/engine_test.go`: `TestEngineCreation()`, `TestFuncMapRegistration()`
- **Steps:**
  1. Write test `TestRefFunction()` that verifies ref() returns qualified table name and registers dependency
  2. Run test to see it fail
  3. Implement `ref()` as Go function in FuncMap with dependency tracking hook
  4. Run test to see it pass
  5. Write tests for `var()`, `config()`, `source()`, `env_var()` functions
  6. Run tests to see them fail
  7. Implement remaining custom functions as Go functions
  8. Run tests to see them pass
  9. Write test `TestRenderSimpleTemplate()` with SQL containing template variables
  10. Run test to see it fail
  11. Implement `Render()` using text/template with custom FuncMap
  12. Run test to see it pass
  13. Add error handling tests for undefined variables, syntax errors
  14. Implement error handling with clear error messages
  15. Run all template tests to verify they pass

### **Phase 4: Database Adapter Abstraction & SQLite Implementation**
- **Objective:** Create DatabaseAdapter interface and implement SQLite adapter using modernc.org/sqlite (pure Go, no CGO)
- **Files/Functions to Create:**
  - `internal/platform/adapter.go` with `DatabaseAdapter` interface
  - `internal/platform/types.go` with `ConnectionConfig`, `QueryResult`, `Schema`, `Transaction` types
  - `internal/platform/sqlite/adapter.go` with `SQLiteAdapter` struct implementing interface
  - `internal/platform/sqlite/connection.go` with connection management
  - `internal/platform/sqlite/migrations.go` with schema migration support
- **Tests to Write:**
  - `internal/platform/sqlite/adapter_test.go`: `TestConnect()`, `TestExecuteQuery()`, `TestExecuteDDL()`, `TestTableExists()`, `TestCreateTableAs()`, `TestCreateView()`
  - `internal/platform/sqlite/integration_test.go`: end-to-end tests with temporary database files
- **Steps:**
  1. Write test `TestConnect()` that verifies successful connection to temporary SQLite database
  2. Run test to see it fail (no implementation)
  3. Add modernc.org/sqlite dependency (`go get modernc.org/sqlite`), implement `Connect()` method
  4. Run test to see it pass
  5. Write test `TestExecuteQuery()` with sample SELECT statement
  6. Run test to see it fail
  7. Implement `ExecuteQuery()` using database/sql interface
  8. Run test to see it pass
  9. Write tests for DDL execution (CREATE TABLE), table existence check, schema retrieval
  10. Run tests to see them fail
  11. Implement remaining adapter methods (ExecuteDDL, TableExists, GetTableSchema)
  12. Run tests to see them pass
  13. Write test for CreateTableAs (CREATE TABLE AS SELECT)
  14. Run test to see it fail
  15. Implement CreateTableAs method
  16. Run test to see it pass
  17. Write integration test using `t.TempDir()` for test database file
  18. Run test to confirm pure Go (CGO_ENABLED=0)
  19. Run all platform tests to verify they pass

### **Phase 5: DAG Construction & Topological Sorting**
- **Objective:** Build dependency graph from ref() calls and implement topological sort for execution order
- **Files/Functions to Create:**
  - `internal/domain/dag/graph.go` with `Graph` struct and `AddNode()`, `AddEdge()` methods
  - `internal/domain/dag/sort.go` with `TopologicalSort()` function
  - `internal/domain/dag/builder.go` with `Builder` struct that extracts dependencies from templates
  - `internal/domain/dag/validator.go` with `DetectCycles()` function
  - `internal/domain/dag/node.go` with `Node` struct representing model
- **Tests to Write:**
  - `internal/domain/dag/graph_test.go`: `TestAddNode()`, `TestAddEdge()`, `TestGraphRepresentation()`
  - `internal/domain/dag/sort_test.go`: `TestTopologicalSort()`, `TestSortOrder()`, `TestEmptyGraph()`, `TestSingleNode()`
  - `internal/domain/dag/builder_test.go`: `TestBuildFromModels()`, `TestDependencyExtraction()`, `TestMultipleRefs()`
  - `internal/domain/dag/validator_test.go`: `TestDetectCycles()`, `TestValidDAG()`, `TestSelfReference()`
- **Steps:**
  1. Write test `TestAddNode()` and `TestAddEdge()` for basic graph operations
  2. Run tests to see them fail
  3. Implement graph data structure using adjacency list (map[string][]string)
  4. Run tests to see them pass
  5. Write test `TestTopologicalSort()` with known graph (3-4 nodes) and expected order
  6. Run test to see it fail
  7. Implement Kahn's algorithm for topological sort
  8. Run test to see it pass
  9. Write test `TestDetectCycles()` with circular dependencies (A -> B -> C -> A)
  10. Run test to see it fail
  11. Implement cycle detection using DFS with recursion stack
  12. Run test to see it pass
  13. Write test `TestBuildFromModels()` that parses templates and builds graph
  14. Run test to see it fail
  15. Implement builder that scans models directory, pre-parses templates for ref() calls using regex
  16. Run test to see it pass
  17. Run all DAG tests to verify they pass

### **Phase 6: CLI Commands (Lightweight Flag-Based Implementation)**
- **Objective:** Implement lightweight CLI with standard library flag package for 'run' and 'compile' commands, with placeholders for 'test' and 'docs'
- **Files/Functions to Create:**
  - `internal/cli/cli.go` with main CLI router
  - `internal/cli/run.go` with `RunCommand()` implementation
  - `internal/cli/compile.go` with `CompileCommand()` implementation
  - `internal/cli/test.go` with `TestCommand()` placeholder (returns "not implemented")
  - `internal/cli/docs.go` with `DocsCommand()` placeholder (returns "not implemented")
  - `internal/cli/flags.go` with common flag definitions
  - Update `cmd/gorchata/main.go` to use CLI router
- **Tests to Write:**
  - `internal/cli/run_test.go`: `TestRunCommand()`, `TestRunWithFlags()`, `TestRunErrors()`
  - `internal/cli/compile_test.go`: `TestCompileCommand()`, `TestCompileOutput()`
  - `internal/cli/test_test.go`: `TestTestCommandPlaceholder()`
  - `internal/cli/docs_test.go`: `TestDocsCommandPlaceholder()`
  - `internal/cli/cli_test.go`: `TestCommandRouting()`, `TestHelp()`, `TestVersion()`
- **Steps:**
  1. Write test `TestCommandRouting()` that verifies CLI routes to correct command based on args
  2. Run test to see it fail
  3. Implement CLI router using flag package with subcommand logic
  4. Run test to see it pass
  5. Write test `TestCompileCommand()` that loads project, compiles templates without execution
  6. Run test to see it fail
  7. Implement `compile` command wiring together config loading, DAG building, template rendering
  8. Run test to see it pass
  9. Write test `TestRunCommand()` that executes full workflow (compile + execute)
  10. Run test to see it fail
  11. Implement `run` command with database execution
  12. Run test to see it pass
  13. Write tests for flag handling (--target, --models, --fail-fast)
  14. Implement flag logic
  15. Write test `TestTestCommandPlaceholder()` that verifies "not implemented" message
  16. Implement placeholder test command
  17. Write test `TestDocsCommandPlaceholder()` that verifies "not implemented" message
  18. Implement placeholder docs command
  19. Run all CLI tests to verify they pass
  20. Test manually via `scripts/build.ps1 -Task run`

### **Phase 7: Materialization Strategies**
- **Objective:** Implement view, table, and incremental materialization strategies for SQLite
- **Files/Functions to Create:**
  - `internal/domain/materialization/strategy.go` with `Strategy` interface
  - `internal/domain/materialization/view.go` with `ViewStrategy` implementation
  - `internal/domain/materialization/table.go` with `TableStrategy` implementation
  - `internal/domain/materialization/incremental.go` with `IncrementalStrategy` implementation
  - `internal/domain/materialization/factory.go` with `GetStrategy()` factory function
  - `internal/domain/materialization/config.go` with materialization config structures
- **Tests to Write:**
  - `internal/domain/materialization/view_test.go`: `TestViewMaterialize()`, `TestViewCreate()`, `TestViewReplace()`
  - `internal/domain/materialization/table_test.go`: `TestTableMaterialize()`, `TestTableDrop()`, `TestTableCreate()`
  - `internal/domain/materialization/incremental_test.go`: `TestIncrementalMaterialize()`, `TestMergeLogic()`, `TestFirstRun()`
  - `internal/domain/materialization/factory_test.go`: `TestGetStrategy()`, `TestDefaultStrategy()`
- **Steps:**
  1. Write test `TestViewMaterialize()` that verifies view creation SQL generation (CREATE VIEW)
  2. Run test to see it fail
  3. Implement `ViewStrategy` with `CREATE VIEW IF NOT EXISTS` or DROP + CREATE logic
  4. Run test to see it pass
  5. Write test `TestTableMaterialize()` with full refresh logic
  6. Run test to see it fail
  7. Implement `TableStrategy` with DROP TABLE IF EXISTS + CREATE TABLE AS logic
  8. Run test to see it pass
  9. Write test `TestIncrementalMaterialize()` with merge logic for new/updated records
  10. Run test to see it fail
  11. Implement `IncrementalStrategy` with temp table + INSERT OR REPLACE/UPSERT logic (SQLite-specific)
  12. Run test to see it pass
  13. Write factory tests for strategy selection based on config (default to table)
  14. Implement factory function with strategy lookup
  15. Run all materialization tests to verify they pass

### **Phase 8: Execution Engine & Integration**
- **Objective:** Build orchestration engine that coordinates DAG execution, template rendering, and materialization
- **Files/Functions to Create:**
  - `internal/domain/executor/engine.go` with `Engine` struct and `Execute()` method
  - `internal/domain/executor/model.go` with `Model` struct representing a transformation
  - `internal/domain/executor/context.go` with execution context tracking
  - `internal/domain/executor/result.go` with `ExecutionResult` struct and logging
  - `internal/domain/executor/logger.go` with execution logging using slog
  - `test/fixtures/sample_project/` with complete sample project (models, configs)
  - `test/integration_test.go` with end-to-end integration test
- **Tests to Write:**
  - `internal/domain/executor/engine_test.go`: `TestExecuteModel()`, `TestExecuteDAG()`, `TestErrorHandling()`, `TestFailFast()`
  - `internal/domain/executor/model_test.go`: `TestModelLoad()`, `TestModelValidation()`, `TestModelMetadata()`
  - `test/integration_test.go`: end-to-end test with sample project (models, config, database)
- **Steps:**
  1. Write test `TestExecuteModel()` that runs single model through full pipeline (render -> materialize -> execute)
  2. Run test to see it fail
  3. Implement `Execute()` method coordinating template rendering, strategy selection, database execution
  4. Run test to see it pass
  5. Write test `TestExecuteDAG()` with 3 dependent models (A, B depends on A, C depends on B)
  6. Run test to see it fail
  7. Implement DAG traversal with sequential execution in topological order
  8. Run test to see it pass
  9. Write error handling tests (SQL errors, template errors, missing dependencies)
  10. Run tests to see them fail
  11. Implement robust error handling with detailed error messages and optional fail-fast
  12. Run tests to see them pass
  13. Create sample project in `test/fixtures/sample_project/` with gorchata_project.yml, profiles.yml, and 2-3 models
  14. Write end-to-end integration test in `test/integration_test.go` that builds and executes sample project
  15. Run test to see it fail
  16. Wire all components together in `internal/app/app.go` (config -> dag -> executor)
  17. Run test to see it pass
  18. Run full project test suite (`scripts/build.ps1 -Task test`)
  19. Run manual end-to-end test with sample project (`scripts/build.ps1 -Task run`)
  20. Update README with final usage examples, sample project structure, and configuration details
  21. Add troubleshooting section to README (common errors, database location, reset steps)

---

## Implementation Decisions Made

1. **Database Platform**: SQLite (modernc.org/sqlite - pure Go, no CGO, perfect for local testing)
2. **Dependencies**: Lightweight libraries only - standard library for CLI (flag package), encoding/yaml for config
3. **Configuration**: YAML format similar to DBT but optimized for Go (e.g., simpler struct tags, Go-friendly naming)
4. **Testing Framework**: Placeholder command created, implementation deferred to future phase
5. **Documentation**: Placeholder command created, implementation deferred to future phase
6. **Build Output**: `bin/` directory
7. **Macro System**: Go functions in text/template FuncMap (internal functions, powerful and testable)

## Key Technical Choices

- **text/template**: Standard library, custom FuncMap for ref(), var(), config(), source()
- **modernc.org/sqlite**: Pure Go SQLite driver, no CGO, database/sql compatible
- **flag package**: Standard library for CLI (no Cobra dependency)
- **encoding/yaml**: Standard library for config parsing (no Viper dependency)
- **slog**: Standard library for structured logging
- **Materialization**: View, Table, Incremental strategies with SQLite-specific implementations
- **DAG**: Custom implementation using adjacency list and Kahn's algorithm

## Testing Strategy

Every phase follows strict TDD:
1. Write test first
2. Run test (confirm failure)
3. Write minimal code
4. Run test (confirm pass)
5. Refactor while keeping tests green

All tests run with `CGO_ENABLED=0` to ensure pure Go compliance.
