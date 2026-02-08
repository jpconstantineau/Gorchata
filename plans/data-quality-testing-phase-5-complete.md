## Phase 5 Complete: Test Execution Engine & Multi-Mode CLI Integration

Implemented the test execution engine with adaptive sampling, test selection, result reporting, and full CLI integration. This phase connects all previous components (test domain models, generic tests, singular tests, schema parsing) into a working test execution system with three CLI modes: `gorchata test`, `gorchata build`, and `gorchata run --test`.

**Files created/changed:**
- internal/domain/test/executor/engine.go
- internal/domain/test/executor/sampler.go
- internal/domain/test/executor/selector.go
- internal/domain/test/executor/discovery.go
- internal/domain/test/executor/result_writer.go
- internal/cli/test.go
- internal/cli/build.go
- internal/cli/run.go
- internal/cli/cli.go
- internal/domain/test/executor/engine_test.go
- internal/domain/test/executor/sampler_test.go
- internal/domain/test/executor/selector_test.go
- internal/domain/test/executor/discovery_test.go
- internal/domain/test/executor/result_writer_test.go
- internal/cli/build_test.go

**Functions created/changed:**

**Test Execution Engine:**
- `NewTestEngine(adapter, templateEngine)` - Constructor with dependency injection
- `TestEngine.ExecuteTests(ctx, tests)` - Sequential test execution with result aggregation
- `TestEngine.ExecuteTest(ctx, test)` - Single test execution with adaptive sampling
- `GetTableRowCount(ctx, adapter, tableName)` - Query table row counts for sampling decisions
- `ApplySampling(sql, sampleSize)` - SQL rewriting with RANDOM() sampling
- `ShouldSample(test, rowCount)` - Adaptive sampling logic (>1M rows → 100K sample)

**Test Selection:**
- `NewTestSelector(includes, excludes, tags, models)` - Constructor for filtering
- `TestSelector.Filter(tests)` - Apply selection criteria to test list
- `TestSelector.Matches(test)` - Pattern matching with wildcards
- `matchesPattern(name, pattern)` - Wildcard pattern matching implementation

**Test Discovery:**
- `DiscoverAllTests(cfg, registry)` - Unified test discovery (singular + schema-based)
- `discoverSingularTests(testPaths)` - Load singular tests from test directories
- `discoverSchemaTests(modelPaths, registry)` - Load schema-based tests from model directories

**Result Writers:**
- `NewConsoleResultWriter()` - Color-coded console output
- `ConsoleResultWriter.WriteResult(result)` - Per-test output with colors
- `ConsoleResultWriter.WriteSummary(summary)` - Summary statistics
- `NewJSONResultWriter(targetDir)` - JSON output writer
- `JSONResultWriter.WriteResult(result)` - Collect results for JSON
- `JSONResultWriter.WriteSummary(summary)` - Write complete JSON file

**CLI Commands:**
- `TestCommand(args)` - Full test command with flags: --select, --exclude, --tags, --models, --fail-fast
- `BuildCommand(args)` - Run models then tests (full workflow)
- `RunCommand` updated - Added --test flag to run tests after models

**Tests created/changed:**

**Executor Package Tests (68 tests, 86.8% coverage):**
- `TestExecuteTests_AllPass` - All tests pass scenario
- `TestExecuteTests_SomeFailures` - Handle test failures
- `TestExecuteTests_WithWarnings` - Handle warning-level tests
- `TestExecuteTest_PassingTest` - Single test execution (success)
- `TestExecuteTest_FailingTest` - Single test execution (failure)
- `TestExecuteTest_WithThresholds` - ErrorIf/WarnIf conditional thresholds
- `TestShouldSample_SmallTable` - No sampling for <1M rows
- `TestShouldSample_LargeTable` - Auto-sample for ≥1M rows
- `TestShouldSample_ExplicitSize` - Respect manual sample_size config
- `TestApplySampling_SQLWrapper` - SQL rewriting with RANDOM()
- `TestSelector_ByNameInclude` - Select by name pattern
- `TestSelector_ByExclude` - Exclude pattern (highest priority)
- `TestSelector_ByTags` - Select by tag filter
- `TestSelector_ByModels` - Select by model pattern
- `TestSelector_CombinedFilters` - Multiple filters (AND logic)
- `TestPatternMatching` - Wildcard patterns (*, prefix, suffix, contains)
- `TestDiscoverAllTests_Singular` - Discover singular tests
- `TestDiscoverAllTests_Schema` - Discover schema-based tests
- `TestDiscoverAllTests_Mixed` - Discover both types
- `TestDiscoverAllTests_MissingDirs` - Graceful handling of missing directories
- `TestConsoleWriter_AllPass` - Console output formatting
- `TestConsoleWriter_WithFailures` - Failure output with colors
- `TestJSONWriter_ValidJSON` - JSON structure validation
- `TestJSONWriter_FileWrite` - JSON file creation

**CLI Tests (15 tests, 60.2% coverage):**
- `TestBuildCommand_Basic` - Build with tests
- `TestBuildCommand_TestFailures` - Exit code on test failures
- `TestTestCommand_Execute` - Basic test command
- `TestTestCommand_WithSelect` - Test selection flag
- `TestTestCommand_FailFast` - Stop on first failure

**Review Status:** ✅ APPROVED

The code review confirms excellent implementation quality with all acceptance criteria met. Only one minor non-blocking issue identified (string comparison vs constant usage). Test coverage exceeds targets (86.8% for executor package). Clean architecture maintained with proper separation of concerns. DatabaseAdapter and template engine integration follows existing patterns. All 68 executor tests and 15 CLI tests passing.

**Key Features Delivered:**

1. **Adaptive Sampling:** Automatically samples tables with ≥1 million rows using 100K sample size, configurable per test
2. **Test Selection:** Filter tests by name, tag, model with wildcard patterns; exclude patterns supported
3. **Test Discovery:** Unified discovery of singular tests (.sql files) and schema-based tests (.yml files)
4. **Result Reporting:** Color-coded console output + structured JSON output to target/test_results.json
5. **CLI Integration:** Three execution modes working correctly
   - `gorchata test` - Dedicated test command with filtering
   - `gorchata build` - Run models + tests in sequence
   - `gorchata run --test` - Run models with optional test execution
6. **Error Handling:** Graceful degradation, continues on failures unless --fail-fast
7. **Status Determination:** Pass (0 failures), Fail (errors), Warning (warnings) with threshold evaluation

**Usage Examples:**

```powershell
# Run all tests
gorchata test

# Run specific tests by pattern
gorchata test --select "not_null_*"

# Run tests for specific models
gorchata test --models "users,orders"

# Exclude certain tests
gorchata test --exclude "*_performance_*"

# Run tests by tags
gorchata test --tags "critical,compliance"

# Stop on first failure
gorchata test --fail-fast

# Build and test workflow
gorchata build

# Run models then test
gorchata run --test
```

**Sample Output:**

```
Running tests...
[PASS] not_null_users_email (45ms, 0 failures)
[FAIL] unique_orders_id (120ms, 5 failures)
[WARN] freshness_users_updated_at (78ms, 1 failures)

Test Summary:
Total: 15 tests
Passed: 12
Failed: 2
Warnings: 1
Duration: 1.2s

Results written to: target/test_results.json
```

**Git Commit Message:**
```
feat: Test execution engine with adaptive sampling and CLI integration

- Implement TestEngine with sequential test execution
- Add adaptive sampling: auto-sample tables with ≥1M rows using 100K sample
- Implement test selector with wildcard pattern matching (name, tag, model, exclude)
- Add unified test discovery for singular and schema-based tests
- Implement console writer with color-coded output (PASS/FAIL/WARN)
- Implement JSON writer for structured output to target/test_results.json
- Add gorchata test command with full filtering capabilities
- Add gorchata build command (run models + tests)
- Add --test flag to gorchata run command
- Implement threshold evaluation (ErrorIf, WarnIf)
- Add graceful error handling with --fail-fast option
- Include 68 executor tests with 86.8% coverage
- Include 15 CLI integration tests with 60.2% coverage
```
