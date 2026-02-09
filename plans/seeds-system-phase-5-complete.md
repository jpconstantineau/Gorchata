## Phase 5 Complete: CLI Seed Command with Scoped Discovery

Implemented the `gorchata seed` CLI command with configurable discovery scopes (file, folder, tree) enabling users to load seed data from CSV files. The command integrates with all previous phases, providing a complete pipeline from CSV discovery through schema inference to database loading. Three discovery scopes support different use cases: single file, shallow directory scan, and recursive tree traversal.

**Files created/changed:**
- internal/domain/seeds/discovery.go
- internal/domain/seeds/discovery_test.go
- internal/cli/seed.go
- internal/cli/seed_test.go
- internal/cli/cli.go

**Functions created/changed:**
- DiscoverSeeds() - Main discovery function with 3 scope types (file/folder/tree)
- discoverFile() - Single CSV file validation
- discoverFolder() - Non-recursive directory scanning using os.ReadDir
- discoverTree() - Recursive tree traversal using filepath.Walk
- SeedCommand() - Main CLI command handler with flag parsing
- loadOrDefaultSeedConfig() - Load seed.yml or apply defaults
- loadSeedsFromPaths() - Discover, parse, infer schema, and load seeds
- filterSeeds() - Filter seeds by --select flag
- executeSeeds() - Execute seeds with progress reporting and error handling

**Tests created/changed:**
- TestDiscoverSeeds_File - Single file discovery validation
- TestDiscoverSeeds_Folder - Non-recursive folder scan
- TestDiscoverSeeds_Tree - Recursive tree traversal
- TestDiscoverSeeds_FilterNonCSV - Filter non-.csv files
- TestDiscoverSeeds_EmptyDirectory - Handle empty directories gracefully
- TestDiscoverSeeds_NonExistent - Error for missing paths
- TestDiscoverSeeds_InvalidScope - Error for invalid scope types
- TestSeedCommand_BasicExecution - End-to-end command execution
- TestSeedCommand_SelectFlag - --select flag filtering
- TestSeedCommand_NoSeedsFound - Graceful handling of no matches
- TestSeedCommand_InvalidFlags - Error handling for bad flags
- TestSeedCommand_MissingConfig - Error handling for missing config

**Discovery Scopes:**
1. ScopeFile ("file") - Single specified CSV file
2. ScopeFolder ("folder") - All CSVs in one directory (non-recursive)
3. ScopeTree ("tree") - All CSVs recursively in folder tree (default)

**CLI Flags Supported:**
- --select - Filter specific seeds (comma-separated)
- --full-refresh - Drop and recreate tables
- --verbose - Detailed progress output
- --target - Target environment profile
- --project-path - Custom project path
- --fail-fast - Stop on first error (default behavior)

**Command Usage Examples:**
```bash
# Run all seeds
gorchata seed

# Run specific seed(s)
gorchata seed --select customers
gorchata seed --select customers,orders

# Full refresh mode
gorchata seed --full-refresh

# Verbose output
gorchata seed --verbose

# Target specific environment
gorchata seed --target prod
```

**Integration Pipeline:**
1. Load project config from gorchata_project.yml
2. Load or default seed config from seed.yml
3. Discover CSV files using configured scope
4. For each file: Parse CSV → Infer schema → Resolve table name
5. Execute seeds with batch processing and transactions
6. Report results with row counts and timing

**Review Status:** APPROVED (84.1% domain coverage, all 12 tests passing)

**Git Commit Message:**
```
feat: implement CLI seed command with scoped discovery

- Add gorchata seed command with flag parsing
- Implement 3 discovery scopes (file/folder/tree)
- Add loadSeedsFromPaths() for complete seed pipeline
- Add executeSeeds() with progress reporting
- Support --select, --full-refresh, --verbose flags
- Integrate with executor, parser, inference, and config
- Add 12 test functions with comprehensive coverage
- Route seed command in cli.go
```
