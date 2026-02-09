## Phase 2 Complete: Seed Configuration System with Naming Strategies

Implemented seed.yml configuration system with flexible table naming strategies (filename, folder, static) and import settings (batch size, scope). The configuration system provides sensible defaults and supports optional table name prefixes. Cross-platform path handling ensures consistent behavior on Windows and Unix systems.

**Files created/changed:**
- internal/config/seed_config.go
- internal/config/seed_config_test.go
- internal/domain/seeds/naming.go
- internal/domain/seeds/naming_test.go
- configs/seed.example.yml

**Functions created/changed:**
- SeedConfig struct (Version, Naming, Import, ColumnTypes, Config)
- NamingConfig struct (Strategy, StaticName, Prefix)
- ImportConfig struct (BatchSize, Scope)
- ParseSeedConfig() - YAML configuration parser with defaults
- applyDefaults() - Sets sensible defaults (batch_size: 1000, scope: tree, strategy: filename)
- ResolveTableName() - Resolves table name based on naming strategy with optional prefix
- Constants: NamingStrategyFilename, NamingStrategyFolder, NamingStrategyStatic
- Constants: ScopeFile, ScopeFolder, ScopeTree

**Tests created/changed:**
- TestParseSeedConfig_NamingStrategy - validates all three naming strategies
- TestParseSeedConfig_ImportConfig - validates batch_size and scope configuration
- TestParseSeedConfig_ColumnTypes - validates column type override parsing
- TestParseSeedConfig_FullExample - validates complete configuration file
- TestParseSeedConfig_Defaults - ensures proper default values applied
- TestParseSeedConfig_InvalidFile - error handling for missing/malformed files
- TestResolveTableName_Filename - filename-based naming (customers.csv → customers)
- TestResolveTableName_Folder - folder-based naming (sales/data.csv → sales)
- TestResolveTableName_Static - static naming from config
- TestResolveTableName_WithPrefix - prefix application (seed_customers)
- TestResolveTableName_Precedence - static strategy precedence
- TestResolveTableName_EdgeCases - nested paths, extensions, cross-platform
- TestResolveTableName_CrossPlatform - Windows/Unix path compatibility

**Review Status:** APPROVED (85.8% config coverage, 94.1% seeds coverage)

**Git Commit Message:**
```
feat: implement seed configuration system with naming strategies

- Add SeedConfig with flexible naming strategies (filename/folder/static)
- Implement ResolveTableName() with cross-platform path support
- Add ImportConfig for batch size and scope (file/folder/tree)
- Support optional table name prefixes
- Apply sensible defaults (batch_size: 1000, scope: tree)
- Create comprehensive seed.example.yml with documentation
- Add 13 test functions with 27+ test cases (all passing)
```
