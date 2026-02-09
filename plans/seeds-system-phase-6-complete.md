## Phase 6 Complete: Template seed() Function and Schema References

Implemented the seed() template function enabling models to reference seed tables in SQL templates, providing a complete integration between seeds and the model compilation system. The function returns qualified table names and integrates seamlessly with existing ref() and source() functions. Added support for seed_config_path references in schema.yml files and populated the Seeds map during model compilation in both compile and run commands.

**Files created/changed:**
- internal/template/functions_seed_test.go (new)
- internal/cli/helpers.go (new)
- internal/template/context.go
- internal/template/functions.go
- internal/template/funcmap.go
- internal/template/context_test.go
- internal/template/integration_test.go
- internal/domain/seeds/seed.go
- internal/domain/test/schema/schema.go
- internal/domain/test/schema/parser_test.go
- internal/cli/seed.go
- internal/cli/compile.go
- internal/cli/run.go

**Functions created/changed:**
- makeSeedFunc() - Creates seed() template function that looks up Seeds map
- LoadSeedsForTemplateContext() - Loads seeds and builds Seeds map for templates
- buildSeedsMapForTemplate() - Builds Seeds map with qualified table names
- Context.Seeds field - Map of seed name to qualified table name
- Seed.ResolvedTableName field - Stores fully qualified table name
- SchemaFile.SeedConfigPath field - References external seed config files

**Tests created/changed:**
- TestSeedFunc_ExistingSeed - Returns qualified table name for valid seed
- TestSeedFunc_WithSchema - Includes schema prefix when configured
- TestSeedFunc_NonexistentSeed - Returns error for unknown seed
- TestSeedFunc_EmptyName - Handles empty seed name gracefully
- TestSeedFunc_MultipleCalls - Multiple seed references in one template
- TestContext_SeedsMap - Verifies Seeds map initialization
- TestContext_SeedsWithQualifiedNames - Verifies schema.tablename format
- TestParseSchemaFile_SeedConfigRef - Parses seed_config_path field
- TestIntegration_ModelReferencesSeed - End-to-end test with 3 scenarios

**Template Function Usage:**
```sql
-- models/customer_analysis.sql
SELECT 
    c.customer_id,
    c.name,
    COUNT(*) as order_count
FROM {{ seed "customers" }} c
LEFT JOIN {{ ref "orders" }} o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name
```

**Schema.yml with seed_config_path:**
```yaml
version: 2

# Reference external seed configuration
seed_config_path: seeds/seed.yml

models:
  - name: customer_analysis
    columns:
      - name: customer_id
        data_tests: [unique, not_null]
```

**Integration Points:**
- seed() function registered in BuildFuncMap() alongside ref() and source()
- Seeds map populated in compile and run commands via LoadSeedsForTemplateContext()
- ResolvedTableName computed during seed loading using ResolveTableName()
- Template context includes Seeds map for all model compilations
- Schema parsing supports seed_config_path references

**Error Handling:**
- Empty seed name returns clear error message
- Nonexistent seed returns "seed %q not found" error
- Consistent error handling with ref() and source() functions

**Review Status:** APPROVED (96.4% coverage, all 127 tests passing)

**Git Commit Message:**
```
feat: implement seed() template function and schema references

- Add seed() template function for referencing seed tables in models
- Add Seeds map to Context struct for qualified table names
- Add ResolvedTableName field to Seed struct
- Add SeedConfigPath field to schema parsing
- Implement LoadSeedsForTemplateContext() for CLI integration
- Populate Seeds map in compile and run commands
- Add 9 test functions with comprehensive coverage
- Integrate seamlessly with existing ref() and source() functions
- Support schema.yml references to external seed configs
```
